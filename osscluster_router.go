package redis

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

// slotResult represents the result of executing a command on a specific slot
type slotResult struct {
	cmd  Cmder
	keys []string
	err  error
}

// routeAndRun routes a command to the appropriate cluster nodes and executes it
func (c *ClusterClient) routeAndRun(ctx context.Context, cmd Cmder) error {
	policy := c.getCommandPolicy(ctx, cmd)

	switch {
	case policy != nil && policy.Request == routing.ReqAllNodes:
		return c.executeOnAllNodes(ctx, cmd, policy)
	case policy != nil && policy.Request == routing.ReqAllShards:
		return c.executeOnAllShards(ctx, cmd, policy)
	case policy != nil && policy.Request == routing.ReqMultiShard:
		return c.executeMultiShard(ctx, cmd, policy)
	case policy != nil && policy.Request == routing.ReqSpecial:
		return c.executeSpecialCommand(ctx, cmd, policy)
	default:
		return c.executeDefault(ctx, cmd, policy)
	}
}

// getCommandPolicy retrieves the routing policy for a command
func (c *ClusterClient) getCommandPolicy(ctx context.Context, cmd Cmder) *routing.CommandPolicy {
	c.hooksMu.RLock()
	defer c.hooksMu.RUnlock()

	if cmdInfo := c.cmdInfo(ctx, cmd.Name()); cmdInfo != nil && cmdInfo.Tips != nil {
		return cmdInfo.Tips
	}
	return nil
}

// executeDefault handles standard command routing based on keys
func (c *ClusterClient) executeDefault(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	if c.hasKeys(cmd) {
		return c.executeOnKeyBasedShard(ctx, cmd, policy)
	}
	return c.executeOnArbitraryShard(ctx, cmd, policy)
}

// executeOnKeyBasedShard routes command to shard based on first key
func (c *ClusterClient) executeOnKeyBasedShard(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	slot := c.cmdSlot(ctx, cmd)
	node, err := c.cmdNode(ctx, cmd.Name(), slot)
	if err != nil {
		return err
	}
	return c.executeSingle(ctx, cmd, node, policy)
}

// executeOnArbitraryShard routes command to an arbitrary shard
func (c *ClusterClient) executeOnArbitraryShard(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	node := c.pickArbitraryShard(ctx)
	if node == nil {
		return errClusterNoNodes
	}
	return c.executeSingle(ctx, cmd, node, policy)
}

// executeOnAllNodes executes command on all nodes (masters and replicas)
func (c *ClusterClient) executeOnAllNodes(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	nodes := append(state.Masters, state.Slaves...)
	if len(nodes) == 0 {
		return errClusterNoNodes
	}

	return c.executeParallel(ctx, cmd, nodes, policy)
}

// executeOnAllShards executes command on all master shards
func (c *ClusterClient) executeOnAllShards(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	if len(state.Masters) == 0 {
		return errClusterNoNodes
	}

	return c.executeParallel(ctx, cmd, state.Masters, policy)
}

// executeMultiShard handles commands that operate on multiple keys across shards
func (c *ClusterClient) executeMultiShard(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	args := cmd.Args()
	firstKeyPos := int(cmdFirstKeyPos(cmd))

	if firstKeyPos == 0 || firstKeyPos >= len(args) {
		return fmt.Errorf("redis: multi-shard command %s has no key arguments", cmd.Name())
	}

	// Group keys by slot
	slotMap := make(map[int][]string)
	keyOrder := make([]string, 0)

	for i := firstKeyPos; i < len(args); i++ {
		key, ok := args[i].(string)
		if !ok {
			return fmt.Errorf("redis: non-string key at position %d: %v", i, args[i])
		}

		slot := hashtag.Slot(key)
		slotMap[slot] = append(slotMap[slot], key)
		keyOrder = append(keyOrder, key)
	}

	return c.executeMultiSlot(ctx, cmd, slotMap, keyOrder, policy)
}

// executeMultiSlot executes commands across multiple slots concurrently
func (c *ClusterClient) executeMultiSlot(ctx context.Context, cmd Cmder, slotMap map[int][]string, keyOrder []string, policy *routing.CommandPolicy) error {
	results := make(chan slotResult, len(slotMap))
	var wg sync.WaitGroup

	// Execute on each slot concurrently
	for slot, keys := range slotMap {
		wg.Add(1)
		go func(slot int, keys []string) {
			defer wg.Done()

			node, err := c.cmdNode(ctx, cmd.Name(), slot)
			if err != nil {
				results <- slotResult{nil, keys, err}
				return
			}

			subCmd := cmd.Clone() //TODO: BUG: cmd.clone copy all the command and not break it into several cmds
			err = node.Client.Process(ctx, subCmd)
			results <- slotResult{subCmd, keys, err}
		}(slot, keys)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return c.aggregateMultiSlotResults(ctx, cmd, results, keyOrder, policy)
}

// executeSpecialCommand handles commands with special routing requirements
func (c *ClusterClient) executeSpecialCommand(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	switch cmd.Name() {
	case "ft.cursor":
		return c.executeCursorCommand(ctx, cmd, policy)
	default:
		if c.hasKeys(cmd) {
			return c.executeOnKeyBasedShard(ctx, cmd, policy)
		}
		return c.executeOnArbitraryShard(ctx, cmd, policy)
	}
}

// executeCursorCommand handles FT.CURSOR commands with sticky routing
func (c *ClusterClient) executeCursorCommand(ctx context.Context, cmd Cmder, policy *routing.CommandPolicy) error {
	args := cmd.Args()
	if len(args) < 4 {
		return fmt.Errorf("redis: FT.CURSOR command requires at least 3 arguments")
	}

	cursorID, ok := args[3].(string)
	if !ok {
		return fmt.Errorf("redis: invalid cursor ID type")
	}

	// Route based on cursor ID to maintain stickiness
	slot := hashtag.Slot(cursorID)
	node, err := c.cmdNode(ctx, cmd.Name(), slot)
	if err != nil {
		return err
	}

	return c.executeSingle(ctx, cmd, node, policy)
}

// executeSingle executes a command on a single node
func (c *ClusterClient) executeSingle(ctx context.Context, cmd Cmder, node *clusterNode, policy *routing.CommandPolicy) error {
	cmdCopy := cmd.Clone()
	err := node.Client.Process(ctx, cmdCopy)
	if err != nil {
		cmd.SetErr(err)
		return err
	}

	return c.aggregateResponses(ctx, cmd, []Cmder{cmdCopy}, policy)
}

// executeParallel executes a command on multiple nodes concurrently
func (c *ClusterClient) executeParallel(ctx context.Context, cmd Cmder, nodes []*clusterNode, policy *routing.CommandPolicy) error {
	if len(nodes) == 0 {
		return errClusterNoNodes
	}

	if len(nodes) == 1 {
		return c.executeSingle(ctx, cmd, nodes[0], policy)
	}

	type nodeResult struct {
		cmd Cmder
		err error
	}

	results := make(chan nodeResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *clusterNode) {
			defer wg.Done()
			cmdCopy := cmd.Clone()
			err := n.Client.Process(ctx, cmdCopy)
			results <- nodeResult{cmdCopy, err}
		}(node)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var cmds []Cmder
	for result := range results {
		cmds = append(cmds, result.cmd)
	}

	return c.aggregateResponses(ctx, cmd, cmds, policy)
}

// aggregateMultiSlotResults aggregates results from multi-slot execution
func (c *ClusterClient) aggregateMultiSlotResults(ctx context.Context, cmd Cmder, results <-chan slotResult, keyOrder []string, policy *routing.CommandPolicy) error {
	keyedResults := make(map[string]Cmder)
	var firstErr error

	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		if result.cmd != nil {
			for _, key := range result.keys {
				keyedResults[key] = result.cmd
			}
		}
	}

	if firstErr != nil {
		cmd.SetErr(firstErr)
		return firstErr
	}

	return c.aggregateKeyedResponses(ctx, cmd, keyedResults, keyOrder, policy)
}

// aggregateKeyedResponses aggregates responses while preserving key order
func (c *ClusterClient) aggregateKeyedResponses(ctx context.Context, cmd Cmder, keyedResults map[string]Cmder, keyOrder []string, policy *routing.CommandPolicy) error {
	if len(keyedResults) == 0 {
		return fmt.Errorf("redis: no results to aggregate")
	}

	aggregator := c.createAggregator(policy, cmd, true)

	// Set key order for keyed aggregators
	if keyedAgg, ok := aggregator.(*routing.DefaultKeyedAggregator); ok {
		keyedAgg.SetKeyOrder(keyOrder)
	}

	// Add results with keys
	for key, shardCmd := range keyedResults {
		value := routing.ExtractCommandValue(shardCmd)
		if keyedAgg, ok := aggregator.(*routing.DefaultKeyedAggregator); ok {
			if err := keyedAgg.AddWithKey(key, value, shardCmd.Err()); err != nil {
				return err
			}
		} else {
			if err := aggregator.Add(value, shardCmd.Err()); err != nil {
				return err
			}
		}
	}

	return c.finishAggregation(cmd, aggregator)
}

// aggregateResponses aggregates multiple shard responses
func (c *ClusterClient) aggregateResponses(ctx context.Context, cmd Cmder, cmds []Cmder, policy *routing.CommandPolicy) error {
	if len(cmds) == 0 {
		return fmt.Errorf("redis: no commands to aggregate")
	}

	// Handle single command case - copy the result directly to avoid aggregation overhead
	if len(cmds) == 1 {
		shardCmd := cmds[0]
		if err := shardCmd.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}

		// For single command, try to directly copy the result using reflection
		// This handles complex command types like ClusterSlotsCmd that don't work with ExtractCommandValue
		return c.copyCommandResult(cmd, shardCmd)
	}

	aggregator := c.createAggregator(policy, cmd, false)

	// Add all results to aggregator
	for _, shardCmd := range cmds {
		value := routing.ExtractCommandValue(shardCmd)
		if err := aggregator.Add(value, shardCmd.Err()); err != nil {
			return err
		}
	}

	return c.finishAggregation(cmd, aggregator)
}

// copyCommandResult copies the result from source command to destination command
func (c *ClusterClient) copyCommandResult(dst, src Cmder) error {
	// If both commands are the same type, we can use a more direct approach
	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)

	if srcVal.Type() == dstVal.Type() {
		// Try to use the SetVal method approach for specific command types
		switch srcCmd := src.(type) {
		case *ClusterSlotsCmd:
			if dstCmd, ok := dst.(*ClusterSlotsCmd); ok {
				dstCmd.SetVal(srcCmd.Val())
				return nil
			}
		case *ClusterShardsCmd:
			if dstCmd, ok := dst.(*ClusterShardsCmd); ok {
				dstCmd.SetVal(srcCmd.Val())
				return nil
			}
		case *ClusterLinksCmd:
			if dstCmd, ok := dst.(*ClusterLinksCmd); ok {
				dstCmd.SetVal(srcCmd.Val())
				return nil
			}
		case *StringCmd:
			if dstCmd, ok := dst.(*StringCmd); ok {
				dstCmd.SetVal(srcCmd.Val())
				return nil
			}
		case *Cmd:
			if dstCmd, ok := dst.(*Cmd); ok {
				dstCmd.SetVal(srcCmd.Val())
				return nil
			}
		default:
			// Try generic reflection approach for other types
			if srcValField := srcVal.Elem().FieldByName("val"); srcValField.IsValid() {
				if dstValField := dstVal.Elem().FieldByName("val"); dstValField.IsValid() && dstValField.CanSet() {
					dstValField.Set(srcValField)
					return nil
				}
			}
		}
	}

	// Fallback to the original extraction method
	value := routing.ExtractCommandValue(src)
	return c.setCommandValue(dst, value)
}

// createAggregator creates the appropriate response aggregator
func (c *ClusterClient) createAggregator(policy *routing.CommandPolicy, cmd Cmder, isKeyed bool) routing.ResponseAggregator {
	if policy != nil {
		return routing.NewResponseAggregator(policy.Response, cmd.Name())
	}

	if !isKeyed {
		firstKeyPos := cmdFirstKeyPos(cmd)
		isKeyed = firstKeyPos > 0
	}

	return routing.NewDefaultAggregator(isKeyed)
}

// finishAggregation completes the aggregation process and sets the result
func (c *ClusterClient) finishAggregation(cmd Cmder, aggregator routing.ResponseAggregator) error {
	finalValue, finalErr := aggregator.Finish()
	if finalErr != nil {
		cmd.SetErr(finalErr)
		return finalErr
	}

	return c.setCommandValue(cmd, finalValue)
}

// pickArbitraryShard selects a master shard using the configured ShardPicker
func (c *ClusterClient) pickArbitraryShard(ctx context.Context) *clusterNode {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil || len(state.Masters) == 0 {
		return nil
	}

	idx := c.opt.ShardPicker.Next(len(state.Masters))
	return state.Masters[idx]
}

// hasKeys checks if a command operates on keys
func (c *ClusterClient) hasKeys(cmd Cmder) bool {
	firstKeyPos := cmdFirstKeyPos(cmd)
	return firstKeyPos > 0
}

// setCommandValue sets the aggregated value on a command using reflection for type safety
func (c *ClusterClient) setCommandValue(cmd Cmder, value interface{}) error {
	// If value is nil, it might mean ExtractCommandValue couldn't extract the value
	// but the command might have executed successfully. In this case, don't set an error.
	if value == nil {
		// Check if the original command has an error - if not, the nil value is not an error
		if cmd.Err() == nil {
			// Command executed successfully but value extraction failed
			// This is common for complex commands like CLUSTER SLOTS
			// The command already has its result set correctly, so just return
			return nil
		}
		// If the command does have an error, set Nil error
		cmd.SetErr(Nil)
		return Nil
	}

	// Use reflection to set the value on the command
	cmdValue := reflect.ValueOf(cmd)
	if cmdValue.Kind() != reflect.Ptr || cmdValue.IsNil() {
		return fmt.Errorf("redis: invalid command pointer")
	}

	// Get the SetVal method
	setValMethod := cmdValue.MethodByName("SetVal")
	if !setValMethod.IsValid() {
		return fmt.Errorf("redis: command %T does not have SetVal method", cmd)
	}

	// Prepare arguments for SetVal call
	args := []reflect.Value{reflect.ValueOf(value)}

	// Handle special cases that need additional arguments
	switch cmd.(type) {
	case *XAutoClaimCmd, *XAutoClaimJustIDCmd:
		args = append(args, reflect.ValueOf("")) // Default start value
	case *ScanCmd:
		args = append(args, reflect.ValueOf(uint64(0))) // Default cursor
	case *KeyValuesCmd, *ZSliceWithKeyCmd:
		// These need a key string and empty slice
		if key, ok := value.(string); ok {
			args = []reflect.Value{reflect.ValueOf(key)}
			// Add appropriate empty slice based on command type
			if _, ok := cmd.(*ZSliceWithKeyCmd); ok {
				args = append(args, reflect.ValueOf([]Z{}))
			} else {
				args = append(args, reflect.ValueOf([]string{}))
			}
		}
	}

	// Call SetVal with proper error handling
	defer func() {
		if r := recover(); r != nil {
			cmd.SetErr(fmt.Errorf("redis: failed to set command value: %v", r))
		}
	}()

	setValMethod.Call(args)
	return nil
}
