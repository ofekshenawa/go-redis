package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

// routeAndRun routes the command based on its COMMAND tips policy.
func (c *ClusterClient) routeAndRun(ctx context.Context, cmd Cmder) error {
	var firstErr error
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	if len(state.Masters) == 0 {
		return errClusterNoNodes
	}

	var pol *routing.CommandPolicy
	if cmd != nil {
		c.hooksMu.RLock()
		if p := c.cmdInfo(ctx, cmd.Name()).Tips; p != nil {
			pol = p
		} else {
			pol = &routing.CommandPolicy{}
		}
		c.hooksMu.RUnlock()
	} else {
		pol = &routing.CommandPolicy{}
	}
	executed := false
	cmds := []Cmder{}
	switch pol.Request {
	case routing.ReqAllNodes:
		executed = true
		firstErr = c.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
			cmdPerShard := cmd.Clone()
			cmds = append(cmds, cmdPerShard)
			return shard.Process(ctx, cmdPerShard)
		})

	case routing.ReqAllShards:
		executed = true
		firstErr = c.ForEachMaster(ctx, func(ctx context.Context, master *Client) error {
			cmdPerShard := cmd.Clone()
			cmds = append(cmds, cmdPerShard)
			return master.Process(ctx, cmdPerShard)
		})

	case routing.ReqMultiShard:
		executed = true
		firstErr = c.processMultiShard(ctx, cmd)

	case routing.ReqSpecial:
		executed = true
		firstErr = c.processSpecial(ctx, cmd)
	}

	// Aggregate responses if we have multiple shard results
	if firstErr != nil {
		cmd.SetErr(firstErr)
		return firstErr
	} else if len(cmds) > 0 {
		return c.aggregateResponses(ctx, cmd, cmds, pol)
	}
	if executed {
		return nil
	}

	// default case (i.e. ReqDefault or unknown)
	if cmdFirstKeyPos(cmd) == 0 {
		// key-less default → pick arbitrary shard
		n := c.pickArbitraryShard(ctx)
		return c.processOnConn(ctx, n.Client, cmd)
	}
	// keyed default → hash-slot based
	return c.processSlotBased(ctx, cmd)
}

// pickArbitraryShard chooses a master shard based on the configured ShardPicker.
func (c *ClusterClient) pickArbitraryShard(ctx context.Context) *clusterNode {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return nil
	}
	masters := state.Masters
	idx := c.opt.ShardPicker.Next(len(masters))
	return masters[idx]
}

// processMultiShard splits a multi-shard command by slot and routes each sub-command accordingly.
func (c *ClusterClient) processMultiShard(ctx context.Context, cmd Cmder) error {
	args := cmd.Args()
	first := int(cmdFirstKeyPos(cmd))
	if first == 0 || first >= len(args) {
		return fmt.Errorf("redis: multi-shard command %s has no key arguments", cmd.Name())
	}
	// group subsequent args by slot
	slotMap := make(map[int][]interface{})
	for i := first; i < len(args); i++ {
		key, ok := args[i].(string)
		if !ok {
			return fmt.Errorf("redis: non-string key arg at pos %d: %v", i, args[i])
		}
		slot := hashtag.Slot(key)
		slotMap[slot] = append(slotMap[slot], key)
	}

	var lastErr error
	// execute sub-commands on each slot's shard
	for slot, keys := range slotMap {
		node, err := c.cmdNode(ctx, cmd.Name(), slot)
		if err != nil {
			return err
		}
		// build new args slice: prefix + subset keys
		subArgs := make([]interface{}, first+len(keys))
		copy(subArgs, args[:first])
		for i, k := range keys {
			subArgs[first+i] = k
		}
		subCmd := NewCmd(ctx, subArgs...)
		err = node.Client.Process(ctx, subCmd)
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// processSpecial handles commands with special routing requirements.
func (c *ClusterClient) processSpecial(ctx context.Context, cmd Cmder) error {
	// Default stub: route based on first key if present, else arbitrary shard
	if cmdFirstKeyPos(cmd) > 0 {
		return c.processSlotBased(ctx, cmd)
	}
	node := c.pickArbitraryShard(ctx)
	return c.processOnConn(ctx, node.Client, cmd)
}

// processOnConn sends the command to the given client connection.
func (c *ClusterClient) processOnConn(ctx context.Context, cl *Client, cmd Cmder) error {
	return cl.Process(ctx, cmd)
}

// processSlotBased routes the command to the shard responsible for its first key.
func (c *ClusterClient) processSlotBased(ctx context.Context, cmd Cmder) error {
	slot := c.cmdSlot(ctx, cmd)
	node, err := c.cmdNode(ctx, cmd.Name(), slot)
	if err != nil {
		return err
	}
	return node.Client.Process(ctx, cmd)
}

// aggregateResponses aggregates multiple shard responses according to the response policy.
func (c *ClusterClient) aggregateResponses(ctx context.Context, cmd Cmder, cmds []Cmder, pol *routing.CommandPolicy) error {
	// Create the appropriate aggregator based on response policy
	var aggregator routing.ResponseAggregator

	if pol != nil {
		// Use explicit response policy
		aggregator = routing.NewResponseAggregator(pol.Response, cmd.Name())
	} else {
		// Use default policy based on whether command has keys
		firstKeyPos := cmdFirstKeyPos(cmd)
		// Special handling for test commands with negative keyPos
		if cmd.firstKeyPos() < 0 {
			firstKeyPos = 0
		}
		isKeyed := firstKeyPos > 0
		aggregator = routing.NewDefaultAggregator(isKeyed)
	}

	// Add all shard results to the aggregator
	for _, shardCmd := range cmds {
		value := routing.ExtractCommandValue(shardCmd)
		err := aggregator.Add(value, shardCmd.Err())
		if err != nil {
			return err
		}
	}

	// Get the final aggregated result
	finalValue, finalErr := aggregator.Finish()
	if finalErr != nil {
		cmd.SetErr(finalErr)
		return finalErr
	}

	// Set the aggregated value on the original command
	return c.setCommandValue(cmd, finalValue)
}

// setCommandValue sets the aggregated value on a command based on its type.
// This method is necessary because each command type has a strongly-typed SetVal method,
// but the aggregator returns interface{}. We need type-safe conversion to set the
// correct typed value on the original command.
func (c *ClusterClient) setCommandValue(cmd Cmder, value interface{}) error {
	// Use concrete type assertions for better reliability
	switch c := cmd.(type) {
	case *StatusCmd:
		if str, ok := value.(string); ok {
			c.SetVal(str)
			return nil
		}
		return fmt.Errorf("cannot set StatusCmd value from %T", value)
	case *IntCmd:
		if i, ok := value.(int64); ok {
			c.SetVal(i)
			return nil
		}
		return fmt.Errorf("cannot set IntCmd value from %T", value)
	case *BoolCmd:
		if b, ok := value.(bool); ok {
			c.SetVal(b)
			return nil
		}
		return fmt.Errorf("cannot set BoolCmd value from %T", value)
	case *StringCmd:
		if str, ok := value.(string); ok {
			c.SetVal(str)
			return nil
		}
		return fmt.Errorf("cannot set StringCmd value from %T", value)
	case *FloatCmd:
		if f, ok := value.(float64); ok {
			c.SetVal(f)
			return nil
		}
		return fmt.Errorf("cannot set FloatCmd value from %T", value)
	case *SliceCmd:
		if slice, ok := value.([]interface{}); ok {
			c.SetVal(slice)
			return nil
		}
		return fmt.Errorf("cannot set SliceCmd value from %T", value)
	case *StringSliceCmd:
		if slice, ok := value.([]string); ok {
			c.SetVal(slice)
			return nil
		}
		return fmt.Errorf("cannot set StringSliceCmd value from %T", value)
	case *IntSliceCmd:
		if slice, ok := value.([]int64); ok {
			c.SetVal(slice)
			return nil
		}
		return fmt.Errorf("cannot set IntSliceCmd value from %T", value)
	case *MapStringStringCmd:
		if m, ok := value.(map[string]string); ok {
			c.SetVal(m)
			return nil
		}
		return fmt.Errorf("cannot set MapStringStringCmd value from %T", value)
	case *MapStringIntCmd:
		if m, ok := value.(map[string]int64); ok {
			c.SetVal(m)
			return nil
		}
		return fmt.Errorf("cannot set MapStringIntCmd value from %T", value)
	case *MapStringInterfaceCmd:
		if m, ok := value.(map[string]interface{}); ok {
			c.SetVal(m)
			return nil
		}
		return fmt.Errorf("cannot set MapStringInterfaceCmd value from %T", value)
	case *Cmd:
		// Generic Cmd can accept any value
		c.SetVal(value)
		return nil
	default:
		return fmt.Errorf("unsupported command type for setting value: %T", cmd)
	}
}
