package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
)

// routeAndRun routes the command based on its COMMAND tips policy.
func (c *ClusterClient) routeAndRun(ctx context.Context, cmd Cmder) error {
	var pol *routing.CommandPolicy
	c.hooksMu.RLock()
	if p := c.cmdInfo(ctx, cmd.Name()).Tips; p != nil {
		pol = p
	}
	c.hooksMu.RUnlock()

	if pol == nil {
		pol = &routing.CommandPolicy{}
	}

	switch pol.Request {
	case routing.ReqAllNodes:
		return c.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
			return shard.Process(ctx, cmd)
		})

	case routing.ReqAllShards:
		return c.ForEachMaster(ctx, func(ctx context.Context, master *Client) error {
			return master.Process(ctx, cmd)
		})

	case routing.ReqMultiShard:
		return c.processMultiShard(ctx, cmd)

	case routing.ReqSpecial:
		return c.processSpecial(ctx, cmd)

	case routing.ReqDefault:
		if cmd.firstKeyPos() == 0 {
			// key-less default → pick arbitrary shard
			n := c.pickArbitraryShard(ctx)
			return c.processOnConn(ctx, n.Client, cmd)
		}
		// keyed default → hash-slot based
		return c.processSlotBased(ctx, cmd)

	default:
		// unknown → treat as default
		if cmd.firstKeyPos() == 0 {
			n := c.pickArbitraryShard(ctx)
			return c.processOnConn(ctx, n.Client, cmd)
		}
		return c.processSlotBased(ctx, cmd)
	}
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
	first := int(cmd.firstKeyPos())
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
	if cmd.firstKeyPos() > 0 {
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
