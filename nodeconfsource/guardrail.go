package nodeconfsource

import (
	"fmt"

	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/go-chash"
)

// validateTransition checks that the transition from the currently active
// configuration to the proposed one keeps at least one surviving tree-node
// replica in every chash partition. Replacing the whole replica set of a
// partition in a single epoch would leave no live source to sync the
// partition's spaces from, so such configurations are rejected.
func validateTransition(prev, next []nodeconf.Node) (err error) {
	prevRing, prevCount, err := treeRing(prev)
	if err != nil {
		return fmt.Errorf("build previous ring: %w", err)
	}
	if prevCount == 0 {
		// no tree nodes before: nothing to preserve
		return nil
	}
	nextRing, nextCount, err := treeRing(next)
	if err != nil {
		return fmt.Errorf("build proposed ring: %w", err)
	}
	if nextCount == 0 {
		return fmt.Errorf("proposed configuration has no tree nodes")
	}
	var violations int
	var example int = -1
	for part := 0; part < nodeconf.PartitionCount; part++ {
		prevMembers, err := prevRing.GetPartitionMembers(part)
		if err != nil {
			return err
		}
		nextMembers, err := nextRing.GetPartitionMembers(part)
		if err != nil {
			return err
		}
		if countOverlap(prevMembers, nextMembers) == 0 {
			violations++
			if example == -1 {
				example = part
			}
		}
	}
	if violations > 0 {
		return fmt.Errorf("unsafe configuration: %d of %d partitions lose all current replicas (e.g. partition %d); apply the change in smaller steps or use force", violations, nodeconf.PartitionCount, example)
	}
	return nil
}

func treeRing(nodes []nodeconf.Node) (ring chash.CHash, memberCount int, err error) {
	if ring, err = chash.New(chash.Config{
		PartitionCount:    nodeconf.PartitionCount,
		ReplicationFactor: nodeconf.ReplicationFactor,
	}); err != nil {
		return
	}
	var members []chash.Member
	for _, n := range nodes {
		if n.HasType(nodeconf.NodeTypeTree) {
			members = append(members, n)
		}
	}
	if len(members) == 0 {
		return ring, 0, nil
	}
	err = ring.AddMembers(members...)
	return ring, len(members), err
}

func countOverlap(a, b []chash.Member) (n int) {
	for _, am := range a {
		for _, bm := range b {
			if am.Id() == bm.Id() {
				n++
				break
			}
		}
	}
	return
}
