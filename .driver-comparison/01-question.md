# Benchmark: Driver MCP vs. local Claude Code (Explore agent)

**Codebase:** kubernetes (at /Users/joeschmid/src/work/driver/kubernetes)
**Date:** 2026-05-14
**Model:** Claude Opus 4.7

## The question posed to both tools

Trace the full lifecycle of a pod through the kube-scheduler's scheduling framework, from entering the SchedulingQueue through to Bind. Cover:

1. **The SchedulingQueue**: the three sub-queues (activeQ, podBackoffQ, unschedulablePods), how a pod moves between them, what triggers `MoveAllToActiveOrBackoffQueue`, and the role of QueueingHints / `EventsToRegister`. Where is this implemented?

2. **The scheduling cycle extension points in order**: PreEnqueue, QueueSort, PreFilter, Filter, PostFilter, PreScore, Score, NormalizeScore, Reserve, Permit, PreBind, Bind, PostBind. For each: what is its contract, what status codes can it return, and what is the runtime's behavior on each status (Success, Error, Unschedulable, UnschedulableAndUnresolvable, Wait, Skip).

3. **Preemption**: how PostFilter (specifically the defaultpreemption plugin) works — candidate selection, NominatedPods/NominatedNodeName, victim selection, and how the next scheduling cycle uses the nomination.

4. **Concrete invariants and gotchas**: the difference between Unschedulable and UnschedulableAndUnresolvable; what happens if a Reserve plugin fails (unreserve order); how Permit's Wait status interacts with timeouts; how the framework handles the "scheduling cycle vs. binding cycle" split.

5. **Recent changes**: DRA (Dynamic Resource Allocation) integration points, QueueingHints maturity, any in-progress refactors visible in the code.

## Files in this directory

- `01-question.md` — this file
- `02-local-explore-output.md` — raw output from the local Claude Code Explore subagent
- `03-driver-mcp-output.md` — raw output from Driver MCP's `gather_task_context`
- `04-analysis.md` — side-by-side analysis
