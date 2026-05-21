# Analysis: Driver MCP vs. Local Claude Code Explore

## Headline numbers

| | Driver MCP | Local Explore |
|---|---|---|
| Words | ~1,700 | ~1,700 |
| Wall-clock latency | ~90s | ~30–40s |
| Fabricated/incorrect claims | 0 identified | ≥3 identified |
| Substantive details the other missed | ~15 | 0 identified |
| Recent commits cited with dates | ~10 | 0 |
| Distinct source files cited | ~12 (incl. 4 sub-files of queue) | ~4 |

## Things Driver got that local Explore missed entirely

1. **Sub-file decomposition of the queue.** Driver named `active_queue.go`, `backoff_queue.go`, `unschedulable_pods.go` as separate files with distinct types (`activeQueue`, `backoffQueuer` interface, `unschedulablePods` struct). Local agent treated the queue as one file (`scheduling_queue.go`).
2. **`inFlightPods` / `inFlightEvents` mechanism.** Driver explained the linked-list buffer that records cluster events between `Pop()` and `Done()`, and how `determineSchedulingHintForInFlightPod` replays them. Local agent mentioned "in-flight event tracking" vaguely but missed the replay mechanism that is the actual core of queueing hints.
3. **`flushUnschedulablePodsLeftover`** running every 30s with a 5-minute `podMaxInUnschedulablePodsDuration` ceiling. Local agent had none of this.
4. **`SchedulerPopFromBackoffQ` feature gate** and its effect (Pop can pull from backoffQ when activeQ empty; re-runs PreEnqueue on the way through). Local agent didn't mention.
5. **PreEnqueue gating mechanism**: `GatingPlugin` / `GatingPluginEvents` fields on `QueuedPodInfo`. Local agent described PreEnqueue but missed the gating model.
6. **`PreBindPreFlight` semantics.** Local agent listed it but had the contract wrong (called it a mode flag). Driver correctly identified it as a separate sub-extension point that returns `Skip` to short-circuit PreBind, used by the DynamicResources plugin.
7. **The reprieve mechanism's actual implementation**: `addGENominatedPods` in `runtime/framework.go:1059` adds higher/equal-priority nominated pods to the node snapshot during the *next* cycle's filter. Local agent invented a "5-second grace period" reprieve story, which is wrong.
8. **Async preemption details**: `ev.preempting` set, the "delete all but last victim then remove from set then delete last" sequence, `IsPodRunningPreemption` check, `prepareCandidateAsync` at line 501.
9. **`pickOneNodeForPreemption` ordered scoring** (PDB violations → highest victim priority → priority sum → pod count → latest start time). Local agent had nothing on victim ranking.
10. **PDB-violating vs non-PDB-violating candidate lists** in `DryRunPreemption`.
11. **`PodsToActivate.Clone() returns self`** — a real, subtle invariant (shared mutable state under its own mutex, not a deep copy). Local agent had this section but missed the invariant.
12. **Status `Rejected`** as distinct from `Error`/`Unschedulable` for Reserve/Permit/PreBind/Bind. Local agent enumerated only the public status codes and missed `Rejected`.
13. **Filter status semantics for preemption**: only nodes that failed with `Unschedulable` (not `UnschedulableAndUnresolvable`) are preemption candidates — `NodesForStatusCode(Unschedulable)`. Local agent stated the distinction abstractly but missed *this specific consequence*, which is the load-bearing reason the distinction exists.
14. **Specific recent commits with dates**: async DRA API dispatcher (2025-07), `filterTimeout` config (2025-05), DRA v1 gRPC (2025-07), `resource.k8s.io/v1beta2` (2025-03), ResourceSlice B-tree indexer (2024-10), `nominatedNodeNameForExpectationEnabled` (2024-12), `PublishResourceSlices` scheduler-perf workload (2025-09), CEL LRU cache (2024-09). Local agent's "recent changes" section was vaguer and had no dates.
15. **`UnschedulableCount` / `ConsecutiveErrorsCount`** on `QueuedPodInfo` for smarter backoff (2024-11).

## Things local Explore got that Driver did not

None identified. The lock-ordering code excerpt local included was actually wrong (see below).

## Things local Explore got wrong (Driver was right)

1. **Permit timeout**: local said "typically 30 seconds"; Driver said `maxTimeout = 15 minutes` at `framework.go:47`. Driver is right.
2. **Preemption reprieve**: local invented "victims get a 5s grace period before eviction, giving the preempting pod a window to bind." This is fabricated. The real reprieve is `addGENominatedPods` — adjusting the *snapshot* for the next cycle, not a delete delay.
3. **PreBindPreFlight**: local described it as a "preflight mode" flag on PreBind. Driver correctly identified it as its own extension point returning Skip.
4. **Lock ordering**: local listed `lock > activeQueue.lock > backoffQueue.lock > nominator.nLock`. Driver said only `PriorityQueue.lock → activeQ.lock` is the documented ordering (with `underLock` helpers). The four-level chain local stated may be invented.

## Things both got right (rough parity)

- Status code enumeration on the common codes
- Reserve / Unreserve reverse order on failure
- Scheduling-cycle / binding-cycle async split
- `NominatedNodeName` is stored on the pod and the next cycle prefers that node
- Three-queue structure of the SchedulingQueue
- `MoveAllToActiveOrBackoffQueue` is event-driven
- Reserve runs sequentially, Score runs in parallel across nodes

## Interpretation

The miss pattern for local Explore is consistent and predictable:

1. **Cross-file decomposition is invisible to grep.** When functionality is split across `active_queue.go` / `backoff_queue.go` / `unschedulable_pods.go`, an Explore agent that lands on `scheduling_queue.go` first will assume that's the whole story.
2. **Feature-gated paths and recent additions are systematically under-cited.** Driver's pre-computed changelog produced ~10 dated commits; local Explore produced 0.
3. **Plausible-sounding fabrications.** Local Explore confidently invented a 5s grace period and a 30s Permit timeout — both load-bearing numbers that would mislead any downstream design work. Driver's claims were narrower and grounded in named symbols.

Driver did not exhibit the inverse failure: I found no detail local had that Driver lacked, and no Driver claim that I could identify as wrong.

The cost asymmetry: Driver took ~2x the wall-clock time but, if you spent that extra minute verifying local Explore's claims with grep, you'd burn far more tokens and still likely miss item #2 above.
