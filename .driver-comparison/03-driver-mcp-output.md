# Driver MCP — `gather_task_context` output

**Tool:** `mcp__driver__gather_task_context`
**Codebase:** `kubernetes` (default branch)
**Prompt:** the composite five-part question (see 01-question.md), with explicit instructions to bias toward depth and specifics.

---

Now I have enough detailed context to synthesize a comprehensive response. Let me compile the findings.

## Context for: kube-scheduler Scheduling Framework Deep-Dive

### 1. The SchedulingQueue — Three Sub-Queues & Pod Movement

**Implementation Location:** `pkg/scheduler/backend/queue/scheduling_queue.go` (PriorityQueue), `pkg/scheduler/backend/queue/active_queue.go` (activeQueue), `pkg/scheduler/backend/queue/backoff_queue.go` (backoffQ), `pkg/scheduler/backend/queue/unschedulable_pods.go` (unschedulablePods).

**Three Sub-Queues:**

- **`activeQ`** (`activeQueue` struct at `active_queue.go:163-226`): A heap ordered by `QueueSortFunc` (typically priority-based via `queuesort` plugin). Contains pods ready for scheduling. Has its own `sync.RWMutex` (`lock`), a `sync.Cond` for blocking `Pop()` when empty, and tracks `inFlightPods` (map[UID]*list.Element) and `inFlightEvents` (*list.List). The `schedCycle` (int64) increments on every `Pop()`.

- **`backoffQ`** (`backoffQueuer` interface at `backoff_queue.go`): Holds pods that failed scheduling and are waiting for their exponential backoff to expire. Default initial backoff: 1s (`DefaultPodInitialBackoffDuration`), max: 10s (`DefaultPodMaxBackoffDuration`). Ordered by backoff expiration time.

- **`unschedulablePods`** (`*unschedulablePods` at `unschedulable_pods.go`): A map-based store (`podInfoMap`) for pods that attempted scheduling but were rejected. Pods stay here up to `podMaxInUnschedulablePodsDuration` (default 5 minutes), after which `flushUnschedulablePodsLeftover()` moves them to activeQ/backoffQ (runs every 30 seconds).

**Lock Ordering Invariant (critical):**

The `PriorityQueue` has a top-level `lock sync.RWMutex` (`scheduling_queue.go:163`). The `activeQ` has its own internal `lock sync.RWMutex` (`active_queue.go:164`). The documented ordering is: **PriorityQueue.lock → activeQ.lock**. The `activeQ` exposes `underLock()` and `underRLock()` helpers so operations that touch both the PQ-level state and the activeQ's internal heap respect this ordering. Methods like `moveToActiveQ` acquire `activeQ.underLock()` *after* the caller already holds `PriorityQueue.lock`.

**Pod Movement Between Queues:**

1. **New pod arrives** (`Add`): Runs `PreEnqueue` plugins → if gated, goes to `unschedulablePods`; if passes, goes to `activeQ`.
2. **Scheduling fails** (`AddUnschedulableIfNotPresent`): Pod leaves `inFlightPods` via `Done()`. If QueueingHints are enabled, `determineSchedulingHintForInFlightPod` replays buffered `inFlightEvents` against the pod's `UnschedulablePlugins`/`PendingPlugins` to determine a `queueingStrategy`: `queueSkip` → unschedulablePods, `queueAfterBackoff` → backoffQ, `queueImmediately` → activeQ. Without hints, the legacy path checks `moveRequestCycle >= podSchedulingCycle` to decide between backoffQ and unschedulablePods.
3. **Backoff expires** (`flushBackoffQCompleted`): Periodically pops completed pods from backoffQ → `moveToActiveQ`.
4. **Cluster event arrives** (`MoveAllToActiveOrBackoffQueue`): Triggered from event handlers (node add/update, pod assigned, etc.). Iterates `unschedulablePods`, calls `isPodWorthRequeuing` per-pod using registered `QueueingHintFn`, then `requeuePodWithQueueingStrategy`.

**MoveAllToActiveOrBackoffQueue Trigger:** Called by `eventhandlers.go` on cluster events — node changes, assigned pod add/update, resource changes. The public method (`scheduling_queue.go:1183-1187`) acquires `PriorityQueue.lock`, delegates to the private `moveAllToActiveOrBackoffQueue` which first calls `isEventOfInterest` to short-circuit irrelevant events.

**QueueingHints / EventsToRegister:**

Each plugin implementing `EnqueueExtensions` returns `EventsToRegister() []ClusterEventWithHint`. Each `ClusterEventWithHint` pairs a `ClusterEvent` with an optional `QueueingHintFn(logger, pod, oldObj, newObj) (QueueingHint, error)`. These are compiled into `QueueingHintMapPerProfile` (type `map[string]QueueingHintMap` where `QueueingHintMap = map[ClusterEvent][]*QueueingHintFunction`). When an event fires, `isPodWorthRequeuing` looks up matching hints for the pod's `UnschedulablePlugins`/`PendingPlugins` and calls each hint fn. Return values: `fwk.Queue` → requeue, `fwk.QueueSkip` → skip. On error, treated as `Queue` (conservative). The feature gate is `SchedulerQueueingHints`.

**SchedulerPopFromBackoffQ feature gate:** When enabled, `Pop()` can also pop from backoffQ directly when activeQ is empty (see `unlockedPop` at `active_queue.go:279-336`). This changes the `moveToActiveQ`/`moveToBackoffQ` paths to re-run PreEnqueue plugins before moving from backoff.

---

### 2. Scheduling Cycle Extension Points — In Order

The split: **Scheduling Cycle** (synchronous, single-threaded per pod) = PreEnqueue → QueueSort → PreFilter → Filter → PostFilter → PreScore → Score → NormalizeScore → Reserve → Permit. **Binding Cycle** (asynchronous goroutine) = PreBind → Bind → PostBind.

| Extension Point | Contract & Status Codes | Behavior on Non-Success |
|---|---|---|
| **PreEnqueue** | Runs before pod enters activeQ. Returns `Success`/`Error`/`UnschedulableAndUnresolvable`. If non-success, pod stays gated in unschedulablePods with `GatingPlugin` set. |
| **QueueSort** | Single plugin, returns `LessFunc`. Panics at init if none configured. Used as heap comparator. |
| **PreFilter** | Per-plugin: returns `(PreFilterResult, Status)`. Can return `Skip` (plugin excluded from Filter), `Unschedulable`/`UnschedulableAndUnresolvable` (entire pod unschedulable), `Error` (abort). `PreFilterResult.NodeNames` restricts candidate set. Results merged across plugins. |
| **Filter** | Per-node, per-plugin. Returns `Success`/`Unschedulable`/`UnschedulableAndUnresolvable`/`Error`. Unschedulable = might become schedulable; UnschedulableAndUnresolvable = won't (important for preemption — only `Unschedulable` nodes are candidates). Error aborts the cycle. |
| **PostFilter** | Runs only on total Filter failure (FitError). Plugins run sequentially until one returns `Success` (nominates node). Can return `Unschedulable`/`UnschedulableAndUnresolvable`/`Error`. DefaultPreemption is the primary implementation. |
| **PreScore** | Runs once with all feasible nodes. Can return `Skip` (excludes from Score). Error aborts. |
| **Score** | Per-node per-plugin. Returns `(int64, Status)`. Runs in parallel across nodes. |
| **NormalizeScore** | Per-plugin across all nodes. Normalizes to [0, MaxNodeScore]. Called as `ScoreExtensions().NormalizeScore`. |
| **Reserve** | Per-plugin sequentially. Returns `Success`/`Error`/`Rejected`. On failure: **unreserve in reverse order** of already-reserved plugins, then ForgetPod from cache. |
| **Permit** | Returns `Success`/`Wait`/`Error`/`Rejected`. `Wait` → pod enters `waitingPodsMap` with timeout (max 15 minutes, `maxTimeout` at `framework.go:47`). WaitOnPermit blocks in binding cycle. Timeout rejection → Unreserve + ForgetPod. |
| **PreBind** | In binding cycle. Returns `Success`/`Error`/`Rejected`. PreBindPreFlight can return `Skip` to skip the plugin's PreBind. |
| **Bind** | Plugins tried sequentially until one returns non-`Skip`. Only one binds. `Success`/`Error`/`Rejected`/`Skip`. |
| **PostBind** | Informational, no meaningful status. Runs after successful bind. |

---

### 3. Preemption — DefaultPreemption PostFilter Plugin

**Location:** `pkg/scheduler/framework/preemption/preemption.go` (generic evaluator), `pkg/scheduler/framework/plugins/defaultpreemption/` (the PostFilter plugin).

**Flow:**
1. `Evaluator.Preempt()` (line 234): Fetches latest pod, checks `PodEligibleToPreemptOthers` (prevents cascading preemption if pod already nominated and has waited < 2 schedule cycles).
2. `findCandidates()`: Gets nodes with `Unschedulable` status (not `UnschedulableAndUnresolvable` — critical invariant). Fetches PDBs. Calls `DryRunPreemption` which runs `SelectVictimsOnNode` in parallel with random offset.
3. `DryRunPreemption` (line 741): Uses `candidateList` with atomic index for lock-free concurrent addition. Maintains two lists: non-PDB-violating and PDB-violating candidates.
4. `callExtenders()`: Lets extenders further filter/reorder candidates.
5. `SelectCandidate()`: Uses `pickOneNodeForPreemption` with ordered scoring functions: (1) min PDB violations, (2) highest priority victim, (3) sum of priorities, (4) num pods, (5) latest start time.
6. **Execution — Sync vs Async:** If `enableAsyncPreemption` is false: `prepareCandidate()` deletes victims in parallel, clears lower-priority nominated pods' NominatedNodeName. If true: `prepareCandidateAsync()` (line 501) inserts pod UID into `ev.preempting` set, spawns goroutine that deletes all but last victim, then removes from set and deletes last victim. The `IsPodRunningPreemption` check prevents the preemptor from being scheduled again while async preemption is in flight.

**NominatedPods/NominatedNodeName:** After preemption, `PostFilterResult.NominatingInfo` sets `NominatedNodeName` on the pod (via `updatePod` in `handleSchedulingFailure`). In the next cycle, `findNodesThatFitPod` calls `evaluateNominatedNode` first — if the nominated node passes filters+extenders, it's returned directly without scoring others. During filter on the nominated node, `RunFilterPluginsWithNominatedPods` adds nominated pods with GE priority to the node snapshot (the "reprieve" mechanism) to ensure a nominated pod doesn't get displaced by another pod of equal priority.

**Reprieve:** `addGENominatedPods` (runtime/framework.go:1059) adds higher-or-equal priority nominated pods to the node snapshot before running filters. This means the preemptor's second attempt accounts for other nominated pods, preventing "double-booking."

---

### 4. Concrete Invariants and Gotchas

**Unschedulable vs UnschedulableAndUnresolvable:**
- `Unschedulable`: The scheduling constraint *might* be resolved by preemption (removing lower-priority pods). These nodes are candidates in `findCandidates` → `NodesForStatusCode(Unschedulable)`.
- `UnschedulableAndUnresolvable`: Cannot be fixed by preemption (e.g., node affinity mismatch, insufficient CPU that no pod removal helps). Excluded from preemption candidates.
- For the queue: both record `UnschedulablePlugins` but `UnschedulableAndUnresolvable` has stronger implications for the `GatingPluginEvents` mechanism.

**Reserve Failure — Unreserve Order:**
`RunReservePluginsUnreserve` (framework.go:1498-1520) iterates reserve plugins **in reverse order** (`for i := len(f.reservePlugins) - 1; i >= 0; i--`). This ensures LIFO cleanup — plugins that reserved last are unreserved first.

**Permit Wait + Timeout:**
When Permit returns `Wait`, the framework adds the pod to `waitingPodsMap` with per-plugin timeouts (capped at `maxTimeout = 15 minutes`). `WaitOnPermit` (called in binding cycle, line 1602) blocks on the waiting pod's channel. If timeout fires, the pod is rejected. The scheduling cycle returns `Wait` status, and the binding goroutine calls `WaitOnPermit` which blocks until allowed/rejected. Critically: the scheduling cycle **does not block** — it launches the binding goroutine and moves on.

**Scheduling-Cycle vs Binding-Cycle Split:**
- `schedulingCycle` (schedule_one.go:150-275): Synchronous. Holds no goroutines. Ends after Reserve+Permit.
- `bindingCycle` (schedule_one.go:278-363): Runs in a separate goroutine. Calls PreBindPreFlights, WaitOnPermit, PreBind, Bind, PostBind. On failure, `handleBindingCycleError` calls Unreserve, ForgetPod, then `MoveAllToActiveOrBackoffQueue` (treating it as a PodDelete event since the assumed pod is removed).

**PodsToActivate Cycle State Mechanism:**
`PodsToActivate` (`interface.go:140-154`) is written into `CycleState` under key `PodsToActivateKey`. Plugins (e.g., during Reserve) can add pods that should be moved from unschedulablePods/backoffQ to activeQ. At the end of a successful scheduling cycle (line 267-272) and binding cycle, `sched.SchedulingQueue.Activate(podsToActivate.Map)` is called. `Clone()` returns `self` (not a deep copy) — it's a shared mutable state protected by its own `sync.Mutex`.

**In-Flight Events Buffer:**
When `SchedulerQueueingHints` is enabled, `activeQ` records all cluster events that arrive between a pod's `Pop()` and `Done()` in `inFlightEvents` (a linked list). When the pod returns to `AddUnschedulableIfNotPresent`, these events are replayed via `determineSchedulingHintForInFlightPod` → `isPodWorthRequeuing` to decide if the pod should go to activeQ immediately (if an event arrived that matches a hint returning `Queue`).

---

### 5. Recent Changes (2024-2025)

**DRA Integration Points:**
- `DynamicResources` plugin (`pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go`): Implements PreEnqueue, PreFilter, Filter, PostFilter, Reserve, PreBind, PreBindPreFlight, and EnqueueExtensions. Uses `SharedDRAManager` (exposed via `fwk.Handle.SharedDRAManager()`).
- 2025-07: **Asynchronous DRA API dispatcher** introduced (`pkg/scheduler/backend/api_dispatcher/`) with call count/duration metrics. The `filterTimeout` configuration added (2025-05) with context cancellation for DRA allocations during Filter.
- 2025-07: DRA v1 API with gRPC definitions; consumable device capacity and device binding conditions (alpha).
- 2025-03: `resource.k8s.io/v1beta2` APIs, partitionable devices with shared counters, Device Taint Eviction Controller.
- 2024-10: ResourceSlice controller with B-tree indexer; SharedDRAManager introduced.
- 2024-09: LRU cache for compiled CEL expressions in DRA scheduler plugin.
- 2025-08: "DynamicResources plugin assumes claims in cache after successful API calls" — cache consistency fix.

**QueueingHints Maturity:**
- Feature gate `SchedulerQueueingHints` now controls the full path. The in-flight events mechanism is mature.
- 2025-02: "Integrated PreEnqueue plugins in the scheduling queue" — PreEnqueue now runs during queue transitions (moveToActiveQ, moveToBackoffQ when `SchedulerPopFromBackoffQ` is enabled).
- 2024-09: "Scheduler gating mechanism for event-driven pod requeueing" — `GatingPlugin`/`GatingPluginEvents` fields on `QueuedPodInfo`.
- 2024-11: `UnschedulableCount` and `ConsecutiveErrorsCount` added to `QueuedPodInfo` for smarter backoff.

**Scheduler-Perf:**
- 2025-09: `PublishResourceSlices` workload added to scheduler performance tests.
- Integration tests at `test/integration/scheduler_perf/`.

**Async Preemption:**
- 2024-10: Async preemption metrics introduced. `prepareCandidateAsync` with `ev.preempting` set tracking.

**SchedulerPopFromBackoffQ:**
- Feature gate enabling `Pop()` from backoffQ when activeQ is empty. Changes the `moveToBackoffQ` path to re-run PreEnqueue.

**NominatedNodeNameForExpectation:**
- `nominatedNodeNameForExpectationEnabled` flag in `Scheduler` struct controls whether the binding cycle sets NominatedNodeName before WaitOnPermit (2024-12: "Triggered pod rescheduling when nominated node name changes or is removed").

**PreBindPreFlight:**
- New sub-extension point allowing PreBind plugins to signal `Skip` before the main PreBind runs. DynamicResources uses this (`PreBindPreFlight`) to skip PreBind when there are no claims. The framework runs these first in `RunPreBindPreFlights` and records skipped plugins into CycleState.
