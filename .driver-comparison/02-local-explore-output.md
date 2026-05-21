# Local Claude Code — Explore subagent output

**Tool:** `Agent` with `subagent_type: Explore` (uses Grep, Glob, Read on raw source only)
**Prompt:** the composite five-part question (see 01-question.md), with instructions to be exhaustive and cite file:line.

---

# Deep-Dive: Kubernetes Scheduler Framework — Lifecycle, Architecture, and Extension Points

## 1. The SchedulingQueue: Three Sub-Queues and Movement Mechanics

The SchedulingQueue is the entry point for all pods entering the scheduler. It consists of three internal sub-queues, each serving a distinct purpose in the pod lifecycle.

**Architecture Overview**

The queue is defined in `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/backend/queue/scheduling_queue.go:91–144`. The interface declares five critical methods:

- `Add(pod)`: Enqueues a pod into activeQ (the "ready to schedule" queue)
- `Activate(pods)`: Moves pods from unschedulablePods or backoffQ to activeQ
- `Pop()`: Returns the next pod to schedule from activeQ
- `MoveAllToActiveOrBackoffQueue(reason, event)`: The key method for re-queuing rejected pods
- `AddUnschedulableIfNotPresent(pod, condition)`: Moves a pod to unschedulablePods after it has failed scheduling

The three sub-queues are:

1. **activeQ**: A min-heap of pods ready for scheduling, ordered by priority and FIFO
2. **podBackoffQ**: Pods that have failed and are temporarily backed off (exponential backoff: 1s, 2s, 4s, up to maxBackoff)
3. **unschedulablePods**: Pods that have failed with UnschedulableAndUnresolvable or similar codes, awaiting cluster state changes

**Movement Between Queues**

The core queue implementation (`/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/backend/queue/scheduling_queue.go:163–209`) defines a PriorityQueue struct with strict lock ordering rules:

```
lock > activeQueue.lock > backoffQueue.lock > nominator.nLock
```

Violation of this order causes deadlocks. The movement mechanics are:

- **Add → activeQ**: A new pod is added to activeQ immediately
- **Pop from activeQ → schedulingCycle**: The scheduler pops a pod and attempts to schedule it
- **Failed scheduling → backoffQ or unschedulablePods**: If scheduling fails, `AddUnschedulableIfNotPresent` (lines 1217–1259) decides placement based on the failure reason
- **Event triggers requeue → activeQ or backoffQ**: When events occur (node update, pod deletion, resource availability), `MoveAllToActiveOrBackoffQueue` is called

**QueueingHints and EventsToRegister**

The scheduling framework plugins can now implement `EventsToRegister()` to declare which events should trigger pod re-queuing, along with a `QueueingHintFn` that determines how to requeue (immediately to activeQ via `queueSkip`, or to backoffQ via `queueAfterBackoff`).

This is implemented in `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/backend/queue/scheduling_queue.go:1183–1214` in `MoveAllToActiveOrBackoffQueue`. The function evaluates each event against each pod's registered queueing hints. If a hint returns `queueSkip`, the pod is moved to activeQ; if `queueAfterBackoff`, it goes to backoffQ; if the hint is not found, a conservative default is applied.

The design reduces thrashing by allowing plugins to opt-in to only relevant events. For example, the defaultpreemption plugin (`/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:154–178`) registers for Pod Delete events with a QueueingHintFn that returns `QueueSkip`, meaning when a pod is deleted, preemption candidates are immediately activated.

## 2. The Scheduling Cycle Extension Points in Order

The full lifecycle of a scheduling attempt is orchestrated in `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/schedule_one.go:65–137`. The ScheduleOne function splits the work into two phases: schedulingCycle (lines 150–275) and bindingCycle (lines 278–286).

**Scheduling Cycle Extension Points**

The schedulingCycle executes a sequence of extension points, each with distinct contracts:

1. **PreEnqueue** (not in schedulingCycle, but in queue.Add): Called before a pod enters the queue. Can return Status codes to reject enqueueing. Implementation is optional.

2. **QueueSort** (in Pop): Determines pod priority in activeQ. Not a plugin extension point, but a framework-level ordering function.

3. **PreFilter** (lines 150–160 of schedulingCycle): Runs before filtering and can reject the pod outright, returning:
   - `Success`: Continue to Filter
   - `Error` or `Unschedulable`: Abort scheduling for this cycle; move to backoffQ or unschedulablePods
   - `UnschedulableAndUnresolvable`: Explicitly mark as requiring cluster changes
   - Returns a `PreFilterResult` that can set `NodeNames` to restrict which nodes to evaluate

4. **Filter** (applied per-node): Tests pod-node compatibility. Can return:
   - `Success`: Node is feasible
   - `Unschedulable`: Node doesn't match pod requirements
   - `Error`: Internal failure; abort with error status
   - `UnschedulableAndUnresolvable`: Explicitly unresolvable

5. **PostFilter** (lines 161–180): Runs only if no nodes passed Filter. Informational plugins run first (status Unschedulable), then remediation plugins (status Success or Error). Returns a `PostFilterResult` with nomination information or a status.

6. **PreScore** (not explicitly in schedule_one, but part of scoring phase): Preparatory work before scoring.

7. **Score** and **NormalizeScore**: Per-node scoring plugins. Return scores and status codes (Success, Error, Unschedulable).

8. **Reserve** (lines 220–240): Called on the selected node to reserve resources. If this fails with an Error status, the framework immediately calls Unreserve on all previously reserved plugins in reverse order (lines 234–239), then moves the pod to backoffQ.

**Binding Cycle Extension Points**

The bindingCycle (lines 278–286 in schedule_one.go) runs after a node is selected:

9. **PreBindPreFlights** (line 282): Runs PreBind plugins with a special "preflight" mode that doesn't fail the entire binding, only individual plugin checks.

10. **Permit** (line 284): Plugins can return:
    - `Success`: Proceed to Bind
    - `Wait`: Block scheduling; the pod enters a waiting list. A timeout (typically 30s) is set; if exceeded, the pod is rejected and re-queued
    - `Error` or `Unschedulable`: Abort binding

11. **PreBind** (not shown in snippet but standard order): Pod-specific preparation on the selected node (e.g., mount volumes).

12. **Bind**: Actuates the binding in the API server (PodBinding in etcd). If this fails, the binding cycle fails.

13. **PostBind**: Post-binding cleanup (informational).

**Critical Status Code Behavior**

Status codes are defined in `/Users/joeschmid/src/work/driver/kubernetes/staging/src/k8s.io/kube-scheduler/framework/interface.go:42–107`:

- `Success (0)`: Continue to next plugin/phase
- `Error (1)`: Internal error; abort and retry later
- `Unschedulable (2)`: Pod doesn't fit; try next node or next cycle
- `UnschedulableAndUnresolvable (3)`: Pod doesn't fit and cluster action is needed (e.g., scale, preemption); explicitly marked for long-term queueing
- `Wait (4)`: Permit plugins only; block until timeout or manual approval
- `Skip (5)`: Bind plugins only; skip binding attempt (another plugin will handle it)
- `Pending (6)`: Reserved for future use

## 3. Preemption: Victim Selection and Nomination

The defaultpreemption plugin (`/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go`) implements both PostFilterPlugin and PreEnqueuePlugin interfaces, enabling asynchronous preemption.

**How PostFilter Preemption Works**

When no nodes pass Filter, PostFilter runs (lines 131–152). The defaultpreemption PostFilter method:

1. Checks if preemption is enabled (can be disabled per-profile)
2. Calls SelectVictimsOnNode for each node to find pods that, if evicted, would free resources for the incoming pod
3. Returns a `PostFilterResult` with `NominatingInfo` containing:
   - `NominatedNodeName`: The node on which preemption would occur
   - `NominatingMode`: `ModeOverride` (the default, meaning the nominated pod overrides other nominations)

**Nomination and NominatedNodeName**

The `NominatedNodeName` is stored on the Pod object in the API server. In the next scheduling cycle, when the pod is popped, the scheduler checks if it has a nomination and skips scheduling (assuming the nominated node still has space). The nominated pod is moved to backoffQ to allow the preempting pod a chance to bind first.

The reprieve logic in SelectVictimsOnNode (`/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:217–300`) ensures that nominated pods get a grace period (typically 5s) before they are actually evicted, allowing the preempting pod to bind within that window.

**Async Preemption Gating**

The defaultpreemption plugin implements PreEnqueue (lines 154–178) to gate whether preemption should be attempted. If a preemption attempt is in progress (a nominated pod exists), the PreEnqueue method returns a special status to prevent redundant preemption attempts on the same pod.

## 4. Concrete Invariants and Gotchas

**Unschedulable vs. UnschedulableAndUnresolvable**

- `Unschedulable`: The pod doesn't fit given current cluster resources (e.g., no node has enough memory). It will be retried on cluster changes.
- `UnschedulableAndUnresolvable`: Explicit marker that the pod cannot be scheduled without external intervention (e.g., PVC is missing). This triggers longer backoff and special handling.

The distinction is critical: PreFilter plugins should return UnschedulableAndUnresolvable only when absolutely certain that no reasonable cluster change will help.

**Reserve Plugin Failure and Unreserve Ordering**

Reserve plugins are called sequentially (lines 220–230 of schedule_one.go). If the i-th plugin fails, the framework immediately calls Unreserve on plugins 0 to i-1 in **reverse order** (lines 234–239). This ensures cleanup happens in the opposite order of reservation, avoiding deadlocks and resource leaks. The pod is then moved to backoffQ.

**Permit Wait Status and Timeouts**

When a Permit plugin returns `Wait`, the pod enters a waiting list. The framework sets a timeout (typically 30 seconds, configurable). If the timeout expires, the pod is rejected and moved to backoffQ. If a plugin explicitly rejects the waiting pod via WaitOnPermit context, it's also moved to backoffQ.

The invariant: **A pod in Permit Wait is not competing for nodes; it is blocked on a specific condition (e.g., volume provisioning).** This prevents deadlock when many pods are waiting.

**Scheduling Cycle vs. Binding Cycle Split**

The split (schedule_one.go:65–137) ensures that:

1. Scheduling (node selection) is deterministic and repeatable
2. Binding (API server mutation) is attempted only after a node is committed
3. If binding fails, only the binding is retried, not the entire scheduling cycle

This separation is crucial for correctness when multiple schedulers are present or when the API server is slow.

**PodsToActivate State Management**

CycleState is initialized in schedule_one.go:104–112 with a `PodsToActivate` state (`/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/interface.go:132–154`). Plugins can stash pods here during scheduling. At the end of the cycle (if scheduling succeeds), these pods are activated. This mechanism allows PreFilter or PostFilter plugins to wake up related pods without explicitly calling `Activate`.

**Lock Ordering and Deadlock Prevention**

The scheduling queue enforces strict lock ordering (scheduling_queue.go:163–209):

```
lock > activeQueue.lock > backoffQueue.lock > nominator.nLock
```

Any deviation causes potential deadlock. This is enforced via code review and testing, not at runtime.

## 5. Recent Changes: DRA Integration and QueueingHints Maturity

**Dynamic Resource Allocation (DRA) Integration**

DRA (Dynamic Resource Allocation) is a feature that allows pods to request dynamically provisioned resources (e.g., GPUs, custom hardware). The scheduler integration is guarded by the `DynamicResourceAllocation` feature gate.

In `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/events.go:151–158`, the `extractPodGeneratedResourceClaimChange` function detects when a pod's resource claims change status. When a ResourceClaim transitions from Pending to Allocated, the scheduler is notified to re-queue the pod.

The DRA manager is initialized in the scheduler and passed to the framework runtime, allowing plugins to query resource claim status and make scheduling decisions based on claim availability.

**QueueingHints Maturity**

QueueingHints have reached a point of broad adoption. The `EventsToRegister()` interface is now the standard way for plugins to declare event interests. In `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/backend/queue/scheduling_queue.go:277–282`, the framework maintains a `QueueingHintMapPerProfile` that maps extension points to event-to-hint associations.

The maturity is evident in the special event labels defined in `/Users/joeschmid/src/work/driver/kubernetes/pkg/scheduler/framework/events.go:29–62`:

- `ScheduleAttemptFailure`: A pod failed scheduling; used by recovery plugins
- `BackoffComplete`: A backoff timer expired; wake up backoffQ pods
- `PopFromBackoffQ`: A pod was popped from backoffQ; used by queueing hints
- `ForceActivate`: Internal signal to force activation regardless of hints
- `UnschedulableTimeout`: A pod has been in unschedulablePods for too long; wake it up

These special labels allow fine-grained control over pod re-queuing without forcing all pods to re-evaluate on every cluster event.

**In-Flight Event Tracking**

The scheduling queue now tracks "in-flight" events to avoid re-queuing pods multiple times for the same event. This is visible in the movePodsToActiveOrBackoffQueue logic (scheduling_queue.go:1217–1259), where the framework maintains a map of events that are currently being processed across all pods. This prevents thrashing when many pods respond to the same event.

---

**Summary**

The Kubernetes scheduler's framework is a sophisticated, event-driven state machine. Pods flow through three queues based on failure reasons and cluster events. Extension points are carefully ordered and gated by status codes to ensure deterministic, replayable scheduling. Preemption integrates seamlessly via nomination and async reprieve. Recent additions like DRA and QueueingHints demonstrate the framework's evolution toward more efficient, plugin-driven scheduling. The invariants—especially around Reserve/Unreserve pairing, lock ordering, and the scheduling/binding split—are foundational to correctness and must be preserved as the framework evolves.
