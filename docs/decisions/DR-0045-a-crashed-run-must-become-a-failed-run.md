---
title: DR-0045 — a workflow run that crashes must become a FAILED run, not sit at `running` forever
type: decision
status: active
created: 2026-08-16
updated: 2026-08-16
---

# DR-0045: a crashed run must become a failed run

**Status: ACTIVE — ratified by Kezia, 2026-08-16**, decision 2 (no compensation) explicitly
affirmed as "the only correct/fail-safe option". Raised by the live incident during DR-0043's
appliance test, 2026-08-16: a one-line bug stranded a billing DigitalOcean droplet for 20
minutes, and almost none of that cost came from the bug.

## Context

`WorkflowEngine.start` spawns each run as a bare task:

```python
task = asyncio.ensure_future(self._run(run_id, token))
self._runs[run_id] = _RunHandle(task=task, cancel_token=token)
task.add_done_callback(lambda _t, rid=run_id: self._runs.pop(rid, None))
```

The done-callback removes the handle from the registry and **never retrieves the task's
exception**. So if `_run` raises anything the engine does not already classify, the exception is
swallowed into asyncio's "Task exception was never retrieved" warning and:

- `workflow_runs.status` stays **`running`**, with `error` NULL and `finished_at` NULL;
- the aggregate never leaves its in-flight state (here: cluster stuck at `destroying`);
- no compensation runs, no outcome event fires, no timer re-arms;
- `seedpodctl workflows list` shows a run that looks alive;
- and real infrastructure keeps billing.

`_run` already knows how to record a terminal failure — an unknown workflow name writes
`status="failed"` with a permanent error (`engine.py:508`). There is simply no catch-all for
anything unanticipated.

## The evidence, and why this is the second occurrence

DR-0043 erratum E3: `WorkflowDispatch.resolve` did not supply an input the destroy workflows
declared, so binding resolution raised `KeyError('snapshot')`. Every destroy died instantly. The
observable state was a run at `running` with an empty `error`, a cluster at `destroying`, and a
live droplet. The bug was one line and took a minute to fix; finding it took far longer, and
finding it at all depended on someone noticing a droplet that should have been gone.

**This exact shape has already bitten this engine once.** `engine.py:646` carries the scar:

> …instead of leaving no scope entry at all and crashing a downstream Ref's `_build_params` with
> a bare `KeyError` **and permanently wedging the run non-terminal**.

That fix removed one *cause* of a wedged run. It did not make a wedged run impossible, and the
class stayed open until DR-0043 found another way in. Removing causes one at a time does not
converge when the number of possible causes is "any unexpected exception".

**A second, independent half — recovery was also impossible.** Fixing dispatch did not rescue the
stuck run. `resume_inflight` replays a run's **persisted args**, frozen at admission, so the
already-admitted run resumed on the fixed build and died on the identical `KeyError`. Recovery
required a hand-written `UPDATE workflow_runs SET args = json_set(...)` against production.

Generalised: **adding a required input to a workflow strands every run admitted before the
change.** There is no migration path for in-flight runs and no warning. Since the workflow grammar
is frozen and CLAUDE.md makes adding a typed input/verb *the* sanctioned way to extend a workflow,
this edge sits squarely on the supported path.

## Decision

**1. `_run` gets an outermost exception boundary.** Any exception that escapes the existing
classification writes a terminal row before propagating no further: `status="failed"`,
`error={"kind": "permanent", "step": <current step_path or None>, "message": str(exc)}`,
`finished_at=clock.now()`. This is not new machinery — it is the `unknown workflow` branch's
existing shape, applied to the general case.

**2. It does NOT compensate.** An unanticipated exception means the engine does not know what
state it is in; running undo steps from there could do real harm on real infrastructure. The run
fails terminally and loudly, and a human decides. Recorded as a deliberate asymmetry with the
ordinary step-failure path, not an oversight.

**3. The done-callback retrieves the exception and logs it.** So "Task exception was never
retrieved" can never again be the *only* trace of a dead run. Belt-and-braces behind decision 1,
and the thing that would have made this incident a 30-second diagnosis.

**4. Resume validates a run's args against its workflow's declared inputs, and fails loudly when
they disagree.** A run whose stored args cannot satisfy the current definition becomes `failed`
with a message naming the missing input — not a `KeyError`, and not an infinite retry. This turns
"stranded forever, no explanation" into "failed, and here is exactly which input went missing",
which is both actionable and greppable.

**5. The aggregate must follow.** A cluster whose destroy run failed belongs in `DESTROY_FAILED`,
which already exists and already has a retry path (`DESTROY_FAILED × DestroyRequested`). The point
of decision 1 is not tidier bookkeeping; it is that the machine's existing recovery routes become
reachable. Today they are not, because nothing ever tells the machine the run died.

## Consequences

- A crashed run costs one clear failure and a retry, instead of a stranded aggregate and whatever
  its infrastructure bills until a human notices.
- `failed` becomes genuinely load-bearing for "the engine broke", not only for "a step returned an
  error". Anything watching run status sees engine defects it previously could not.
- Decision 4 makes workflow-input changes safe to deploy while runs are in flight — currently they
  are not, and nothing says so.
- Slightly more can reach `DESTROY_FAILED`/`FAILED`. That is the intent: a visible failed state is
  strictly better than an invisible wedged one.
- **Not addressed here:** whether a wedged run should be detectable *after the fact* — e.g. a
  sweep for runs `running` with no live task after a restart. Decisions 1 and 4 stop new ones
  being created; they do not clean up one already in the database. Flagged rather than folded in.

## What would pin it

1. A step whose binding refers to a missing scope key ends as `status="failed"` with the offending
   name in `error.message` — the DR-0043 scenario, as a test.
2. A step verb that raises an unexpected non-`ProviderError` exception (not a classified transient
   or permanent) likewise ends `failed`, and **no** compensation steps run (decision 2).
3. A cluster whose destroy run crashes reaches `DESTROY_FAILED`, and a subsequent
   `DestroyRequested` is accepted (decision 5) — proving the recovery route is reachable.
4. `resume_inflight` on a run whose stored `args` lack an input the definition now declares ends
   `failed` naming that input, rather than raising (decision 4). This is the exact case that made
   the 2026-08-16 recovery need a manual DB write.
5. The done-callback logs a retrieved exception — assert against the log record, since the whole
   point is that it is the trace of last resort (decision 3).

---

## What actually landed — and a correction to this DR's own problem statement

**"What would pin it" item 2 was wrong, and writing it found the error.** It assumed a step verb
raising an unexpected exception would reach the new boundary. It does not: `_run_step` already
classifies anything a verb throws through §2.3.1's taxonomy, so that path failed and compensated
correctly long before this DR. The unprotected surface was never step execution — it was the
engine's **own machinery around** it: binding resolution, scope construction, outcome-event
building. Which is exactly where DR-0043 erratum E3's `KeyError` lived.

So the boundary is real and needed, but narrower than this DR implied when it was ratified. The
suite reflects the corrected shape (`tests/engine/test_crashed_run_is_terminal.py`):

- unsatisfiable args → terminal `failed`, naming the missing input and saying retrying cannot fix
  it, with **no step rows written** (decisions 1 and 4);
- the aggregate follows — `outcome.failed` is dispatched, so the machine's existing recovery
  routes become reachable (decision 5);
- no compensation on the engine-level path, asserted against a workflow that is
  `on_failure: compensate` so an ordinary failure *would* have undone (decision 2);
- and a regression guard that a verb's own exception still takes the classified route with its
  own message and `failed_step` — i.e. that this DR's catch-all did not swallow behaviour that
  already worked.

Decision 3's done-callback is implemented and logs with `exc_info`, but is deliberately left
unasserted: it is now unreachable-by-design behind decision 1, and a test that had to reach it
would have to break decision 1 first. Recorded as a known coverage gap rather than a fake.

Suite: 2515 passed, 44 skipped.
