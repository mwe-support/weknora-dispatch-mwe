"""Executable design counterexamples; NOT the WeKnora runtime implementation.

Run: python docs/mwe/design/model_check.py
Uses an in-memory SQLite database only to check transactional state/outbox
and uniqueness properties. PostgreSQL locking, actual workers, MCP, vector
stores, and production migrations still require the acceptance tests in the
design document. All fixtures are synthetic.
"""

from dataclasses import dataclass, replace
import sqlite3


@dataclass
class Job:
    generation: int = 1
    current: bool = True
    published: bool = False
    publication_epoch: int = 0
    completeness: str = "complete"
    canceled: bool = False
    rollback_pin: bool = False
    deleting: bool = False
    active_index_manifest: str = ""


@dataclass
class Step:
    phase: str = "prepare"
    status: str = "running"
    attempt: int = 1
    dispatch: int = 1
    lease: str = "lease-1"
    expires: int = 120
    fingerprint: str = "input-1"
    expected_publication_epoch: int = 0
    successes: int = 0


def eligible(job, step):
    if job.canceled:
        return False
    if step.phase == "prepare":
        return job.current
    if step.phase == "projection":
        return job.published and step.expected_publication_epoch == job.publication_epoch
    if step.phase == "retire":
        return not (job.current or job.published or job.rollback_pin or job.deleting)
    raise ValueError("unknown phase")


def finish(job, step, *, generation=1, attempt=1, dispatch=1,
           lease="lease-1", fingerprint="input-1", now=10):
    """Model the conditions of one compare-and-set result commit."""
    if not (eligible(job, step) and step.status == "running"
            and generation == job.generation and attempt == step.attempt
            and dispatch == step.dispatch and lease == step.lease
            and fingerprint == step.fingerprint and now < step.expires):
        return False
    step.status = "succeeded"
    step.successes += 1
    return True


def ready(*, sealed, expected, states, complete=True):
    return (complete and sealed and expected == len(states)
            and all(state == "succeeded" for state in states))


def after_lease_loss(operation_phase):
    if operation_phase == "export_maybe_sent":
        return "blocked", "export_start_uncertain"
    if operation_phase == "provider_task_known":
        return "waiting_external", "poll_existing"
    return "retry_wait", "idempotent_resume"


def invalidation_closure(changed, dependencies):
    invalid = set(changed)
    while True:
        expanded = invalid | {stage for stage, deps in dependencies.items() if invalid & deps}
        if expanded == invalid:
            return invalid
        invalid = expanded


def successor_resolves(*, failed_stage, completed_equivalent, published):
    # Old contribution retirement has its own obligation, never inherited.
    return published and failed_stage not in {"retire", "cleanup"} and failed_stage in completed_equivalent


def pin_for_rollback(job):
    if job.deleting or job.canceled or job.completeness != "complete" or not job.active_index_manifest:
        return False
    job.rollback_pin = True
    return True


def searchable(job, manifest):
    return job.published and job.completeness == "complete" and manifest == job.active_index_manifest


def observe(step, inspector_available):
    # A monitoring failure is not a state transition or a lease revocation.
    return step.status, "known" if inspector_available else "unknown"


def check():
    passed = []

    def accept(name, condition):
        assert condition, name
        passed.append(name)

    job, step = Job(), Step()
    accept("duplicate completion acknowledges once", finish(job, step) and not finish(job, step) and step.successes == 1)
    for name, kwargs in [
        ("stale generation", {"generation": 0}),
        ("stale attempt", {"attempt": 0}),
        ("stale dispatch", {"dispatch": 0}),
        ("stale lease", {"lease": "old"}),
        ("expired lease", {"now": 120}),
        ("changed input", {"fingerprint": "new-input"}),
    ]:
        candidate = Step()
        accept(name + " cannot commit", not finish(Job(), candidate, **kwargs) and candidate.status == "running")
    accept("canceled job cannot resurrect", not finish(Job(canceled=True), Step()))
    accept("superseded prepare cannot commit", not finish(Job(current=False), Step()))

    old_published = Job(current=False, published=True, publication_epoch=7)
    projection = Step(phase="projection", expected_publication_epoch=7)
    accept("old published may finish while new current prepares", finish(old_published, projection))
    accept("old publication callback rejected after rollback", not finish(replace(old_published, publication_epoch=9), Step(phase="projection", expected_publication_epoch=7)))
    retiring = Job(current=False, published=False)
    accept("precise obsolete retirement remains eligible", eligible(retiring, Step(phase="retire")))
    accept("rollback pin blocks cleanup claim", not eligible(replace(retiring, rollback_pin=True), Step(phase="retire")))
    rollback_target = replace(retiring, active_index_manifest="verified-old-index")
    accept("available old version can be pinned for rollback", pin_for_rollback(rollback_target) and rollback_target.rollback_pin)
    accept("deleting version cannot be directly rolled back", not pin_for_rollback(replace(retiring, deleting=True, active_index_manifest="verified-old-index")))
    observed = Step()
    accept("monitor failure does not mark active work failed", observe(observed, False) == ("running", "unknown") and observed.status == "running")

    accept("unsealed zero counter is not complete", not ready(sealed=False, expected=0, states=[]))
    accept("missing fanout item is not complete", not ready(sealed=True, expected=2, states=["succeeded"]))
    accept("failed fanout item is not complete", not ready(sealed=True, expected=2, states=["succeeded", "failed"]))
    accept("unsupported or missing source blocks publication", not ready(sealed=True, expected=1, states=["succeeded"], complete=False))
    accept("ready barrier independent of post-publication work", ready(sealed=True, expected=2, states=["succeeded", "succeeded"]))

    accept("unknown export must not auto-restart", after_lease_loss("export_maybe_sent") == ("blocked", "export_start_uncertain"))
    accept("known export resumes its own task", after_lease_loss("provider_task_known") == ("waiting_external", "poll_existing"))
    accept("idempotent stage may resume after lease loss", after_lease_loss("read") == ("retry_wait", "idempotent_resume"))

    dependencies = {
        "normalize": {"fetch"}, "parse": {"normalize"}, "chunk": {"parse"},
        "text_index": {"chunk"}, "ocr": {"asset"}, "image_index": {"ocr", "chunk"},
        "summary": {"text_index"}, "graph": {"text_index"}, "wiki": {"summary", "graph"},
    }
    accept("summary repair does not repeat fetch or text index", invalidation_closure({"summary"}, dependencies) == {"summary", "wiki"})
    accept("single OCR repair preserves text work", invalidation_closure({"ocr"}, dependencies) == {"ocr", "image_index"})
    accept("shared model changes invalidate actual dependents", invalidation_closure({"summary", "graph"}, dependencies) == {"summary", "graph", "wiki"})
    accept("cleanup repair does not reparse", invalidation_closure({"cleanup"}, dependencies) == {"cleanup"})
    accept("chunk changes invalidate both text and image indexes", {"text_index", "image_index"} <= invalidation_closure({"chunk"}, dependencies))

    accept("publication alone cannot resolve a summary incident", not successor_resolves(failed_stage="summary", completed_equivalent=set(), published=True))
    accept("equivalent successful successor resolves its stage", successor_resolves(failed_stage="summary", completed_equivalent={"summary"}, published=True))
    accept("new publication cannot resolve old cleanup", not successor_resolves(failed_stage="cleanup", completed_equivalent={"cleanup"}, published=True))
    index_job = Job(published=True, active_index_manifest="attempt-2-confirmed")
    accept("same-generation stale index is not searchable", searchable(index_job, "attempt-2-confirmed") and not searchable(index_job, "attempt-1-unconfirmed"))

    # ponytail: SQLite models transactions; PostgreSQL concurrent claims remain a release gate.
    # A small durable-state analog: independent current/published uniqueness,
    # transaction rollback, idempotent operator request, and run membership.
    db = sqlite3.connect(":memory:")
    db.executescript("""
      CREATE TABLE jobs (id TEXT PRIMARY KEY, source TEXT, generation INTEGER,
                         current INTEGER, published INTEGER);
      CREATE UNIQUE INDEX current_job ON jobs(source) WHERE current=1;
      CREATE UNIQUE INDEX published_job ON jobs(source) WHERE published=1;
      CREATE TABLE steps (id TEXT PRIMARY KEY, state TEXT);
      CREATE TABLE events (id INTEGER PRIMARY KEY, step_id TEXT, kind TEXT,
                           tenant TEXT, action TEXT, request_id TEXT, result TEXT);
      CREATE UNIQUE INDEX operation_request ON events(tenant,action,request_id)
        WHERE request_id IS NOT NULL;
      CREATE TABLE outbox (operation_key TEXT PRIMARY KEY, step_id TEXT);
      CREATE TABLE run_items (run TEXT, item TEXT, kind TEXT, job TEXT,
                              PRIMARY KEY(run,item));
      CREATE TABLE run_finished (run TEXT PRIMARY KEY, failed INTEGER);
    """)
    with db:
        db.execute("INSERT INTO jobs VALUES ('v1','source',1,0,1)")
        db.execute("INSERT INTO jobs VALUES ('v2','source',2,1,0)")
    accept("current differs from still-available published", db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2)
    for name, current, published in [("current", 1, 0), ("published", 0, 1)]:
        rejected = False
        try:
            with db:
                db.execute("INSERT INTO jobs VALUES ('duplicate','source',3,?,?)", (current, published))
        except sqlite3.IntegrityError:
            rejected = True
        accept("unique " + name + " rejects competing head", rejected)

    try:
        with db:
            db.execute("INSERT INTO steps VALUES ('s1','enqueue_pending')")
            db.execute("INSERT INTO events(step_id,kind) VALUES ('s1','planned')")
            raise RuntimeError("crash before outbox")
    except RuntimeError:
        pass
    accept("state and event roll back when outbox transaction fails", db.execute("SELECT COUNT(*) FROM steps").fetchone()[0] == 0 and db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0)
    with db:
        db.execute("INSERT INTO steps VALUES ('s1','enqueue_pending')")
        db.execute("INSERT INTO events(step_id,kind) VALUES ('s1','planned')")
        db.execute("INSERT INTO outbox VALUES ('s1:1:1','s1')")
    accept("committed plan survives missing queue delivery", db.execute("SELECT state FROM steps WHERE id='s1'").fetchone()[0] == "enqueue_pending" and db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1)
    with db:
        db.execute("INSERT OR IGNORE INTO outbox VALUES ('s1:1:1','s1')")
        db.execute("INSERT INTO events(tenant,action,request_id,result) VALUES ('t','rebuild','request-1','v2')")
        db.execute("INSERT OR IGNORE INTO events(tenant,action,request_id,result) VALUES ('t','rebuild','request-1','v3')")
        db.execute("INSERT INTO run_items VALUES ('r1','file','file','v2')")
        db.execute("INSERT OR IGNORE INTO run_items VALUES ('r1','file','file','v2')")
        db.execute("INSERT INTO run_items VALUES ('r2','file','file','v2')")
        db.execute("INSERT INTO run_items VALUES ('r1','folder','folder',NULL)")
        db.execute("INSERT INTO run_finished VALUES ('r1',1)")
    accept("repeated outbox intent is one operation", db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1)
    accept("duplicate manual rebuild returns original operation", db.execute("SELECT result FROM events WHERE request_id='request-1'").fetchone()[0] == "v2")
    accept("two scans can share one job without double file count", db.execute("SELECT COUNT(*) FROM run_items WHERE kind='file'").fetchone()[0] == 2)
    accept("directory is separate from document count", db.execute("SELECT COUNT(*) FROM run_items WHERE run='r1' AND kind='file'").fetchone()[0] == 1)
    with db:
        db.execute("INSERT INTO events(step_id,kind) VALUES ('s1','recovered_later')")
    accept("late recovery does not erase run-end result", db.execute("SELECT failed FROM run_finished WHERE run='r1'").fetchone()[0] == 1)
    db.close()

    for name in passed:
        print("PASS", name)
    print(f"DESIGN_MODEL: {len(passed)} checks passed; production implementation not exercised")


if __name__ == "__main__":
    check()
