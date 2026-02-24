# Repo Rules

- Spec-first: no code without a spec/task.
- Do ONLY what the current phase asks. No scope creep.
- Raw data is immutable. Write only to data/raw/ during ingest.
- All scripts run from repo root: `python -m src.<module>`
- Fail loudly on errors. Print row counts. Exit non-zero on failure.
- No extra features, refactors, or "nice to haves" beyond the task.
