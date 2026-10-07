# Online migrations

Statements that PostgreSQL cannot run inside a transaction live here:
`CREATE INDEX CONCURRENTLY` above all. They are applied by hand, not by
`golang-migrate` — see [the runbook](../../docs/runbooks/online-index-migration.md).

Nothing in this directory is part of the migration chain. A file here is a
record of a statement that was applied to deployments, kept so the next person
can see that the index exists, why it is not in `migrations/`, and how to
re-apply it safely.

`make check-migration-safety` fails if a `CONCURRENTLY` statement appears in
`migrations/`, which is how this channel stays the only way in.
