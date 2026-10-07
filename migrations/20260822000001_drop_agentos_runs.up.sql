-- agentos_runs is superseded by run_backend_index (20260617000001), which is
-- what run routing and the outbox tenant joins read; no query in the codebase
-- references this table anymore, so it is dropped rather than left as an
-- unmaintained twin of the live registry.
DROP TABLE IF EXISTS agentos_runs;
