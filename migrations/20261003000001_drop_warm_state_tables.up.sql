-- Drop the warm/cold state tables: the log-first ruling removed the layer they
-- served. Message prose rides the run timeline facts (TEXT_MESSAGE_END carries
-- the accumulated content); conversation continuation uses claim-check blob
-- snapshots. Nothing read or wrote these tables at deletion time.
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS tool_results;
DROP TABLE IF EXISTS archives;
