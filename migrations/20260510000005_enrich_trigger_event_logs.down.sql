ALTER TABLE trigger_event_logs
  DROP COLUMN IF EXISTS event_data,
  DROP COLUMN IF EXISTS agent_prompt,
  DROP COLUMN IF EXISTS exec_variables;
