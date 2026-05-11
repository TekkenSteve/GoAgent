ALTER TABLE trigger_event_logs
  ADD COLUMN event_data TEXT NOT NULL DEFAULT '',
  ADD COLUMN agent_prompt TEXT NOT NULL DEFAULT '',
  ADD COLUMN exec_variables JSONB NOT NULL DEFAULT '{}';
