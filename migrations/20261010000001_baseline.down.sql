-- Rolling the baseline back removes everything it created; there is no
-- earlier schema to return to. Tables drop first and CASCADE, which
-- takes their triggers and indexes with them and frees the functions;
-- golang-migrate's schema_migrations book table is untouched - it
-- belongs to the migrator, not the schema. A database that needs its
-- data back restores from a backup instead of migrating down.
DROP TABLE IF EXISTS public.agent_versions CASCADE;
DROP TABLE IF EXISTS public.agentos_artifact_schemas CASCADE;
DROP TABLE IF EXISTS public.agentos_capabilities CASCADE;
DROP TABLE IF EXISTS public.agentos_conversation_deferred_projections CASCADE;
DROP TABLE IF EXISTS public.agentos_conversation_event_outbox CASCADE;
DROP TABLE IF EXISTS public.agentos_conversation_events CASCADE;
DROP TABLE IF EXISTS public.agentos_conversation_runs CASCADE;
DROP TABLE IF EXISTS public.agentos_messages CASCADE;
DROP TABLE IF EXISTS public.agentos_run_event_outbox CASCADE;
DROP TABLE IF EXISTS public.agentos_run_events CASCADE;
DROP TABLE IF EXISTS public.agentos_threads CASCADE;
DROP TABLE IF EXISTS public.agentos_tool_idempotency CASCADE;
DROP TABLE IF EXISTS public.agents CASCADE;
DROP TABLE IF EXISTS public.artifacts CASCADE;
DROP TABLE IF EXISTS public.audit_logs CASCADE;
DROP TABLE IF EXISTS public.credit_accounts CASCADE;
DROP TABLE IF EXISTS public.credit_ledger CASCADE;
DROP TABLE IF EXISTS public.governed_action_status_updates CASCADE;
DROP TABLE IF EXISTS public.governed_actions CASCADE;
DROP TABLE IF EXISTS public.ledger_entries CASCADE;
DROP TABLE IF EXISTS public.plan_commands CASCADE;
DROP TABLE IF EXISTS public.plan_events CASCADE;
DROP TABLE IF EXISTS public.plan_metric_checkpoints CASCADE;
DROP TABLE IF EXISTS public.plan_metric_samples CASCADE;
DROP TABLE IF EXISTS public.plan_nodes CASCADE;
DROP TABLE IF EXISTS public.plans CASCADE;
DROP TABLE IF EXISTS public.process_events CASCADE;
DROP TABLE IF EXISTS public.process_status_updates CASCADE;
DROP TABLE IF EXISTS public.processes CASCADE;
DROP TABLE IF EXISTS public.run_backend_index CASCADE;
DROP TABLE IF EXISTS public.usage_records CASCADE;
DROP TABLE IF EXISTS public.workflow_templates CASCADE;
DROP TABLE IF EXISTS public.workset_chunk_results CASCADE;
DROP TABLE IF EXISTS public.workset_status_updates CASCADE;
DROP TABLE IF EXISTS public.worksets CASCADE;

DROP FUNCTION IF EXISTS public.enforce_delivered_plan_command_audit();
DROP FUNCTION IF EXISTS public.enforce_plan_command_status_lifecycle();
DROP FUNCTION IF EXISTS public.protect_agentos_run_events_append_only();
DROP FUNCTION IF EXISTS public.protect_artifacts_append_only();
DROP FUNCTION IF EXISTS public.protect_audit_logs_append_only();
DROP FUNCTION IF EXISTS public.protect_plan_command_identity();
DROP FUNCTION IF EXISTS public.protect_plan_events_append_only();
DROP FUNCTION IF EXISTS public.protect_plan_identity();
DROP FUNCTION IF EXISTS public.protect_plan_metric_samples_append_only();
DROP FUNCTION IF EXISTS public.protect_plan_node_identity();
DROP FUNCTION IF EXISTS public.protect_run_backend_ownership();
