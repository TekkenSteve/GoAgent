DROP TRIGGER IF EXISTS plan_node_identity_immutable ON plan_nodes;
DROP FUNCTION IF EXISTS protect_plan_node_identity();

DROP TRIGGER IF EXISTS plan_identity_immutable ON plans;
DROP FUNCTION IF EXISTS protect_plan_identity();
