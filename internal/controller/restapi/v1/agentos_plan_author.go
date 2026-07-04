package v1

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/gofiber/fiber/v2"
)

// @Summary     AgentOS plan author
// @Description Render a RunPlanSpec authoring surface backed by the public AgentOS schema.
// @ID          agentos-plan-author
// @Tags        agentos
// @Produce     html
// @Success     200 {string} string "HTML authoring surface"
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/author [get]
func (r *V1) agentOSPlanAuthor(ctx *fiber.Ctx) error {
	view := agentOSPlanAuthorView{
		SchemaEndpoint: "/v1/agentos/plans/schemas/run-plan",
		StartEndpoint:  "/v1/agentos/plans",
		StartEnabled:   r.planRuntime != nil,
		BackendKinds: []agentos.BackendKind{
			agentos.BackendKindNative,
			agentos.BackendKindTemporalExternal,
			agentos.BackendKindHTTP,
			agentos.BackendKindGRPC,
		},
		EdgeTriggers: []agentos.EdgeTrigger{
			agentos.EdgeOnSuccess,
			agentos.EdgeOnError,
			agentos.EdgeOnComplete,
			agentos.EdgeOnAlways,
		},
		JoinStrategies: []agentos.PlanJoinStrategy{
			agentos.PlanJoinAll,
			agentos.PlanJoinAny,
			agentos.PlanJoinFirst,
		},
		DefaultBackendName: agentos.BackendNameGoAgentNative,
		DefaultCapability:  agentos.CapabilityRun,
	}

	var body bytes.Buffer

	tmpl, err := newAgentOSPlanAuthorTemplate()
	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, fmt.Sprintf("parse plan author template: %v", err))
	}

	if err := tmpl.Execute(&body, view); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, fmt.Sprintf("render plan author: %v", err))
	}

	ctx.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)

	return ctx.Status(http.StatusOK).Send(body.Bytes())
}

func newAgentOSPlanAuthorTemplate() (*template.Template, error) {
	return template.New("agentos_plan_author").Parse(agentOSPlanAuthorHTML)
}

type agentOSPlanAuthorView struct {
	SchemaEndpoint     string
	StartEndpoint      string
	StartEnabled       bool
	BackendKinds       []agentos.BackendKind
	EdgeTriggers       []agentos.EdgeTrigger
	JoinStrategies     []agentos.PlanJoinStrategy
	DefaultBackendName string
	DefaultCapability  string
}

const agentOSPlanAuthorHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AgentOS Plan Author</title>
<style>
:root {
  color-scheme: light;
  --bg: #f6f8fb;
  --surface: #ffffff;
  --ink: #172033;
  --muted: #637083;
  --line: #d8dee8;
  --active: #2459d6;
  --success: #1f7a4d;
  --danger: #b42318;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
  line-height: 1.45;
}
button, input, select, textarea {
  font: inherit;
}
a {
  color: var(--active);
  text-decoration: none;
}
a:hover { text-decoration: underline; }
.shell {
  width: min(1440px, 100%);
  margin: 0 auto;
  padding: 20px;
}
.topbar {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  padding: 12px 0 18px;
  border-bottom: 1px solid var(--line);
}
.eyebrow {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}
h1, h2 {
  margin: 0;
  letter-spacing: 0;
}
h1 {
  font-size: clamp(22px, 3vw, 30px);
  line-height: 1.15;
}
h2 {
  font-size: 16px;
}
.toolbar {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  align-items: center;
  gap: 8px;
}
.section {
  padding: 18px 0;
  border-bottom: 1px solid var(--line);
}
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}
.grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.field {
  display: grid;
  gap: 4px;
  min-width: 0;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}
.field input,
.field select,
.field textarea {
  width: 100%;
  min-height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 6px 8px;
  background: var(--surface);
  color: var(--ink);
}
.field textarea {
  min-height: 84px;
  resize: vertical;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
.field.full {
  grid-column: 1 / -1;
}
.row-list {
  display: grid;
  gap: 10px;
}
.row {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr)) auto;
  gap: 8px;
  padding: 10px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
}
.row .wide {
  grid-column: span 2;
}
.row .full {
  grid-column: 1 / -2;
}
.button {
  min-height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 6px 10px;
  background: var(--surface);
  color: var(--ink);
  cursor: pointer;
  white-space: nowrap;
}
.button:hover { border-color: #aab4c3; }
.button:disabled { cursor: not-allowed; opacity: .58; }
.button.primary { border-color: #b8c8f4; color: var(--active); }
.button.danger { border-color: #f0b8b3; color: var(--danger); }
.output {
  width: 100%;
  min-height: 460px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 10px;
  background: var(--surface);
  color: #263244;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  line-height: 1.5;
  resize: vertical;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
.status {
  min-height: 20px;
  color: var(--muted);
  overflow-wrap: anywhere;
}
.status.success { color: var(--success); }
.status.danger { color: var(--danger); }
.remove {
  align-self: end;
}
@media (max-width: 1100px) {
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .row { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .row .wide, .row .full { grid-column: 1 / -1; }
  .remove { grid-column: 1 / -1; justify-self: start; }
}
@media (max-width: 760px) {
  .topbar { display: grid; }
  .toolbar { justify-content: flex-start; }
  .grid { grid-template-columns: 1fr; }
  .shell { padding: 14px; }
}
</style>
</head>
<body
  data-schema-endpoint="{{ .SchemaEndpoint }}"
  data-start-endpoint="{{ .StartEndpoint }}"
  data-start-enabled="{{ .StartEnabled }}"
  data-default-backend-name="{{ .DefaultBackendName }}"
  data-default-capability="{{ .DefaultCapability }}">
<main class="shell">
  <header class="topbar">
    <div>
      <div class="eyebrow">AgentOS Plan Author</div>
      <h1>RunPlanSpec</h1>
    </div>
    <div class="toolbar">
      <a class="mono" href="{{ .SchemaEndpoint }}">schema json</a>
      <button id="generate-plan" class="button" type="button">Generate</button>
      <button id="start-plan" class="button primary" type="button" {{ if not .StartEnabled }}disabled{{ end }}>Start Plan</button>
      <div id="author-status" class="status" role="status" aria-live="polite"></div>
    </div>
  </header>

  <section class="section">
    <div class="section-head"><h2>Plan</h2></div>
    <div class="grid">
      <label class="field">Plan ID<input id="plan-id" autocomplete="off"></label>
      <label class="field">Thread ID<input id="thread-id" autocomplete="off"></label>
      <label class="field">Account ID<input id="account-id" autocomplete="off"></label>
      <label class="field">Project ID<input id="project-id" autocomplete="off"></label>
      <label class="field">Idempotency Key<input id="idempotency-key" autocomplete="off"></label>
      <label class="field">Max Nodes<input id="max-nodes" inputmode="numeric" autocomplete="off"></label>
      <label class="field">Max Parallel<input id="max-parallel-nodes" inputmode="numeric" autocomplete="off"></label>
      <label class="field">Budget Cents<input id="budget-cents" inputmode="numeric" autocomplete="off"></label>
      <label class="field full">Inputs JSON<textarea id="plan-inputs">{}</textarea></label>
      <label class="field full">Metadata JSON<textarea id="plan-metadata">{}</textarea></label>
    </div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Nodes</h2>
      <button id="add-node" class="button" type="button">Add Node</button>
    </div>
    <div id="node-list" class="row-list">
      <div class="row" data-node-row>
        <label class="field">Node ID<input data-field="node_id" autocomplete="off" value="node-1"></label>
        <label class="field">Capability<input data-field="capability" autocomplete="off" value="{{ .DefaultCapability }}"></label>
        <label class="field">Run ID<input data-field="run_id" autocomplete="off" value="run-1"></label>
        <label class="field">Backend Kind<select data-field="backend_kind">{{ range .BackendKinds }}<option value="{{ . }}">{{ . }}</option>{{ end }}</select></label>
        <label class="field">Backend Name<input data-field="backend_name" autocomplete="off" value="{{ .DefaultBackendName }}"></label>
        <label class="field">Join<select data-field="join"><option value=""></option>{{ range .JoinStrategies }}<option value="{{ . }}">{{ . }}</option>{{ end }}</select></label>
        <label class="field wide">Agent ID<input data-field="agent_id" autocomplete="off"></label>
        <label class="field wide">Model Ref<input data-field="model_ref" autocomplete="off"></label>
        <label class="field wide">User Message<input data-field="user_message" autocomplete="off"></label>
        <label class="field wide">Timeout Seconds<input data-field="timeout_seconds" inputmode="numeric" autocomplete="off"></label>
        <label class="field full">Run Input JSON<textarea data-field="run_input">{}</textarea></label>
        <label class="field full">Input Mappings JSON<textarea data-field="inputs">[]</textarea></label>
        <label class="field full">Outputs JSON<textarea data-field="outputs">[]</textarea></label>
        <label class="field full">Conditions JSON<textarea data-field="conditions">[]</textarea></label>
        <button class="button danger remove" type="button" data-remove-row>Remove</button>
      </div>
    </div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Edges</h2>
      <button id="add-edge" class="button" type="button">Add Edge</button>
    </div>
    <div id="edge-list" class="row-list">
      <div class="row" data-edge-row>
        <label class="field">Edge ID<input data-field="edge_id" autocomplete="off"></label>
        <label class="field">From<input data-field="from" autocomplete="off"></label>
        <label class="field">To<input data-field="to" autocomplete="off"></label>
        <label class="field">On<select data-field="on"><option value=""></option>{{ range .EdgeTriggers }}<option value="{{ . }}">{{ . }}</option>{{ end }}</select></label>
        <label class="field wide">Condition<input data-field="condition" autocomplete="off"></label>
        <label class="field full">Input Mapping JSON<textarea data-field="input_mapping">[]</textarea></label>
        <button class="button danger remove" type="button" data-remove-row>Remove</button>
      </div>
    </div>
  </section>

  <section class="section">
    <div class="section-head"><h2>Generated JSON</h2></div>
    <textarea id="plan-output" class="output" spellcheck="false"></textarea>
  </section>
</main>
<script>
(() => {
  const root = document.body.dataset;
  const status = document.getElementById("author-status");
  const output = document.getElementById("plan-output");
  const nodeList = document.getElementById("node-list");
  const edgeList = document.getElementById("edge-list");

  function setStatus(message, className) {
    status.className = className ? "status " + className : "status";
    status.textContent = message;
  }

  function value(id) {
    return document.getElementById(id).value.trim();
  }

  function rowValue(row, field) {
    const input = row.querySelector('[data-field="' + field + '"]');
    return input ? input.value.trim() : "";
  }

  function parseJSON(label, raw, fallback) {
    if (!raw.trim()) {
      return fallback;
    }
    try {
      return JSON.parse(raw);
    } catch (error) {
      throw new Error(label + ": " + error.message);
    }
  }

  function optionalObject(label, raw) {
    const parsed = parseJSON(label, raw, {});
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return Object.keys(parsed).length ? parsed : undefined;
    }
    throw new Error(label + " must be a JSON object");
  }

  function optionalArray(label, raw) {
    const parsed = parseJSON(label, raw, []);
    if (Array.isArray(parsed)) {
      return parsed.length ? parsed : undefined;
    }
    throw new Error(label + " must be a JSON array");
  }

  function optionalInt(raw) {
    if (!raw) {
      return undefined;
    }
    const parsed = Number(raw);
    if (!Number.isInteger(parsed)) {
      throw new Error(raw + " is not an integer");
    }
    return parsed;
  }

  function assignOptional(target, key, value) {
    if (value !== undefined && value !== "") {
      target[key] = value;
    }
  }

  function buildPolicy() {
    const policy = {};
    assignOptional(policy, "max_nodes", optionalInt(value("max-nodes")));
    assignOptional(policy, "max_parallel_nodes", optionalInt(value("max-parallel-nodes")));
    assignOptional(policy, "budget_cents", optionalInt(value("budget-cents")));
    return Object.keys(policy).length ? policy : undefined;
  }

  function buildNode(row, index) {
    const runInput = optionalObject("node " + (index + 1) + " run input", rowValue(row, "run_input"));
    const inputs = optionalArray("node " + (index + 1) + " input mappings", rowValue(row, "inputs"));
    const outputs = optionalArray("node " + (index + 1) + " outputs", rowValue(row, "outputs"));
    const conditions = optionalArray("node " + (index + 1) + " conditions", rowValue(row, "conditions"));
    const nodePolicy = {};
    assignOptional(nodePolicy, "join", rowValue(row, "join"));
    assignOptional(nodePolicy, "timeout_seconds", optionalInt(rowValue(row, "timeout_seconds")));

    const run = {
      run_id: rowValue(row, "run_id"),
      account_id: value("account-id"),
      project_id: value("project-id"),
      backend: {
        kind: rowValue(row, "backend_kind"),
        name: rowValue(row, "backend_name"),
      },
    };
    assignOptional(run, "thread_id", value("thread-id"));
    assignOptional(run, "agent_id", rowValue(row, "agent_id"));
    assignOptional(run, "model_ref", rowValue(row, "model_ref"));
    assignOptional(run, "user_message", rowValue(row, "user_message"));
    assignOptional(run, "input", runInput);

    const node = {
      node_id: rowValue(row, "node_id"),
      run,
    };
    assignOptional(node, "capability", rowValue(row, "capability"));
    assignOptional(node, "inputs", inputs);
    assignOptional(node, "outputs", outputs);
    assignOptional(node, "conditions", conditions);
    if (Object.keys(nodePolicy).length) {
      node.policy = nodePolicy;
    }
    return node;
  }

  function buildEdge(row, index) {
    const mapping = optionalArray("edge " + (index + 1) + " input mapping", rowValue(row, "input_mapping"));
    const edge = {
      from: rowValue(row, "from"),
      to: rowValue(row, "to"),
    };
    assignOptional(edge, "edge_id", rowValue(row, "edge_id"));
    assignOptional(edge, "on", rowValue(row, "on"));
    assignOptional(edge, "condition", rowValue(row, "condition"));
    assignOptional(edge, "input_mapping", mapping);
    return edge;
  }

  function buildSpec() {
    const inputs = optionalObject("plan inputs", value("plan-inputs"));
    const metadata = optionalObject("plan metadata", value("plan-metadata"));
    const policy = buildPolicy();
    const nodes = Array.from(nodeList.querySelectorAll("[data-node-row]")).map(buildNode);
    const edges = Array.from(edgeList.querySelectorAll("[data-edge-row]"))
      .map(buildEdge)
      .filter((edge) => edge.from || edge.to || edge.edge_id || edge.condition || edge.input_mapping);

    const spec = {
      plan_id: value("plan-id"),
      thread_id: value("thread-id"),
      account_id: value("account-id"),
      project_id: value("project-id"),
      nodes,
    };
    assignOptional(spec, "idempotency_key", value("idempotency-key"));
    assignOptional(spec, "inputs", inputs);
    assignOptional(spec, "metadata", metadata);
    assignOptional(spec, "edges", edges.length ? edges : undefined);
    assignOptional(spec, "policy", policy);
    return spec;
  }

  function generate() {
    const spec = buildSpec();
    output.value = JSON.stringify(spec, null, 2);
    setStatus("generated", "success");
    return spec;
  }

  function cloneRow(selector, container) {
    const row = container.querySelector(selector);
    const clone = row.cloneNode(true);
    clone.querySelectorAll("input, textarea").forEach((input) => {
      if (input.dataset.field === "backend_name") {
        input.value = root.defaultBackendName;
      } else if (input.dataset.field === "capability") {
        input.value = root.defaultCapability;
      } else if (input.dataset.field === "run_input") {
        input.value = "{}";
      } else if (input.tagName === "TEXTAREA") {
        input.value = "[]";
      } else {
        input.value = "";
      }
    });
    clone.querySelectorAll("select").forEach((select) => {
      select.selectedIndex = 0;
    });
    container.appendChild(clone);
  }

  document.getElementById("add-node").addEventListener("click", () => {
    cloneRow("[data-node-row]", nodeList);
  });

  document.getElementById("add-edge").addEventListener("click", () => {
    cloneRow("[data-edge-row]", edgeList);
  });

  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-remove-row]");
    if (!button) {
      return;
    }
    const row = button.closest(".row");
    const container = row.parentElement;
    if (container.querySelectorAll(".row").length > 1) {
      row.remove();
    }
  });

  document.getElementById("generate-plan").addEventListener("click", () => {
    try {
      generate();
    } catch (error) {
      setStatus(error.message, "danger");
    }
  });

  document.getElementById("start-plan").addEventListener("click", async (event) => {
    if (root.startEnabled !== "true") {
      return;
    }
    const button = event.currentTarget;
    button.disabled = true;
    try {
      const spec = generate();
      const response = await fetch(root.startEndpoint, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: output.value,
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const statusBody = await response.json();
      const planID = encodeURIComponent(statusBody.plan_id || spec.plan_id);
      const accountID = encodeURIComponent(spec.account_id);
      const projectID = encodeURIComponent(spec.project_id);
      setStatus("accepted: " + root.startEndpoint + "/" + planID + "/console?account_id=" + accountID + "&project_id=" + projectID, "success");
    } catch (error) {
      setStatus(error.message, "danger");
    } finally {
      button.disabled = false;
    }
  });

  generate();
})();
</script>
</body>
</html>
`
