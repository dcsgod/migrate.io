# Migrate.io  (Universal Data Migration Platform) — Project Plan

## 1. Vision

A platform where a user connects **any source** and **any destination** —
object storage (S3, ADLS, GCS, MinIO), warehouses/lakehouses (Databricks,
Snowflake, BigQuery, Redshift), RDBMS (Teradata, Postgres, MySQL, Oracle),
ERP systems (SAP ECC/S4HANA), or generic JDBC — regardless of storage
*type* (files, tables, blobs). The system builds a **schema relationship
graph**, and the user issues a **natural language command** that can
include **on-the-fly transformation logic across multiple tables** (joins,
filters, dedup, SCD merges, type casts, masking). The system compiles this
into a **Spark DAG**, runs it **staged**, shows the resulting **DataFrame**
for approval, and only then commits to the destination.

Core pipeline:
`Connect (any source/dest) → Build Graph → NL Intent (incl. transforms) →
Logical DAG → Validate → Compiled Spark Plan → Staged Run → Preview
DataFrame → Approve → Production Commit`

Every step above is **user-visible in the UI** — not a black box.

---

## 2. Connector Generalization (not S3-specific)

Connectors are grouped by **capability class**, not by vendor, so the
platform is genuinely storage-agnostic:

| Class | Examples | Read shape |
|---|---|---|
| Object storage | S3, ADLS Gen2, GCS, MinIO | files (parquet/csv/json/avro) |
| Warehouse/Lakehouse | Databricks, Snowflake, BigQuery, Redshift | tables |
| RDBMS / JDBC | Teradata, Postgres, MySQL, Oracle, SQL Server | tables via JDBC |
| ERP | SAP ECC, SAP S4HANA (via OData/RFC/BW extractors) | business objects/tables |
| Streaming (future) | Kafka, Kinesis, Event Hubs | topics/streams |

Every connector implements the same interface (`connect`, `list_objects`,
`read_schema`, `read` as Spark DataFrame, `write` as Spark DataFrame,
`capabilities`). Spark already abstracts most read/write mechanics (JDBC,
cloud file readers, connectors for SAP/Snowflake exist as Spark packages)
— your connector layer mainly owns **auth, discovery, and capability
declaration**, not reinventing I/O.

This is what makes "any cloud, any storage type" actually true instead of
aspirational — a new source is a new adapter class implementing one
interface, nothing else changes.

---

## 3. On-the-Fly Transformation (first-class, not an afterthought)

Transformation is not a post-copy ETL step — it's **DAG nodes that
execute inline** during the same Spark job that reads and writes:

- `read(node)` → `filter` → `join(node_a, node_b, keys)` → `transform`
  (dedupe, pivot, SCD merge, type cast, PII mask, custom expr) → `write`
- Multi-table joins are graph-aware: the DAG builder resolves join keys
  from the schema graph, not from LLM guesswork.
- Transform ops are pluggable (`transform/` package below) so adding a new
  transform type (e.g. fuzzy-match dedup, window functions) doesn't touch
  the DAG engine.
- Because it all compiles to one Spark job, "on-the-fly" is literal —
  there's no intermediate landing zone until the staging step (§5).

---

## 4. Graph Store: in-memory by default, Neo4j for reuse

- **Default**: graph is built fresh (or from cache) in-memory via
  `networkx` for the session — fast, zero infra dependency.
- **Persist on demand**: user clicks "Save graph" → exports the graph
  (nodes, edges, confidence scores, inferred vs explicit) to Neo4j, keyed
  by a hash of (source connection, destination connection).
- **Reuse**: next time the same source/destination pair is opened, the
  platform checks Neo4j first; if found, loads it and only re-crawls for
  schema drift (diff against the stored graph) instead of rebuilding from
  scratch — a big latency/cost win on repeat migrations.
- Multi-tenant: Neo4j graphs are namespaced per tenant/connection-pair so
  one client's schema graph is never visible to another.

---

## 5. Staged → Production, all inside Spark/Delta (no separate infra)

"Staged" does **not** mean a separate system — it means:

1. Compiled Spark job runs and writes to a **staging Delta path/table**
   (`_staging` suffix or schema) — this is where the on-the-fly
   transforms actually execute.
2. Preview layer reads a `.limit(N)` / sampled view **from the staging
   output** — user sees a real materialized DataFrame, not a plan.
3. On approval: **atomic swap or `MERGE`** from staging into the real
   destination table. Delta's transaction log makes this safe and gives
   free rollback (time travel) if something's wrong post-commit.
4. On rejection: staging is discarded, nothing touches production.

This keeps "staged then production" simple — one engine (Spark), one
storage format (Delta), no extra moving parts.

---

## 6. Full Step Visibility in UI (this is a differentiator, treat it as core)

Every run is a pipeline of inspectable steps, each with its own UI panel:

1. **Connections** — source/destination status, credentials, capabilities
2. **Schema Graph** — visual graph (nodes = tables, edges = relationships,
   color-coded explicit vs inferred, confidence score on hover)
3. **NL Command → Intent JSON** — show the raw LLM output before grounding
4. **Grounded Intent** — intent mapped onto actual graph nodes/edges, with
   any low-confidence joins flagged for user confirmation
5. **Logical DAG** — engine-agnostic operation graph, editable node-by-node
6. **Validation results** — type mismatches, cycles, missing columns,
   flagged low-confidence edges
7. **Compiled Spark Plan** — actual generated PySpark/SQL code, viewable
   and copyable (transparency for power users / auditors)
8. **Staged Run Log** — execution timeline, row counts in/out per node,
   errors if any
9. **Preview DataFrame** — sampled result grid + schema diff vs current
   destination schema
10. **Approve/Reject** — explicit gate
11. **Production Commit Log** — what was written, when, by whom, lineage
    link back to the exact DAG version used

This turns the pipeline into an auditable, replayable object — not just a
one-shot script.

---

## 7. SOTA Feature Additions

- **Incremental / CDC support** — watermark-based reads so repeat runs
  only move new/changed data, not full reloads
- **Schema drift detection** — diff live source schema against the stored
  graph on each run; alert rather than silently break
- **Cost/row estimation before execution** — use source stats (row counts,
  partition sizes) to estimate job cost/duration before running staged
- **Dry-run / simulation mode** — validate + estimate without materializing
  any data, for quick iteration on the NL command
- **Plan versioning & rollback** — every approved plan is a versioned
  artifact (git-like); re-running a past version is one click; Delta time
  travel gives destination-side rollback
- **Explainability** — LLM surfaces *why* it chose a join/mapping
  ("matched cust_id -> customer_id on 94% value overlap"), not just the
  result — critical for enterprise trust Xai is Must
- **Data quality gates** — null%, duplicate, and referential-integrity
  checks as DAG validation nodes, configurable per migration
- **Connector plugin SDK** — third parties/clients can add a connector by
  implementing the base interface, without touching core engine code
- **Multi-tenant isolation** — per-tenant credential vault, RBAC, and
  namespaced graph storage
- **Cost-based DAG optimization** — predicate pushdown, join reordering
  before Spark compilation (mirrors Catalyst but at your logical-DAG level
  so you can reorder before code is even generated)
- **Streaming mode** — same DAG abstraction compiled to
  Structured Streaming for near-real-time migrations instead of batch

---

## 8. Build Approach (phased, still MVP-first)

### Phase 0 — Skeleton & contracts 
Interfaces : `Connector`, `SchemaIntrospector`, `GraphNode/Edge`,
`DAGNode`, `TransformOp`, `Plan`. One mock connector per capability class
wired through the full pipeline end-to-end with fake data.

### Phase 1 — MVP: one real pair, full step visibility 
Real object-storage connector (S3) + real warehouse connector
(Databricks). Real graph build (explicit edges only). NL -> intent -> DAG
-> compiled Spark -> staged run -> preview -> commit, with every UI step
from §6 working, even if primitive. This proves the whole architecture,
not just data movement.

### Phase 2 — Multi-table transforms + inference 
Add inferred-edge detection, on-the-fly join/transform DAG nodes, DAG
patch/iteration ("exclude cancelled orders"), Neo4j graph persistence +
reuse.

### Phase 3 — Second capability class + SOTA layer 
Add RDBMS (Teradata) or ERP (SAP) connector to prove generalization.
Add explainability, data quality gates, cost estimation, dry-run mode.

### Phase 4 — Enterprise hardening 
Multi-tenant vault, RBAC, plugin SDK, CDC/incremental, plan versioning,
streaming.

---

## 9. Suggested Stack

| Layer | Choice | Why |
|---|---|---|
| Compute engine | Spark (Databricks) | Deep existing operational experience |
| Canonical storage | Delta Lake | ACID writes, MERGE for safe staged commits, time travel for rollback |
| Graph store | networkx (in-memory) + Neo4j (persisted, on-demand) | Fast by default, reusable on demand |
| Governance/lineage | Unity Catalog | Free lineage + audit if using Databricks |
| LLM layer (paid) | Claude via Databricks Model Serving | 
| LLM layer (Free) | Any Groq model free or using ollama | Switchable using ENV |
| Orchestration | Databricks Workflows / Airflow | Retries, scheduling, incremental triggers |
| Backend API | FastAPI | Async, plays well with LLM + Spark job calls |
| Frontend | React (graph viewer, DAG viewer, code viewer, preview grid) | Needed for full step-visibility UX |

---

## 10. Directory Skeleton

```
data-migrator/
├── project.md
├── README.md
├── pyproject.toml
│
├── connectors/
│   ├── base/
│   │   ├── connector.py            # abstract Connector class
│   │   ├── introspector.py         # abstract SchemaIntrospector
│   │   └── capabilities.py         # CDC, bulk, streaming support flags
│   ├── object_storage/
│   │   ├── s3/
│   │   ├── adls/
│   │   ├── gcs/
│   │   └── minio/
│   ├── warehouse/
│   │   ├── databricks/
│   │   ├── snowflake/
│   │   ├── bigquery/
│   │   └── redshift/
│   ├── rdbms/
│   │   ├── teradata/
│   │   ├── postgres/
│   │   ├── mysql/
│   │   ├── oracle/
│   │   └── generic_jdbc/
│   ├── erp/
│   │   ├── sap_ecc/
│   │   └── sap_s4hana/
│   └── streaming/                  # future: kafka, kinesis, event_hubs
│
├── graph/
│   ├── builder.py                  # crawls source+dest, builds nodes/edges
│   ├── inference.py                # name similarity, value overlap, LLM-assisted
│   ├── models.py                   # GraphNode, GraphEdge, confidence scoring
│   ├── store.py                    # networkx in-memory backend
│   └── persistence.py              # export/import to Neo4j, cache lookup by connection-pair hash
│
├── intent/
│   ├── parser.py                   # NL command + graph context -> intent JSON
│   ├── schema.py                   # intent JSON schema / pydantic models
│   ├── grounding.py                # resolve intent entities to graph nodes/edges
│   ├── explainability.py           # reasoning trace for each mapping decision
│   └── prompts/
│
├── transform/                      # pluggable transform op implementations
│   ├── join.py
│   ├── dedupe.py
│   ├── pivot.py
│   ├── scd_merge.py
│   ├── type_cast.py
│   ├── mask.py                     # PII masking
│   └── custom_expr.py
│
├── dag/
│   ├── nodes.py                    # DAGNode types: read, filter, join, transform, write
│   ├── builder.py                  # intent -> logical DAG
│   ├── validator.py                # type checks, cycles, low-confidence edge flags
│   ├── optimizer.py                # predicate pushdown, join reordering
│   ├── patch.py                    # apply user edits/iteration without full re-parse
│   └── versioning.py                # plan version history, rollback
│
├── compiler/
│   ├── spark_compiler.py           # logical DAG -> PySpark/SQL
│   ├── sql_compiler.py             # future: DAG -> warehouse SQL
│   └── streaming_compiler.py       # future: DAG -> Structured Streaming
│
├── execution/
│   ├── staging.py                  # staged write to Delta staging path/table
│   ├── preview.py                  # sampled DataFrame, schema diff, row-count deltas
│   ├── quality_gates.py            # null%, duplicate, referential-integrity checks
│   ├── cost_estimator.py           # pre-run cost/row estimates from source stats
│   ├── executor.py                 # full staged run
│   └── commit.py                   # staged -> destination atomic swap/MERGE
│
├── observability/
│   ├── step_tracer.py              # captures every pipeline step for UI (§6)
│   ├── lineage.py                  # plan + code + row counts -> Unity Catalog / metadata store
│   └── execution_log.py
│
├── api/                             # FastAPI backend
│   ├── main.py
│   ├── routes/
│   │   ├── connections.py
│   │   ├── graph.py
│   │   ├── commands.py
│   │   ├── dag.py
│   │   ├── preview.py
│   │   └── commit.py
│   └── auth/                       # per-tenant credential vault, RBAC
│
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── ConnectionSetup/
│   │   │   ├── GraphViewer/
│   │   │   ├── CommandInput/
│   │   │   ├── IntentViewer/
│   │   │   ├── DAGViewer/
│   │   │   ├── CodeViewer/         # compiled Spark code
│   │   │   ├── RunTimeline/
│   │   │   └── PreviewGrid/
│   │   └── api/
│
├── tests/
│   ├── connectors/
│   ├── graph/
│   ├── intent/
│   ├── dag/
│   ├── transform/
│   └── e2e/                        # full pipeline tests with mock connectors
│
└── infra/
    ├── docker-compose.yml           # local dev: mock connectors, Neo4j, graph store
    └── terraform/                   # cloud resources per environment
```

---

## 11. Key Design Principles

- **Connector interface first** — new source/destination = one new adapter,
  zero changes elsewhere.
- **Transforms are DAG nodes, not a separate ETL phase** — everything
  happens inline in one compiled Spark job.
- **Logical DAG stays engine-agnostic** — Spark specifics live only in
  `compiler/`.
- **LLM never writes directly to the destination** — it only produces
  intent JSON; everything downstream is deterministic, inspectable code.
- **Staged write + atomic swap/MERGE always** — no direct production
  writes, ever.
- **Every step is inspectable in the UI** — graph, intent, DAG, compiled
  code, run log, preview — nothing is a black box.
- **Graph is cheap by default, durable on demand** — in-memory unless the
  user explicitly asks to persist for reuse.
- **Low-confidence inferred edges/joins are surfaced, never silently used.**