# Migrate.io

<div align="center">

### AI-Powered Data Migration, ETL & PySpark Pipeline Platform

**Natural language → schema-aware data pipeline → PySpark → staged execution → atomic Delta Lake commit**

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-ETL-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org/docs/latest/api/python/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/React-Frontend-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://react.dev/)
[![Neo4j](https://img.shields.io/badge/Neo4j-Schema_Graph-4581C3?style=for-the-badge&logo=neo4j&logoColor=white)](https://neo4j.com/)
[![License](https://img.shields.io/badge/License-MIT-4267E8?style=for-the-badge)](LICENSE)

**[Quick Start](#-quick-start) · [How It Works](#-how-it-works) · [Features](#-key-features) · [Architecture](#-platform-architecture) · [Testing](#-testing)**

</div>

---

## What is Migrate.io?

**Migrate.io** is an open-source **AI data migration and ETL platform** that turns plain-English migration instructions into **validated, executable PySpark pipelines**.

It connects heterogeneous data sources such as **S3, ADLS Gen2, GCS, MinIO, PostgreSQL, SAP ECC, and Databricks**, discovers relationships between schemas, grounds natural-language instructions against real metadata, builds a logical DAG, compiles that DAG to PySpark, executes into isolated staging, and commits approved changes atomically to Delta Lake.

```text
Natural language
      │
      ▼
Schema discovery + relationship graph
      │
      ▼
Grounded intent + explainable mappings
      │
      ▼
Validated logical DAG
      │
      ▼
PySpark compiler
      │
      ▼
Isolated Delta staging
      │
      ▼
Preview + approval
      │
      ▼
Atomic Delta MERGE / swap
```

### The problem

Traditional data migration projects often require manually writing:

- source and destination mappings
- schema matching logic
- joins and transformations
- PySpark / Spark SQL pipelines
- data quality checks
- migration validation
- deployment and rollback procedures

Migrate.io makes the migration plan **machine-readable, inspectable, testable, and reversible** before production data is changed.

---

## Why Migrate.io?

| Traditional migration | Migrate.io |
|---|---|
| Manual schema mapping | **Schema relationship graph** |
| Hand-written ETL code | **Natural language → logical DAG → PySpark** |
| Hidden join assumptions | **Grounded entity and relationship resolution** |
| Direct production writes | **Staging-first execution** |
| Hard-to-audit transformations | **Explainable migration decisions** |
| Risky migrations | **Preview + explicit approval** |
| Manual rollback | **Versioned plans + Delta time travel** |
| Single source type | **Multi-source connector architecture** |

---

## Key Features

- 🔌 **Universal data connectors** for object storage, warehouses, RDBMS, ERP and streaming sources
- 🕸️ **Schema relationship graph** with automated crawling, fuzzy relationship inference and Neo4j persistence
- 💬 **Natural-language data migration** through multi-LLM intent parsing
- 🧠 **Grounded intent resolution** against the actual schema graph instead of unconstrained generation
- 🔍 **Explainable AI** for table resolution, joins and column mappings
- 📊 **Logical DAG compiler** with cycle detection, type validation, cost-based join reordering and iterative patching
- ⚙️ **PySpark code generation** for production-oriented ETL transformations
- 🛡️ **Staging-first execution** that keeps production isolated until approval
- 💾 **Delta Lake atomic commits** through `MERGE INTO` or atomic swaps
- 📋 **Migration plan versioning** with auditable rollback support
- 🧪 **End-to-end testing** across connectors, graph inference, intent parsing, DAG compilation and execution
- 🖥️ **Interactive React workflow** for inspecting every migration stage

## Use Cases

### Data warehouse migration

Move data between PostgreSQL, Databricks, object storage and other enterprise systems while preserving explicit schema and transformation logic.

### Legacy ETL modernization

Convert migration requirements written in natural language into structured DAGs and generated PySpark pipelines.

### Databricks Lakehouse ingestion

Build schema-aware ingestion pipelines into Delta Lake with staging, preview, validation and atomic production commits.

### Enterprise data integration

Connect heterogeneous systems such as SAP, PostgreSQL, S3, ADLS, GCS and Databricks through a common migration workflow.

### AI-assisted data engineering

Use LLMs for intent understanding while keeping execution grounded in deterministic schemas, graph relationships and validated intermediate representations.

---

## How It Works

```text
1. Connect
   └── Register source + destination systems

2. Discover
   └── Crawl schemas and build relationship graph

3. Describe
   └── Write migration instructions in natural language

4. Ground
   └── Resolve tables, columns, keys and relationships

5. Compile
   └── Build and validate a logical DAG

6. Generate
   └── Compile DAG → PySpark

7. Stage
   └── Execute against isolated Delta staging

8. Preview
   └── Inspect rows, schemas and migration impact

9. Approve
   └── Commit through Delta MERGE / atomic swap

10. Audit
    └── Persist immutable migration plan + commit history
```

---

## Platform Architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│                         React Migration Studio                       │
│ Connections · Graph · Intent · DAG · Spark · Preview · Commit       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ REST / WebSocket
┌──────────────────────────────▼───────────────────────────────────────┐
│                           FastAPI Backend                            │
│                                                                      │
│  Connectors     Schema Graph     Intent / LLM     Logical DAG       │
│  S3 / ADLS      Neo4j            Grounding        Validation        │
│  GCS / DBX      Inference        Explainability   Optimization      │
│  Postgres / SAP                                                        │
└─────────────┬────────────────┬────────────────┬─────────────────────┘
              │                │                │
              └────────────────▼────────────────┘
                         Spark Compiler
                              │
                              ▼
                    PySpark Transformation
                              │
                              ▼
                     Delta Staging Layer
                              │
                         Preview / QA
                              │
                         User Approval
                              │
                              ▼
                    Atomic Delta Commit
                              │
                              ▼
                    Production Lakehouse
```

---

## 📂 Project Layout

```
Migrate.io/
├── api/                        # FastAPI backend
│   ├── auth/                   # JWT & multi-tenant RBAC middleware
│   ├── routes/                 # 7 route groups (connections, graph, commands, dag, preview, commit, plans)
│   └── main.py                 # FastAPI application factory
├── compiler/                   # Code generation engines
│   └── spark_compiler.py       # Logical DAG → PySpark script generator
├── connectors/                 # Connector plugin ecosystem
│   ├── base/                   # Connector & SchemaIntrospector interfaces, capability declarations
│   ├── mock/                   # 5 synthetic mock connectors (object storage, warehouse, rdbms, erp, streaming)
│   ├── object_storage/         # S3, ADLS Gen2, GCS, MinIO implementations
│   ├── warehouse/              # Databricks Unity Catalog implementation
│   ├── rdbms/                  # PostgreSQL implementation
│   └── erp/                    # SAP ECC RFC implementation
├── dag/                        # Logical DAG engine
│   ├── nodes.py                # ReadNode, FilterNode, JoinNode, TransformNode, QualityGateNode, WriteNode
│   ├── builder.py              # GroundedIntent → DAG builder
│   ├── validator.py            # Cycle detection, structural & type validator
│   ├── optimizer.py            # Cost-based join reordering & predicate pushdown
│   ├── patch.py                # Iterative DAG editing (add_filter, edit_join, add_transform)
│   └── versioning.py           # Versioned plan snapshot store
├── execution/                  # Execution & staging management
│   ├── staging.py              # Isolated Delta staging path writer
│   └── commit.py               # Delta MERGE INTO & atomic swap commit manager
├── frontend/                   # Vite + React + TypeScript web application
│   ├── src/
│   │   ├── api/client.ts       # Typed Axios API client & WebSocket factory
│   │   ├── components/         # 10 pipeline step components (ConnectionSetup, GraphViewer, etc.)
│   │   ├── pages/              # MigrationPage layout
│   │   ├── App.tsx             # Root application shell
│   │   └── index.css           # Glassmorphism design system & React Flow overrides
│   ├── package.json
│   └── vite.config.ts
├── graph/                      # Schema Relationship Graph
│   ├── models.py               # GraphNode, GraphEdge, SchemaGraph domain models
│   ├── builder.py              # Multi-connector graph crawler
│   ├── inference.py            # Levenshtein & Jaccard relationship inferrer
│   ├── store.py                # In-memory graph index
│   └── persistence.py          # Neo4j Cypher persistence & tenant snapshot loader
├── infra/                      # Docker & deployment configs
│   ├── docker-compose.yml      # Neo4j, MinIO, FastAPI API, Vite frontend
│   ├── Dockerfile.api
│   └── Dockerfile.frontend
├── intent/                     # Natural Language Intent parsing
│   ├── schema.py               # IntentJSON & GroundedIntent Pydantic models
│   ├── parser.py               # Multi-LLM provider client (Groq, Anthropic, Ollama, Databricks)
│   ├── grounding.py            # Entity resolution against SchemaGraph
│   └── explainability.py       # XAI tracer generating decision rationales
├── observability/              # Telemetry & step tracking
│   └── step_tracer.py          # PipelineStep tracer with sync/async context managers & WS queues
├── tests/                      # Pytest suite
│   ├── connectors/             # Connector interface contract tests
│   ├── graph/                  # Graph building & edge inference tests
│   ├── intent/                 # Intent parser unit tests (mocked LLM)
│   ├── dag/                    # DAG builder, validator & patcher tests
│   └── e2e/                    # Full green-path & rejection-path integration tests
├── .env.example                # Template environment variables
├── pyproject.toml              # Project dependencies & package config
└── README.md
```

---

## ⚡ Quick Start

### Prerequisites

- **Python**: 3.11+ (managed via `uv` recommended)
- **Node.js**: 20+ & `npm`
- **Docker & Docker Compose** (optional, for Neo4j + MinIO + full container stack)

---

### Option 1: Local Development

1. **Clone & Set Up Environment**:
   ```bash
   git clone https://github.com/dcsgod/migrate.io.git
   cd Migrate.io

   # Create .env from template
   cp .env.example .env
   ```

2. **Backend Setup**:
   ```bash
   # Create virtual environment and install dependencies
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate

   # Install package with core dependencies
   uv pip install -e ".[neo4j,s3,postgres]"
   uv pip install fastapi "uvicorn[standard]" groq structlog networkx Levenshtein "python-jose[cryptography]" python-dotenv

   # Start FastAPI dev server
   uvicorn api.main:app --reload --port 8000
   ```
   API Docs available at: `http://localhost:8000/docs`

3. **Frontend Setup** (in a separate terminal):
   ```bash
   cd frontend
   npm install
   npm run dev
   ```
   Access UI at: `http://localhost:5173`

---

### Option 2: Docker Compose (Full Product Stack)

Run the entire application stack including Neo4j graph database, MinIO object storage, FastAPI backend, and Vite frontend with a single command:

```bash
# Set your Groq API key in .env
echo "GROQ_API_KEY=your_groq_api_key_here" >> .env

# Launch services
docker compose -f infra/docker-compose.yml up --build
```

#### Running Services:
- 🖥️ **Web Application**: `http://localhost:5173`
- ⚡ **API Documentation**: `http://localhost:8000/docs`
- 🕸️ **Neo4j Browser**: `http://localhost:7474` (Auth: `neo4j` / `migrate_io_neo4j`)
- 📦 **MinIO Console**: `http://localhost:9001` (Auth: `minioadmin` / `minioadmin`)

---

## 🔑 LLM Provider Configuration

The LLM provider for Natural Language parsing is switchable via environment variables in `.env`:

| Provider | `LLM_PROVIDER` Value | Required Environment Variables | Notes |
|----------|----------------------|--------------------------------|-------|
| **Groq** *(Default)* | `groq` | `GROQ_API_KEY` | Ultra-fast Llama 3 70B inference (Free tier) |
| **Anthropic** | `anthropic` | `ANTHROPIC_API_KEY` | Claude 3.5 Sonnet / Claude 3 Opus |
| **Ollama** | `ollama` | *(Local server on `http://localhost:11434`)* | 100% offline / local execution |
| **Databricks** | `databricks` | `DATABRICKS_TOKEN`, `DATABRICKS_HOST` | Databricks Foundation Model Serving |

---

## 🛠️ The 10-Step Interactive Workflow

1. **Connections**: Register and test source & destination connectors.
2. **Schema Graph**: Crawl metadata, run edge inference, and view interactive node-edge diagrams.
3. **NL Command**: Type migration instructions in plain English (e.g. *"Copy orders from S3 to Databricks, join with customers on customer_id, mask email, exclude cancelled status"*).
4. **Grounded Intent**: Inspect resolved GraphNode IDs, confidence scores, and XAI reasoning.
5. **Logical DAG**: View and edit the compiled logical DAG and run structural validation.
6. **Compiled Spark Code**: View, copy, or execute the auto-generated PySpark script.
7. **Staged Run**: Monitor execution timeline, duration, and row counts written to isolated staging paths.
8. **Preview**: View materialized preview data grid and schema diff before committing.
9. **Approve / Reject**: Confirm atomic production commit (Delta `MERGE INTO` or swap) or discard staging data.
10. **Commit Log**: View immutable audit history of all approved migrations with one-click re-run capabilities.

---

## 🧪 Testing

Run the comprehensive pytest suite covering connectors, graph inference, intent parsing, DAG validation, and end-to-end pipelines:

```bash
# Run all tests
pytest tests/ -v

# Run specific test suites
pytest tests/connectors/ -v   # Connector interface contract tests
pytest tests/graph/ -v        # Schema graph & edge inference tests
pytest tests/intent/ -v       # Intent parser tests (uses mocked LLM)
pytest tests/dag/ -v          # DAG builder, validator & patcher tests
pytest tests/e2e/ -v          # Green path & rejection path integration tests
```

---

## 🛡️ License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
