# ⚡ Migrate.io - Universal Data Migration Platform

> **Natural Language to PySpark Pipeline Compiler with Schema Relationship Graphs & Staging-First Atomic Delta Commits.**

Migrate.io is a full-stack, enterprise-grade data migration platform. It connects any source (object storage, data warehouses, RDBMS, ERPs, streaming) to any destination, automatically builds a multi-tenant schema relationship graph, converts natural-language instructions into deterministic logical DAGs, compiles them to clean PySpark code, runs staged execution, and performs atomic production commits via Delta MERGE.

---

## 🌟 Key Features

- 🔌 **Universal Connector Framework**: Standardized plugin interface supporting S3, ADLS Gen2, GCS, MinIO, Databricks, PostgreSQL, SAP ECC, plus 5 synthetic mock connectors.
- 🕸️ **Schema Relationship Graph**: Automated schema crawling, fuzzy edge inference (Levenshtein name similarity & Jaccard value overlap), schema drift detection, and persistent Neo4j backing.
- 💬 **Natural Language Command Parser**: Multi-LLM engine (Groq, Anthropic, Ollama, Databricks Model Serving) that translates instructions into structured `IntentJSON`.
- 🔍 **Explainable AI (XAI)**: Generates human-readable reasoning and confidence scores for every table resolution, join condition, and column mapping decision.
- 📊 **Engine-Agnostic Logical DAG**: Intermediate representation with automated structural validation (cycle detection, missing keys), cost-based join reordering, and iterative patch editing.
- ⚙️ **PySpark Compiler**: Generates clean, production-ready PySpark code supporting inline transforms (deduplication, hashing/masking, SCD Type 1/2 merges, pivoting, type casting).
- 🛡️ **Staging-First Safety**: Data is executed into isolated Delta staging paths. Production tables are updated atomically via Delta `MERGE INTO` or atomic swaps only after explicit user approval.
- 📋 **Immutable Plan Versioning & Rollback**: Versioned migration plans saved as JSON artifacts; Delta time-travel enables instant point-in-time production rollbacks.
- 🖥️ **Interactive 10-Step React UI**: Dark-themed glassmorphism interface powered by Vite, React Flow, TypeScript, and Zustand.

---

## 🏗️ Platform Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   10-Step React UI                                      │
└───────┬─────────────────────────────────────────────────────────────────────────┬───────┘
        │ REST API / WebSocket                                                    │
┌───────▼─────────────────────────────────────────────────────────────────────────▼───────┐
│                                   FastAPI Backend                                       │
│                                                                                         │
│  ┌────────────────┐   ┌─────────────────┐   ┌────────────────┐   ┌───────────────────┐  │
│  │ Connectors     │   │ Schema Graph    │   │ LLM Parser     │   │ Logical DAG       │  │
│  │ S3, Databricks,│   │ Crawling,       │   │ Groq / Ollama, │   │ Validator,        │  │
│  │ Postgres, SAP  │   │ Inference, Neo4j│   │ Grounding, XAI │   │ Optimizer, Patch  │  │
│  └───────┬────────┘   └────────┬────────┘   └───────┬────────┘   └─────────┬─────────┘  │
└──────────┼─────────────────────┼────────────────────┼──────────────────────┼────────────┘
           │                     │                    │                      │
┌──────────▼─────────────────────▼────────────────────▼──────────────────────▼────────────┐
│                             Spark Compiler & Execution                                  │
│                                                                                         │
│     DAG ──► PySpark Code ──► Staging Write (Delta) ──► Preview ──► Delta MERGE Commit    │
└─────────────────────────────────────────────────────────────────────────────────────────┘
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
   git clone https://github.com/your-org/migrate-io.git
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
