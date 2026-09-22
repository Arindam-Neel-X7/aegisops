# AegisOps

**AI-Powered Predictive Incident Detection, Causal Root-Cause Analysis & Safe Remediation Platform**

[![CI](https://img.shields.io/badge/CI-passing-brightgreen)]()
[![Python](https://img.shields.io/badge/Python-3.11-blue)]()
[![Next.js](https://img.shields.io/badge/Next.js-14-black)]()
[![License](https://img.shields.io/badge/License-Proprietary-red)]()

---

## Overview

AegisOps is an intelligent operational platform that detects anomalies in distributed systems, performs causal root-cause analysis, retrieves historical operational knowledge, and proposes risk-assessed remediations with human-in-the-loop approval — all within a controlled, verifiable environment.

### Core Intelligence Pipeline

```
Distributed System Simulator
    ↓
Failure Injection
    ↓
Metrics + Logs + Service/Dependency Telemetry
    ↓
Kafka (Event Transport)
    ↓
ML Anomaly Detection + Scoring
    ↓
Incident Creation + Cross-Service Correlation
    ↓
Topology / Blast Radius (Neo4j)
    ↓
Evidence-Based Causal/Topology-Aware RCA
    ↓
Historical Knowledge + RAG (Milvus)
    ↓
LangGraph Investigation Agent
    ↓
Risk Assessment
    ↓
Human Approval (HITL)
    ↓
Controlled Remediation (Simulator)
    ↓
Verification
    ↓
Rollback on Verification Failure
```

---

## Technology Stack

| Layer              | Technology                                          |
| ------------------ | --------------------------------------------------- |
| **Frontend**       | React 18, Next.js 14, TypeScript                    |
| **UI**             | Tailwind CSS, Radix UI, Lucide React                |
| **State**          | TanStack Query (server), Zustand (client), Socket.IO (realtime) |
| **Visualization**  | Cytoscape.js (topology), ECharts (charts)           |
| **Editor**         | Monaco Editor (read-only tool output/diffs)         |
| **Backend**        | Python 3.11, FastAPI                                |
| **ORM/Migrations** | SQLAlchemy, Alembic                                 |
| **Database**       | PostgreSQL 15                                       |
| **Time-Series**    | VictoriaMetrics                                     |
| **Logs/Search**    | OpenSearch                                          |
| **Graph**          | Neo4j                                               |
| **Vector**         | Milvus                                              |
| **Messaging**      | Kafka                                               |
| **Background Jobs**| Celery + Redis                                      |
| **AI/LLM**         | Claude Sonnet (primary), Llama 3 70B (fallback)     |
| **Agent**          | LangGraph                                           |
| **Anomaly ML**     | Prophet, Isolation Forest, Autoencoders             |
| **Embeddings**     | BGE-M3                                              |
| **Guardrails**     | NeMo Guardrails + custom validation                 |

---

## Directory Structure

```
aegisops/
├── frontend/                  # Next.js 14 + React 18 + TypeScript
│   ├── app/                   # Next.js App Router pages & layouts
│   ├── components/            # Reusable UI components
│   ├── hooks/                 # Custom React hooks
│   ├── lib/                   # Utilities, API client, providers
│   ├── stores/                # Zustand state stores
│   ├── tokens/                # Semantic design tokens (colors, spacing, typography)
│   └── tests/                 # Frontend test suites
│
├── backend/                   # Python 3.11 + FastAPI
│   ├── app/                   # Application source
│   │   ├── api/               # API route handlers
│   │   ├── agents/            # LangGraph investigation agent
│   │   ├── correlation/       # Incident correlation service
│   │   ├── detection/         # ML anomaly detection
│   │   ├── knowledge/         # RAG / knowledge base
│   │   ├── models/            # SQLAlchemy ORM models
│   │   ├── repositories/      # Data access layer
│   │   ├── rca/               # Root-cause analysis
│   │   ├── remediation/       # Remediation execution
│   │   ├── risk/              # Risk scoring engine
│   │   ├── simulator/         # Distributed system simulator
│   │   ├── telemetry/         # Telemetry pipeline
│   │   ├── workflows/         # Workflow state machine
│   │   └── audit/             # Audit trail
│   ├── migrations/            # Alembic database migrations
│   └── tests/                 # Backend test suites
│
├── research/                  # Research artifacts & experiments
│   ├── datasets/              # Dataset manifests & references
│   ├── experiments/           # Experiment configurations
│   ├── notebooks/             # Jupyter notebooks
│   ├── configs/               # Model & fault configs
│   ├── results/               # Raw & processed results
│   └── reports/               # Model cards, RAG evals, agent traces
│
├── infra/                     # Infrastructure configuration
│   ├── docker/                # Dockerfiles
│   ├── compose/               # Docker Compose overrides
│   ├── terraform/             # IaC (post-December)
│   └── monitoring/            # Monitoring configs
│
├── docs/                      # Documentation
│   ├── architecture/          # Architecture diagrams & docs
│   ├── decisions/             # Architectural Decision Records (ADRs)
│   ├── experiments/           # Benchmark reports
│   ├── runbooks/              # Operational runbooks & demo scripts
│   └── thesis/                # M.Tech thesis artifacts
│
├── scripts/                   # Build, setup & utility scripts
├── .env.example               # Environment variable template
├── docker-compose.yml         # Multi-profile Docker Compose
├── Makefile                   # Common development commands
└── README.md                  # This file
```

---

## Prerequisites

- **Python** 3.11+
- **Node.js** 22+ (LTS)
- **Docker** & Docker Compose v2
- **Poetry** (Python dependency management)
- **Git**

---

## Quick Start

### 1. Clone & Install

```bash
git clone <repository-url>
cd aegisops

# Backend
cd backend
poetry install
cd ..

# Frontend
cd frontend
npm install
cd ..
```

### 2. Environment Setup

```bash
cp .env.example .env
# Edit .env with your local configuration
```

### 3. Start Infrastructure (Core, Messaging, Storage & Vector Profiles)

```bash
docker compose --profile core --profile messaging --profile storage --profile vector up -d
```

### 4. Run Database Migrations

```bash
cd backend
poetry run alembic upgrade head
```

The initial migration enables PostgreSQL's `pgcrypto` extension and creates the
global `plans` table plus the tenant, user, membership, and session foundation
tables. It also creates the required indexes, timestamp triggers, and row-level
security policies. The three plan records are seeded because a tenant requires
a valid `plan_id` from its first insert.

### 5. Start Development Servers

```bash
# Terminal 1 — Backend
cd backend
poetry run uvicorn app.main:app --reload --port 8000

# Terminal 2 — Frontend
cd frontend
npm run dev
```

### 6. Verify

- Backend health: http://localhost:8000/health
- Backend docs: http://localhost:8000/docs
- Frontend: http://localhost:3000

### Current Foundation Verification

Phase 0 Step 2 is verified locally with the following backend checks:

```bash
cd backend
poetry run pytest tests -q
```

These tests cover the health and readiness response contracts, correlation ID
propagation, security headers, and the OpenAPI document. The readiness endpoint
checks PostgreSQL connectivity and returns `503 {"status":"not_ready"}` until
the database is reachable.

### Database Verification

Once PostgreSQL is running with the connection details in `.env`, run:

```bash
cd backend
poetry run alembic upgrade head
poetry run alembic current
poetry run pytest tests -q
```

To validate the migration rollback in a disposable local database:

```bash
poetry run alembic downgrade base
poetry run alembic upgrade head
```

---

## Docker Compose Profiles

The local environment supports service profiles to avoid running every infrastructure component simultaneously:

| Profile         | Services                              | Use Case                        |
| --------------- | ------------------------------------- | ------------------------------- |
| `core`          | PostgreSQL, Redis                     | API/auth/domain development     |
| `stream`        | Kafka + core                          | Telemetry/event development     |
| `observability` | VictoriaMetrics + OpenSearch + core   | Metrics/log investigation       |
| `topology`      | Neo4j + core                          | Dependency graph & blast radius |
| `knowledge`     | Milvus + core                         | RAG development                 |
| `full`          | All December dependencies             | End-to-end demo & integration   |

```bash
# Start a specific profile
docker compose --profile core up -d

# Start multiple profiles
docker compose --profile core --profile stream up -d

# Start everything
docker compose --profile full up -d

# Stop all
docker compose down
```

Docker Desktop with Compose v2 is required before running the database-backed
Phase 0 Step 3 workflow. Copy `.env.example` to `.env` and replace the
development values before using any shared or deployed environment.

---

## Development Commands

```bash
# Using Make
make setup          # Install all dependencies
make dev            # Start backend + frontend dev servers
make test           # Run all tests
make lint           # Run all linters
make typecheck      # Run type checkers
make migrate        # Run database migrations
make docker-core    # Start core Docker profile
make docker-full    # Start full Docker profile
make clean          # Clean build artifacts
```

---

## State Ownership Rules

| State Type              | Owner          | Rule                                                    |
| ----------------------- | -------------- | ------------------------------------------------------- |
| Server/backend state    | TanStack Query | Fetching, caching, invalidation, refetching, sync       |
| Global UI/client state  | Zustand        | Filters, modals, command palette, selected environment   |
| Local transient state   | React state    | Form drafts, hover/expanded, component-only interactions |
| Realtime transport      | Socket.IO      | Receives/pushes updates; NOT a second state store        |

---

## Task Ownership

| System      | Responsibility                          |
| ----------- | --------------------------------------- |
| Kafka       | Telemetry & event transport             |
| LangGraph   | AI investigation & reasoning            |
| Celery      | General background tasks                |
| Redis       | Transient cache/broker support          |
| PostgreSQL  | Durable application state               |
| Temporal    | Post-December durable workflows         |

---

## Safety Rules

- Agent recommendation ≠ authorization
- Risk score precedes every action
- Risk > 70 → dual human approval required
- No production remediation in December
- Verification failure → automatic rollback

---

## Engineering Conventions

- One feature branch per bounded engineering task; merge only after tests and lint pass.
- No secrets in source control; use `.env.example` for names and local secret injection.
- Every cross-service event has an explicit schema/version.
- Every research experiment has a config, dataset/version reference, seed, metrics, and output artifact.
- Every architectural deviation is recorded in `docs/decisions/`.
- API contracts are versioned and documented via FastAPI/OpenAPI.
- Database changes are migration-first; destructive schema changes require explicit review.
- No direct UI dependence on internal database structures; frontend consumes API/query models.

---

## Project Timeline

| Period         | Primary Goal                    |
| -------------- | ------------------------------- |
| Sep 2026       | Foundation (Phase 0)            |
| Oct 2026       | Simulator + Telemetry           |
| Nov 2026       | Intelligence Loop               |
| Dec 2026       | Showcase V1                     |
| Jan–Feb 2027   | Research & Platform Expansion   |
| Mar 2027       | Evaluation & Refinement         |
| Apr 2027       | Final Integration & Hardening   |
| May 2027       | Thesis & Final Demonstration    |

---

## License

Proprietary — All rights reserved.

---

## Author

**Arindam Karmakar**
M.Tech (CSE), JAIN (deemed-to-be) University
USN: 25MTRCS001
Guide: Dr. Deepak Kumar Sinha
Academic Year: 2026–2027
