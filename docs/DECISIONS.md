# Architectural Decisions & Trade-Offs

Key technical design choices made in this distributed processing engine.

---

## 1. NATS JetStream vs. Celery / RabbitMQ / Redis Streams

### Decision
Use **NATS JetStream** (`JOBS` stream with `WorkQueue` retention policy).

### Why
- **Single Binary / Ultra-Low Overhead**: NATS provides pub/sub, streaming, and work queues in a lightweight Go binary (< 30MB RAM footprint).
- **WorkQueue Retention**: In `WorkQueue` mode, messages auto-delete once acknowledged, making stream size represent only pending queue depth rather than infinite storage bloat.
- **Built-in Competing Consumers**: NATS JetStream queue groups allow horizontal scaling of worker processes with no broker-side locking contention.

---

## 2. Dual-Tier State: PostgreSQL 18 + S3 Audit Archival

### Decision
Store lean operational state machines in **PostgreSQL 18** and batch raw high-frequency JSON execution logs into **RustFS S3** as compressed `.jsonl.gz` files.

### Why
- **Prevent Database Table Bloat**: High-throughput distributed pipelines generate thousands of debug trace events per second. Writing every granular event to PostgreSQL causes massive WAL write amplification and autovacuum degradation.
- **ACID Queryability for UI & APIs**: PostgreSQL `jobs` table keeps single-row lifecycle records (`PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `COMPLETED`) with indexed lookups on `job_id`, `job_type`, and `status`.
- **Decoupled Audit Consumer**: Flushes buffered event batches to S3 (100 events or 15s) and writes the S3 object key back to PostgreSQL, enabling direct analytical queries with **DuckDB** without hitting PostgreSQL.

---

## 3. Pluggable Processor Architecture

### Decision
Define a minimal `JobProcessor` protocol and dynamic `ProcessorRegistry` instead of a monolithic task handler or distributed Celery task imports.

### Why
- **Extensibility**: Adding a new task type (e.g., audio transcoding, PDF OCR, ML embeddings) requires only writing a single class implementing `process(payload, ctx)` and registering it.
- **Decoupled Dependencies**: Workers only execute the registered logic for the matching `job_type` string received in the NATS message envelope.
