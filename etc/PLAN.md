# Production-Ready Features Implementation Plan

**Last Updated**: 2026-03-01

## Overview
Transform StreamForge into a production-ready system with SQL queries, autonomous operation, metrics, alerting, and offset tracking.

## 🟢 Current State (2026-03-01)

### Query Engine — Fully Verified End-to-End

Sessions 6–7 fixed all known windowed aggregation bugs and verified the full SQL feature set works in production conditions (real HTTP sources, file sinks, daemon mode):

- **Tumbling / Sliding / Session windows** — all three tested and passing
- **Single-agg and multi-agg paths** — both tested (separate code paths in executor)
- **GROUP BY** — key correctly embedded in result events
- **WHERE / HAVING** — both verified (HAVING requires multi-agg code path)
- **All aggregation functions** — COUNT, SUM, AVG, MIN, MAX, MEDIAN

SQL clause ordering enforced by parser: `WHERE → GROUP BY → HAVING → WINDOW`. Out-of-order clauses are silently ignored.

**5 new unit tests** added to `src/query/executor.rs` covering all regression cases.

### Next Priorities

1. **External connectors** — Kafka source/sink is the highest-value addition
2. **Metrics sink** — emit aggregation results to Prometheus
3. **Query optimizer** — filter pushdown, predicate evaluation order
4. **Integration tests** — automated end-to-end test suite (currently manual scripts)

---

## ✅ Completed Features

### Priority 1: Source Offset Tracking & Resumable Processing ✅ **COMPLETE**

### Status: ✅ Complete
- ✅ Created `OffsetTracker` module
- ✅ Integrated with `FileSource` to support resuming from line number
- ✅ Integrated with job executor to track offsets
- ✅ Added offset persistence on job restart

## Priority 2: Sink Write Tracking & Idempotency ✅ **COMPLETE**

### Status: ✅ Complete
- ✅ Created `SinkTracker` module
- ✅ Integrated with sinks to track write positions
- ✅ Added transaction ID tracking for exactly-once semantics
- ✅ Persist write positions

## Priority 3: Autonomous Operation (Daemon Mode) ✅ **COMPLETE**

### Status: ✅ Complete
- ✅ Long-running job support
- ✅ Continuous source reading (tail -f style)
- ✅ Job restart on failure
- ✅ Background daemon mode

**Implemented:**
- `--daemon` flag in CLI
- Job supervisor with configurable restart policy
- Continuous file tailing (`follow: true`)
- HTTP polling support
- Automatic restart on failure

## Priority 4: SQL Query Execution Engine ✅ **COMPLETE**

### Status: ✅ Complete
- ✅ Complete SQL parser (SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, WINDOW, JOIN)
- ✅ Aggregation functions (COUNT, SUM, AVG, MIN, MAX, MEDIAN)
- ✅ Window functions (fully integrated with streaming execution)
- ✅ JOIN support (INNER, LEFT, RIGHT, FULL OUTER)
- ✅ Scalar functions (UPPER, LOWER, SUBSTRING, LENGTH, TRIM, ABS, ROUND, FLOOR, CEIL)
- ✅ Streaming windowed aggregations with watermark-based triggering
- ⚠️ Query optimizer (placeholder - next priority)
- ✅ Query executor integration
- ✅ Distributed query execution

### ✅ Fully Implemented

**SQL Parser:**
- ✅ SELECT with field projections and wildcard
- ✅ WHERE clauses with comparisons (>, <, >=, <=, =, !=)
- ✅ WHERE clauses with AND/OR
- ✅ GROUP BY with multiple fields
- ✅ Aggregation functions: COUNT, SUM, AVG, MIN, MAX, MEDIAN
- ✅ Window specifications (TUMBLING, SLIDING, SESSION) - parsed
- ✅ Field aliases (AS keyword)
- ✅ Numeric, string, and boolean literals

**Query Executor:**
- ✅ Streaming execution for non-aggregated queries
- ✅ Batch execution for aggregated queries
- ✅ WHERE filtering (streaming)
- ✅ Field projection (SELECT fields)
- ✅ All aggregation functions
- ✅ GROUP BY aggregations (per-group)
- ✅ Global aggregations (no GROUP BY)
- ✅ Integration with job executor
- ✅ Works with file sources and sinks

**Expression Evaluation:**
- ✅ Field references
- ✅ Literal values (string, integer, float, boolean)
- ✅ Binary operations (comparisons, arithmetic, logical)
- ✅ Type coercion for comparisons

### ✅ Fully Implemented Features

**Windowing:**
- ✅ Window specifications fully integrated with streaming execution
- ✅ Streaming windowed aggregations with incremental processing
- ✅ Watermark-based window triggering
- ✅ Late data handling
- ✅ All window types (TUMBLING, SLIDING, SESSION)

**Streaming Aggregations:**
- ✅ Incremental processing (no need to collect all events)
- ✅ Works for both bounded and unbounded streams
- ✅ Memory-efficient for large datasets
- ✅ Watermark-based triggering

**Function Calls:**
- ✅ Scalar functions work in expressions (UPPER, LOWER, SUBSTRING, etc.)
- ✅ Aggregation functions fully supported
- ✅ Field extraction from JSON events

**SQL Features:**
- ✅ JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- ✅ HAVING clause
- ✅ ORDER BY
- ✅ LIMIT/OFFSET
- ✅ Scalar functions (UPPER, LOWER, SUBSTRING, LENGTH, TRIM, ABS, ROUND, FLOOR, CEIL)
- ✅ Windowed aggregations with streaming execution

### ❌ Not Yet Implemented

**Advanced SQL Features:**
- ❌ Subqueries
- ❌ UNION/INTERSECT/EXCEPT
- ❌ Window functions (ROW_NUMBER, RANK, DENSE_RANK, etc.)
- ❌ DISTINCT
- ❌ CASE expressions

**Query Optimization:**
- ❌ Query optimizer is a placeholder
- ❌ No cost-based optimization
- ❌ No index selection
- ❌ No filter pushdown optimization

**Error Handling:**
- ⚠️ Basic error messages
- ❌ No detailed error locations (line/column)
- ❌ No query validation before execution

### Current Capabilities

**What Works Well:**
1. Basic queries: `SELECT * FROM stream WHERE condition`
2. Aggregations: `SELECT field, AVG(value) FROM stream GROUP BY field`
3. Multiple aggregations: `SELECT field, MIN(x), MAX(x), MEDIAN(x), AVG(x), COUNT(*) FROM stream GROUP BY field`
4. Filtered aggregations: `SELECT field, AVG(value) FROM stream WHERE condition GROUP BY field`
5. Field projection: `SELECT field1, field2 FROM stream`
6. Field aliases: `SELECT field AS alias FROM stream`
7. Windowed aggregations: `SELECT field, COUNT(*) FROM stream WINDOW TUMBLING 60s`
8. JOINs: `SELECT * FROM stream1 JOIN stream2 ON stream1.id = stream2.id`
9. HAVING clause: `SELECT field, AVG(value) FROM stream GROUP BY field HAVING AVG(value) > 100`
10. ORDER BY and LIMIT: `SELECT * FROM stream ORDER BY field DESC LIMIT 10`
11. Scalar functions: `SELECT UPPER(name), LENGTH(description) FROM stream`

**What's Missing for Production:**
1. External connectors (Kafka, databases, message queues)
2. Per-job metrics and alerting
3. Query optimization (cost-based, filter pushdown)
4. Advanced SQL features (subqueries, UNION, window functions)
5. Integration tests and chaos testing

## Priority 5: Integration & Connectors (HIGH) 🔴

### Status: Pending
- ⏳ Kafka source/sink
- ⏳ Database connectors (PostgreSQL, MySQL, etc.)
- ⏳ Message queue connectors (RabbitMQ, NATS, etc.)
- ⏳ Cloud storage (S3, GCS, Azure Blob)
- ⏳ Generic connector framework

### Implementation:
1. Create connector trait/interface
2. Implement Kafka connector (producer/consumer)
3. Implement database connectors
4. Add connector configuration
5. Test with real systems

**Why It's Important:**
- Required for real-world deployments
- Enables integration with existing systems
- Use-case dependent but critical

**Estimated Effort**: 2-4 weeks (per connector)

## Priority 6: Per-Job Metrics & Reporting

### Status: Pending
- ⏳ Job-level metrics (throughput, latency, errors)
- ⏳ Source metrics (events read, offset lag)
- ⏳ Sink metrics (events written, write latency)
- ⏳ Operator metrics (processing time, backpressure)
- ⏳ Metrics export (Prometheus, JSON)

### Implementation:
1. Create `JobMetrics` collector
2. Track metrics per job
3. Export metrics via HTTP endpoint
4. Add metrics to job status command

## Priority 7: Alerting System

### Status: Pending
- ⏳ Alert rule definition (thresholds, conditions)
- ⏳ Alert channels (email, webhook, Slack)
- ⏳ Alert state management (firing, resolved)
- ⏳ Alert history

### Implementation:
1. Define alert rule configuration
2. Create alert evaluator
3. Implement alert channels
4. Add alert state tracking

## Implementation Order

### ✅ Completed
1. ✅ **Source Offset Tracking** (Critical for resumable processing)
2. ✅ **Sink Write Tracking** (Critical for exactly-once semantics)
3. ✅ **Autonomous Operation** (Critical for production use)
4. ✅ **SQL Query Execution** (High value feature)
5. ✅ **Streaming Windowed Aggregations** (Performance improvement)
6. ✅ **SQL JOIN Support** (High-value feature)
7. ✅ **RPC Server for Query Execution** (Distributed execution)

### 🔴 Next Priorities (High)
1. **Integration & Connectors** (Priority 5) - Kafka, databases, message queues
   - Required for real-world deployments
   - Enables integration with existing systems
   - Estimated: 2-4 weeks per connector

2. **Metrics & Reporting** (Priority 6) - Per-job metrics and alerting
   - Operational visibility
   - Proactive issue detection
   - Estimated: 1-2 weeks

3. **Alerting System** (Priority 7) - Alert rules and channels
   - Operational reliability
   - Early failure detection
   - Estimated: 1-2 weeks

### 🟡 Medium Priority
4. **Query Optimization** (Priority 4) - Cost-based optimization
   - Performance improvement
   - Filter pushdown
   - Estimated: 2-3 weeks

5. **Testing & Quality** (Priority 8) - Integration tests, chaos testing
   - Reliability validation
   - Production confidence
   - Estimated: 2-3 weeks

### 🟢 Lower Priority
6. **Advanced SQL Features** - Subqueries, UNION, window functions
7. **Performance Tuning** - Query plan caching, index selection

## Next Steps

1. **Start with Integration & Connectors** - Begin with Kafka connector (most common use case)
2. **Implement Per-Job Metrics** - Track throughput, latency, errors per job
3. **Add Alerting System** - Define alert rules and implement channels
4. **Query Optimization** - Implement cost-based optimizer and filter pushdown
5. **Integration Testing** - Add end-to-end tests and chaos testing

