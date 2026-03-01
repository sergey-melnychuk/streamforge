# Retrospective: Windowed Aggregations Failure

## Date
November 22, 2025

## Problem Statement
The executor was not properly aggregating/windowing events. A simple counter job with multiple aggregations (COUNT, MIN, MAX) in a 10-second tumbling window was producing no results in the output file.

## Root Cause Analysis

### The Core Issue
The `apply_windowed_aggregations` method in `src/query/executor.rs` was not handling multiple aggregations correctly. When multiple aggregations were present, it would:
1. Fall through to `execute_windowed_aggregation` 
2. Which only handled single aggregations
3. For multiple aggregations, it would return an empty `Vec<Event>`
4. Result: No output written to the sink

### Technical Details

**File**: `src/query/executor.rs`
**Method**: `apply_windowed_aggregations` (around line 1147)
**Method**: `execute_windowed_aggregation` (around line 1235)

The flow was:
```
execute_streaming_windowed_incremental (line 435)
  -> For multiple aggregations, uses WindowedBatchSink
  -> WindowedBatchSink.process_batch() (line 547)
  -> Calls executor.apply_windowed_aggregations() (line 726)
  -> apply_windowed_aggregations() creates WindowedStream
  -> Calls execute_windowed_aggregation() (line 1235)
  -> execute_windowed_aggregation() checks if aggregations.len() == 1
  -> If not, falls through to default case which returns empty Vec
```

**The Bug**: `execute_windowed_aggregation` at line 1235 has:
```rust
if aggregations.len() == 1 {
    // Handle single aggregation
} else {
    // Multiple aggregations: returns empty Vec
    return Ok(Vec::new());
}
```

### What Was Attempted

1. **Added source logging** (`src/sources/http.rs`):
   - Added `source_log_path` config option
   - Logs raw events to JSONL file
   - Uses `spawn_blocking` for file I/O
   - Status: Implemented but not tested

2. **Created WindowedBatchSink** (`src/query/executor.rs` line 537):
   - Custom sink that batches events
   - Processes batches every `window_secs`
   - Has duplicate logic paths (periodic task vs manual batching)
   - Status: Overcomplicated, not working correctly

3. **Fixed apply_windowed_aggregations** (line 1147):
   - Added logic to handle multiple aggregations
   - Groups events by window manually
   - Computes all aggregations for each window
   - Status: **This is the correct fix, but needs testing**

### Current State

**Job Config**: `demo/jobs/simple_counter.toml`
```toml
source = { type = "http", urls = ["http://127.0.0.1:8093/counter"], poll_interval = 1, timeout = 5, source_log_path = "/tmp/streamforge_counter_source.jsonl" }
sql = """
SELECT 
    COUNT(*) AS count,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM counter_events
WINDOW TUMBLING 10 SECONDS
"""
sink = { type = "file", path = "/tmp/streamforge_counter_output.jsonl", format = "json", append = true }
```

**Expected Behavior**:
- HTTP source polls `http://127.0.0.1:8093/counter` every 1 second
- Counter returns: `{"timestamp": ..., "value": N}` where N increments
- Events should be grouped into 10-second windows
- Each window should produce: `{"count": N, "min_value": X, "max_value": Y, "window_start": ..., "window_end": ...}`
- Results written to `/tmp/streamforge_counter_output.jsonl`
- Raw events logged to `/tmp/streamforge_counter_source.jsonl`

**Actual Behavior**:
- Source is polling (errors in logs show connection attempts)
- No events in source log file (logging may not be working)
- No events in output file (aggregations not producing results)

## What Went Wrong

### 1. Too Many Changes at Once
- Added source logging
- Added multiple aggregation handling
- Created WindowedBatchSink
- Modified apply_windowed_aggregations
- All without incremental testing

### 2. No Incremental Testing
- Should have started with COUNT only
- Verified it worked
- Then added MIN
- Then added MAX
- Then added source logging

### 3. Insufficient Debugging
- No logging to see if events reach aggregator
- No logging to see if windows are created
- No logging to see if aggregations are computed
- No verification of data flow

### 4. Assumed Instead of Verified
- Assumed `apply_windowed_aggregations` worked for multiple aggregations
- Assumed periodic batch processor was triggering
- Didn't verify actual data flow

### 5. Overcomplicated Solution
- Created WindowedBatchSink with complex state management
- Had duplicate logic paths
- Should have fixed core `apply_windowed_aggregations` first

## How to Fix (Minimal Test-Driven Approach)

### Step 1: Minimal Test - COUNT Only
**Goal**: Verify basic windowed aggregation works

1. Modify `demo/jobs/simple_counter.toml`:
   ```toml
   sql = """
   SELECT COUNT(*) AS count
   FROM counter_events
   WINDOW TUMBLING 10 SECONDS
   """
   ```

2. Add logging to `apply_windowed_aggregations`:
   ```rust
   info!("apply_windowed_aggregations: received {} events", events.len());
   ```

3. Add logging to `execute_windowed_aggregation`:
   ```rust
   info!("execute_windowed_aggregation: {} aggregations", aggregations.len());
   ```

4. Run test: `./demo/test-simple-counter.sh`
5. Verify:
   - Events appear in logs
   - Windows are created
   - COUNT is computed
   - Results written to output file

### Step 2: Add MIN Aggregation
**Goal**: Verify multiple aggregations work

1. Modify SQL:
   ```toml
   sql = """
   SELECT 
       COUNT(*) AS count,
       MIN(value) AS min_value
   FROM counter_events
   WINDOW TUMBLING 10 SECONDS
   """
   ```

2. Verify the fix in `apply_windowed_aggregations` (line 1147) handles this:
   - Should detect `aggregations.len() > 1`
   - Should group events by window
   - Should compute both COUNT and MIN
   - Should combine into single result event

3. Run test and verify both aggregations appear in output

### Step 3: Add MAX Aggregation
**Goal**: Complete the original requirement

1. Modify SQL to include MAX
2. Verify all three aggregations work
3. Verify output format is correct

### Step 4: Add Source Logging
**Goal**: Log raw events for debugging

1. Verify source logging in `src/sources/http.rs` (line 176)
2. Test that events are written to source log file
3. Verify format: `{"value": N, "timestamp": T, "raw": {...}}`

### Step 5: Clean Up
**Goal**: Remove unnecessary complexity

1. Review WindowedBatchSink - is it needed?
2. Remove duplicate logic paths
3. Simplify code

## Key Files to Focus On

1. **`src/query/executor.rs`**:
   - Line 435: `apply_streaming_windowed_aggregations` - entry point for windowed queries
   - Line 515: Multiple aggregations path - uses WindowedBatchSink
   - Line 1147: `apply_windowed_aggregations` - **THE FIX IS HERE** - needs testing
   - Line 1235: `execute_windowed_aggregation` - only handles single aggregations

2. **`src/sources/http.rs`**:
   - Line 176: Source logging implementation
   - Uses `spawn_blocking` for file I/O
   - May need testing/verification

3. **`demo/jobs/simple_counter.toml`**:
   - Job configuration
   - SQL query with multiple aggregations
   - Source and sink configs

4. **`demo/test-simple-counter.sh`**:
   - Test script
   - Starts counter server, node, submits job
   - Checks output files

## Debugging Commands

```bash
# Check if counter server is running
curl http://127.0.0.1:8093/counter

# Check source log
cat /tmp/streamforge_counter_source.jsonl

# Check output log
cat /tmp/streamforge_counter_output.jsonl

# Check node logs
tail -f /tmp/node1.log | grep -iE "(window|aggregat|batch|event)"

# Check job logs
tail -f /tmp/streamforge_job_simple_counter_*.log
```

## Success Criteria

1. ✅ Source logs raw events to `/tmp/streamforge_counter_source.jsonl`
2. ✅ Events are grouped into 10-second windows
3. ✅ COUNT, MIN, MAX are computed for each window
4. ✅ Results are written to `/tmp/streamforge_counter_output.jsonl`
5. ✅ Output format: `{"count": N, "min_value": X, "max_value": Y, "window_start": ..., "window_end": ...}`

## Lessons Learned

1. **Build incrementally**: One feature at a time, test each step
2. **Test-driven**: Write test first, then implement
3. **Debug early**: Add logging to verify data flow
4. **Simplify first**: Fix core issue before adding features
5. **Verify assumptions**: Don't assume code works, test it

## Next Steps

1. Start with Step 1 (COUNT only)
2. Add logging to trace execution
3. Verify each step works before proceeding
4. Only add complexity when simple version works
5. Clean up unnecessary code after everything works

---

# Recovery: Sessions 6 & 7

## Session 6 — Multi-Aggregation Windowed Queries Fixed (2025-11-22 → 2026-03-01)

### What Was Broken
The counter job (`COUNT(*) AS count, MIN(value) AS min_value, MAX(value) AS max_value`) produced no output in the file sink.

### Root Causes and Fixes

**1. `project_event` silently skipped `SelectField::Aggregation`**

`apply_projection` matched `SelectField::Aggregation` but had no body — the arm fell through without inserting anything into the result. Fixed by reading the pre-computed alias field back from the event JSON:

```rust
SelectField::Aggregation(agg) => {
    let field_name = agg.alias.as_ref()
        .unwrap_or(&agg.function.to_lowercase())
        .clone();
    if let EventValue::Json(json) = &event.value {
        if let Some(value) = json.get(&field_name).or_else(|| json.get("value")) {
            result_json.insert(field_name, value.clone());
        }
    }
}
```

**2. `window_start` / `window_end` not preserved through projection**

Added explicit pass-through of windowing metadata fields in `project_event`.

**3. Test infrastructure issues**
- `demo/load/Cargo.toml` was missing `chrono` dependency
- Test script used release builds (slow) and wrong binary paths
- Old node process not being killed cleanly — fixed with `pkill -f "streamforge"` + `lsof -ti:9001 | xargs kill -9`

**4. `STREAMFORGE_DATA_DIR` env var**
Added to `src/cli/job.rs` and `src/cli/executor.rs` to isolate job state to `target/test-state/` during tests.

**5. Dead dependencies removed**
Removed `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`, `opentelemetry-jaeger`, `tracing-opentelemetry`, `metrics = "0.21"`. Made `rocksdb` optional behind a feature flag.

### What Was Verified Working
After session 6 fixes:
- `simple_counter.toml`: `COUNT(*) AS count, MIN(value) AS min_value, MAX(value) AS max_value` — tumbling window, all aggregations present in output.

---

## Session 7 — Full SQL Coverage, GROUP BY, HAVING, Window Types, Unit Tests (2026-03-01)

### What Was Tested and Fixed

**1. GROUP BY key not embedded in result**

`apply_windowed_aggregations` iterated groups with `_key` (discarded), produced result events without the group key field. Fixed by embedding GROUP BY field value into the result JSON map, and using `EventKey::None` on the result event to avoid a redundant `"key"` field in file sink output.

**2. Single-aggregation "value" key fallback**

`StreamingWindowedAggregator` (used for single-aggregation queries) stores the scalar result under `"value"`, not the alias. `project_event` only looked for the alias and found nothing. Fixed with:

```rust
json.get(&field_name).or_else(|| json.get("value"))
```

Output uses the alias name regardless of which key was found.

**3. HAVING not applied in windowed batch path**

`apply_windowed_aggregations` computed results but never evaluated `self.query.having`. Fixed by adding `results.retain(...)` before returning:

```rust
if let Some(ref having) = self.query.having {
    results.retain(|e| match having.condition.evaluate(e) {
        Value::Boolean(b) => b,
        _ => false,
    });
}
```

**4. HAVING parser ordering**

The SQL parser expects `... GROUP BY field HAVING condition WINDOW ...`. Placing HAVING after WINDOW causes it to be silently ignored. All job configs fixed to use the correct order.

**5. `test-simple-counter.sh` path resolution bug**

Script `cd`s to `$SCRIPT_DIR` after building, so a relative path argument like `demo/jobs/single_agg_test.toml` resolved to `demo/demo/jobs/...`. Fixed by resolving the argument to an absolute path at script start.

### What Was Verified Working

| Job | Window Type | Features |
|-----|-------------|---------|
| `simple_counter.toml` | Tumbling | COUNT, MIN, MAX |
| `single_agg_test.toml` | Tumbling | COUNT (single-agg path) |
| `ad_analytics.toml` | Session | COUNT, SUM, AVG + GROUP BY |
| `price_oracle.toml` | Sliding | MEDIAN, AVG, MIN, MAX, SUM, COUNT + GROUP BY |

WHERE clause: `WHERE value > 5` correctly reduced event count.
HAVING clause: `HAVING event_count > 1` correctly filtered single-event sessions.
Sliding windows: overlapping windows confirmed (each event in multiple windows).
Session windows: per-event session boundaries confirmed.

### Unit Tests Added (`src/query/executor.rs`, `mod tests`)

1. `test_project_event_aggregation_alias_preserved` — regression for session-6 `SelectField::Aggregation` skip bug
2. `test_project_event_aggregation_single_agg_value_fallback` — "value" → alias fallback for single-agg path
3. `test_project_event_field_and_all` — `SelectField::Field` extraction and exclusion of unreferenced fields
4. `test_apply_windowed_aggregations_group_by_key_embedded` — GROUP BY field present in result event
5. `test_apply_windowed_aggregations_having_filters` — HAVING correctly retains/excludes groups

### Lessons Reinforced

- **Static vs instance method**: `project_event` is a static associated function (`fn project_event(event, fields)`), not `&self`. Test calls must use `QueryExecutor::project_event(&event, &executor.query.select.fields)`.
- **Two separate code paths for single vs multi-agg**: changes to one path don't affect the other — test both explicitly.
- **SQL clause ordering is load-bearing**: the parser enforces `WHERE → GROUP BY → HAVING → WINDOW`; out-of-order clauses are silently ignored, not errors.

