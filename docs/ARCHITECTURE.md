# Apache Auron Architecture Documentation

> **Complete technical reference** for understanding, navigating, and extending Auron's native execution engine for Apache Spark.

*Last updated: 2025-05*

---

## At a Glance

| Aspect | Details |
|--------|---------|
| **Purpose** | Native vectorized execution accelerator for Apache Spark |
| **Performance** | 2-10x faster query execution for supported operators |
| **Languages** | Scala (~20k LOC), Rust (~30k LOC), Java (~3k LOC) |
| **Spark Versions** | 3.2, 3.3, 3.4, 3.5 |
| **Key Technologies** | Apache DataFusion, Apache Arrow, Protocol Buffers |
| **Integration** | Drop-in Spark extension—no query changes required |

---

## Table of Contents

- [System Overview](#auron-system-overview)
- [How It Works](#how-it-works)
  - [Join Execution](#join-execution)
  - [Aggregation](#aggregation)
  - [Shuffle](#shuffle)
  - [Memory Management](#memory-management)
- [Module Deep Dives](#auron-module-deep-dives)
- [Critical Execution Paths](#critical-execution-paths)
- [Extension Guide](#auron-extension-guide)

## How to Read This Document

| If you want to... | Start with... |
|-------------------|---------------|
| Understand what Auron does | [System Overview](#auron-system-overview) |
| Learn how joins/agg/shuffle work | [How It Works](#how-it-works) |
| Explore the codebase | [Module Deep Dives](#auron-module-deep-dives) |
| Trace code execution | [Critical Execution Paths](#critical-execution-paths) |
| Add a new operator | [Extension Guide](#auron-extension-guide) |

---

# Auron System Overview

## What is Auron?

Apache Auron is a **native vectorized execution accelerator** for Apache Spark. It replaces Spark's row-based execution with columnar, vectorized processing powered by Apache DataFusion (a Rust-based query engine) and Apache Arrow (a columnar memory format).

### Why Auron Exists

Spark's default execution model processes data row-by-row in the JVM. While this approach is flexible and well-integrated with the JVM ecosystem, it leaves significant performance on the table for compute-intensive analytical workloads.

**The Problem in Practice:**
- A TPC-H Q1 aggregation that takes 45 seconds in Spark can complete in 8 seconds with Auron
- Large sort operations that trigger multiple spill cycles in Spark often complete in-memory with Auron's more efficient memory layout
- Join-heavy workloads see 3-5x improvements due to vectorized hash table operations

| Challenge | Spark's Approach | Auron's Solution |
|-----------|------------------|------------------|
| Memory overhead | JVM objects with headers (16+ bytes per object) | Arrow columnar format (zero per-value overhead) |
| CPU efficiency | Row-at-a-time processing | Vectorized batch processing (thousands of values per operation) |
| JVM limitations | GC pauses, memory management | Native Rust with deterministic memory control |
| SIMD utilization | Limited JIT optimization | Explicit SIMD via DataFusion (AVX2/AVX-512) |

### Key Benefits

1. **Performance**: 2-10x faster query execution for supported operators through vectorized processing and SIMD optimization
2. **Memory efficiency**: Reduced GC pressure and smaller memory footprint due to Arrow's columnar layout
3. **Transparency**: Works with existing Spark SQL queries without code changes—simply enable the extension and existing queries accelerate automatically
4. **Compatibility**: Falls back to Spark execution for unsupported operations, ensuring all queries complete correctly even when native execution isn't possible

## High-Level Architecture

Auron is split across three execution layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SPARK JVM LAYER                                   │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────────────────┐ │
│  │ Spark SQL   │───▶│   Catalyst   │───▶│ AuronSparkSessionExtension      │ │
│  │   Query     │    │   Optimizer  │    │ (Installs columnar rule)        │ │
│  └─────────────┘    └──────────────┘    └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Plan conversion
┌─────────────────────────────────────────────────────────────────────────────┐
│                      AURON SPARK EXTENSION LAYER                            │
│  ┌─────────────────────┐         ┌───────────────────────────────────────┐  │
│  │  AuronConverters    │────────▶│  NativeXxxExec operators              │  │
│  │  (Plan conversion)  │         │  (Protobuf plan generation)           │  │
│  └─────────────────────┘         └───────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ JNI + Protobuf
┌─────────────────────────────────────────────────────────────────────────────┐
│                          RUST NATIVE LAYER                                  │
│  ┌─────────────────────┐    ┌──────────────┐    ┌─────────────────────────┐ │
│  │ NativeExecution     │───▶│  auron-serde │───▶│      DataFusion         │ │
│  │ Runtime (JNI entry) │    │  (Protobuf   │    │   (Plan execution)      │ │
│  │                     │    │   → Plans)   │    │                         │ │
│  └─────────────────────┘    └──────────────┘    └─────────────────────────┘ │
│                                                            │                │
│                                                            ▼                │
│                                                   Arrow RecordBatch         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Arrow FFI
                            Results back to Spark
```

1. **Spark JVM layer**: Spark receives SQL, Catalyst builds a physical plan, and `AuronSparkSessionExtension` installs a columnar rule that can replace supported Spark operators with native Auron operators.

2. **Auron Spark extension layer**: `AuronConverters` and `NativeConverters` translate Spark plans and expressions into Auron native operators and Protobuf messages.

3. **Rust native layer**: JNI calls enter the `auron` crate, `NativeExecutionRuntime` deserializes the Protobuf task definition, DataFusion executes the plan, and Arrow batches are returned to Spark through Arrow FFI.

**Boundary Crossings:**
- **Protobuf**: Serializes plan structure across JVM/native boundary
- **JNI**: Control flow for starting execution, getting batches, and cleanup
- **Arrow FFI**: Zero-copy data transfer for result batches

### Components

| Component | Language | Purpose |
|-----------|----------|---------|
| **spark-extension** | Scala | Base classes for native operators, converters |
| **spark-extension-shims-spark3** | Scala | Spark 3.x version-specific implementations |
| **auron-core** | Java | JNI bridge, memory management, configuration |
| **native-engine/auron** | Rust | Main entry point, runtime coordination |
| **native-engine/auron-serde** | Rust | Protobuf serialization/deserialization |
| **native-engine/datafusion-ext-plans** | Rust | Custom DataFusion physical plan operators |
| **native-engine/datafusion-ext-exprs** | Rust | Custom expression implementations |
| **native-engine/auron-jni-bridge** | Rust | JNI utilities and macros |

## Data Flow

### Query Execution Flow

1. **Spark query planning**: the user submits SQL, Catalyst produces an optimized physical plan, and the injected Auron columnar rule inspects that plan before Spark inserts columnar transitions.
2. **Plan conversion in Scala**: `AuronConvertStrategy` marks each node with a conversion decision, then `AuronConverters.convertSparkPlanRecursively()` replaces supported Spark operators with `NativeXxxExec` operators. `NativeConverters.convertExpr()` converts supported Spark expressions into Protobuf expression nodes.
3. **Native plan serialization**: each `NativeXxxBase.doExecuteNative()` implementation creates a `NativeRDD` whose partition function builds a `PhysicalPlanNode`.
4. **JNI boundary crossing**: `NativeRDD.compute()` creates an `AuronCallNativeWrapper`, serializes a `TaskDefinition`, and calls `JniBridge.callNative()`.
5. **Rust native execution**: `NativeExecutionRuntime::start()` decodes the task definition, `auron-serde` converts the Protobuf plan into DataFusion `ExecutionPlan` nodes, and DataFusion produces Arrow `RecordBatch` output.
6. **Result return**: `JniBridge.nextBatch()` drives the Rust stream. Each batch is exported through Arrow FFI, imported by `AuronCallNativeWrapper.importBatch()`, and exposed back to Spark as rows or columnar batches.

### Memory Flow

Spark-side objects, task context, and SQL metrics remain on the JVM side. Arrow buffers, DataFusion operators, and intermediate native state live in Rust-managed native memory. Arrow FFI connects the two sides without row-by-row serialization. JVM spill paths are coordinated through `OnHeapSpillManager`; native aggregation, sort, and join operators reserve and release memory through `MemManager`, spilling native data when pressure is too high.

## Supported Operations

### Operators

| Operator | Support | Notes |
|----------|---------|-------|
| Filter | ✅ Full | All predicate types |
| Project | ✅ Full | Column selection, expressions |
| Sort | ✅ Full | Multi-column, nulls ordering |
| Aggregate | ✅ Full | Hash and Sort aggregation |
| Join | ✅ Full | Sort-merge, broadcast, hash |
| Scan (Parquet) | ✅ Full | Predicate pushdown, column pruning |
| Scan (ORC) | ✅ Full | Similar to Parquet |
| Exchange (Shuffle) | ✅ Full | Native shuffle writer |
| Window | ✅ Full | Row number, rank, dense_rank |
| Union | ✅ Full | Union all |
| Limit | ✅ Full | Global and local limits |
| Expand | ✅ Full | For ROLLUP/CUBE |
| Generate | ✅ Full | explode, posexplode |

### Expressions

| Category | Examples | Support |
|----------|----------|---------|
| Arithmetic | +, -, *, /, % | ✅ |
| Comparison | =, <>, <, >, <=, >= | ✅ |
| Logical | AND, OR, NOT | ✅ |
| String | substring, concat, like, trim | ✅ |
| Date/Time | date_add, date_sub, to_date | ✅ |
| Aggregate | count, sum, avg, min, max | ✅ |
| Cast | CAST expressions | ✅ |
| Case | CASE WHEN | ✅ |
| Null | IS NULL, COALESCE | ✅ |

### Data Types

| Type | Support | Notes |
|------|---------|-------|
| Boolean | ✅ | |
| Byte, Short, Int, Long | ✅ | |
| Float, Double | ✅ | |
| Decimal | ✅ | 64-bit precision |
| String | ✅ | |
| Date, Timestamp | ✅ | |
| Array | ✅ | |
| Map | ✅ | |
| Struct | ✅ | |

## Configuration

### Enabling Auron

```scala
spark.conf.set("spark.auron.enable", "true")  // Default: true
```

### Key Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `spark.auron.enable` | true | Enable/disable Auron |
| `spark.auron.memory.fraction` | 0.6 | Fraction of executor memory for native |
| `spark.auron.enable.scan` | true | Enable native scans |
| `spark.auron.enable.project` | true | Enable native projections |
| `spark.auron.enable.filter` | true | Enable native filters |
| `spark.auron.enable.sort` | true | Enable native sorts |
| `spark.auron.enable.aggregate` | true | Enable native aggregations |
| `spark.auron.enable.join` | true | Enable native joins |

## Fallback Behavior

When an operation cannot be executed natively, Auron automatically falls back to Spark execution. This ensures **correctness first**—every query that works in Spark continues to work with Auron enabled.

**How Fallback Works:**

Fallback is expressed in the physical plan as conversion boundaries. A supported subtree stays native, an unsupported operator runs in Spark, and `ConvertToNative` or `ConvertFromNative` operators bridge the representation at the boundary.

> **Key Insight:** The converter avoids creating "native islands"—small native sections surrounded by Spark execution—because the conversion overhead would outweigh any performance benefit. A supported operator may remain in Spark if its surrounding plan would require expensive transitions.

**Common Fallback Reasons:**
- **Unsupported expression types**: Complex expressions without native implementation (check logs for specific expressions)
- **UDFs**: User-defined functions require special handling; some can be wrapped, others cause fallback
- **Data types**: Types without Arrow mapping (rare, but possible with custom types)
- **Non-deterministic operations**: Operations that must produce identical results across retries

**Diagnosing Fallback:**
```scala
// Enable conversion logging to see why operators fall back
spark.conf.set("spark.auron.log.level", "debug")
df.explain(true)  // Shows which operators are native vs Spark
```

## Next Steps

Based on what you want to do:

| Goal | Next Section |
|------|--------------|
| Understand how joins/agg/shuffle work | [How It Works](#how-it-works) |
| Explore module structure and code | [Module Deep Dives](#auron-module-deep-dives) |
| Trace execution through the system | [Critical Execution Paths](#critical-execution-paths) |
| Add a new operator or expression | [Extension Guide](#auron-extension-guide) |
| Contribute to Auron | [CONTRIBUTING.md](../../CONTRIBUTING.md) |

---

# How It Works

This section explains the core logic behind Auron's main operations: joins, aggregation, shuffle, and memory management. Understanding these internals helps when debugging performance issues or extending Auron with new operators.

---

## Join Execution

Auron supports three join strategies that execute natively in Rust. The strategy is determined by Spark's query planner based on table sizes, join keys, and existing data ordering—Auron respects Spark's choice and provides native implementations for each.

### How Spark Chooses Join Strategy

| Strategy | When Used | Auron Native? |
|----------|-----------|---------------|
| **Broadcast Hash Join** | One side < `spark.sql.autoBroadcastJoinThreshold` (default 10MB) | ✅ Yes |
| **Sort-Merge Join** | Both sides large, join keys sortable | ✅ Yes |
| **Shuffle Hash Join** | Disabled by default; enabled via config | ✅ Yes |
| **Broadcast Nested Loop** | Cross joins or non-equi joins | ❌ Falls back to Spark |

### Sort-Merge Join

Used when both inputs are large and already sorted (or will be sorted) by join keys. This is Spark's default strategy for large-table joins.

> **Key Insight:** Sort-merge join's memory usage is bounded by batch size, not table size. It streams both sides and only buffers matching rows, making it suitable for joining tables of any size.

**Execution Flow:**
1. Both inputs stream in sorted order (Spark ensures sorting upstream if needed)
2. `StreamCursor` tracks position in each stream with pre-computed key rows
3. Compare cursors: advance the smaller side, collect matches on equality
4. Flush matched rows to output batches when buffer fills

**Key Components:**
- `SortMergeJoinExec` (Rust) — Main execution plan in `datafusion-ext-plans/src/sort_merge_join_exec.rs`
- `StreamCursor` — Optimized cursor with key comparison and buffering
- Joiners: Inner, LeftOuter, RightOuter, FullOuter, Semi, Anti

**Code Path:**
```
NativeSortMergeJoinBase.doExecuteNative()
  → SortMergeJoinExecNode (protobuf)
  → SortMergeJoinExec.execute_join()
    → StreamCursor compare loop
    → Batch output
```

### Broadcast Hash Join

Used when one side is small enough to broadcast (typically < 10MB, configurable via `spark.sql.autoBroadcastJoinThreshold`). The small side is sent to all executors and built into a hash map.

> **Key Insight:** Auron's broadcast hash join uses SIMD-optimized hash lookups. The hash map stores 8 hashes per 64-byte cache line, allowing parallel comparison of 8 potential matches in a single CPU instruction.

**Execution Flow:**
1. **Build phase**: Materialize broadcast side into hash map
   - Extract key columns, compute Spark-compatible Murmur3 hashes
   - Store in SIMD-aligned `MapValueGroup` (64-byte cache lines)
   - Build happens once; result can be cached across stages
2. **Probe phase**: Stream probe side, look up each row
   - Hash probe keys using same Murmur3 algorithm
   - SIMD compare against 8 candidates per cache line
   - Output joined rows

**Hash Map Structure:**
```
MapValueGroup (64 bytes, cache-line aligned):
┌────────────────────────────────────────────────────┐
│ hash[0..7]  (8 × 4-byte hashes for SIMD compare)   │
│ value_or_idx[0..7] (row indices or chain pointers) │
└────────────────────────────────────────────────────┘
```
- Single values stored inline; collisions chain via `mapped_indices`
- Cached hash maps reusable across stages (`cachedBuildHashMapId`)

**Code Path:**
```
NativeBroadcastJoinBase.doExecuteNative()
  → BroadcastJoinExecNode (protobuf)
  → BroadcastJoinExec.execute_join()
    → Build: JoinHashMap construction
    → Probe: Streaming hash lookups
```

### Join Types

| Type | Behavior |
|------|----------|
| INNER | Output rows where both sides match |
| LEFT | All left rows + matched right (null if no match) |
| RIGHT | All right rows + matched left (null if no match) |
| FULL | All rows from both sides |
| LEFT_SEMI | Left rows that have a match (no right columns) |
| LEFT_ANTI | Left rows that have NO match |

---

---

## Aggregation

Auron implements native aggregation with two execution modes and three processing phases. Understanding when each mode is used helps diagnose performance characteristics.

### Aggregation Modes

Spark chooses the aggregation mode based on data characteristics and configuration:

**Hash Aggregation** (default for most queries):
- Uses hash table keyed by grouping columns
- Best for high-cardinality groupings (many distinct groups)
- Memory-intensive; spills to disk when hash table exceeds memory budget
- Spark uses this when: grouping columns are hashable and `spark.sql.execution.useObjectHashAggregateExec` is false

**Sort Aggregation** (used when data is pre-sorted):
- Assumes input is already sorted by grouping keys
- Streaming output as each group completes
- Lower memory footprint (only needs to buffer one group)
- Spark uses this when: input is already sorted by group keys, or sort-based aggregation is forced via config

> **Key Insight:** If you see high memory usage during aggregation, check whether hash aggregation is spilling. Pre-sorting data by group keys can switch to streaming sort aggregation, dramatically reducing memory.

### Aggregation Phases

| Phase | Input | Output | Purpose |
|-------|-------|--------|---------|
| Partial | Raw data | Intermediate state | Per-partition aggregation |
| PartialMerge | Intermediate states | Merged state | Combine after shuffle |
| Final | Merged state | Final values | Produce output |

### Hash Aggregation Flow

1. **Process batch**: For each input row
   - Evaluate grouping keys → create binary row key
   - SIMD hash lookup in `AggHashMap`
   - If new: insert key, allocate accumulators
   - If exists: retrieve accumulator index
   - Update accumulators (sum, count, etc.)

2. **Memory pressure**: When `MemManager` triggers spill
   - Freeze current hash table to binary format
   - Write to spill file (compressed)
   - Start fresh hash table

3. **Output**:
   - Single table: Direct output in reverse order
   - Multiple spills: Radix-tree merge by hash bucket

**Key Components:**
- `AggExec` - Main execution plan
- `AggHashMap` - SIMD-optimized hash table (64-byte aligned)
- `AccTable` - Accumulator storage (typed columns)
- `AggContext` - Schema, modes, partial skipping config

### Partial Skipping Optimization

When cardinality approaches input size, maintaining a hash table provides little benefit and wastes memory. Auron detects this and skips partial aggregation.

**How It Works:**
- During partial aggregation, Auron tracks the ratio: `distinct_groups / rows_processed`
- If this ratio exceeds **99.9%** (configurable), partial aggregation is skipped
- Data passes through unchanged to the final aggregation phase
- This avoids building an expensive hash table that provides <0.1% reduction

**When This Triggers:**
- Aggregating over nearly-unique columns (e.g., `GROUP BY user_id` when most users have one row)
- Aggregations with many grouping columns that create high cardinality
- Skewed data where some partitions have unique keys

> **Key Insight:** If you see "partial skipping" in metrics, it's actually good—Auron detected that partial aggregation wasn't helping and saved the memory/CPU cost.

**Code Path:**
```
NativeAggBase.doExecuteNative()
  → AggExecNode (protobuf)
  → AggExec.execute_agg_with_grouping_hash()
    → AggTable.process_input_batch()
      → AggHashMap.upsert_many() [SIMD lookup/insert]
      → AccTable updates
    → Spill if memory pressure
    → AggTable.output() → RecordBatch
```

---

## Shuffle

Native shuffle writes partitioned data for redistribution across executors. Shuffle is often the most expensive operation in a Spark job, so Auron's native implementation can provide significant speedups.

### Partitioning Schemes

| Scheme | Algorithm | Use Case | Native? |
|--------|-----------|----------|---------|
| Hash | `murmur3(keys) % n` | Default for joins, aggregations | ✅ |
| Range | Binary search in sampled bounds | Sorted output (ORDER BY) | ✅ |
| RoundRobin | `(row_idx + offset) % n` | Load balancing | ✅ |
| Single | All to one partition | `collect()`, `coalesce(1)` | ✅ |

### Shuffle Write Flow

1. **Buffer input batches** (`BufferedData`)
   - Stage batches until ~4MB threshold
   - Track memory via `MemManager`

2. **Sort by partition ID**
   - Evaluate partition for each row
   - Radix sort all `(partition_id, batch_idx, row_idx)`
   - Compute partition boundaries

3. **Write output files**
   - Data file: Compressed Arrow IPC blocks (LZ4/ZSTD)
   - Index file: 64-bit partition offsets

**Spilling Strategy:**
- When memory exceeds threshold: flush sorted buffers to disk
- Multiple spills merged by partition during final write
- Uses same compression as output

### Compression Codec Tradeoffs

Configure via `spark.auron.shuffle.compression.codec`:

| Codec | Compression Ratio | Speed | Best For |
|-------|-------------------|-------|----------|
| **lz4** (default) | ~2-3x | Very fast | Most workloads; good balance |
| **zstd** | ~3-5x | Moderate | Network-bound shuffles, slow disks |
| **none** | 1x | Fastest | Fast local SSDs, small shuffles |

> **Key Insight:** If shuffle is network-bound (common in cloud environments), switching to ZSTD can reduce transfer time despite slower compression. If shuffle is CPU-bound, LZ4 or no compression may be faster.

### Range Partitioning

Requires sampling to compute bounds:
1. Sample key values from input RDD
2. `RangePartitioner.determineBounds()` computes partition boundaries
3. Each row → binary search → partition ID

### Output Format

**Data file (.data):**
```
[4-byte block length][compressed Arrow IPC][repeat...]
```

**Index file (.index):**
```
i64[0] = 0
i64[1] = partition 0 end offset
i64[n] = total file size
```

**Code Path:**
```
NativeShuffleExchangeBase.doExecuteNative()
  → ShuffleWriterExecNode (protobuf)
  → ShuffleWriterExec.execute()
    → SortShuffleRepartitioner.insert_batch()
      → BufferedData staging/sorting
    → shuffle_write()
      → Merge spills if needed
      → Write data + index files
```

---

## Memory Management

Auron coordinates memory between JVM and native execution with automatic spilling. This section explains how memory is budgeted, monitored, and reclaimed under pressure.

### Memory Budget

The native memory budget is derived from Spark's executor memory overhead:

```
Example: 8GB executor with 2GB overhead, 0.6 fraction
─────────────────────────────────────────────────────
executor_memory_overhead (2GB) × MEMORY_FRACTION (0.6)
                    ↓
           Native Budget (1.2GB)
                    ↓
        Divided among active consumers
        (sort: 400MB, agg: 400MB, shuffle: 400MB)
```

Configure via:
- `spark.executor.memoryOverhead` — Total overhead (Spark config)
- `spark.auron.memory.fraction` — Fraction for native (default: 0.6)

### MemManager (Rust Singleton)

Tracks all memory-consuming operators:
- Maintains total budget and current usage
- Monitors per-consumer limits
- Triggers spilling when thresholds exceeded

**Per-Consumer Limit:**
```
consumer_max = (total - jvm_direct - unspillable) / num_spillables
consumer_min = consumer_max / 8
```

### MemConsumer Trait

Operators implement this to participate in memory management:

```rust
trait MemConsumer {
    fn update_mem_used(&self, new_used: usize);
    fn spill(&self);  // Called when over limit
}
```

### Spill Decision Logic

When an operator reports memory growth, the `MemManager` decides whether to allow it or trigger spilling:

```
┌─────────────────────────────────────────────────────────────────┐
│ Operator requests memory growth                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │ total_used + request > budget? │
              └───────────────────────────────┘
                     │                │
                    YES              NO
                     │                │
                     ▼                ▼
         ┌─────────────────┐    ┌──────────────┐
         │ size > 16MB AND │    │ Allow growth │
         │ growing?        │    └──────────────┘
         └─────────────────┘
              │         │
             YES       NO
              │         │
              ▼         ▼
    ┌─────────────┐  ┌────────────────────┐
    │ SPILL now   │  │ WAIT up to 10 sec  │
    │ (if > min)  │  │ then force spill   │
    └─────────────┘  └────────────────────┘
```

**Concrete Example:**
- Budget: 1.2GB total
- 3 active consumers (sort, agg, shuffle)
- Per-consumer max: ~400MB (`(1.2GB - overhead) / 3`)
- Per-consumer min: ~50MB (`max / 8`)
- Sort operator at 450MB requests 50MB more → triggers spill
- After spilling 200MB to disk, sort continues with 250MB in memory

### Two-Tier Spilling

**Tier 1: On-Heap (JVM)**
- Uses Spark's execution memory pool
- Fast memory-to-memory copy
- Only when JVM heap < 90% full

**Tier 2: Disk**
- Compressed temp files (LZ4 default)
- Used when JVM heap exhausted or on driver

### Operator Integration

**Sort (ExternalSorter):**
- Tracks in-memory sorted blocks
- Spill: Merge blocks → write to disk
- Output: K-way merge of spill files + memory

**Aggregation (AggTable):**
- Tracks hash table + accumulators
- Spill: Freeze table → write to disk
- Output: Radix merge of spill buckets

**Shuffle (BufferedData):**
- Tracks staged + sorted buffers
- Spill: Write sorted partition data
- Output: Merge all spills by partition

### Metrics

| Metric | Description |
|--------|-------------|
| `mem_spill_count` | Number of spill events |
| `mem_spill_size` | Bytes spilled to on-heap |
| `disk_spill_size` | Bytes spilled to disk |
| `mem_spill_iotime` | Spill I/O time |

---

## Error Handling

When native execution encounters errors, Auron ensures clean error propagation back to Spark.

### Error Categories

| Error Type | What Happens | Recovery |
|------------|--------------|----------|
| **Out of Memory** | Spill attempted; if disk full, error raised | Increase memory or enable larger spill paths |
| **Unsupported Operation** | Caught during conversion, falls back to Spark | Automatic; check logs for details |
| **Data Corruption** | Arrow validation fails, error raised | Check upstream data sources |
| **JNI Failure** | Native library issues, RuntimeException | Check native library installation |
| **Task Cancellation** | Spark cancels task, native cleanup triggered | Automatic; resources freed |

### Error Propagation

1. **Rust errors** convert to `DataFusionError` with context
2. **JNI bridge** catches Rust panics and converts to Java exceptions
3. **Spark** receives exceptions and handles retry/failure as normal
4. **Metrics** updated to reflect partial progress before failure

> **Key Insight:** Native errors appear as standard Spark exceptions. Check the full stack trace—native context is preserved and shows the Rust-side error message.

---

# Auron Module Deep Dives

> Detailed documentation of each module with key files, code patterns, and entry points. Use this section when exploring the codebase or tracing how components interact.

## Module Dependency Graph

```
                              JVM SIDE
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   ┌─────────────────────┐         ┌──────────────────────────────────────┐  │
│   │   spark-extension   │◀────────│  spark-extension-shims-spark3        │  │
│   │   (base classes,    │         │  (Spark 3.x concrete operators)      │  │
│   │    converters)      │         └──────────────────────────────────────┘  │
│   └─────────────────────┘                                                   │
│            │                                                                │
│            ▼                                                                │
│   ┌─────────────────────┐                                                   │
│   │     auron-core      │  (JNI bridge, configuration, memory mgmt)         │
│   └─────────────────────┘                                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              │ JNI + Protobuf
                              ▼
                             RUST SIDE
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   ┌─────────────────────┐                                                   │
│   │       auron         │  (JNI entry points, runtime lifecycle)            │
│   └─────────────────────┘                                                   │
│        │           │                                                        │
│        ▼           ▼                                                        │
│   ┌──────────┐  ┌─────────────────┐                                         │
│   │ auron-   │  │ auron-jni-      │                                         │
│   │ serde    │  │ bridge          │                                         │
│   │(protobuf)│  │ (JNI utilities) │                                         │
│   └──────────┘  └─────────────────┘                                         │
│        │                                                                    │
│        ▼                                                                    │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                    datafusion-ext-plans                             │   │
│   │                    (physical operators)                             │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│        │                                                                    │
│        ├──────────────────┬─────────────────────┐                           │
│        ▼                  ▼                     ▼                           │
│   ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐                │
│   │ datafusion-  │  │ datafusion-  │  │ datafusion-ext-    │                │
│   │ ext-exprs    │  │ ext-functions│  │ commons            │                │
│   │ (expressions)│  │ (SQL funcs)  │  │ (shared utilities) │                │
│   └──────────────┘  └──────────────┘  └────────────────────┘                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Reading the Diagram:**
- Arrows point from dependent → dependency (A → B means A depends on B)
- JVM modules compile to JARs; Rust modules compile to a single native library
- The JNI boundary is the only runtime connection between the two sides

---

## 1. Spark Extension Layer (spark-extension)

### Purpose

The core Scala module that integrates Auron with Apache Spark. This is where the "magic" happens—Spark's physical plans get intercepted and converted to native execution.

**Why This Matters:** If you're debugging why a query isn't going native, or adding support for a new operator, this module is your starting point. The conversion logic here determines what runs natively vs. falls back to Spark.

> **Key Insight:** The base classes in this module are abstract—they define *what* native operators do (schema, metrics, Protobuf generation) but not Spark-version-specific details. Concrete implementations live in the shims module.

### Location
`spark-extension/src/main/scala/org/apache/spark/sql/`

### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `auron/AuronSparkSessionExtension.scala` | Main entry point, Spark extension hook | ✅ Yes |
| `auron/AuronSparkSessionExtension.scala` (`AuronColumnarOverrides`) | ColumnarRule that triggers conversion | ✅ Yes |
| `auron/AuronConverters.scala` | Converts SparkPlan to native operators | ✅ Yes |
| `auron/AuronConvertStrategy.scala` | Analyzes plan convertibility | |
| `auron/NativeConverters.scala` | Converts expressions to Protobuf | |
| `auron/NativeRDD.scala` | RDD wrapper for native execution | |
| `auron/NativeHelper.scala` | Utility functions for native execution | |
| `auron/AuronCallNativeWrapper.scala` | Manages single native task execution | |
| `auron/Shims.scala` | Version abstraction interface | |
| `execution/auron/plan/*.scala` | Base classes for all native operators | |

### Key File Details

#### `auron/AuronSparkSessionExtension.scala`

Main code snippet:
```scala
class AuronSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  Shims.get.initExtension()

  override def apply(extensions: SparkSessionExtensions): Unit = {
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
    Shims.get.onApplyingExtension()

    extensions.injectColumnar(sparkSession => {
      AuronColumnarOverrides(sparkSession)
    })
  }
}
```

**How it works:** this is the Spark entry point. Spark loads it from session extension configuration, it forces AQE settings needed by Auron's conversion strategy, initializes Spark-version shims, and injects `AuronColumnarOverrides` so plan conversion happens during Spark's columnar planning phase.

#### `auron/AuronSparkSessionExtension.scala` (`AuronColumnarOverrides`)

Main code snippet:
```scala
case class AuronColumnarOverrides(sparkSession: SparkSession) extends ColumnarRule with Logging {
  override def preColumnarTransitions: Rule[SparkPlan] = {
    new Rule[SparkPlan] {
      override def apply(sparkPlan: SparkPlan): SparkPlan = {
        if (!sparkPlan.conf.getConf(auronEnabledKey)) {
          return sparkPlan
        }
        AuronConvertStrategy.apply(sparkPlan)
        AuronConverters.convertSparkPlanRecursively(sparkPlan)
      }
    }
  }
}
```

**How it works:** this file contains the rule that decides whether a Spark physical plan should be rewritten. It checks `spark.auron.enable`, skips plans that should remain untouched, tags nodes with conversion strategy, and hands the plan to the recursive converter before Spark inserts row/columnar transitions.

#### `auron/AuronConverters.scala`

Main code snippet:
```scala
object AuronConverters extends Logging {
  def convertSparkPlanRecursively(exec: SparkPlan): SparkPlan = {
    var danglingConverted: Seq[SparkPlan] = Nil
    exec.foreachUp { exec =>
      val (newDanglingConverted, newChildren) =
        danglingConverted.splitAt(danglingConverted.length - exec.children.length)

      var newExec = exec.withNewChildren(newChildren)
      exec.getTagValue(convertibleTag).foreach(newExec.setTagValue(convertibleTag, _))
      exec.getTagValue(convertStrategyTag).foreach(newExec.setTagValue(convertStrategyTag, _))

      if (!isNeverConvert(newExec)) {
        newExec = convertSparkPlan(newExec)
      }
      danglingConverted = newDanglingConverted :+ newExec
    }
    danglingConverted.head
  }
}
```

**How it works:** this is the main SparkPlan rewrite engine. It walks the tree bottom-up, preserves unsupported nodes, and asks `Shims` to create concrete `NativeXxxExec` nodes for supported operators. It also owns special handling for adaptive query stages, shuffle boundaries, native wrappers, and extension conversion providers.

#### `auron/AuronConvertStrategy.scala`

Main code snippet:
```scala
object AuronConvertStrategy extends Logging {
  val convertibleTag = TreeNodeTag[Boolean]("auron.convertible")
  val convertStrategyTag = TreeNodeTag[AuronConvertStrategy]("auron.convert.strategy")

  def isNeverConvert(exec: SparkPlan): Boolean = {
    exec.getTagValue(convertStrategyTag).contains(NeverConvert)
  }

  def isAlwaysConvert(exec: SparkPlan): Boolean = {
    exec.getTagValue(convertStrategyTag).contains(AlwaysConvert)
  }
}
```

**How it works:** this file separates convertibility analysis from the actual rewrite. It tags each `SparkPlan` with whether conversion is allowed, records reasons for non-conversion, and removes native islands that would be slower because their children or consumers would still require row-based Spark execution.

#### `auron/NativeConverters.scala`

Main code snippet:
```scala
object NativeConverters extends Logging {
  def convertExpr(sparkExpr: Expression): pb.PhysicalExprNode = {
    def fallbackToError: Expression => pb.PhysicalExprNode = { e =>
      throw new NotImplementedError(s"unsupported expression: (${e.getClass}) $e")
    }

    try {
      // try native conversion first
      convertExprWithFallback(sparkExpr, isPruningExpr = false, fallbackToError)
    } catch {
      case e: NotImplementedError =>
        logWarning(s"Falling back expression: $e")
        // bind convertible children and wrap the remaining Spark expression as a UDF
        val exprString = sparkExpr.toString()
        // fallback wrapper construction continues here
    }
  }
}
```

**How it works:** this file converts Catalyst expressions into Auron Protobuf expression nodes. Operator base classes call it while building `PhysicalPlanNode` messages, so every native filter predicate, projection expression, sort key, join key, and aggregate expression passes through this compatibility layer before Rust sees it.

#### `auron/NativeRDD.scala`

Main code snippet:
```scala
class NativeRDD(
    sc: SparkContext,
    val metrics: MetricNode,
    partitions: Array[Partition],
    partitioner: Option[Partitioner],
    dependencies: Seq[Dependency[_]],
    shuffleReadFull: Boolean,
    val nativePlan: (Partition, TaskContext) => PhysicalPlanNode)
    extends RDD[InternalRow](sc, dependencies) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val computingNativePlan = nativePlan(split, context)
    NativeHelper.executeNativePlan(computingNativePlan, metrics, split, Some(context))
  }
}
```

**How it works:** `NativeRDD` is the Spark execution wrapper for native plans. It keeps the Spark partitioning/dependency contract while delaying Protobuf plan construction until a concrete partition is computed, then delegates execution to `NativeHelper`.

#### `auron/NativeHelper.scala`

Main code snippet:
```scala
object NativeHelper extends Logging {
  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: Option[TaskContext]): Iterator[InternalRow] = {
    if (nativePlan == null) {
      return Iterator.empty
    }
    AuronCallNativeWrapper(nativePlan, partition, context, metrics).getRowIterator
  }
}
```

**How it works:** this file centralizes runtime helpers used by native operators. It creates wrappers for native execution, exposes configured native memory limits, builds standard metrics, and hides the JNI wrapper lifecycle from individual operator implementations.

#### `auron/AuronCallNativeWrapper.scala`

Main code snippet:
```scala
case class AuronCallNativeWrapper(
    nativePlan: PhysicalPlanNode,
    partition: Partition,
    context: Option[TaskContext],
    metrics: MetricNode)
    extends Logging {

  AuronCallNativeWrapper.initNative()

  private var nativeRuntimePtr =
    JniBridge.callNative(NativeHelper.nativeMemory, AuronConf.NATIVE_LOG_LEVEL.stringConf(), this)

  private lazy val rowIterator = new Iterator[InternalRow] {
    override def hasNext: Boolean = {
      if (batchCurRowIdx < batchRows.length) {
        return true
      }
      batchRows.clear()
      batchCurRowIdx = 0
      nativeRuntimePtr != 0 && JniBridge.nextBatch(nativeRuntimePtr) && hasNext
    }

    override def next(): InternalRow = {
      val batchRow = batchRows(batchCurRowIdx)
      batchCurRowIdx += 1
      batchRow
    }
  }

  def getRowIterator: Iterator[InternalRow] =
    CompletionIterator[InternalRow, Iterator[InternalRow]](rowIterator, close())
}
```

**How it works:** this wrapper owns one native task execution from the JVM side. It loads the native library, calls into Rust, receives Arrow FFI callbacks, converts imported Arrow data into Spark rows, updates metrics, and finalizes the native runtime pointer when the Spark iterator closes.

#### `auron/Shims.scala`

Main code snippet:
```scala
abstract class Shims {
  def initExtension(): Unit
  def onApplyingExtension(): Unit

  def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase
  def createNativeProjectExec(projectList: Seq[NamedExpression], child: SparkPlan): NativeProjectBase
}
```

**How it works:** `Shims` is the compatibility interface between common Auron conversion logic and Spark-version-specific classes. The common converter never directly constructs Spark 3.x concrete operators; it calls this interface so each supported Spark version can handle constructor and API differences.

#### `execution/auron/plan/*.scala`

Main code snippet:
```scala
abstract class NativeFilterBase(condition: Expression, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeFilterExprs = this.nativeFilterExprs
    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeFilterExec = FilterExecNode.newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeFilterExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setFilter(nativeFilterExec).build()
      })
  }
}
```

**How it works:** the plan package contains the shared base classes for native Spark operators. Each base class preserves Spark metadata such as output schema, ordering, partitioning, and metrics while implementing `doExecuteNative()` to build the Protobuf operator node consumed by Rust.

### Dependencies
- **Depends on**: Apache Spark core, auron-core
- **Depended by**: spark-extension-shims-spark3

### Entry Points
1. `AuronSparkSessionExtension.apply()` - Called by Spark to register extension
2. `AuronColumnarOverrides.preColumnarTransitions()` - Triggers plan conversion
3. `AuronConverters.convertSparkPlanRecursively()` - Recursive plan conversion

---

---

## 2. Spark Extension Shims (spark-extension-shims-spark3)

### Purpose

Contains Spark 3.x version-specific implementations of native operators. Handles API differences between Spark versions so the core extension code remains clean.

**Why This Matters:** Spark's internal APIs change between minor versions. This module isolates those changes, letting the core conversion logic work identically across Spark 3.2-3.5.

> **Key Insight:** When adding a new operator, you only need to add a thin wrapper here—the actual Protobuf generation and native execution logic stays in the base classes in `spark-extension`.

### Location
`spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/`

### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `auron/ShimsImpl.scala` | Version-specific factory implementation | ✅ Yes |
| `execution/auron/plan/*.scala` | Concrete operator implementations | ✅ Yes |

### Key File Details

#### `auron/ShimsImpl.scala`

Main code snippet:
```scala
class ShimsImpl extends Shims {
  override def createNativeFilterExec(condition: Expression, child: SparkPlan): SparkPlan =
    NativeFilterExec(condition, child)

  override def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan): SparkPlan =
    NativeProjectExec(projectList, child)
}
```

**How it works:** this Spark 3 shim implements the common `Shims` interface with concrete Spark 3 operator classes. When `AuronConverters` decides a plan node should become native, `ShimsImpl` supplies the actual `NativeXxxExec` class that matches the active Spark API.

#### `execution/auron/plan/*.scala`

Main code snippet:
```scala
case class NativeFilterExec(condition: Expression, override val child: SparkPlan)
    extends NativeFilterBase(condition, child) {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
```

**How it works:** the shim plan package contains thin concrete operator classes that extend the shared base classes from `spark-extension`. Their main job is to satisfy Spark-version-specific tree-copying and constructor requirements while leaving native plan generation in the common base classes.

### Version Differences Handled
- `withNewChildInternal()` (Spark 3.2+) vs `withNewChildren()` (Spark 3.0/3.1)
- Column statistics API changes
- Adaptive query execution API evolution
- Expression evaluation changes

### Dependencies
- **Depends on**: spark-extension (base classes)
- **Depended by**: None (end of dependency chain)

### Entry Points
1. `Shims.get` loads `ShimsImpl` via reflection
2. `ShimsImpl.createXxx()` factory methods create operators

---

---

## 3. Protobuf Serialization (auron-serde)

### Purpose

Defines the Protocol Buffer schema for serializing execution plans and expressions. This is the "contract" between Scala and Rust—every operator and expression that crosses the JNI boundary must be defined here.

**Why This Matters:** When adding a new native operator or expression, the Protobuf schema is your first stop. The schema defines what information flows from Scala to Rust.

> **Key Insight:** The `.proto` file is the source of truth. Both Scala (via scalapb) and Rust (via prost) generate code from it, ensuring type-safe serialization across the language boundary.

### Location
`native-engine/auron-serde/`

### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `proto/auron.proto` | Main Protobuf schema definition | ✅ Yes |
| `src/from_proto.rs` | Deserialize Protobuf to Rust types | ✅ Yes |
| `src/lib.rs` | Module exports | |
| `build.rs` | Code generation during build | |

### Key File Details

#### `proto/auron.proto`

Main code snippet:
```protobuf
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    ShuffleWriterExecNode shuffle_writer = 2;
    ParquetScanExecNode parquet_scan = 5;
    ProjectionExecNode projection = 6;
    SortExecNode sort = 7;
    FilterExecNode filter = 8;
    AggExecNode agg = 16;
    WindowExecNode window = 22;
    OrcScanExecNode orc_scan = 25;
  }
}
```

**How it works:** this schema is the wire contract between Scala and Rust. Scala operator base classes build these messages, Java serializes them as part of `TaskDefinition`, and Rust deserializes them into DataFusion operators. Adding a native operator or expression requires extending this schema first.

#### `src/from_proto.rs`

Main code snippet:
```rust
impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!("unsupported physical plan '{:?}'", self))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => { /* build ProjectExec */ }
            PhysicalPlanType::Filter(filter) => { /* build FilterExec */ }
            PhysicalPlanType::Agg(agg) => { /* build AggExec */ }
            _ => { /* other operators */ }
        }
    }
}
```

**How it works:** this is the Rust deserialization layer for physical plans and expressions. It validates required Protobuf fields, converts nested inputs recursively, maps Spark-compatible expression/function names, and returns concrete `Arc<dyn ExecutionPlan>` values for the native runtime.

#### `src/lib.rs`

Main code snippet:
```rust
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/plan.protobuf.rs"));
}

pub mod error;
pub mod from_proto;

#[macro_export]
macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}
```

**How it works:** `lib.rs` exposes generated Protobuf types, the `from_proto` module, serde errors, and helper macros for required-field conversion. The generated Rust code is included from Cargo's build output, keeping checked-in source focused on conversion logic.

#### `build.rs`

Main code snippet:
```rust
fn main() -> Result<(), String> {
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rerun-if-changed=proto/auron.proto");

    let mut prost_build = tonic_build::Config::new();
    prost_build
        .compile_protos(&["proto/auron.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))
}
```

**How it works:** this Cargo build script generates Rust Protobuf bindings from `proto/auron.proto`. It also supports the repository's Maven-provided `protoc` path, making native builds align with the Java/Scala Protobuf generation pipeline.

### Dependencies
- **Depends on**: datafusion-ext-commons, prost (protobuf library)
- **Depended by**: auron (main runtime)

### Entry Points
1. Protobuf schema defines serialization format
2. `from_proto::convert_physical_plan()` - Plan deserialization
3. `from_proto::convert_physical_expr()` - Expression deserialization

---

---

## 4. JNI Bridge Layer

This spans two locations: Java/Scala side and Rust side. Together, they form the bridge that lets Spark call into native code and receive results back.

**Why This Matters:** JNI is notoriously tricky—memory management, threading, and error handling all require careful coordination. This module encapsulates that complexity so operator implementations don't need to worry about it.

> **Key Insight:** The JNI bridge uses a "pull" model: Spark calls `nextBatch()` repeatedly, and Rust responds with Arrow data via FFI callbacks. This keeps control flow simple and avoids complex async coordination.

### Java Side (auron-core)

#### Purpose
Provides the Java interface for calling into native Rust code via JNI. Also manages configuration and spill support.

#### Location
`auron-core/src/main/java/org/apache/auron/`

#### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `jni/JniBridge.java` | JNI native method declarations | ✅ Yes |
| `jni/AuronAdaptor.java` | Abstract adaptor for Auron engine | ✅ Yes |
| `configuration/AuronConfiguration.java` | Engine configuration interface | |
| `memory/OnHeapSpillManager.java` | JVM memory management | |

#### Key File Details

##### `jni/JniBridge.java`

Main code snippet:
```java
public class JniBridge {
    public static native long callNative(
            long initNativeMemory,
            String logLevel,
            AuronCallNativeWrapper wrapper);

    public static native boolean nextBatch(long ptr);
    public static native void finalizeNative(long ptr);
    public static native void onExit();
}
```

**How it works:** this class declares the JVM methods implemented by Rust JNI symbols. It also exposes callback helpers Rust needs, including resource lookup, Hadoop file wrappers, direct-memory usage, task-running checks, and access to the current `OnHeapSpillManager`.

##### `jni/AuronAdaptor.java`

Main code snippet:
```java
public abstract class AuronAdaptor {
    private static AuronAdaptor INSTANCE = null;

    public static synchronized void initInstance(AuronAdaptor auronAdaptor) {
        if (INSTANCE == null) {
            INSTANCE = auronAdaptor;
        }
    }

    public abstract void loadAuronLib();
    public abstract AuronConfiguration getAuronConfiguration();
}
```

**How it works:** `AuronAdaptor` is the Java-side embedding interface for the native engine. Spark-specific code installs an implementation that knows how to load the native library, expose engine configuration, report memory limits, provide spill management, and create UDF wrapper contexts.

##### `configuration/AuronConfiguration.java`

Main code snippet:
```java
public abstract class AuronConfiguration {
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("auron.batchSize").intType().defaultValue(10000);

    public static final ConfigOption<Double> MEMORY_FRACTION =
            ConfigOptions.key("auron.memoryFraction").doubleType().defaultValue(0.6);

    public abstract <T> Optional<T> getOptional(ConfigOption<T> option);

    public <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }
}
```

**How it works:** this interface abstracts configuration lookup for Java and Rust callback code. The native side can request Spark/Auron settings through JNI without depending directly on Spark's `SparkConf` implementation.

##### `memory/OnHeapSpillManager.java`

Main code snippet:
```java
public abstract class OnHeapSpillManager {
    abstract boolean isOnHeapAvailable();
    abstract int newSpill();
    abstract void writeSpill(int spillId, ByteBuffer buffer);
    abstract int readSpill(int spillId, ByteBuffer buffer);
    abstract void releaseSpill(int spillId);
}
```

**How it works:** this class is the JVM spill contract used by native code when data must be staged through Spark-managed on-heap resources. The disabled implementation throws for spill operations; Spark integrations provide a real task-scoped manager when on-heap spilling is available.

#### Core Abstractions

```java
// auron-core/.../jni/JniBridge.java:20
public class JniBridge {
    // Start native execution, returns runtime pointer
    public static native long callNative(
        long initNativeMemory,
        String logLevel,
        AuronCallNativeWrapper wrapper);

    // Get next Arrow batch
    public static native boolean nextBatch(long ptr);

    // Release native resources
    public static native void finalizeNative(long ptr);

    // Cleanup on JVM exit
    public static native void onExit();
}
```

### Rust Side (auron-jni-bridge)

#### Purpose
Rust utilities for JNI interop: thread-local JNI environment, local reference management, Java class caching.

#### Location
`native-engine/auron-jni-bridge/src/`

#### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `jni_bridge.rs` | JNI utilities and thread-local env | ✅ Yes |
| `lib.rs` | Module exports | |

#### Key File Details

##### `jni_bridge.rs`

Main code snippet:
```rust
thread_local! {
    pub static THREAD_JNIENV: RefCell<Option<*mut JNIEnv>> = RefCell::new(None);
}

pub struct JavaClasses {
    pub classloader: GlobalRef,
    pub jni_bridge: GlobalRef,
    pub auron_call_native_wrapper: GlobalRef,
    pub runtime_exception: GlobalRef,
}
```

**How it works:** this file owns the low-level JNI utility layer used by Rust. It stores the thread-local `JNIEnv`, caches Java class and method references, and defines macros/helpers that make Rust-to-Java calls concise while preserving exception handling.

##### `lib.rs`

Main code snippet:
```rust
pub mod conf;
pub mod jni_bridge;

pub fn ensure_jni_bridge_inited() -> Result<()> {
    if is_jni_bridge_inited() {
        Ok(())
    } else {
        Err(DataFusionError::Execution("JNIEnv not initialized".to_string()))
    }
}

pub fn is_task_running() -> bool {
    if !is_jni_bridge_inited() {
        return true;
    }
    is_task_running_impl().expect("calling JniBridge.isTaskRunning() error")
}
```

**How it works:** `lib.rs` exposes JNI bridge modules and task-state helpers to the rest of the native engine. Operators and runtime code call these helpers to ensure JNI has been initialized and to stop native work when Spark cancels or completes a task.

#### Core Abstractions

```rust
// native-engine/auron-jni-bridge/src/jni_bridge.rs:30
thread_local! {
    // Thread-local JNI environment
    pub static THREAD_JNIENV: RefCell<Option<*mut JNIEnv>> = RefCell::new(None);
}

// RAII wrapper for JNI local references
pub struct LocalRef<'a> {
    obj: JObject<'a>,
    env: &'a JNIEnv<'a>,
}

// Cached Java class references
pub struct JavaClasses {
    pub auron_call_native_wrapper: GlobalRef,
    pub runtime_exception: GlobalRef,
    // ... other cached classes
}
```

### JNI Call Flow
`JniBridge.callNative()` initializes the Rust-side JNI bridge on first use, configures logging and DataFusion session state, creates a `NativeExecutionRuntime`, and returns its raw pointer as a JVM `long`. `JniBridge.nextBatch(ptr)` asks that runtime for the next Arrow batch and calls back into `AuronCallNativeWrapper.importBatch()` to hand the FFI pointers to the JVM. `JniBridge.finalizeNative(ptr)` consumes the pointer and drops the runtime so Rust resources are released.

### Dependencies
- **Java depends on**: JNI libraries
- **Rust depends on**: jni crate
- **Depended by**: auron (main runtime)

---

---

## 5. Rust Execution Engine (auron)

### Purpose

Main Rust crate that coordinates native execution. This is the "front door" for JNI calls—it receives serialized plans from Spark and orchestrates DataFusion execution.

**Why This Matters:** This module owns the runtime lifecycle. Understanding it helps debug issues like memory leaks, task cancellation, and native crashes.

> **Key Insight:** Each Spark task gets its own `NativeExecutionRuntime` instance. The runtime is created on `callNative()`, produces batches via `nextBatch()`, and is destroyed on `finalizeNative()`. This 1:1 mapping keeps isolation simple.

### Location
`native-engine/auron/src/`

### Key Files

| File | Purpose | Start Here? |
|------|---------|-------------|
| `lib.rs` | Panic handling and module wiring | |
| `rt.rs` | NativeExecutionRuntime definition | ✅ Yes |
| `exec.rs` | JNI entry points and runtime creation | ✅ Yes |
| `logging.rs` | Logging configuration | |

### Key File Details

#### `lib.rs`

Main code snippet:
```rust
mod alloc;
mod exec;
mod logging;
mod metrics;
mod rt;

fn handle_unwinded_scope<T: Default, E: Debug>(scope: impl FnOnce() -> Result<T, E>) -> T {
    match std::panic::catch_unwind(AssertUnwindSafe(|| scope().unwrap())) {
        Ok(v) => v,
        Err(err) => {
            handle_unwinded(err);
            T::default()
        }
    }
}
```

**How it works:** this file wires the native runtime modules together and provides shared panic-handling helpers for JNI entry points. The exported JNI functions live in `exec.rs`, but they use `handle_unwinded_scope()` from this file so Rust panics become JVM exceptions instead of unwinding across JNI.

#### `rt.rs`

Main code snippet:
```rust
pub struct NativeExecutionRuntime {
    exec_ctx: Arc<ExecutionContext>,
    native_wrapper: GlobalRef,
    plan: Arc<dyn ExecutionPlan>,
    batch_receiver: Receiver<Result<Option<RecordBatch>>>,
    tokio_runtime: Runtime,
    join_handle: JoinHandle<()>,
}

impl NativeExecutionRuntime {
    pub fn start(native_wrapper: GlobalRef, context: Arc<TaskContext>) -> Result<Self> {
        let task_definition = TaskDefinition::decode(raw_task_definition.as_slice())?;
        let execution_plan: Arc<dyn ExecutionPlan> = plan.try_into()?;
        // start async execution and return the runtime holder
    }
}
```

**How it works:** `rt.rs` owns the lifetime of one native task. It decodes the JVM-provided task definition, turns the Protobuf plan into a DataFusion plan, starts execution on a Tokio runtime, receives output batches, and calls back into the JVM wrapper for Arrow FFI import.

#### `exec.rs`

Main code snippet:
```rust
#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_spark_sql_auron_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    executor_memory_overhead: i64,
    log_level: JString,
    native_wrapper: JObject,
) -> i64 {
    handle_unwinded_scope(|| -> Result<i64> {
        JavaClasses::init(&env);
        MemManager::init((executor_memory_overhead as f64 * memory_fraction) as usize);
        let runtime = Box::new(NativeExecutionRuntime::start(native_wrapper, task_ctx)?);
        Ok(Box::into_raw(runtime) as usize as i64)
    })
}

pub extern "system" fn Java_org_apache_spark_sql_auron_JniBridge_nextBatch(
    _: JNIEnv,
    _: JClass,
    raw_ptr: i64,
) -> bool {
    let runtime = unsafe { &*(raw_ptr as usize as *const NativeExecutionRuntime) };
    runtime.next_batch()
}
```

**How it works:** this file contains the JNI methods Java calls after loading the native library. It initializes the native environment on the first `callNative()`, creates `NativeExecutionRuntime`, drives the stream one batch at a time through `nextBatch()`, and finalizes runtime pointers when the JVM asks for cleanup.

#### `logging.rs`

Main code snippet:
```rust
thread_local! {
    pub static THREAD_TID: Cell<usize> = Cell::new(0);
    pub static THREAD_STAGE_ID: Cell<usize> = Cell::new(0);
    pub static THREAD_PARTITION_ID: Cell<usize> = Cell::new(0);
}

pub fn init_logging(level: &str) {
    let log_level = Level::from_str(level).unwrap_or(DEFAULT_MAX_LEVEL);
    log::set_max_level(LevelFilter::Info);
}
```

**How it works:** native logging is initialized once and enriches log lines with task, stage, and partition identifiers. Runtime worker threads set those thread-local values so native logs can be correlated with Spark task execution.

### Dependencies
- **Depends on**: auron-serde, auron-jni-bridge, datafusion-ext-plans, DataFusion
- **Depended by**: None (top of Rust dependency chain)

### Entry Points
1. `Java_org_apache_spark_sql_auron_JniBridge_callNative` - Start execution
2. `Java_org_apache_spark_sql_auron_JniBridge_nextBatch` - Get next batch
3. `Java_org_apache_spark_sql_auron_JniBridge_finalizeNative` - Cleanup

---

---

## 6. DataFusion Extensions (datafusion-ext-*)

### Purpose

Custom DataFusion physical plan operators and expressions that extend DataFusion's capabilities for Spark compatibility. While DataFusion provides a solid foundation, Spark has specific behaviors that require custom implementations.

**Why This Matters:** This is where the actual computation happens. Understanding these operators helps debug performance issues and extend Auron with new capabilities.

> **Key Insight:** These are *not* forks of DataFusion—they're extensions that work alongside DataFusion's built-in operators. Auron uses DataFusion's execution framework but provides Spark-compatible implementations for joins, aggregations, and other operations.

### Location
`native-engine/datafusion-ext-plans/src/` and related crates

### Key Files

| Crate | Key Files | Purpose |
|-------|-----------|---------|
| **datafusion-ext-plans** | | |
| | `lib.rs` | Module exports |
| | `agg_exec.rs` | Hash/Sort aggregation |
| | `filter_exec.rs` | Filter execution |
| | `project_exec.rs` | Projection execution |
| | `sort_exec.rs` | Sort execution |
| | `sort_merge_join_exec.rs` | Sort-merge join |
| | `broadcast_join_exec.rs` | Broadcast join |
| | `shuffle_writer_exec.rs` | Shuffle write |
| | `parquet_exec.rs` | Parquet scan |
| | `window_exec.rs` and `window/` | Window functions |
| | `memmgr/mod.rs` | Memory management |
| **datafusion-ext-exprs** | | |
| | `lib.rs` | Expression exports |
| | `cast.rs` | Cast expressions |
| | `string_contains.rs`, `string_starts_with.rs`, `string_ends_with.rs` | String predicates |
| **datafusion-ext-functions** | | |
| | `lib.rs` | SQL function registry |
| **datafusion-ext-commons** | | |
| | `lib.rs` | Shared utilities |

### Key File Details

#### `datafusion-ext-plans/src/lib.rs`

Main code snippet:
```rust
pub mod agg;
pub mod agg_exec;
pub mod broadcast_join_exec;
pub mod filter_exec;
pub mod parquet_exec;
pub mod project_exec;
pub mod shuffle_writer_exec;
pub mod sort_exec;
pub mod sort_merge_join_exec;
pub mod window_exec;

pub mod memmgr;
pub mod common;
pub mod shuffle;
pub mod window;
```

**How it works:** this crate root exposes Auron's custom DataFusion physical operators and helper modules. `auron-serde` imports these modules when turning Protobuf nodes into executable plans, so new native operators must be exported here before they can be deserialized.

#### `datafusion-ext-plans/src/agg_exec.rs`

Main code snippet:
```rust
#[derive(Debug)]
pub struct AggExec {
    input: Arc<dyn ExecutionPlan>,
    agg_ctx: Arc<AggContext>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl AggExec {
    pub fn try_new(
        exec_mode: AggExecMode,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        supports_partial_skipping: bool,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let agg_ctx = Arc::new(AggContext::try_new(exec_mode, input.schema(), groupings, aggs, supports_partial_skipping, false)?);
        Ok(Self { input, agg_ctx, metrics: ExecutionPlanMetricsSet::new(), props: OnceCell::new() })
    }
}
```

**How it works:** `AggExec` implements native hash and sort aggregation. It builds an `AggContext` from grouping and aggregate expressions, manages aggregate state through the `agg` submodule, integrates with native memory management, and emits DataFusion-compatible record batches.

#### `datafusion-ext-plans/src/filter_exec.rs`

Main code snippet:
```rust
#[derive(Debug, Clone)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicates: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl FilterExec {
    pub fn try_new(predicates: Vec<PhysicalExprRef>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if predicates.is_empty() {
            df_execution_err!("Filter requires at least one predicate")?;
        }
        Ok(Self { input, predicates, metrics: ExecutionPlanMetricsSet::new(), props: OnceCell::new() })
    }
}
```

**How it works:** the native filter operator validates boolean predicates, executes its input stream, evaluates predicates against each Arrow batch, and returns only matching rows. It also participates in column pruning when downstream operators do not require every input column.

#### `datafusion-ext-plans/src/project_exec.rs`

Main code snippet:
```rust
#[derive(Debug, Clone)]
pub struct ProjectExec {
    expr: Vec<(PhysicalExprRef, String)>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl ProjectExec {
    pub fn try_new(expr: Vec<(PhysicalExprRef, String)>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let schema = Arc::new(Schema::new(expr.iter().map(|(e, name)| {
            Ok(Field::new(name, e.data_type(&input_schema)?, e.nullable(&input_schema)?))
        }).collect::<Result<Fields>>()?));
        Ok(Self { expr, input, schema, metrics: ExecutionPlanMetricsSet::new(), props: OnceCell::new() })
    }
}
```

**How it works:** `ProjectExec` evaluates native expressions and constructs the output Arrow schema from expression data types and nullability. It is used for Spark projections, computed columns, and intermediate projections inserted to support pruning or operator-specific layouts.

#### `datafusion-ext-plans/src/sort_exec.rs`

Main code snippet:
```rust
pub struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<PhysicalSortExpr>,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    record_output: bool,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** this file implements native sort with memory-aware buffering and spill support. It evaluates sort keys, stores sorted blocks in memory or spill files, and performs k-way merge when needed to produce globally ordered Arrow batches.

#### `datafusion-ext-plans/src/sort_merge_join_exec.rs`

Main code snippet:
```rust
pub struct SortMergeJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** this operator performs Spark-compatible sort-merge joins over sorted left and right streams. It owns join-key comparison, stream cursor management, join-type behavior, output projection, and metrics for matched/unmatched rows.

#### `datafusion-ext-plans/src/broadcast_join_exec.rs`

Main code snippet:
```rust
pub struct BroadcastJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    broadcast_side: JoinSide,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** broadcast join reads a prebuilt small-side relation and probes it from the streamed side. This file coordinates build/probe schemas, join projection, join type semantics, and column pruning for native broadcast hash join execution.

#### `datafusion-ext-plans/src/shuffle_writer_exec.rs`

Main code snippet:
```rust
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
    output_data_file: String,
    output_index_file: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** this operator writes native shuffle output for Spark stages. It evaluates partitioning, repartitions Arrow batches, writes IPC/compressed blocks, and reports shuffle metrics back through Spark's native wrapper.

#### `datafusion-ext-plans/src/parquet_exec.rs`

Main code snippet:
```rust
pub struct ParquetExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    predicate: Option<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** native Parquet scan builds DataFusion file-scan configuration from Spark file metadata. It applies projection, predicate pushdown, schema adaptation, metrics collection, and Hadoop filesystem access through JNI-backed wrappers.

#### `datafusion-ext-plans/src/window_exec.rs` and `window/`

Main code snippet:
```rust
pub struct WindowExec {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<WindowContext>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}
```

**How it works:** `window_exec.rs` is the physical operator wrapper and `window/` contains the implementation details for window contexts and functions. Together they evaluate partitioned, ordered window expressions such as rank-like functions while preserving Spark output schema expectations.

#### `datafusion-ext-plans/src/memmgr/mod.rs`

Main code snippet:
```rust
pub struct MemManager {
    total: usize,
    consumers: Mutex<Vec<Arc<MemConsumerInfo>>>,
    status: Mutex<MemManagerStatus>,
    cv: Condvar,
}

impl MemManager {
    pub fn init(total: usize) {
        MEM_MANAGER.get_or_init(|| Arc::new(MemManager {
            total,
            consumers: Mutex::default(),
            status: Mutex::default(),
            cv: Condvar::default(),
        }));
    }
}
```

**How it works:** `MemManager` is the native memory coordinator. Sort, aggregation, and shuffle consumers register with it, reserve memory before growing buffers, and trigger spill behavior when usage approaches the configured native memory limit.

#### `datafusion-ext-exprs/src/lib.rs`

Main code snippet:
```rust
pub mod bloom_filter_might_contain;
pub mod cast;
pub mod get_indexed_field;
pub mod get_map_value;
pub mod named_struct;
pub mod row_num;
pub mod spark_udf_wrapper;
pub mod string_contains;
pub mod string_ends_with;
pub mod string_starts_with;
```

**How it works:** this crate exposes physical expression implementations that DataFusion does not provide with Spark-compatible behavior. `from_proto.rs` constructs these expressions for casts, nested-field access, string predicates, row numbers, scalar subqueries, and Spark UDF callbacks.

#### `datafusion-ext-exprs/src/cast.rs`

Main code snippet:
```rust
pub struct TryCastExpr {
    expr: PhysicalExprRef,
    cast_type: DataType,
}
```

**How it works:** cast support handles Spark-specific conversion behavior that differs from vanilla DataFusion. The serde layer uses these expressions when Spark plans contain `CAST` or `TRY_CAST` nodes that need native evaluation.

#### `datafusion-ext-exprs/src/string_contains.rs`, `string_starts_with.rs`, `string_ends_with.rs`

Main code snippet:
```rust
pub struct StringContainsExpr {
    expr: PhysicalExprRef,
    infix: String,
}
```

**How it works:** these files implement Spark string predicate expressions as DataFusion physical expressions. They allow converted Spark filters and projections to evaluate `contains`, `startsWith`, and `endsWith` semantics natively over Arrow string arrays.

#### `datafusion-ext-functions/src/lib.rs`

Main code snippet:
```rust
pub fn create_spark_ext_function(name: &str) -> Result<ScalarFunctionImplementation> {
    Ok(match name {
        "NullIf" => Arc::new(spark_null_if::spark_null_if),
        "MakeDecimal" => Arc::new(spark_make_decimal::spark_make_decimal),
        "GetJsonObject" => Arc::new(spark_get_json_object::spark_get_json_object),
        "StringConcat" => Arc::new(spark_strings::string_concat),
        "Year" => Arc::new(spark_dates::spark_year),
        _ => df_unimplemented_err!("spark ext function not implemented: {name}")?,
    })
}
```

**How it works:** this crate is the registry for scalar functions that need Spark-compatible behavior. During Protobuf expression deserialization, Spark function names are resolved here into DataFusion scalar function implementations.

#### `datafusion-ext-commons/src/lib.rs`

Main code snippet:
```rust
#[macro_export]
macro_rules! df_execution_err {
    ($($arg:tt)*) => {
        Err(datafusion::common::DataFusionError::Execution(format!($($arg)*)))
    }
}

pub fn batch_size() -> usize {
    const CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();
    *CACHED_BATCH_SIZE.get_or_init(|| BATCH_SIZE.value().unwrap_or(10000) as usize)
}
```

**How it works:** commons contains shared error macros, Arrow helpers, hashing utilities, batch sizing, serialization helpers, and Spark-compatible scalar structures. It is intentionally dependency-light so plans, expressions, functions, and serde code can share common behavior.

### Dependencies
- **datafusion-ext-plans depends on**: DataFusion, datafusion-ext-exprs, datafusion-ext-functions, datafusion-ext-commons
- **datafusion-ext-exprs depends on**: DataFusion, datafusion-ext-commons
- **datafusion-ext-functions depends on**: DataFusion, datafusion-ext-commons
- **datafusion-ext-commons depends on**: Arrow, DataFusion (minimal)

### Entry Points
1. Operators registered via `from_proto.rs` deserialization
2. Direct instantiation in Rust code for testing

---

## Module Summary

| Module | Language | Lines of Code* | Primary Responsibility |
|--------|----------|----------------|------------------------|
| spark-extension | Scala | ~15,000 | Spark integration, base operators |
| spark-extension-shims-spark3 | Scala | ~5,000 | Spark 3.x compatibility |
| auron-core | Java | ~3,000 | JNI bridge, configuration |
| auron | Rust | ~2,000 | Runtime coordination |
| auron-serde | Rust | ~4,000 | Protobuf serialization |
| auron-jni-bridge | Rust | ~1,000 | JNI utilities |
| datafusion-ext-plans | Rust | ~20,000 | Physical operators |
| datafusion-ext-exprs | Rust | ~5,000 | Expression evaluation |
| datafusion-ext-functions | Rust | ~2,000 | SQL functions |
| datafusion-ext-commons | Rust | ~3,000 | Shared utilities |

*Approximate, for relative comparison

## Next Steps

| Goal | Next Section |
|------|--------------|
| See how code flows end-to-end | [Critical Execution Paths](#critical-execution-paths) |
| Add a new operator or expression | [Extension Guide](#auron-extension-guide) |
| Understand core operations | [How It Works](#how-it-works) |

---

# Critical Execution Paths

> End-to-end code traces showing how queries flow through the system. Use these paths when debugging issues or understanding how components connect.

## Quick Reference: Common Entry Points

| I want to... | Start here | Key function |
|--------------|------------|--------------|
| Debug why a query isn't native | `AuronConvertStrategy.scala` | `analyzeConvertibility()` |
| Trace plan conversion | `AuronConverters.scala` | `convertSparkPlanRecursively()` |
| Debug native execution | `exec.rs` | `Java_..._callNative()` |
| Trace expression conversion | `NativeConverters.scala` | `convertExpr()` |
| Debug memory issues | `memmgr/mod.rs` | `MemManager::try_grow()` |
| Trace Arrow data flow | `AuronCallNativeWrapper.scala` | `importBatch()` |

---

## Path 1: Query Conversion (SparkPlan → NativeExec → Protobuf)

This path shows how a Spark physical plan is converted to native execution. Follow this when debugging why an operator is or isn't going native.

This path shows how a Spark physical plan is converted to native execution.

### Step 1: Extension Registration

**What happens:** When Spark starts, it loads configured extensions. Auron registers its columnar rule here.

**What to look for:** If Auron isn't activating at all, check that the extension is properly configured in `spark.sql.extensions`.

```scala
// File: spark-extension/.../AuronSparkSessionExtension.scala

class AuronSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    if (conf.auronEnabled) {
      extensions.injectColumnar(_ => AuronColumnarOverrides)
      // Forces adaptive execution for better plan optimization
    }
  }
}
```

### Step 2: Columnar Rule Triggers

**What happens:** Before Spark finalizes the physical plan, it runs columnar rules. Auron's rule intercepts the plan here.

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronSparkSessionExtension.scala

object AuronColumnarOverrides extends ColumnarRule {
  override def preColumnarTransitions(plan: SparkPlan): SparkPlan = {
    // 1. Analyze plan convertibility
    AuronConvertStrategy.apply(plan)

    // 2. Convert to native operators
    val converted = AuronConverters.convertSparkPlanRecursively(plan)

    // 3. Apply version-specific transforms
    Shims.get.postTransform(converted)
  }
}
```

### Step 3: Plan Analysis

**What happens:** Before converting, Auron analyzes each node to determine if it can go native. Results are tagged on nodes.

**What to look for:** If an operator isn't converting, check the `neverConvertReasonTag`—it contains the specific reason (unsupported expression, data type, etc.).

**Debugging tip:** Enable debug logging to see conversion decisions:
```scala
spark.conf.set("spark.auron.log.level", "debug")
```

```scala
// File: spark-extension/.../AuronConvertStrategy.scala

object AuronConvertStrategy {
  def apply(plan: SparkPlan): Unit = {
    plan.foreach { node =>
      val canConvert = analyzeConvertibility(node)
      node.setTagValue(convertibleTag, canConvert)
      if (!canConvert) {
        node.setTagValue(neverConvertReasonTag, reason)
      }
    }
  }

  private def analyzeConvertibility(node: SparkPlan): Boolean = {
    // 1. Check operator type support
    // 2. Check expression support
    // 3. Check data type support
    // 4. Propagate from children (if child can't convert, parent might not either)
  }
}
```

### Step 4: Recursive Conversion

**What happens:** With convertibility determined, the converter walks the tree bottom-up, replacing supported Spark operators with native equivalents.

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala

object AuronConverters {
  def convertSparkPlanRecursively(plan: SparkPlan): SparkPlan = {
    plan match {
      case filter: FilterExec if canConvert(filter) =>
        val nativeChild = convertSparkPlanRecursively(filter.child)
        Shims.get.createNativeFilterExec(filter.condition, nativeChild)

      case project: ProjectExec if canConvert(project) =>
        val nativeChild = convertSparkPlanRecursively(project.child)
        Shims.get.createNativeProjectExec(project.projectList, nativeChild)

      case agg: HashAggregateExec if canConvert(agg) =>
        // Convert to NativeAggExec
        convertAggregate(agg)

      case join: SortMergeJoinExec if canConvert(join) =>
        // Convert to NativeSortMergeJoinExec
        convertSortMergeJoin(join)

      // ... more operator patterns

      case other =>
        // Not convertible, keep as Spark operator
        other.withNewChildren(other.children.map(convertSparkPlanRecursively))
    }
  }
}
```

### Step 5: Shims Creates Concrete Operator

```
File: spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/auron/ShimsImpl.scala

class ShimsImpl extends Shims {
  override def createNativeFilterExec(
      condition: Expression,
      child: SparkPlan): SparkPlan = {
    NativeFilterExec(condition, child)
  }
}

File: spark-extension-shims-spark3/.../plan/NativeFilterExec.scala

case class NativeFilterExec(condition: Expression, override val child: SparkPlan)
    extends NativeFilterBase(condition, child) {

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
```

### Step 6: Base Class Generates Protobuf

```
File: spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeFilterBase.scala

abstract class NativeFilterBase(condition: Expression, override val child: SparkPlan)
    extends UnaryExecNode with NativeSupports {

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val metrics = MetricNode(getDefaultNativeMetrics(), Nil)

    new NativeRDD(
      sparkContext,
      metrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      (partition, taskContext) => {
        // This closure is called per-partition
        val inputPlan = inputRDD.nativePlan(partition, taskContext)

        // Convert condition to Protobuf expression
        val filterExpr = NativeConverters.convertExpr(condition)

        // Create FilterExecNode protobuf message
        pb.PhysicalPlanNode.newBuilder()
          .setFilter(
            pb.FilterExecNode.newBuilder()
              .setInput(inputPlan)
              .addExpr(filterExpr)
          )
          .build()
      }
    )
  }
}
```

### Step 7: Expression Conversion

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeConverters.scala

object NativeConverters {
  def convertExpr(expr: Expression): pb.PhysicalExprNode = {
    expr match {
      case attr: AttributeReference =>
        pb.PhysicalExprNode.newBuilder()
          .setColumn(pb.PhysicalColumn.newBuilder()
            .setName(attr.name)
            .setIndex(attr.exprId.id.toInt))
          .build()

      case lit: Literal =>
        pb.PhysicalExprNode.newBuilder()
          .setLiteral(convertLiteral(lit))
          .build()

      case BinaryComparison(left, right) =>
        pb.PhysicalExprNode.newBuilder()
          .setBinaryExpr(pb.PhysicalBinaryExprNode.newBuilder()
            .setL(convertExpr(left))
            .setR(convertExpr(right))
            .setOp(convertOperator(expr)))
          .build()

      // ... more expression patterns
    }
  }
}
```

---

---

## Path 2: Execution (JNI → Rust Deserialization → DataFusion)

This path traces how a serialized plan is executed in the native engine. Follow this when debugging native execution issues or understanding the JNI boundary.

**Performance note:** The JNI boundary crossing happens once per task (for setup) and once per batch (for data). Batch sizes of ~10,000 rows amortize the overhead effectively.

### Step 1: NativeRDD Compute

**What happens:** Spark schedules the task, and `NativeRDD.compute()` is called for each partition.

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeRDD.scala

class NativeRDD(
    @transient private val _sc: SparkContext,
    metrics: MetricNode,
    partitions: Array[Partition],
    deps: Seq[Dependency[_]],
    nativePlan: (Partition, TaskContext) => pb.PhysicalPlanNode
) extends RDD[InternalRow](_sc, deps) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // Generate plan for this partition
    val plan = nativePlan(split, context)

    // Execute via native helper
    NativeHelper.executeNativePlan(plan, metrics, context)
  }
}
```

### Step 2: Native Helper Execution

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeHelper.scala

object NativeHelper {
  def executeNativePlan(
      plan: pb.PhysicalPlanNode,
      metrics: MetricNode,
      context: TaskContext): Iterator[InternalRow] = {

    // Create task definition
    val taskDef = pb.TaskDefinition.newBuilder()
      .setPlan(plan)
      .setTaskId(context.taskAttemptId())
      .setPartitionId(context.partitionId())
      .build()

    // Create native wrapper
    val wrapper = new AuronCallNativeWrapper(
      taskDef.toByteArray,
      metrics,
      context
    )

    // Return row iterator
    wrapper.getRowIterator
  }
}
```

### Step 3: JNI Call Initialization

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronCallNativeWrapper.scala

class AuronCallNativeWrapper(
    taskDefBytes: Array[Byte],
    metrics: MetricNode,
    context: TaskContext) {

  // Load native library
  AuronAdaptor.loadAuronLib()

  // Initialize native execution
  private val runtimePtr: Long = JniBridge.callNative(
    NativeHelper.nativeMemory,
    logLevel,
    this  // Pass wrapper for callbacks
  )

  def getRowIterator: Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var currentBatch: ColumnarBatch = _
      private var rowIndex: Int = 0

      override def hasNext: Boolean = {
        if (rowIndex < currentBatch.numRows()) {
          true
        } else {
          // Get next batch from native
          if (JniBridge.nextBatch(runtimePtr)) {
            rowIndex = 0
            true
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = {
        val row = currentBatch.getRow(rowIndex)
        rowIndex += 1
        row
      }
    }
  }

  // Called from Rust via JNI
  def importSchema(ffiSchemaPtr: Long): Unit = {
    schema = ArrowSchema.import(ffiSchemaPtr)
  }

  // Called from Rust via JNI
  def importBatch(ffiArrayPtr: Long): Unit = {
    val batch = ArrowRecordBatch.import(ffiArrayPtr, schema)
    currentBatch = toColumnarBatch(batch)
  }
}
```

### Step 4: Rust JNI Entry Point

```
File: native-engine/auron/src/lib.rs

#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_auron_JniBridge_callNative(
    env: JNIEnv,
    _class: JClass,
    executor_memory_overhead: i64,
    log_level: JString,
    native_wrapper: JObject,
) -> i64 {
    // Set up thread-local JNI environment
    jni_bridge::set_thread_jnienv(env);

    // Initialize logging
    init_logging(&log_level);

    // Create DataFusion session context
    let session_ctx = create_session_context(executor_memory_overhead);

    // Get task definition from wrapper
    let task_def_bytes = get_task_definition(env, &native_wrapper);

    // Deserialize protobuf
    let task_def: TaskDefinition = TaskDefinition::decode(&task_def_bytes[..])?;

    // Convert to DataFusion plan
    let plan = from_proto::convert_physical_plan(&task_def.plan, &session_ctx)?;

    // Create runtime
    let runtime = NativeExecutionRuntime::new(
        session_ctx,
        plan,
        task_def.partition_id as usize,
        GlobalRef::new(env, native_wrapper),
    );

    // Return pointer
    Box::into_raw(Box::new(runtime)) as i64
}
```

### Step 5: Protobuf Deserialization

```
File: native-engine/auron-serde/src/from_proto.rs

pub fn convert_physical_plan(
    plan: &PhysicalPlanNode,
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    use plan::PhysicalPlanType::*;

    match plan.physical_plan_type.as_ref().unwrap() {
        Filter(filter) => {
            // Recursively convert input plan
            let input = convert_physical_plan(
                filter.input.as_ref().unwrap(),
                ctx
            )?;

            // Convert filter predicates
            let predicates: Vec<Arc<dyn PhysicalExpr>> = filter.expr
                .iter()
                .map(|e| convert_physical_expr(e, input.schema()))
                .collect::<Result<Vec<_>>>()?;

            // Create FilterExec
            Ok(Arc::new(FilterExec::new(predicates, input)))
        }

        Projection(proj) => {
            let input = convert_physical_plan(proj.input.as_ref().unwrap(), ctx)?;
            let exprs = proj.expr
                .iter()
                .map(|e| convert_physical_expr(e, input.schema()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(ProjectionExec::new(exprs, input)))
        }

        Agg(agg) => {
            let input = convert_physical_plan(agg.input.as_ref().unwrap(), ctx)?;
            let mode = match agg.mode() {
                AggExecMode::HashAgg => agg::AggExecMode::HashAgg,
                AggExecMode::SortAgg => agg::AggExecMode::SortAgg,
            };
            // ... build AggExec
        }

        // ... more operator conversions
    }
}

pub fn convert_physical_expr(
    expr: &PhysicalExprNode,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    use expr::ExprType::*;

    match expr.expr_type.as_ref().unwrap() {
        Column(col) => {
            Ok(Arc::new(Column::new(&col.name, col.index as usize)))
        }

        Literal(lit) => {
            let value = convert_scalar_value(lit)?;
            Ok(Arc::new(Literal::new(value)))
        }

        BinaryExpr(binary) => {
            let left = convert_physical_expr(binary.l.as_ref().unwrap(), schema)?;
            let right = convert_physical_expr(binary.r.as_ref().unwrap(), schema)?;
            let op = convert_operator(&binary.op)?;
            Ok(Arc::new(BinaryExpr::new(left, op, right)))
        }

        // ... more expression conversions
    }
}
```

### Step 6: DataFusion Execution

```
File: native-engine/auron/src/exec.rs

impl NativeExecutionRuntime {
    pub fn next_batch(&mut self) -> Result<bool> {
        // Initialize stream if needed
        if self.stream.is_none() {
            let context = Arc::new(TaskContext::default());
            self.stream = Some(
                self.plan.execute(self.partition, context)?
            );
        }

        // Get next batch from stream
        let stream = self.stream.as_mut().unwrap();
        match futures::executor::block_on(stream.next()) {
            Some(Ok(batch)) => {
                // Export batch to Java via FFI
                self.export_batch_to_java(batch)?;
                Ok(true)
            }
            Some(Err(e)) => Err(e),
            None => Ok(false),
        }
    }
}
```

---

---

## Path 3: Data Return (Arrow RecordBatch → Spark InternalRow)

This path shows how native results flow back to Spark. Follow this when debugging data corruption issues or understanding memory ownership.

**Performance note:** Arrow FFI enables zero-copy data sharing. The actual bytes stay in native memory; only pointers cross the JNI boundary.

### Step 1: Rust Exports Arrow via FFI

**What happens:** After DataFusion produces a batch, it's exported via Arrow FFI (C Data Interface) to the JVM.

```
File: native-engine/auron/src/exec.rs

impl NativeExecutionRuntime {
    fn export_batch_to_java(&self, batch: RecordBatch) -> Result<()> {
        // Get JNI environment
        let env = jni_bridge::get_thread_jnienv();

        // Export schema (first batch only)
        if !self.schema_exported {
            let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())?;
            let schema_ptr = Box::into_raw(Box::new(ffi_schema));

            // Call Java: wrapper.importSchema(schemaPtr)
            env.call_method(
                &self.native_wrapper,
                "importSchema",
                "(J)V",
                &[JValue::Long(schema_ptr as i64)]
            )?;
        }

        // Export record batch
        let ffi_array = FFI_ArrowArray::try_from(&batch)?;
        let array_ptr = Box::into_raw(Box::new(ffi_array));

        // Call Java: wrapper.importBatch(arrayPtr)
        env.call_method(
            &self.native_wrapper,
            "importBatch",
            "(J)V",
            &[JValue::Long(array_ptr as i64)]
        )?;

        Ok(())
    }
}
```

### Step 2: Java Imports Arrow Data

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronCallNativeWrapper.scala

class AuronCallNativeWrapper(...) {
  private var schema: Schema = _
  private var currentBatch: ColumnarBatch = _

  // Called from Rust
  def importSchema(ffiSchemaPtr: Long): Unit = {
    schema = ArrowSchema.importFromFFI(ffiSchemaPtr)
    // Free the FFI struct after import
    ArrowSchema.freeFFI(ffiSchemaPtr)
  }

  // Called from Rust
  def importBatch(ffiArrayPtr: Long): Unit = {
    val recordBatch = ArrowRecordBatch.importFromFFI(ffiArrayPtr, schema)
    currentBatch = convertToColumnarBatch(recordBatch)
    ArrowRecordBatch.freeFFI(ffiArrayPtr)
  }

  private def convertToColumnarBatch(batch: RecordBatch): ColumnarBatch = {
    val columns = batch.columns.map { array =>
      new AuronArrowColumnVector(array)
    }
    new ColumnarBatch(columns.toArray, batch.numRows)
  }
}
```

### Step 3: Columnar to Row Conversion

```
File: spark-extension/.../columnar/AuronColumnarBatchRow.scala

class AuronColumnarBatchRow(batch: ColumnarBatch, rowId: Int)
    extends InternalRow {

  override def numFields: Int = batch.numCols()

  override def isNullAt(ordinal: Int): Boolean =
    batch.column(ordinal).isNullAt(rowId)

  override def getInt(ordinal: Int): Int =
    batch.column(ordinal).getInt(rowId)

  override def getLong(ordinal: Int): Long =
    batch.column(ordinal).getLong(rowId)

  override def getDouble(ordinal: Int): Double =
    batch.column(ordinal).getDouble(rowId)

  override def getUTF8String(ordinal: Int): UTF8String =
    batch.column(ordinal).getUTF8String(rowId)

  // ... other type accessors
}
```

---

---

## Path 4: Memory Management

This path shows how memory is tracked and reclaimed. Follow this when debugging OOM errors or understanding spill behavior.

**Debugging tip:** Watch these metrics during execution:
- `mem_spill_count` — Number of times operators spilled
- `disk_spill_size` — Bytes written to disk (high values suggest memory pressure)

### JVM Side Memory Tracking

**What happens:** The JVM side provides spill support for native code. When native memory is under pressure, it can spill to JVM heap as a first tier.

```
File: auron-core/src/main/java/org/apache/auron/memory/OnHeapSpillManager.java

public class OnHeapSpillManager {
    private final long maxMemory;
    private final AtomicLong usedMemory = new AtomicLong(0);

    public boolean tryAcquire(long size) {
        while (true) {
            long current = usedMemory.get();
            if (current + size > maxMemory) {
                return false;
            }
            if (usedMemory.compareAndSet(current, current + size)) {
                return true;
            }
        }
    }

    public void release(long size) {
        usedMemory.addAndGet(-size);
    }
}
```

### Rust Side Memory Management

```
File: native-engine/datafusion-ext-plans/src/memmgr/mod.rs

pub struct MemManager {
    /// Total memory limit
    memory_limit: usize,
    /// Currently used memory
    used: AtomicUsize,
    /// Consumers that can spill
    spillable_consumers: Mutex<Vec<Arc<dyn SpillableConsumer>>>,
}

impl MemManager {
    pub fn try_grow(&self, consumer: &impl MemConsumer, size: usize) -> Result<()> {
        let mut attempts = 0;
        loop {
            let current = self.used.load(Ordering::SeqCst);
            if current + size <= self.memory_limit {
                if self.used
                    .compare_exchange(current, current + size, ...)
                    .is_ok()
                {
                    return Ok(());
                }
            } else if attempts < MAX_SPILL_ATTEMPTS {
                // Trigger spill to disk
                self.trigger_spill(size)?;
                attempts += 1;
            } else {
                return Err(Error::OutOfMemory);
            }
        }
    }

    fn trigger_spill(&self, needed: usize) -> Result<usize> {
        let consumers = self.spillable_consumers.lock().unwrap();
        let mut freed = 0;
        for consumer in consumers.iter() {
            freed += consumer.spill(needed - freed)?;
            if freed >= needed {
                break;
            }
        }
        Ok(freed)
    }
}
```

### Memory Flow

Executor memory is split between Spark-managed JVM memory and Auron-managed native memory. JVM-side spill support is exposed through `OnHeapSpillManager`. Native execution initializes `MemManager` from the configured memory fraction and uses it for large native consumers such as aggregation buffers, sort buffers, and join hash tables.

---

## Quick Reference: Key Code Locations

| Operation | File | Function/Class |
|-----------|------|----------------|
| **Spark Integration** | | |
| Extension entry | `spark-extension/.../AuronSparkSessionExtension.scala` | `apply()` |
| Plan conversion | `spark-extension/.../AuronConverters.scala` | `convertSparkPlanRecursively()` |
| Expression conversion | `spark-extension/.../NativeConverters.scala` | `convertExpr()` |
| Native plan generation | `spark-extension/.../plan/NativeXxxBase.scala` | `doExecuteNative()` |
| **JNI Boundary** | | |
| JNI call (Java) | `auron-core/.../JniBridge.java` | `callNative()` |
| JNI entry (Rust) | `native-engine/auron/src/exec.rs` | `Java_..._callNative` |
| Arrow import | `spark-extension/.../AuronCallNativeWrapper.scala` | `importBatch()` |
| **Native Execution** | | |
| Plan deserialization | `native-engine/auron-serde/src/from_proto.rs` | `convert_physical_plan()` |
| Batch execution | `native-engine/auron/src/exec.rs` | `next_batch()` |
| Arrow export | `native-engine/auron/src/exec.rs` | `export_batch_to_java()` |
| Memory management | `native-engine/datafusion-ext-plans/src/memmgr/mod.rs` | `MemManager` |

---

# Auron Extension Guide

> Step-by-step instructions for adding operators, expressions, and data sources. Follow these guides when extending Auron's native execution capabilities.

## Before You Start

**Understanding the Architecture:**
1. Read the [How It Works](#how-it-works) section to understand execution flow
2. Review the [Module Deep Dives](#auron-module-deep-dives) for the components you'll modify
3. Study an existing similar operator as a template

**Development Setup:**
```bash
# Build everything (first time takes ~10 minutes)
./build/mvn clean package -DskipTests

# Run tests for a specific module
./build/mvn test -pl spark-extension

# Build native code only
cd native-engine && cargo build --release
```

## Quick Links

| Task | Difficulty | Time Estimate | Guide |
|------|------------|---------------|-------|
| Add new operator | Medium | 2-4 hours | [Adding a New Operator](#adding-a-new-operator) |
| Add new expression | Easy | 1-2 hours | [Adding a New Expression](#adding-a-new-expression) |
| Add new data source | Hard | 4-8 hours | [Adding a New Data Source](#adding-a-new-data-source) |
| Add configuration | Easy | 30 min | [Configuration Reference](#configuration-reference) |

For detailed examples, see [CONTRIBUTING.md](../../CONTRIBUTING.md).

---

## Adding a New Operator

### Overview

Adding a new operator requires changes across both JVM and Rust codebases. The process follows a consistent pattern that ensures type safety across the language boundary.

**High-Level Flow:**
```
1. Define Protobuf schema (the "contract")
2. Create Scala base class (plan metadata + Protobuf generation)
3. Create Scala concrete class (Spark version compatibility)
4. Add Shims factory method (version abstraction)
5. Implement Rust execution plan (actual computation)
6. Add Protobuf deserialization (Rust side)
7. Add conversion logic (Scala side)
8. Write tests (both sides)
```

Adding a new operator requires changes in 4-5 locations:

```
1. Protobuf schema         → native-engine/auron-serde/proto/auron.proto
2. Scala base class        → spark-extension/.../plan/NativeXxxBase.scala
3. Scala concrete class    → spark-extension-shims-spark3/.../plan/NativeXxxExec.scala
4. Shims factory method    → spark-extension-shims-spark3/.../ShimsImpl.scala
5. Rust execution plan     → native-engine/datafusion-ext-plans/src/xxx_exec.rs
6. Protobuf deserialization → native-engine/auron-serde/src/from_proto.rs
```

### Step 1: Define Protobuf Message

```protobuf
// native-engine/auron-serde/proto/auron.proto

// Add to PhysicalPlanNode oneof (pick next available field number)
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    // ... existing operators ...
    MyOperatorExecNode my_operator = 99;  // Use next available number
  }
}

// Define the operator message
message MyOperatorExecNode {
  // Input plan (most operators have one or two inputs)
  PhysicalPlanNode input = 1;

  // Operator-specific fields
  repeated PhysicalExprNode expressions = 2;
  bool some_flag = 3;
  int32 some_value = 4;
}
```

### Step 2: Create Scala Base Class

```scala
// spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeMyOperatorBase.scala

package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.auron._

abstract class NativeMyOperatorBase(
    expressions: Seq[Expression],
    someFlag: Boolean,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports
    with Logging {

  // Operator name shown in explain plans
  override val nodeName: String = "NativeMyOperator"

  // Output schema (often same as child for transforms)
  override def output: Seq[Attribute] = child.output

  // Define metrics for monitoring
  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getDefaultNativeMetrics(sparkContext)

  override def doExecuteNative(): NativeRDD = {
    // Get native input RDD from child
    val inputRDD = NativeHelper.executeNative(child)

    // Create metrics node
    val metricsNode = MetricNode(metrics, inputRDD.metrics :: Nil)

    // Create NativeRDD with plan generator
    new NativeRDD(
      sparkContext,
      metricsNode,
      inputRDD.partitions,
      inputRDD.dependencies,
      (partition, taskContext) => {
        // Get input plan
        val inputPlan = inputRDD.nativePlan(partition, taskContext)

        // Convert expressions to protobuf
        val protoExprs = expressions.map(NativeConverters.convertExpr)

        // Build operator node
        pb.PhysicalPlanNode.newBuilder()
          .setMyOperator(
            pb.MyOperatorExecNode.newBuilder()
              .setInput(inputPlan)
              .addAllExpressions(protoExprs.asJava)
              .setSomeFlag(someFlag)
          )
          .build()
      }
    )
  }
}
```

### Step 3: Create Scala Concrete Class

```scala
// spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeMyOperatorExec.scala

package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._

case class NativeMyOperatorExec(
    expressions: Seq[Expression],
    someFlag: Boolean,
    override val child: SparkPlan)
    extends NativeMyOperatorBase(expressions, someFlag, child) {

  // Required for Spark 3.2+
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
```

### Step 4: Add Shims Factory Method

```scala
// spark-extension/.../auron/Shims.scala (add abstract method)
trait Shims {
  // ... existing methods ...
  def createNativeMyOperatorExec(
    expressions: Seq[Expression],
    someFlag: Boolean,
    child: SparkPlan): SparkPlan
}

// spark-extension-shims-spark3/.../auron/ShimsImpl.scala (add implementation)
class ShimsImpl extends Shims {
  // ... existing implementations ...

  override def createNativeMyOperatorExec(
      expressions: Seq[Expression],
      someFlag: Boolean,
      child: SparkPlan): SparkPlan = {
    NativeMyOperatorExec(expressions, someFlag, child)
  }
}
```

### Step 5: Add Conversion Logic

```scala
// spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala

object AuronConverters {
  def convertSparkPlanRecursively(plan: SparkPlan): SparkPlan = {
    plan match {
      // ... existing patterns ...

      // Add pattern for your operator
      case op: MySparkOperator if canConvertMyOperator(op) =>
        val nativeChild = convertSparkPlanRecursively(op.child)
        Shims.get.createNativeMyOperatorExec(
          op.expressions,
          op.someFlag,
          nativeChild
        )

      // ... rest of patterns ...
    }
  }

  private def canConvertMyOperator(op: MySparkOperator): Boolean = {
    // Check if all expressions are convertible
    op.expressions.forall(NativeConverters.canConvertExpr)
  }
}
```

### Step 6: Implement Rust Execution Plan

```rust
// native-engine/datafusion-ext-plans/src/my_operator_exec.rs

use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::arrow::datatypes::SchemaRef;

#[derive(Debug)]
pub struct MyOperatorExec {
    input: Arc<dyn ExecutionPlan>,
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    some_flag: bool,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl MyOperatorExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        some_flag: bool,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            expressions,
            some_flag,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for MyOperatorExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.expressions.clone(),
            self.some_flag,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Execute input
        let input_stream = self.input.execute(partition, context)?;

        // Apply transformation
        let output_stream = MyOperatorStream::new(
            input_stream,
            self.expressions.clone(),
            self.some_flag,
            self.metrics.clone(),
        );

        Ok(Box::pin(output_stream))
    }
}
```

### Step 7: Add Protobuf Deserialization

```rust
// native-engine/auron-serde/src/from_proto.rs

pub fn convert_physical_plan(
    plan: &PhysicalPlanNode,
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    match plan.physical_plan_type.as_ref().unwrap() {
        // ... existing patterns ...

        PhysicalPlanType::MyOperator(node) => {
            let input = convert_physical_plan(node.input.as_ref().unwrap(), ctx)?;
            let expressions = node.expressions
                .iter()
                .map(|e| convert_physical_expr(e, input.schema()))
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(MyOperatorExec::new(
                input,
                expressions,
                node.some_flag,
            )))
        }

        // ... other patterns ...
    }
}
```

### Common Mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Forgetting Shims method | Compile error in `AuronConverters` | Add abstract method to `Shims.scala` and implement in `ShimsImpl.scala` |
| Wrong Protobuf field number | Deserialization fails silently | Use next available number, don't reuse deleted fields |
| Missing `withNewChildInternal` | Runtime error on plan copy | Implement in Spark 3.2+ concrete class |
| Schema mismatch | Data corruption or crashes | Ensure Rust operator's `schema()` matches Scala's `output` |
| Forgetting conversion logic | Operator always falls back | Add pattern to `AuronConverters.convertSparkPlanRecursively()` |

### Operator Checklist

Before submitting:
- [ ] Protobuf message defined with appropriate field numbers
- [ ] Scala base class with `doExecuteNative()` implementation
- [ ] Scala concrete class with `withNewChildInternal()` (Spark 3.2+)
- [ ] Shims factory method added
- [ ] Conversion pattern added to `AuronConverters`
- [ ] Rust execution plan implements `ExecutionPlan` trait
- [ ] Protobuf deserialization added to `from_proto.rs`
- [ ] Unit tests for Rust operator
- [ ] Integration tests for Scala operator
- [ ] Verified operator shows in `explain()` output

---

## Adding a New Expression

### Overview

Adding a new expression requires changes in 2-3 locations:

```
1. Protobuf schema         → native-engine/auron-serde/proto/auron.proto
2. Scala converter         → spark-extension/.../NativeConverters.scala
3. Rust expression impl    → native-engine/datafusion-ext-exprs/src/xxx.rs
4. Protobuf deserialization → native-engine/auron-serde/src/from_proto.rs
```

### Step 1: Define Protobuf Message

```protobuf
// native-engine/auron-serde/proto/auron.proto

message PhysicalExprNode {
  oneof ExprType {
    // ... existing expressions ...
    MyExprNode my_expr = 99;
  }
}

message MyExprNode {
  PhysicalExprNode input = 1;
  string param = 2;
}
```

### Step 2: Add Scala Conversion

```scala
// spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeConverters.scala

object NativeConverters {
  def convertExpr(expr: Expression): pb.PhysicalExprNode = {
    expr match {
      // ... existing patterns ...

      case MyExpr(child, param) =>
        pb.PhysicalExprNode.newBuilder()
          .setMyExpr(
            pb.MyExprNode.newBuilder()
              .setInput(convertExpr(child))
              .setParam(param)
          )
          .build()

      // ... other patterns ...
    }
  }

  def canConvertExpr(expr: Expression): Boolean = {
    expr match {
      // ... existing patterns ...
      case MyExpr(child, _) => canConvertExpr(child)
      // ... other patterns ...
    }
  }
}
```

### Step 3: Implement Rust Expression

```rust
// native-engine/datafusion-ext-exprs/src/my_expr.rs

use datafusion::physical_expr::PhysicalExpr;
use datafusion::arrow::array::ArrayRef;

#[derive(Debug)]
pub struct MyExpr {
    input: Arc<dyn PhysicalExpr>,
    param: String,
}

impl MyExpr {
    pub fn new(input: Arc<dyn PhysicalExpr>, param: String) -> Self {
        Self { input, param }
    }
}

impl PhysicalExpr for MyExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        // Return output data type
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.input.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Evaluate input
        let input_value = self.input.evaluate(batch)?;

        // Apply transformation
        match input_value {
            ColumnarValue::Array(array) => {
                let result = apply_my_transformation(&array, &self.param)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let result = apply_my_transformation_scalar(&scalar, &self.param)?;
                Ok(ColumnarValue::Scalar(result))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.param.clone())))
    }
}
```

### Step 4: Add Protobuf Deserialization

```rust
// native-engine/auron-serde/src/from_proto.rs

pub fn convert_physical_expr(
    expr: &PhysicalExprNode,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match expr.expr_type.as_ref().unwrap() {
        // ... existing patterns ...

        ExprType::MyExpr(node) => {
            let input = convert_physical_expr(node.input.as_ref().unwrap(), schema)?;
            Ok(Arc::new(MyExpr::new(input, node.param.clone())))
        }

        // ... other patterns ...
    }
}
```

### Common Mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Wrong `data_type()` return | Type errors downstream | Match Spark's type exactly |
| Missing `nullable()` handling | Null values cause crashes | Propagate nullability from children |
| Forgetting `canConvertExpr` | Expression always falls back | Add pattern in `NativeConverters` |

### Expression Checklist

Before submitting:
- [ ] Protobuf message defined in `PhysicalExprNode` oneof
- [ ] Scala conversion in `NativeConverters.convertExpr()`
- [ ] Scala check in `NativeConverters.canConvertExpr()`
- [ ] Rust expression implements `PhysicalExpr` trait
- [ ] Correct `data_type()` and `nullable()` implementations
- [ ] Protobuf deserialization in `from_proto.rs`
- [ ] Unit tests with various input types
- [ ] Edge case tests (nulls, empty arrays, etc.)

---

## Adding a New Data Source

### Overview

Adding a new data source (e.g., a new file format) is more complex than operators or expressions because it involves file I/O, schema inference, and often predicate pushdown.

**Prerequisites:**
- Understanding of the file format you're adding
- Familiarity with Arrow's I/O traits
- Knowledge of Spark's `DataSourceScanExec` hierarchy

**Components to implement:**

```
1. Protobuf schema         → native-engine/auron-serde/proto/auron.proto
2. Scala scan operator     → spark-extension/.../plan/NativeXxxScanBase.scala
3. Rust scan execution     → native-engine/datafusion-ext-plans/src/xxx_exec.rs
4. File reader integration → Connect to Arrow/DataFusion readers
```

> **Key Insight:** Start by checking if DataFusion already has a reader for your format. If so, you mainly need to wire up the Protobuf and Scala layers. If not, you'll need to implement the reader too.

### Example: Adding a CSV Data Source

#### Step 1: Define Protobuf Message

```protobuf
message CsvScanExecNode {
  Schema base_schema = 1;
  repeated int32 projection = 2;
  repeated string file_paths = 3;
  bool has_header = 4;
  string delimiter = 5;
  repeated PhysicalExprNode filters = 6;
}
```

#### Step 2: Create Scala Scan Operator

```scala
abstract class NativeCsvScanBase(
    relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression])
    extends DataSourceScanExec
    with NativeSupports {

  override def doExecuteNative(): NativeRDD = {
    val filePaths = getFilePaths(relation)
    val metrics = NativeHelper.getDefaultFileScanMetrics(sparkContext)

    new NativeRDD(
      sparkContext,
      MetricNode(metrics, Nil),
      computePartitions(),
      Nil,
      (partition, taskContext) => {
        val partitionFiles = getPartitionFiles(partition)

        pb.PhysicalPlanNode.newBuilder()
          .setCsvScan(
            pb.CsvScanExecNode.newBuilder()
              .setBaseSchema(convertSchema(relation.schema))
              .addAllProjection(projection.asJava)
              .addAllFilePaths(partitionFiles.asJava)
              .setHasHeader(hasHeader)
              .setDelimiter(delimiter)
          )
          .build()
      }
    )
  }
}
```

#### Step 3: Implement Rust Scan Execution

```rust
pub struct CsvScanExec {
    base_schema: SchemaRef,
    projection: Vec<usize>,
    file_paths: Vec<String>,
    has_header: bool,
    delimiter: u8,
    filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl ExecutionPlan for CsvScanExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Build CSV reader for partition files
        let reader = CsvReader::new()
            .with_schema(self.base_schema.clone())
            .with_projection(self.projection.clone())
            .with_header(self.has_header)
            .with_delimiter(self.delimiter);

        // Open file and create stream
        let file_path = &self.file_paths[partition];
        let stream = reader.read_file(file_path)?;

        // Apply filter pushdown if possible
        let filtered = apply_filters(stream, &self.filters)?;

        Ok(Box::pin(filtered))
    }
}
```

---

## Configuration Reference

### Core Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `spark.auron.enable` | boolean | true | Enable/disable Auron extension |
| `spark.auron.memory.fraction` | double | 0.6 | Fraction of executor memory for native execution |
| `spark.auron.batch.size` | int | 10000 | Target batch size for vectorized execution |

### Operator Toggles

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `spark.auron.enable.scan` | boolean | true | Enable native file scans |
| `spark.auron.enable.project` | boolean | true | Enable native projections |
| `spark.auron.enable.filter` | boolean | true | Enable native filters |
| `spark.auron.enable.sort` | boolean | true | Enable native sorts |
| `spark.auron.enable.aggregate` | boolean | true | Enable native aggregations |
| `spark.auron.enable.join` | boolean | true | Enable native joins |
| `spark.auron.enable.shuffle` | boolean | true | Enable native shuffle |
| `spark.auron.enable.window` | boolean | true | Enable native window functions |
| `spark.auron.enable.union` | boolean | true | Enable native unions |
| `spark.auron.enable.limit` | boolean | true | Enable native limits |
| `spark.auron.enable.expand` | boolean | true | Enable native expand |
| `spark.auron.enable.generate` | boolean | true | Enable native generate |

### Expression Toggles

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `spark.auron.udf.json.enabled` | boolean | true | Enable JSON UDF support |
| `spark.auron.udf.brickhouse.enabled` | boolean | true | Enable Brickhouse UDFs |
| `spark.auron.decimal.arithmetic.enabled` | boolean | false | Enable decimal arithmetic |

### Performance Tuning

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `spark.auron.parquet.predicate.pushdown` | boolean | true | Push predicates to Parquet reader |
| `spark.auron.parquet.bloom.filter` | boolean | true | Use Bloom filters for Parquet |
| `spark.auron.orc.predicate.pushdown` | boolean | true | Push predicates to ORC reader |
| `spark.auron.shuffle.compression.codec` | string | lz4 | Shuffle compression (none, lz4, zstd) |

### Logging and Debugging

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `spark.auron.log.level` | string | warn | Native log level (trace, debug, info, warn, error) |
| `spark.auron.explain.native` | boolean | false | Show native plan in explain output |

### Adding New Configuration

To add a new configuration option:

1. **Define in AuronConf.scala**:
```scala
// spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConf.scala

val MY_NEW_SETTING = buildConf("spark.auron.my.setting")
  .doc("Description of the setting")
  .booleanConf
  .createWithDefault(true)
```

2. **Access in code**:
```scala
val enabled = AuronConf.get.getConf(AuronConf.MY_NEW_SETTING)
```

3. **Pass to native if needed** (via TaskDefinition protobuf):
```protobuf
message TaskDefinition {
  // ... existing fields ...
  bool my_new_setting = 99;
}
```

---

## Testing Your Extension

Testing is critical—native bugs can cause crashes or data corruption that are hard to debug.

### Test Strategy

| Test Type | What It Covers | When to Run |
|-----------|----------------|-------------|
| Rust unit tests | Individual operators in isolation | During development |
| Scala unit tests | Conversion logic, schema handling | During development |
| Integration tests | End-to-end with Spark | Before PR |
| Comparison tests | Native vs Spark results | Before PR |

### Scala Unit Tests

Test that conversion works and produces correct results:

```scala
// spark-extension/src/test/scala/org/apache/spark/sql/auron/MyOperatorSuite.scala

class MyOperatorSuite extends AuronTestBase {
  test("my operator basic functionality") {
    withTempTable("test_data") {
      val result = spark.sql("SELECT my_function(col) FROM test_data")
      checkAnswer(result, expectedData)
    }
  }

  test("my operator falls back gracefully for unsupported types") {
    // Test that unsupported cases don't crash
  }
}
```

### Integration Tests

Verify native execution is actually happening:

```scala
test("my operator end-to-end") {
  spark.conf.set("spark.auron.enable", "true")

  val df = spark.read.parquet(testDataPath)
    .transform(myOperatorTransform)

  // IMPORTANT: Verify native execution actually happened
  assert(
    df.queryExecution.executedPlan.find(_.isInstanceOf[NativeMyOperatorExec]).isDefined,
    "Expected native execution but got Spark fallback"
  )

  // Verify results match expected
  checkAnswer(df, expectedResults)
}
```

### Comparison Tests

Compare native vs Spark results for correctness:

```scala
test("native results match Spark results") {
  val input = generateTestData()

  // Run with Auron
  spark.conf.set("spark.auron.enable", "true")
  val nativeResult = runQuery(input).collect()

  // Run with Spark
  spark.conf.set("spark.auron.enable", "false")
  val sparkResult = runQuery(input).collect()

  // Compare
  assert(nativeResult.sameElements(sparkResult))
}
```

### Rust Tests

Test the execution plan in isolation:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_my_operator() {
        let input = create_test_batch();
        let exec = MyOperatorExec::new(
            Arc::new(MemoryExec::try_new(&[vec![input]], schema, None).unwrap()),
            vec![],
            true,
        );

        let result = collect(Arc::new(exec), TaskContext::default()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), expected_rows);
    }

    #[tokio::test]
    async fn test_my_operator_empty_input() {
        // Edge case: empty input should produce empty output
    }

    #[tokio::test]
    async fn test_my_operator_nulls() {
        // Edge case: null handling
    }
}
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Protobuf compilation errors** | Build fails with missing message types | Run `mvn clean` and rebuild to regenerate protobuf classes |
| **JNI linking errors** | `UnsatisfiedLinkError` at runtime | Ensure native library is built for your platform (`./auron-build.sh`) |
| **Type conversion failures** | `NotImplementedError` during conversion | Check that all types are mapped in `NativeConverters.convertDataType()` |
| **Expression not converted** | Query works but falls back to Spark | Verify pattern matching in `NativeConverters.convertExpr()` |
| **Operator fallback** | `NativeXxxExec` not in explain output | Check `AuronConvertStrategy` logs for why the operator wasn't converted |
| **Native crash** | JVM crash with signal | Enable debug logging, check for null pointer or memory issues |
| **Wrong results** | Data mismatch vs Spark | Run comparison tests, check schema handling and null propagation |

### Debugging Tips

**Scala side debugging:**
```scala
// Enable native explain to see which operators are native
spark.conf.set("spark.auron.explain.native", "true")
df.explain(true)

// Enable debug logging to see conversion decisions
spark.conf.set("spark.auron.log.level", "debug")

// Check why a specific operator didn't convert
df.queryExecution.executedPlan.foreach { node =>
  println(s"${node.nodeName}: ${node.getTagValue(AuronConvertStrategy.neverConvertReasonTag)}")
}
```

**Rust side debugging:**
```rust
// Add tracing in Rust code
tracing::debug!("Executing MyOperatorExec with {} rows", batch.num_rows());

// Enable backtrace for panics
// Set RUST_BACKTRACE=1 environment variable
```

**Memory debugging:**
```scala
// Watch memory metrics
df.queryExecution.executedPlan.foreach { node =>
  if (node.isInstanceOf[NativeSupports]) {
    println(s"${node.nodeName} metrics: ${node.metrics}")
  }
}
```

---

## Document Info

| | |
|---|---|
| **Last Updated** | 2025-05 |
| **Applies To** | Auron 1.x, Spark 3.2-3.5 |
| **Maintainers** | Auron Core Team |

**Feedback:** If you find errors or have suggestions for improving this document, please open an issue on the [Auron GitHub repository](https://github.com/apache/auron).
