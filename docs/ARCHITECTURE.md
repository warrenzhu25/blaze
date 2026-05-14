# Apache Auron Architecture Documentation

> **Single-file reference** for system architecture, module details, execution paths, and extension patterns.

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
- [Glossary](#auron-glossary)

## How to Read This Document

| If you want to... | Start with... |
|-------------------|---------------|
| Understand what Auron does | [System Overview](#auron-system-overview) |
| Learn how joins/agg/shuffle work | [How It Works](#how-it-works) |
| Explore the codebase | [Module Deep Dives](#auron-module-deep-dives) |
| Trace code execution | [Critical Execution Paths](#critical-execution-paths) |
| Add a new operator | [Extension Guide](#auron-extension-guide) |
| Look up a term | [Glossary](#auron-glossary) |

---

# Auron System Overview

## What is Auron?

Apache Auron is a **native vectorized execution accelerator** for Apache Spark. It replaces Spark's row-based execution with columnar, vectorized processing powered by Apache DataFusion (a Rust-based query engine) and Apache Arrow (a columnar memory format).

### Why Auron Exists

Spark's default execution model processes data row-by-row in the JVM. This approach has limitations:

| Challenge | Spark's Approach | Auron's Solution |
|-----------|------------------|------------------|
| Memory overhead | JVM objects with headers | Arrow columnar format (no overhead) |
| CPU efficiency | Row-at-a-time processing | Vectorized batch processing |
| JVM limitations | GC pauses, memory management | Native Rust with manual memory control |
| SIMD utilization | Limited JIT optimization | Explicit SIMD via DataFusion |

### Key Benefits

1. **Performance**: 2-10x faster query execution for supported operators
2. **Memory efficiency**: Reduced GC pressure and memory footprint
3. **Transparency**: Works with existing Spark SQL queries without code changes
4. **Compatibility**: Falls back to Spark execution for unsupported operations

## High-Level Architecture

Auron is split across three execution layers:

1. **Spark JVM layer**: Spark receives SQL, Catalyst builds a physical plan, and `AuronSparkSessionExtension` installs a columnar rule that can replace supported Spark operators with native Auron operators.
2. **Auron Spark extension layer**: `AuronConverters` and `NativeConverters` translate Spark plans and expressions into Auron native operators and Protobuf messages.
3. **Rust native layer**: JNI calls enter the `auron` crate, `NativeExecutionRuntime` deserializes the Protobuf task definition, DataFusion executes the plan, and Arrow batches are returned to Spark through Arrow FFI.

The main boundary crossings are Protobuf for plan shape, JNI for control calls, and Arrow FFI for batch data.

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

When an operation cannot be executed natively, Auron automatically falls back to Spark execution:

Fallback is expressed in the physical plan as conversion boundaries. A supported subtree can stay native, an unsupported operator runs in Spark, and `ConvertToNative` or `ConvertFromNative` operators bridge the representation at the boundary. The converter tries to avoid inefficient native islands, so a supported operator may remain in Spark if its surrounding plan would require expensive transitions.

Common fallback reasons:
- Unsupported expression types
- Complex UDFs without native implementation
- Data types without Arrow mapping
- Operations requiring non-deterministic behavior

## Next Steps

- **Module Deep Dives**: See [Module Deep Dives](#auron-module-deep-dives) for detailed component documentation
- **Execution Paths**: See [Critical Execution Paths](#critical-execution-paths) for code-level tracing
- **Contributing**: See [Extension Guide](#auron-extension-guide) and [CONTRIBUTING.md](../../CONTRIBUTING.md)

---

# How It Works

This section explains the core logic behind Auron's main operations: joins, aggregation, shuffle, and memory management.

## Join Execution

Auron supports three join strategies that execute natively in Rust.

### Sort-Merge Join

Used when both inputs are already sorted by join keys.

**Execution Flow:**
1. Both inputs stream in sorted order
2. `StreamCursor` tracks position in each stream with pre-computed key rows
3. Compare cursors: advance the smaller side, collect matches on equality
4. Flush matched rows to output batches

**Key Components:**
- `SortMergeJoinExec` (Rust) - Main execution plan
- `StreamCursor` - Optimized cursor with key comparison
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

Used when one side is small enough to broadcast.

**Execution Flow:**
1. **Build phase**: Materialize broadcast side into hash map
   - Extract key columns, compute Spark-compatible Murmur3 hashes
   - Store in SIMD-aligned `MapValueGroup` (64-byte cache lines)
2. **Probe phase**: Stream probe side, look up each row
   - Hash probe keys, find matches in map
   - Output joined rows

**Hash Map Structure:**
- 8 hashes per cache line (SIMD comparison)
- Single values stored inline, collisions chain via `mapped_indices`
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

## Aggregation

Auron implements native aggregation with two modes and three phases.

### Aggregation Modes

**Hash Aggregation** (default):
- Uses hash table keyed by grouping columns
- Fast for high-cardinality groupings
- Memory-intensive, spills when needed

**Sort Aggregation**:
- Assumes input sorted by grouping keys
- Streaming output as groups complete
- Lower memory footprint

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

When cardinality approaches input size (>99.9%), aggregation is skipped:
- Pass data through unchanged
- Let downstream handle aggregation
- Avoids expensive hash table with little reduction

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

Native shuffle writes partitioned data for redistribution across executors.

### Partitioning Schemes

| Scheme | Algorithm | Use Case |
|--------|-----------|----------|
| Hash | `murmur3(keys) % n` | Default for joins, aggregations |
| Range | Binary search in sampled bounds | Sorted output |
| RoundRobin | `(row_idx + offset) % n` | Load balancing |
| Single | All to one partition | `collect()`, `coalesce(1)` |

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

Auron coordinates memory between JVM and native execution with automatic spilling.

### Memory Budget

```
executor_memory_overhead × MEMORY_FRACTION (0.6 default)
         ↓
    Native Budget
         ↓
   Divided among consumers (sort, agg, shuffle, etc.)
```

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

When operator reports memory growth:
```
IF total_used > budget AND size > 16MB AND growing:
    IF spillable AND size > consumer_min:
        → SPILL immediately
    ELSE:
        → WAIT (10 sec timeout, then force spill)
ELSE:
    → Continue
```

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

# Auron Module Deep Dives

> Detailed documentation of each module with key files, code patterns, and entry points.

## Module Dependency Graph

The JVM side starts in `spark-extension`, which owns plan conversion, expression conversion, and native operator base classes. `spark-extension-shims-spark3` depends on those base classes and supplies Spark-version-specific concrete operators. The Java bridge and configuration code provide the JNI surface used by the Scala wrapper.

The Rust side starts in `auron`, which owns JNI entry points and runtime lifecycle. It depends on `auron-serde` for Protobuf-to-DataFusion conversion, `auron-jni-bridge` for JNI utilities, and `datafusion-ext-plans` for physical operators. `datafusion-ext-plans` depends on the expression, function, and commons crates for Spark-compatible behavior.

---

## 1. Spark Extension Layer (spark-extension)

### Purpose
The core Scala module that integrates Auron with Apache Spark. Contains base classes for all native operators, expression converters, and the main extension hook.

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

Detailed explanation: this is the Spark entry point. Spark loads it from session extension configuration, it forces AQE settings needed by Auron's conversion strategy, initializes Spark-version shims, and injects `AuronColumnarOverrides` so plan conversion happens during Spark's columnar planning phase.

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

Detailed explanation: this file contains the rule that decides whether a Spark physical plan should be rewritten. It checks `spark.auron.enable`, skips plans that should remain untouched, tags nodes with conversion strategy, and hands the plan to the recursive converter before Spark inserts row/columnar transitions.

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

Detailed explanation: this is the main SparkPlan rewrite engine. It walks the tree bottom-up, preserves unsupported nodes, and asks `Shims` to create concrete `NativeXxxExec` nodes for supported operators. It also owns special handling for adaptive query stages, shuffle boundaries, native wrappers, and extension conversion providers.

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

Detailed explanation: this file separates convertibility analysis from the actual rewrite. It tags each `SparkPlan` with whether conversion is allowed, records reasons for non-conversion, and removes native islands that would be slower because their children or consumers would still require row-based Spark execution.

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

Detailed explanation: this file converts Catalyst expressions into Auron Protobuf expression nodes. Operator base classes call it while building `PhysicalPlanNode` messages, so every native filter predicate, projection expression, sort key, join key, and aggregate expression passes through this compatibility layer before Rust sees it.

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

Detailed explanation: `NativeRDD` is the Spark execution wrapper for native plans. It keeps the Spark partitioning/dependency contract while delaying Protobuf plan construction until a concrete partition is computed, then delegates execution to `NativeHelper`.

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

Detailed explanation: this file centralizes runtime helpers used by native operators. It creates wrappers for native execution, exposes configured native memory limits, builds standard metrics, and hides the JNI wrapper lifecycle from individual operator implementations.

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

Detailed explanation: this wrapper owns one native task execution from the JVM side. It loads the native library, calls into Rust, receives Arrow FFI callbacks, converts imported Arrow data into Spark rows, updates metrics, and finalizes the native runtime pointer when the Spark iterator closes.

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

Detailed explanation: `Shims` is the compatibility interface between common Auron conversion logic and Spark-version-specific classes. The common converter never directly constructs Spark 3.x concrete operators; it calls this interface so each supported Spark version can handle constructor and API differences.

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

Detailed explanation: the plan package contains the shared base classes for native Spark operators. Each base class preserves Spark metadata such as output schema, ordering, partitioning, and metrics while implementing `doExecuteNative()` to build the Protobuf operator node consumed by Rust.

### Dependencies
- **Depends on**: Apache Spark core, auron-core
- **Depended by**: spark-extension-shims-spark3

### Entry Points
1. `AuronSparkSessionExtension.apply()` - Called by Spark to register extension
2. `AuronColumnarOverrides.preColumnarTransitions()` - Triggers plan conversion
3. `AuronConverters.convertSparkPlanRecursively()` - Recursive plan conversion

---

## 2. Spark Extension Shims (spark-extension-shims-spark3)

### Purpose
Contains Spark 3.x version-specific implementations of native operators. Handles API differences between Spark 3.0, 3.1, 3.2, 3.3, 3.4, and 3.5.

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

Detailed explanation: this Spark 3 shim implements the common `Shims` interface with concrete Spark 3 operator classes. When `AuronConverters` decides a plan node should become native, `ShimsImpl` supplies the actual `NativeXxxExec` class that matches the active Spark API.

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

Detailed explanation: the shim plan package contains thin concrete operator classes that extend the shared base classes from `spark-extension`. Their main job is to satisfy Spark-version-specific tree-copying and constructor requirements while leaving native plan generation in the common base classes.

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

## 3. Protobuf Serialization (auron-serde)

### Purpose
Defines the Protocol Buffer schema for serializing execution plans and expressions. Provides Rust serialization/deserialization code.

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

Detailed explanation: this schema is the wire contract between Scala and Rust. Scala operator base classes build these messages, Java serializes them as part of `TaskDefinition`, and Rust deserializes them into DataFusion operators. Adding a native operator or expression requires extending this schema first.

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

Detailed explanation: this is the Rust deserialization layer for physical plans and expressions. It validates required Protobuf fields, converts nested inputs recursively, maps Spark-compatible expression/function names, and returns concrete `Arc<dyn ExecutionPlan>` values for the native runtime.

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

Detailed explanation: `lib.rs` exposes generated Protobuf types, the `from_proto` module, serde errors, and helper macros for required-field conversion. The generated Rust code is included from Cargo's build output, keeping checked-in source focused on conversion logic.

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

Detailed explanation: this Cargo build script generates Rust Protobuf bindings from `proto/auron.proto`. It also supports the repository's Maven-provided `protoc` path, making native builds align with the Java/Scala Protobuf generation pipeline.

### Dependencies
- **Depends on**: datafusion-ext-commons, prost (protobuf library)
- **Depended by**: auron (main runtime)

### Entry Points
1. Protobuf schema defines serialization format
2. `from_proto::convert_physical_plan()` - Plan deserialization
3. `from_proto::convert_physical_expr()` - Expression deserialization

---

## 4. JNI Bridge Layer

This spans two locations: Java/Scala side and Rust side.

### Java Side (auron-core)

#### Purpose
Provides the Java interface for calling into native Rust code via JNI.

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

Detailed explanation: this class declares the JVM methods implemented by Rust JNI symbols. It also exposes callback helpers Rust needs, including resource lookup, Hadoop file wrappers, direct-memory usage, task-running checks, and access to the current `OnHeapSpillManager`.

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

Detailed explanation: `AuronAdaptor` is the Java-side embedding interface for the native engine. Spark-specific code installs an implementation that knows how to load the native library, expose engine configuration, report memory limits, provide spill management, and create UDF wrapper contexts.

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

Detailed explanation: this interface abstracts configuration lookup for Java and Rust callback code. The native side can request Spark/Auron settings through JNI without depending directly on Spark's `SparkConf` implementation.

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

Detailed explanation: this class is the JVM spill contract used by native code when data must be staged through Spark-managed on-heap resources. The disabled implementation throws for spill operations; Spark integrations provide a real task-scoped manager when on-heap spilling is available.

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

Detailed explanation: this file owns the low-level JNI utility layer used by Rust. It stores the thread-local `JNIEnv`, caches Java class and method references, and defines macros/helpers that make Rust-to-Java calls concise while preserving exception handling.

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

Detailed explanation: `lib.rs` exposes JNI bridge modules and task-state helpers to the rest of the native engine. Operators and runtime code call these helpers to ensure JNI has been initialized and to stop native work when Spark cancels or completes a task.

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

## 5. Rust Execution Engine (auron)

### Purpose
Main Rust crate that coordinates native execution. Contains JNI entry points, runtime management, and DataFusion session configuration.

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

Detailed explanation: this file wires the native runtime modules together and provides shared panic-handling helpers for JNI entry points. The exported JNI functions live in `exec.rs`, but they use `handle_unwinded_scope()` from this file so Rust panics become JVM exceptions instead of unwinding across JNI.

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

Detailed explanation: `rt.rs` owns the lifetime of one native task. It decodes the JVM-provided task definition, turns the Protobuf plan into a DataFusion plan, starts execution on a Tokio runtime, receives output batches, and calls back into the JVM wrapper for Arrow FFI import.

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

Detailed explanation: this file contains the JNI methods Java calls after loading the native library. It initializes the native environment on the first `callNative()`, creates `NativeExecutionRuntime`, drives the stream one batch at a time through `nextBatch()`, and finalizes runtime pointers when the JVM asks for cleanup.

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

Detailed explanation: native logging is initialized once and enriches log lines with task, stage, and partition identifiers. Runtime worker threads set those thread-local values so native logs can be correlated with Spark task execution.

### Dependencies
- **Depends on**: auron-serde, auron-jni-bridge, datafusion-ext-plans, DataFusion
- **Depended by**: None (top of Rust dependency chain)

### Entry Points
1. `Java_org_apache_spark_sql_auron_JniBridge_callNative` - Start execution
2. `Java_org_apache_spark_sql_auron_JniBridge_nextBatch` - Get next batch
3. `Java_org_apache_spark_sql_auron_JniBridge_finalizeNative` - Cleanup

---

## 6. DataFusion Extensions (datafusion-ext-*)

### Purpose
Custom DataFusion physical plan operators and expressions that extend DataFusion's capabilities for Spark compatibility.

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

Detailed explanation: this crate root exposes Auron's custom DataFusion physical operators and helper modules. `auron-serde` imports these modules when turning Protobuf nodes into executable plans, so new native operators must be exported here before they can be deserialized.

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

Detailed explanation: `AggExec` implements native hash and sort aggregation. It builds an `AggContext` from grouping and aggregate expressions, manages aggregate state through the `agg` submodule, integrates with native memory management, and emits DataFusion-compatible record batches.

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

Detailed explanation: the native filter operator validates boolean predicates, executes its input stream, evaluates predicates against each Arrow batch, and returns only matching rows. It also participates in column pruning when downstream operators do not require every input column.

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

Detailed explanation: `ProjectExec` evaluates native expressions and constructs the output Arrow schema from expression data types and nullability. It is used for Spark projections, computed columns, and intermediate projections inserted to support pruning or operator-specific layouts.

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

Detailed explanation: this file implements native sort with memory-aware buffering and spill support. It evaluates sort keys, stores sorted blocks in memory or spill files, and performs k-way merge when needed to produce globally ordered Arrow batches.

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

Detailed explanation: this operator performs Spark-compatible sort-merge joins over sorted left and right streams. It owns join-key comparison, stream cursor management, join-type behavior, output projection, and metrics for matched/unmatched rows.

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

Detailed explanation: broadcast join reads a prebuilt small-side relation and probes it from the streamed side. This file coordinates build/probe schemas, join projection, join type semantics, and column pruning for native broadcast hash join execution.

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

Detailed explanation: this operator writes native shuffle output for Spark stages. It evaluates partitioning, repartitions Arrow batches, writes IPC/compressed blocks, and reports shuffle metrics back through Spark's native wrapper.

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

Detailed explanation: native Parquet scan builds DataFusion file-scan configuration from Spark file metadata. It applies projection, predicate pushdown, schema adaptation, metrics collection, and Hadoop filesystem access through JNI-backed wrappers.

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

Detailed explanation: `window_exec.rs` is the physical operator wrapper and `window/` contains the implementation details for window contexts and functions. Together they evaluate partitioned, ordered window expressions such as rank-like functions while preserving Spark output schema expectations.

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

Detailed explanation: `MemManager` is the native memory coordinator. Sort, aggregation, and shuffle consumers register with it, reserve memory before growing buffers, and trigger spill behavior when usage approaches the configured native memory limit.

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

Detailed explanation: this crate exposes physical expression implementations that DataFusion does not provide with Spark-compatible behavior. `from_proto.rs` constructs these expressions for casts, nested-field access, string predicates, row numbers, scalar subqueries, and Spark UDF callbacks.

#### `datafusion-ext-exprs/src/cast.rs`

Main code snippet:
```rust
pub struct TryCastExpr {
    expr: PhysicalExprRef,
    cast_type: DataType,
}
```

Detailed explanation: cast support handles Spark-specific conversion behavior that differs from vanilla DataFusion. The serde layer uses these expressions when Spark plans contain `CAST` or `TRY_CAST` nodes that need native evaluation.

#### `datafusion-ext-exprs/src/string_contains.rs`, `string_starts_with.rs`, `string_ends_with.rs`

Main code snippet:
```rust
pub struct StringContainsExpr {
    expr: PhysicalExprRef,
    infix: String,
}
```

Detailed explanation: these files implement Spark string predicate expressions as DataFusion physical expressions. They allow converted Spark filters and projections to evaluate `contains`, `startsWith`, and `endsWith` semantics natively over Arrow string arrays.

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

Detailed explanation: this crate is the registry for scalar functions that need Spark-compatible behavior. During Protobuf expression deserialization, Spark function names are resolved here into DataFusion scalar function implementations.

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

Detailed explanation: commons contains shared error macros, Arrow helpers, hashing utilities, batch sizing, serialization helpers, and Spark-compatible scalar structures. It is intentionally dependency-light so plans, expressions, functions, and serde code can share common behavior.

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

- **Trace execution paths**: See [Critical Execution Paths](#critical-execution-paths)
- **Add new operators**: See [Extension Guide](#auron-extension-guide)
- **Key terms**: See [Glossary](#auron-glossary)

---

# Critical Execution Paths

> End-to-end code traces showing how queries flow through the system.

## Path 1: Query Conversion (SparkPlan → NativeExec → Protobuf)

This path shows how a Spark physical plan is converted to native execution.

### Step 1: Extension Registration

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronSparkSessionExtension.scala

class AuronSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    if (conf.auronEnabled) {
      extensions.injectColumnar(_ => AuronColumnarOverrides)
      // Forces adaptive execution for better plan optimization
      SparkSession.builder().enableHiveSupport()
    }
  }
}
```

### Step 2: Columnar Rule Triggers

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

```
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConvertStrategy.scala

object AuronConvertStrategy {
  def apply(plan: SparkPlan): Unit = {
    // Tag each node with convertibility
    plan.foreach { node =>
      val canConvert = analyzeConvertibility(node)
      node.setTagValue(convertibleTag, canConvert)
      if (!canConvert) {
        node.setTagValue(neverConvertReasonTag, reason)
      }
    }
  }

  private def analyzeConvertibility(node: SparkPlan): Boolean = {
    // Check operator type support
    // Check expression support
    // Check data type support
    // Propagate from children
  }
}
```

### Step 4: Recursive Conversion

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

## Path 2: Execution (JNI → Rust Deserialization → DataFusion)

This path traces how a serialized plan is executed in the native engine.

### Step 1: NativeRDD Compute

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

## Path 3: Data Return (Arrow RecordBatch → Spark InternalRow)

### Step 1: Rust Exports Arrow via FFI

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

## Path 4: Memory Management

### JVM Side Memory Tracking

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
| Extension entry | `AuronSparkSessionExtension.scala` | `apply()` |
| Plan conversion | `AuronConverters.scala` | `convertSparkPlanRecursively()` |
| Expression conversion | `NativeConverters.scala` | `convertExpr()` |
| Native plan generation | `NativeXxxBase.scala` | `doExecuteNative()` |
| JNI call (Java) | `JniBridge.java` | `callNative()` |
| JNI entry (Rust) | `lib.rs` | `Java_..._callNative` |
| Plan deserialization | `from_proto.rs` | `convert_physical_plan()` |
| Batch execution | `exec.rs` | `next_batch()` |
| Arrow export | `exec.rs` | `export_batch_to_java()` |
| Arrow import | `AuronCallNativeWrapper.scala` | `importBatch()` |
| Memory management | `memmgr/mod.rs` | `MemManager` |

---

# Auron Extension Guide

> Step-by-step instructions for adding operators, expressions, and data sources.

## Quick Links

- **New Operator**: [Adding a New Operator](#adding-a-new-operator)
- **New Expression**: [Adding a New Expression](#adding-a-new-expression)
- **New Data Source**: [Adding a New Data Source](#adding-a-new-data-source)
- **Configuration**: [Configuration Reference](#configuration-reference)

For detailed operator implementation examples, see [CONTRIBUTING.md](../../CONTRIBUTING.md).

---

## Adding a New Operator

### Overview

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

---

## Adding a New Data Source

### Overview

Adding a new data source (e.g., a new file format) requires:

```
1. Protobuf schema         → native-engine/auron-serde/proto/auron.proto
2. Scala scan operator     → spark-extension/.../plan/NativeXxxScanBase.scala
3. Rust scan execution     → native-engine/datafusion-ext-plans/src/xxx_exec.rs
```

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

### Unit Tests

```scala
// spark-extension/src/test/scala/org/apache/spark/sql/auron/MyOperatorSuite.scala

class MyOperatorSuite extends AuronTestBase {
  test("my operator basic functionality") {
    withTempTable("test_data") {
      val result = spark.sql("SELECT my_function(col) FROM test_data")
      checkAnswer(result, expectedData)
    }
  }
}
```

### Integration Tests

```scala
test("my operator end-to-end") {
  spark.conf.set("spark.auron.enable", "true")

  val df = spark.read.parquet(testDataPath)
    .transform(myOperatorTransform)

  // Verify native execution
  assert(df.queryExecution.executedPlan.find(_.isInstanceOf[NativeMyOperatorExec]).isDefined)

  // Verify results
  checkAnswer(df, expectedResults)
}
```

### Rust Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_operator() {
        let input = create_test_batch();
        let exec = MyOperatorExec::new(
            Arc::new(MemoryExec::new(&[vec![input]])),
            vec![],
            true,
        );

        let result = collect(Arc::new(exec)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), expected_rows);
    }
}
```

---

## Troubleshooting

### Common Issues

1. **Protobuf compilation errors**: Run `mvn clean` and rebuild to regenerate protobuf classes

2. **JNI linking errors**: Ensure native library is built for your platform (`./auron-build.sh`)

3. **Type conversion failures**: Check that all types are mapped in `NativeConverters.convertDataType()`

4. **Expression not converted**: Verify pattern matching in `NativeConverters.convertExpr()`

5. **Operator fallback**: Check `AuronConvertStrategy` for why the operator wasn't converted

### Debugging Tips

```scala
// Enable native explain
spark.conf.set("spark.auron.explain.native", "true")
df.explain(true)

// Enable debug logging
spark.conf.set("spark.auron.log.level", "debug")
```

```rust
// Add tracing in Rust code
tracing::debug!("Executing MyOperatorExec with {} rows", batch.num_rows());
```

---

# Auron Glossary

> Definitions of key terms, type mappings, and quick file references.

## A

### Adaptive Query Execution (AQE)
Spark 3.x feature that re-optimizes query plans at runtime based on statistics. Auron requires AQE to be enabled for optimal plan conversion.

### Apache Arrow
A columnar memory format for flat and hierarchical data. Auron uses Arrow for zero-copy data transfer between Rust and JVM via the Arrow C Data Interface (FFI).

### AuronCallNativeWrapper
**File**: `spark-extension/.../AuronCallNativeWrapper.scala`

Scala class that manages a single native execution task. Wraps the JNI calls to start execution, iterate batches, and cleanup. Receives Arrow data via FFI callbacks.

### AuronColumnarOverrides
**File**: `spark-extension/.../AuronSparkSessionExtension.scala`

Spark `ColumnarRule` that intercepts physical plans and triggers conversion to native operators. Entry point for plan transformation.

### AuronConverters
**File**: `spark-extension/.../AuronConverters.scala`

Object containing pattern-matching logic to convert Spark operators to their native equivalents. The `convertSparkPlanRecursively()` method is the main entry point.

### AuronSparkSessionExtension
**File**: `spark-extension/.../AuronSparkSessionExtension.scala`

Main entry point for Auron. Implements Spark's extension interface to inject columnar rules and configure the session.

---

## B

### Batch
A collection of rows in columnar format. In Auron, batches are Apache Arrow `RecordBatch` instances, typically containing 1,000-10,000 rows.

### BinaryExec
Spark operator with two child plans (e.g., joins). Native equivalents extend `NativeSupports` and implement `doExecuteNative()` with both children.

### Broadcast Join
Join strategy where the smaller table is broadcast to all executors. Auron implements this as `NativeBroadcastJoinExec` with separate build and probe phases.

---

## C

### Catalyst
Spark's query optimizer. Auron hooks into the physical planning phase after Catalyst has produced an optimized physical plan.

### ColumnarBatch
Spark's representation of columnar data. Auron converts Arrow `RecordBatch` to `ColumnarBatch` for compatibility with Spark's internal APIs.

### ColumnarRule
Spark interface for injecting columnar transformations. `AuronColumnarOverrides` implements this to convert plans to native execution.

### ConvertToNative / ConvertFromNative
Boundary operators inserted where native and Spark execution meet. `ConvertToNative` converts Spark rows to native format; `ConvertFromNative` does the reverse.

---

## D

### DataFusion
Apache DataFusion is a Rust query engine that Auron uses for native execution. It provides vectorized operators and expressions with SIMD optimization.

### datafusion-ext-plans
**Location**: `native-engine/datafusion-ext-plans/`

Rust crate containing custom DataFusion `ExecutionPlan` implementations for Auron. Includes aggregation, joins, scans, and other operators.

### datafusion-ext-exprs
**Location**: `native-engine/datafusion-ext-exprs/`

Rust crate containing custom DataFusion `PhysicalExpr` implementations. Includes Spark-compatible cast, string functions, and date/time operations.

### doExecuteNative()
Method that all native operators must implement. Returns a `NativeRDD` containing the serialized execution plan.

---

## E

### ExecutionPlan
DataFusion trait for physical operators. All Rust operators implement this to participate in query execution. Key method is `execute()` which returns a `SendableRecordBatchStream`.

### ExprId
Spark's expression identifier, a unique ID for each expression in a query. Used to track column references across plan transformations.

### Expression
Spark class representing a computation (e.g., arithmetic, comparison, function call). Converted to Protobuf `PhysicalExprNode` for native execution.

---

## F

### FFI (Foreign Function Interface)
Mechanism for crossing language boundaries. Auron uses Arrow's C Data Interface (FFI) to transfer Arrow data between Rust and JVM without serialization.

### FilterExec / NativeFilterExec
Operator that applies predicates to filter rows. Native version serializes predicates as `PhysicalExprNode` and executes via DataFusion's `FilterExec`.

### from_proto.rs
**File**: `native-engine/auron-serde/src/from_proto.rs`

Rust module that deserializes Protobuf messages into DataFusion `ExecutionPlan` and `PhysicalExpr` instances.

---

## G

### GlobalRef
JNI type for persistent references to Java objects. Used in Rust to hold references to Java callbacks across JNI calls.

---

## H

### HashAggregate
Aggregation strategy using hash tables. Faster than sort-based aggregation but requires more memory. Auron's `AggExec` supports both modes.

---

## I

### InternalRow
Spark's internal row representation. Native execution produces Arrow batches, which are converted to `InternalRow` via `AuronColumnarBatchRow`.

---

## J

### JNI (Java Native Interface)
Standard interface for JVM to call native code. Auron uses JNI to invoke Rust functions from Scala/Java.

### JniBridge
**File**: `auron-core/.../JniBridge.java`

Java class declaring native methods for Auron execution: `callNative()`, `nextBatch()`, `finalizeNative()`.

---

## L

### LeafExec
Spark operator with no children (e.g., table scans). Native scan operators extend appropriate base classes and return data directly.

### LocalRef
JNI local reference with automatic cleanup. Auron's `LocalRef` struct in Rust provides RAII semantics for JNI references.

---

## M

### MemManager
**File**: `native-engine/datafusion-ext-plans/src/memmgr/`

Rust memory manager that tracks native memory usage and triggers spilling when limits are reached.

### MetricNode
Tree structure for collecting execution metrics. Passed through the plan to aggregate statistics from all operators.

### Metrics
Counters and timers for monitoring execution. Auron collects metrics like `output_rows`, `elapsed_compute`, and `mem_spill_count` and reports them to Spark UI.

---

## N

### NativeConverters
**File**: `spark-extension/.../NativeConverters.scala`

Object for converting Spark expressions to Protobuf. Key methods: `convertExpr()`, `convertDataType()`, `convertAggregateExpr()`.

### NativeHelper
**File**: `spark-extension/.../NativeHelper.scala`

Utility object with helper functions for native execution, including memory calculations and metric definitions.

### NativeRDD
**File**: `spark-extension/.../NativeRDD.scala`

Spark RDD implementation that wraps native execution. Contains the plan generator function and manages per-partition execution.

### NativeSupports
**File**: `spark-extension/.../NativeSupports.scala`

Trait that all native operators implement. Requires `doExecuteNative(): NativeRDD` and overrides `doExecute()` to route through native path.

---

## O

### OnHeapSpillManager
**File**: `auron-core/.../OnHeapSpillManager.java`

JVM-side memory manager that coordinates with Spark's memory management system.

---

## P

### Partition
A unit of parallel execution. Each partition is processed independently, potentially on different executors.

### PhysicalExpr
DataFusion trait for evaluating expressions on `RecordBatch` data. Returns `ColumnarValue` (array or scalar).

### PhysicalExprNode
Protobuf message representing a serialized expression. Contains a `oneof` with all supported expression types.

### PhysicalPlanNode
Protobuf message representing a serialized execution plan. Contains a `oneof` with all supported operator types.

### Projection / ProjectExec
Operator that selects or transforms columns. `NativeProjectExec` evaluates expressions and produces new columns.

---

## R

### RecordBatch
Arrow's unit of columnar data. Contains a schema and a collection of arrays, one per column.

### RSS (Remote Shuffle Service)
External shuffle storage service. Auron supports RSS via `RssShuffleWriterExec` for improved shuffle scalability.

---

## S

### Schema
Description of data structure (column names and types). Both Spark and Arrow have schema concepts; Auron converts between them.

### SendableRecordBatchStream
Rust async stream type producing `RecordBatch` results. All `ExecutionPlan::execute()` methods return this type.

### SessionContext
DataFusion's main entry point. Holds configuration, registered functions, and execution state.

### Shims
**File**: `spark-extension/.../Shims.scala`

Abstraction layer for Spark version differences. `ShimsImpl` provides version-specific implementations loaded via reflection.

### Shuffle
Data redistribution across partitions. Auron implements native shuffle writing via `NativeShuffleExchangeExec`.

### Sort / SortExec
Operator that orders rows by specified columns. `NativeSortExec` implements sorting with spill support.

### SortMergeJoin
Join strategy that sorts both inputs then merges. `NativeSortMergeJoinExec` implements this with configurable join types.

### SparkPlan
Spark's abstract class for physical operators. All Spark operators extend this; native operators wrap Spark operators.

### Spill
Writing intermediate data to disk when memory is exhausted. Both Rust (`MemManager`) and JVM (`OnHeapSpillManager`) support spilling.

---

## T

### TaskContext
Spark's per-task execution context. Provides task ID, partition ID, and metrics registration.

### TaskDefinition
Protobuf message containing all information for executing a partition: plan, task ID, partition ID, configuration.

### THREAD_JNIENV
Thread-local storage for JNI environment pointer. Set by Rust JNI entry points; used by callbacks to Java.

---

## U

### UDF (User-Defined Function)
Custom function defined in application code. Auron supports some UDFs via `SparkUdfWrapperExpr` which calls back to JVM.

### UnaryExec
Spark operator with one child plan (e.g., filter, project, sort). Native equivalents extend `UnaryExecNode` and `NativeSupports`.

---

## V

### Vectorized Execution
Processing data in batches (vectors) rather than row-by-row. Enables SIMD operations and better cache utilization.

---

## W

### Window Function
Computation over a window of rows (e.g., row_number, rank). `NativeWindowExec` implements window functions with frame support.

### withNewChildInternal
Spark 3.2+ method for copying operators with new children. All concrete native operators must implement this.

---

## Type Mappings

### Spark → Arrow Type Mapping

| Spark Type | Arrow Type |
|------------|------------|
| BooleanType | Boolean |
| ByteType | Int8 |
| ShortType | Int16 |
| IntegerType | Int32 |
| LongType | Int64 |
| FloatType | Float32 |
| DoubleType | Float64 |
| StringType | Utf8 |
| BinaryType | Binary |
| DateType | Date32 |
| TimestampType | Timestamp (microseconds) |
| DecimalType(p, s) | Decimal128(p, s) |
| ArrayType(elem) | List(elem) |
| MapType(k, v) | Map(k, v) |
| StructType(fields) | Struct(fields) |

### Aggregation Modes

| Spark Mode | Protobuf Enum | Description |
|------------|---------------|-------------|
| Partial | PARTIAL | First aggregation phase, produces partial results |
| PartialMerge | PARTIAL_MERGE | Merges partial results from shuffle |
| Final | FINAL | Produces final aggregation results |

### Join Types

| Spark Type | Protobuf Enum |
|------------|---------------|
| Inner | INNER |
| LeftOuter | LEFT |
| RightOuter | RIGHT |
| FullOuter | FULL |
| LeftSemi | LEFT_SEMI |
| LeftAnti | LEFT_ANTI |
| Cross | CROSS |

---

## File Quick Reference

| Concept | Primary File(s) |
|---------|-----------------|
| Entry point | `AuronSparkSessionExtension.scala` |
| Plan conversion | `AuronConverters.scala` |
| Expression conversion | `NativeConverters.scala` |
| Native operators (base) | `spark-extension/.../plan/*.scala` |
| Native operators (concrete) | `spark-extension-shims-spark3/.../plan/*.scala` |
| JNI bridge (Java) | `JniBridge.java` |
| JNI bridge (Rust) | `jni_bridge.rs` |
| Rust entry point | `native-engine/auron/src/lib.rs` |
| Plan deserialization | `from_proto.rs` |
| Protobuf schema | `auron.proto` |
| Rust operators | `datafusion-ext-plans/src/*.rs` |
| Memory management | `memmgr/mod.rs` |
