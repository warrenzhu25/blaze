# Operator Fallback and Data Conversions

> How Auron decides when to use native execution and minimizes row/columnar conversion overhead.

---

## Table of Contents

- [Overview](#overview)
- [Conversion Strategy](#conversion-strategy)
- [Avoiding Inefficient Conversions](#avoiding-inefficient-conversions)
- [Conversion Operators](#conversion-operators)
- [Zero-Copy Techniques](#zero-copy-techniques)
- [Configuration](#configuration)

---

## Overview

Auron replaces Spark operators with native (Rust) equivalents when beneficial. However, not all operators can be converted, and mixing native/non-native operators requires data format conversions:

- **Row → Columnar (R2C)**: Spark's `InternalRow` → Arrow `RecordBatch`
- **Columnar → Row (C2R)**: Arrow `RecordBatch` → Spark's `InternalRow`

These conversions have overhead, so Auron's strategy carefully avoids unnecessary conversions.

### Ideal: Full Native Pipeline

```
FileSourceScan → Filter → Project → Agg → Shuffle → Agg
     ↓            ↓         ↓        ↓       ↓        ↓
   Native      Native    Native   Native  Native   Native

→ No conversions needed, maximum performance
```

### Mixed Pipeline (Conversions Required)

```
CustomUDFExec (Spark)     ← Falls back to Spark
      │
─── C2R (Columnar→Row) ── ← Conversion overhead
      │
HashAggregateExec (Native)
      │
─── R2C (Row→Columnar) ── ← Conversion overhead
      │
FileSourceScanExec (Native)
```

---

## Conversion Strategy

### Strategy Tags

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConvertStrategy.scala`

Auron tags each operator with a conversion strategy:

```scala
sealed trait ConvertStrategy
case object Default extends ConvertStrategy       // Not yet decided
case object AlwaysConvert extends ConvertStrategy // Convert to native
case object NeverConvert extends ConvertStrategy  // Keep in Spark
```

### Decision Flow

The `AuronConvertStrategy.apply()` method analyzes the plan bottom-up:

```scala
def apply(exec: SparkPlan): Unit = {
  // 1. Try converting each operator
  exec.foreachUp { exec =>
    val converted = convertSparkPlan(exec)
    if (NativeHelper.isNative(converted)) {
      exec.setTagValue(convertibleTag, true)
    } else {
      exec.setTagValue(convertibleTag, false)
      exec.setTagValue(neverConvertReasonTag, "not supported")
    }
  }

  // 2. Remove inefficient conversion patterns
  removeInefficientConverts(exec)

  // 3. Finalize strategy based on context
  exec.foreachUp {
    case e: FileSourceScanExec => AlwaysConvert  // Scans benefit from native
    case e: SortExec => AlwaysConvert            // Sort benefits even if child is non-native
    case e: FilterExec if isNative(e.child) => AlwaysConvert
    case e: HashAggregateExec if isNative(e.child) => AlwaysConvert
    // ...
  }
}
```

### Operator Conversion Rules

| Operator | Convert If... | Reason |
|----------|---------------|--------|
| `FileSourceScanExec` | Always | Native scan is faster |
| `FilterExec` | Child is native | Avoids R2C for all input rows |
| `ProjectExec` | Child is native | Lightweight, piggyback on native |
| `SortExec` | Always (prefer) | Native sort is significantly faster |
| `HashAggregateExec` | Child is native | Avoids R2C for all input rows |
| `SortMergeJoinExec` | Any child is native | Join reduces data, worth conversion |
| `BroadcastHashJoinExec` | Both children native | Avoids conversions on both sides |
| `ShuffleExchangeExec` | Child is native or not agg | Native shuffle benefits downstream |

---

## Avoiding Inefficient Conversions

### The Problem: Native Islands

A "native island" is a native operator surrounded by non-native operators:

```
NonNative → NativeOp → NonNative
              ↑
         Requires R2C AND C2R!
```

This is often worse than staying in Spark entirely.

### removeInefficientConverts()

**File:** `AuronConvertStrategy.scala:211-293`

This method iteratively removes inefficient native conversions:

```scala
private def removeInefficientConverts(exec: SparkPlan): Unit = {
  var finished = false

  while (!finished) {
    finished = true

    exec.foreach { e =>
      // Pattern 1: NonNative → NativeFilter
      // Filter sees ALL input rows - R2C overhead too high
      if (e.isInstanceOf[FilterExec] && isNeverConvert(e.child)) {
        e.setTagValue(convertStrategyTag, NeverConvert)
        e.setTagValue(neverConvertReasonTag, "child is not native")
        finished = false
      }

      // Pattern 2: NonNative → NativeAgg
      // Aggregation sees ALL input rows - R2C overhead too high
      if (isAggregate(e) && isNeverConvert(e.child)) {
        e.setTagValue(convertStrategyTag, NeverConvert)
        finished = false
      }

      // Pattern 3: Agg → NativeShuffle
      // Next stage likely uses non-native shuffle reader
      if (e.isInstanceOf[ShuffleExchangeLike]
          && isAggregate(e.child) && isNeverConvert(e.child)) {
        e.setTagValue(convertStrategyTag, NeverConvert)
        finished = false
      }

      // Pattern 4: NativeExpand → NonNative
      // Expand multiplies rows - C2R overhead too high
      if (isNeverConvert(e)) {
        e.children.find(_.isInstanceOf[ExpandExec]).foreach { expand =>
          expand.setTagValue(convertStrategyTag, NeverConvert)
          finished = false
        }
      }

      // Pattern 5: NativeParquetScan → NonNative
      // Scan outputs ALL rows - C2R overhead too high
      if (isNeverConvert(e)) {
        e.children.find(_.isInstanceOf[FileSourceScanExec]).foreach { scan =>
          scan.setTagValue(convertStrategyTag, NeverConvert)
          finished = false
        }
      }

      // Pattern 6: NonNative → NativeSort → NonNative
      // Sandwiched sort - double conversion overhead
      if (isNeverConvert(e)) {
        e.children.filter(_.isInstanceOf[SortExec]).foreach { sort =>
          if (!isNeverConvert(sort) && isNeverConvert(sort.child)) {
            sort.setTagValue(convertStrategyTag, NeverConvert)
            finished = false
          }
        }
      }
    }
  }
}
```

### Pattern Summary

| Pattern | Action | Reason |
|---------|--------|--------|
| NonNative → NativeFilter | Skip native | Filter sees all rows, R2C too expensive |
| NonNative → NativeAgg | Skip native | Agg sees all rows, R2C too expensive |
| NativeScan → NonNative | Skip native | Scan outputs all rows, C2R too expensive |
| NativeExpand → NonNative | Skip native | Expand multiplies rows, C2R too expensive |
| NonNative → NativeSort → NonNative | Skip native | Double conversion (R2C + C2R) |
| Agg → NativeShuffle (non-native agg) | Skip native | Next stage likely non-native |

### When Conversion IS Worth It

Conversions are worthwhile when:

1. **Data reduction before conversion**: Native filter/agg significantly reduces rows before C2R
2. **Long native chain**: Amortize R2C cost over many native operators
3. **Expensive operations**: Native sort/join performance gain exceeds conversion cost

---

## Conversion Operators

### ConvertToNative (Row → Columnar)

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/ConvertToNativeBase.scala`

Inserted when transitioning from Spark to native execution:

```scala
abstract class ConvertToNativeBase(override val child: SparkPlan)
    extends UnaryExecNode with NativeSupports {

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = child.execute()  // Spark row iterator

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      (partition, context) => {
        val inputRowIter = inputRDD.compute(partition, context)

        // Register exporter for Rust to pull Arrow batches
        val resourceId = s"ConvertToNative:${UUID.randomUUID()}"
        JniBridge.resourcesMap.put(resourceId,
          new ArrowFFIExporter(inputRowIter, schema))

        // Native plan reads via FFI
        PhysicalPlanNode.newBuilder()
          .setFfiReader(FFIReaderExecNode.newBuilder()
            .setSchema(nativeSchema)
            .setExportIterProviderResourceId(resourceId))
          .build()
      })
  }
}
```

### ArrowFFIExporter (Row Batching)

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/arrowio/ArrowFFIExporter.scala`

Converts Spark rows to Arrow batches in a background thread:

```scala
class ArrowFFIExporter(rowIter: Iterator[InternalRow], schema: StructType) {
  private val maxBatchNumRows = 10000    // Configurable batch size
  private val maxBatchMemorySize = 4MB   // Memory limit per batch

  // Background thread batches rows into Arrow format
  private val outputThread = new Thread {
    override def run(): Unit = {
      while (rowIter.hasNext) {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        val arrowWriter = ArrowWriter.create(root)

        // Batch rows until size/count limit
        while (rowIter.hasNext
            && allocator.getAllocatedMemory < maxBatchMemorySize
            && arrowWriter.currentCount < maxBatchNumRows) {
          arrowWriter.write(rowIter.next())
        }
        arrowWriter.finish()

        // Export via queue for FFI consumption
        outputQueue.put(NextBatch)
        processingQueue.take()  // Wait for Rust to consume
      }
    }
  }

  def exportNextBatch(exportArrowArrayPtr: Long): Boolean = {
    // Zero-copy export via Arrow C Data Interface
    Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, currentRoot, exportArray)
  }
}
```

### Native → Row Conversion (importBatch)

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronCallNativeWrapper.scala`

When native results return to Spark:

```scala
case class AuronCallNativeWrapper(...) {

  // Called by Rust via JNI for each output batch
  protected def importBatch(ffiArrayPtr: Long): Unit = {
    // Import Arrow array via FFI (zero-copy)
    Using.resources(
      ArrowArray.wrap(ffiArrayPtr),
      VectorSchemaRoot.create(arrowSchema, ROOT_ALLOCATOR)
    ) { case (ffiArray, root) =>
      Data.importIntoVectorSchemaRoot(ROOT_ALLOCATOR, ffiArray, root, dictionaryProvider)

      // Convert to Spark rows
      batchRows.append(
        ColumnarHelper.rootRowsIter(root)
          .map(row => toUnsafe(row).copy())  // Copy to UnsafeRow
          .toSeq: _*)
    }
  }

  // Iterator for Spark to consume
  def getRowIterator: Iterator[InternalRow] = rowIterator
}
```

---

## Zero-Copy Techniques

### 1. Arrow C Data Interface (FFI)

Data transfer between JVM and Rust uses Arrow's C Data Interface:

```
JVM (Arrow Java)              Rust (Arrow-rs)
       │                            │
       │  ┌────────────────────┐   │
       │  │ ArrowArray struct  │   │
       │  │ (pointers only)    │───────→ Same memory buffers
       │  └────────────────────┘   │     No serialization!
       │                            │
       │  Direct Memory            │
       │  ┌────────────────────┐   │
       │  │ Actual data buffers│←──────── Rust reads directly
       │  └────────────────────┘   │
```

**Key benefit:** No serialization or memory copy for data transfer.

### 2. AuronColumnarBatchRow (Zero-Copy View)

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/columnar/AuronColumnarBatchRow.scala`

Provides row-level access to columnar data without copying:

```scala
class AuronColumnarBatchRow(columns: Array[AuronColumnVector], var rowId: Int)
    extends InternalRow {

  // Direct access to Arrow vectors - no copy!
  override def getInt(ordinal: Int): Int =
    columns(ordinal).getInt(rowId)

  override def getLong(ordinal: Int): Long =
    columns(ordinal).getLong(rowId)

  override def getUTF8String(ordinal: Int): UTF8String =
    columns(ordinal).getUTF8String(rowId)

  override def getDouble(ordinal: Int): Double =
    columns(ordinal).getDouble(rowId)

  // Row iteration is just incrementing rowId
  // row.rowId = 0, 1, 2, ... (no allocation!)
}
```

### 3. ColumnarHelper (Batch Iteration)

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/columnar/ColumnarHelper.scala`

```scala
object ColumnarHelper {
  def rootRowsIter(root: VectorSchemaRoot): Iterator[AuronColumnarBatchRow] = {
    // Single row object, reused for all rows
    val row = rootRowReusable(root)
    val numRows = root.getRowCount

    // Just increment rowId - no allocation per row
    Range(0, numRows).iterator.map { rowId =>
      row.rowId = rowId
      row
    }
  }
}
```

### 4. Lazy Copy Semantics

Copies only happen when absolutely necessary:

```scala
// Zero-copy: row is just a view
val row = columnarBatchRow

// Zero-copy: reading values
val value = row.getInt(0)

// Copy only when row must outlive batch
val copied = row.copy()  // Now allocates GenericInternalRow

// UnsafeRow conversion (required for Spark shuffle)
val unsafe = toUnsafe(row).copy()
```

---

## Configuration

### Batch Size Settings

| Config | Default | Description |
|--------|---------|-------------|
| `spark.auron.batchSize` | 10000 | Max rows per Arrow batch |
| `spark.auron.batch.suggested.mem.size` | 4194304 (4MB) | Target memory per batch |

### Tuning Guidance

**Larger batches:**
- Better throughput (fewer JNI calls)
- More memory usage
- Better for simple transformations

**Smaller batches:**
- Lower latency
- Less memory pressure
- Better for complex operations (joins, aggregations)

---

## Metrics

### Conversion Metrics

| Metric | Description |
|--------|-------------|
| `output_rows` | Rows output by ConvertToNative |
| `elapsed_compute` | Time spent in conversion |
| `batch_bytes_size` | Total bytes converted |

### Debugging Conversion Decisions

Enable explain to see conversion strategy:

```scala
spark.conf.set("spark.auron.explain.native", "true")
df.explain(true)
```

Output shows which operators are native vs Spark:

```
== Physical Plan ==
*(2) HashAggregate (Native)
+- ShuffleExchange (Native)
   +- *(1) HashAggregate (Native)
      +- *(1) Project (Native)
         +- *(1) Filter (Native)
            +- *(1) FileScan parquet (Native)
```

---

## Summary

| Technique | Benefit |
|-----------|---------|
| Strategy analysis | Avoids conversions that cost more than they save |
| Pattern detection | Prevents inefficient native islands |
| Arrow FFI | Zero-copy JVM ↔ Rust data transfer |
| Batching | Amortizes JNI call overhead |
| AuronColumnarBatchRow | Zero-copy row view into columnar data |
| Lazy copying | Only copies when data must escape batch |

### Key Insight

The best conversion is no conversion at all. Auron maximizes native operator coverage while carefully avoiding patterns where conversion overhead exceeds native execution benefits.
