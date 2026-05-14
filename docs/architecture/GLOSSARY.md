# Auron Glossary

This glossary defines key terms and concepts used throughout the Auron codebase.

---

## A

### Adaptive Query Execution (AQE)
Spark 3.x feature that re-optimizes query plans at runtime based on statistics. Auron requires AQE to be enabled for optimal plan conversion.

### Apache Arrow
A columnar memory format for flat and hierarchical data. Auron uses Arrow for zero-copy data transfer between Rust and JVM via the Arrow C Data Interface (FFI).

### AuronCallNativeWrapper
**File**: `spark-extension/.../AuronCallNativeWrapper.scala`

Scala class that manages a single native execution task. Wraps the JNI calls to start execution, iterate batches, and cleanup. Receives Arrow data via FFI callbacks.

### AuronColumnarOverrides
**File**: `spark-extension/.../AuronColumnarOverrides.scala`

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
