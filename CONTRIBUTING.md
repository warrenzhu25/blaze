# Contributing to Apache Auron

Thank you for your interest in contributing to Apache Auron! This guide will help you understand how to add new operator support to Auron, from start to finish.

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [End-to-End Example: Filter Operator](#end-to-end-example-filter-operator)
- [Step-by-Step Guide for Adding a New Operator](#step-by-step-guide-for-adding-a-new-operator)
- [Testing Your Operator](#testing-your-operator)
- [Build and Verification](#build-and-verification)
- [Code Quality Standards](#code-quality-standards)
- [Troubleshooting](#troubleshooting)
- [Additional Resources](#additional-resources)

## Introduction

Apache Auron accelerates big data engines (like Apache Spark) by leveraging native vectorized execution through Apache DataFusion. When you add a new operator to Auron, you're creating a bridge between Spark's execution plans and DataFusion's native Rust implementation.

### How Auron Works

1. **Query Execution**: Spark creates an optimized physical execution plan
2. **Conversion**: Auron converts Spark operators to native equivalents
3. **Serialization**: Native plans are serialized using Protocol Buffers
4. **Native Execution**: The Rust engine executes using DataFusion
5. **Results**: Data flows back to Spark as Apache Arrow record batches

### Why Two Implementations?

Each operator requires:
- **Scala code**: Integrates with Spark, handles conversion, serializes to Protobuf
- **Rust code**: Native execution using DataFusion for high performance

## Prerequisites

### Required Knowledge

- **Apache Spark internals**: Understanding of SparkPlan, Expression, RDD
- **Scala**: Familiarity with case classes, traits, pattern matching
- **Rust**: Basic understanding of traits, Arc, async/await
- **Protocol Buffers**: Message definitions and serialization
- **Apache Arrow**: Columnar data format basics

### Development Environment

1. **Install Rust** (nightly toolchain recommended)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   rustup default nightly
   ```

2. **Install JDK 8, 11, or 17**
   ```bash
   # Set JAVA_HOME environment variable
   export JAVA_HOME=/path/to/jdk
   ```

3. **Build tools**
   - Maven (via `./build/sbt`)
   - Protobuf compiler (installed automatically during build)

4. **Clone the repository**
   ```bash
   git clone https://github.com/apache/auron.git
   cd auron
   ```

5. **Initial build**
   ```bash
   ./auron-build.sh --help  # See build options
   ./auron-build.sh         # Build locally
   ```

## Architecture Overview

Auron uses a **three-layer architecture** for operators:

### Layer 1: Base Classes (spark-extension module)

**Location**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/`

These abstract classes contain the core operator logic:
- Implement `NativeSupports` trait
- Define `doExecuteNative()` method
- Handle expression conversion
- Create protobuf messages
- Manage metrics

**Example**: `NativeFilterBase.scala`, `NativeProjectBase.scala`

### Layer 2: Concrete Implementations (spark-extension-shims-spark3 module)

**Location**: `spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/execution/auron/plan/`

Version-specific concrete classes:
- Extend base classes
- Handle Spark version compatibility
- Implement `withNewChildInternal()` for Spark 3.2+
- Implement `withNewChildren()` for Spark 3.0/3.1

**Example**: `NativeFilterExec.scala`, `NativeProjectExec.scala`

### Layer 3: Protobuf Definitions

**Location**: `native-engine/auron-serde/proto/auron.proto`

Protocol Buffer messages define the serialization format:
- Operator-specific messages (e.g., `FilterExecNode`)
- Expression messages (`PhysicalExprNode`)
- Schema and data type definitions

### Key Components

#### NativeSupports Trait
```scala
trait NativeSupports extends SparkPlan {
  protected def doExecuteNative(): NativeRDD
  override protected def doExecute(): RDD[InternalRow] = doExecuteNative()
  def executeNative(): NativeRDD = executeQuery { doExecuteNative() }
}
```

#### NativeRDD
Wraps native execution in an RDD interface:
- Manages partitions and dependencies
- Serializes plan to protobuf
- Integrates with Spark's execution model

#### Converters
- **AuronConverters**: Converts Spark operators to native equivalents
- **NativeConverters**: Converts Spark expressions to protobuf
- **from_proto.rs**: Deserializes protobuf to Rust execution plans

### Data Flow Diagram

```
SparkPlan (Spark)
    ↓
AuronConverters.scala (pattern matching)
    ↓
NativeXxxExec (Scala concrete class)
    ↓
NativeXxxBase.doExecuteNative() (Scala base class)
    ↓
XxxExecNode (Protobuf message)
    ↓
from_proto.rs (Rust deserialization)
    ↓
XxxExec (Rust execution plan)
    ↓
DataFusion execution
    ↓
Arrow RecordBatch → Spark InternalRow
```

## End-to-End Example: Filter Operator

Let's walk through the complete implementation of the Filter operator, which applies predicates to filter rows.

### 1. Protobuf Definition

**File**: `native-engine/auron-serde/proto/auron.proto`

```protobuf
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    // ... other operators ...
    FilterExecNode filter = 8;
    // ... other operators ...
  }
}

message FilterExecNode {
  PhysicalPlanNode input = 1;
  repeated PhysicalExprNode expr = 2;
}
```

**Key points**:
- `input`: The child operator (e.g., table scan)
- `expr`: List of filter predicates (AND-ed together)
- Each operator gets a unique field number in `PhysicalPlanNode`

### 2. Scala Base Class

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeFilterBase.scala`

```scala
abstract class NativeFilterBase(condition: Expression, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  // Define metrics for monitoring
  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(
        Set(
          "stage_id",
          "output_rows",
          "elapsed_compute",
          "input_batch_count",
          "input_batch_mem_size",
          "input_row_count"))
      .toSeq: _*)

  // Preserve output schema and partitioning from child
  override def output: Seq[Attribute] = FilterExec(condition, child).output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Convert Spark expressions to protobuf, splitting AND expressions
  private def nativeFilterExprs = {
    val splittedExprs = ArrayBuffer[PhysicalExprNode]()

    def isNaiveIsNotNullColumns(expr: Expression): Boolean = {
      expr match {
        case IsNotNull(_: AttributeReference) => true
        case And(lhs, rhs) if isNaiveIsNotNullColumns(lhs) &&
          isNaiveIsNotNullColumns(rhs) => true
        case _ => false
      }
    }

    def split(expr: Expression): Unit = {
      expr match {
        case e @ And(lhs, rhs) if !isNaiveIsNotNullColumns(e) =>
          split(lhs)
          split(rhs)
        case expr => splittedExprs.append(NativeConverters.convertExpr(expr))
      }
    }
    split(condition)
    splittedExprs
  }

  // Validate that conversion is supported (throws if not)
  nativeFilterExprs

  // Main execution method: creates NativeRDD with protobuf plan
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
        val nativeFilterExec = FilterExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeFilterExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setFilter(nativeFilterExec).build()
      },
      friendlyName = "NativeRDD.Filter")
  }
}
```

**Key implementation details**:
1. **Metrics**: Define which metrics to collect
2. **Output preservation**: Match Spark's FilterExec behavior
3. **Expression splitting**: Split AND expressions for better optimization
4. **NativeRDD creation**: Build protobuf message in partition function
5. **One-to-one dependency**: Each output partition depends on one input partition

### 3. Scala Concrete Class

**File**: `spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeFilterExec.scala`

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

**Key implementation details**:
1. **Case class**: Enables pattern matching and copying
2. **Version compatibility**: Different Spark versions use different methods
3. **@sparkver annotation**: Documents which Spark versions use which method
4. **Minimal code**: All logic is in the base class

### 4. Shims Integration

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/Shims.scala`

```scala
trait Shims {
  // ... other operators ...
  def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase
  // ... other operators ...
}
```

**File**: `spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/auron/ShimsImpl.scala`

```scala
class ShimsImpl extends Shims {
  // ... other operators ...
  override def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase =
    NativeFilterExec(condition, child)
  // ... other operators ...
}
```

**Why Shims?**
- Abstracts Spark version differences
- Enables supporting multiple Spark versions
- Centralizes operator creation

### 5. Conversion Logic

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala`

**Feature flag**:
```scala
def enableFilter: Boolean =
  getBooleanConf("spark.auron.enable.filter", defaultValue = true)
```

**Conversion pattern matching**:
```scala
def convertToNative(exec: SparkPlan): SparkPlan = {
  exec match {
    // ... other operators ...
    case e: FilterExec if enableFilter => // filter
      tryConvert(e, convertFilterExec)
    // ... other operators ...
  }
}
```

**Conversion method**:
```scala
def convertFilterExec(exec: FilterExec): SparkPlan = {
  logDebugPlanConversion(exec, Seq("condition" -> exec.condition))
  Shims.get.createNativeFilterExec(
    exec.condition,
    addRenameColumnsExec(convertToNative(exec.child)))
}
```

**Key implementation details**:
1. **Feature flag**: Allows enabling/disabling via config
2. **Pattern matching**: Identifies Spark's FilterExec
3. **Recursive conversion**: Converts child plan first
4. **Rename columns**: Handles ExprId-based field naming

### 6. Rust Execution Plan

**File**: `native-engine/datafusion-ext-plans/src/filter_exec.rs`

```rust
#[derive(Debug, Clone)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicates: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl FilterExec {
    pub fn try_new(
        predicates: Vec<PhysicalExprRef>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = input.schema();

        // Validate predicates
        if predicates.is_empty() {
            df_execution_err!("Filter requires at least one predicate")?;
        }
        if !predicates
            .iter()
            .all(|pred| matches!(pred.data_type(&schema), Ok(DataType::Boolean)))
        {
            df_execution_err!("Filter predicate must return boolean values")?;
        }

        Ok(Self {
            input,
            predicates,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    pub fn predicates(&self) -> &[PhysicalExprRef] {
        &self.predicates
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &str {
        "FilterExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let predicates = self.predicates.clone();
        let exec_ctx = ExecutionContext::new(
            context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let filtered = execute_filter(input, predicates, exec_ctx.clone())?;
        Ok(exec_ctx.coalesce_with_default_batch_size(filtered))
    }

    // ... other required methods ...
}
```

**Key implementation details**:
1. **Validation**: Check predicates are non-empty and boolean
2. **Schema preservation**: Filter doesn't change schema
3. **Execution**: Apply predicates using DataFusion expressions
4. **Metrics**: Track computation time and output rows

### 7. Protobuf Deserialization

**File**: `native-engine/auron-serde/src/from_proto.rs`

```rust
impl TryFrom<&protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = PlanSerDeError;

    fn try_from(node: &protobuf::PhysicalPlanNode) -> Result<Self, Self::Error> {
        let plan = node.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error("PhysicalPlanNode::physical_plan_type is None")
        })?;

        use protobuf::physical_plan_node::PhysicalPlanType;
        match plan {
            // ... other operators ...
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicates = filter
                    .expr
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                    .collect::<Result<_, Self::Error>>()?;
                Ok(Arc::new(FilterExec::try_new(predicates, input)?))
            }
            // ... other operators ...
        }
    }
}
```

**Key implementation details**:
1. **Pattern matching**: Match `PhysicalPlanType::Filter`
2. **Recursive deserialization**: Convert input plan first
3. **Expression parsing**: Convert each predicate expression
4. **Error handling**: Propagate errors with context

### 8. Testing

**File**: `spark-extension-shims-spark3/src/test/scala/org/apache/spark/sql/auron/AuronQuerySuite.scala`

```scala
class AuronQuerySuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  test("test filter with year function") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkAnswer(
        sql("""
            |select year, count(*)
            |from (select event_time, year(event_time) as year from t1) t
            |where year <= 2024
            |group by year
            |""".stripMargin),
        Seq(Row(2024, 1)))
    }
  }

  test("test filter with complex conditions") {
    withTable("t1") {
      sql("create table t1 using parquet as select 1 as id, 'test' as name, 100 as value")
      checkAnswer(
        sql("select * from t1 where id > 0 AND name = 'test' AND value >= 100"),
        Seq(Row(1, "test", 100)))
    }
  }
}
```

**Testing best practices**:
1. **Use `withTable`**: Automatically cleans up test tables
2. **Use `checkAnswer`**: Validates results match expected
3. **Test edge cases**: Empty results, NULL handling, complex predicates
4. **Test integration**: Combine with other operators (join, aggregate, etc.)

## Step-by-Step Guide for Adding a New Operator

This section provides a generic checklist for implementing any new operator.

### Step 1: Define Protobuf Message

**File**: `native-engine/auron-serde/proto/auron.proto`

1. Add your operator to `PhysicalPlanNode`:
   ```protobuf
   message PhysicalPlanNode {
     oneof PhysicalPlanType {
       // ... existing operators ...
       YourOperatorExecNode your_operator = <next_available_number>;
     }
   }
   ```

2. Define the operator message:
   ```protobuf
   message YourOperatorExecNode {
     PhysicalPlanNode input = 1;  // For unary operators
     // Add operator-specific fields
     repeated PhysicalExprNode expressions = 2;
     YourOperatorConfig config = 3;
     // etc.
   }
   ```

**Tips**:
- Use descriptive field names
- Number fields sequentially starting from 1
- Use `repeated` for lists
- Reference existing operators for patterns

### Step 2: Create Base Class

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeYourOperatorBase.scala`

```scala
abstract class NativeYourOperatorBase(
    // Operator parameters
    param1: Type1,
    param2: Type2,
    override val child: SparkPlan)  // or children for binary operators
    extends UnaryExecNode  // or BinaryExecNode
    with NativeSupports {

  // 1. Define metrics
  override lazy val metrics: Map[String, SQLMetric] =
    SortedMap[String, SQLMetric]() ++ Map(
      NativeHelper.getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set(
          "stage_id",
          "output_rows",
          "elapsed_compute",
          // Add operator-specific metrics
        ))
        .toSeq: _*)

  // 2. Define output schema and partitioning
  override def output: Seq[Attribute] = /* ... */
  override def outputPartitioning: Partitioning = /* ... */
  override def outputOrdering: Seq[SortOrder] = /* ... */

  // 3. Convert expressions to protobuf
  private def nativeExprs = {
    // Convert Spark expressions to PhysicalExprNode
    expressions.map(NativeConverters.convertExpr)
  }

  // 4. Implement native execution
  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = /* determine partitioner */,
      rddDependencies = /* define dependencies */,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        // Build protobuf message
        val nativeExec = YourOperatorExecNode.newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpressions(nativeExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder()
          .setYourOperator(nativeExec)
          .build()
      },
      friendlyName = "NativeRDD.YourOperator")
  }
}
```

### Step 3: Create Concrete Class

**File**: `spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeYourOperatorExec.scala`

```scala
case class NativeYourOperatorExec(
    param1: Type1,
    param2: Type2,
    override val child: SparkPlan)
    extends NativeYourOperatorBase(param1, param2, child) {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
```

### Step 4: Add Shims Interface

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/Shims.scala`

```scala
trait Shims {
  // ... existing methods ...
  def createNativeYourOperatorExec(
      param1: Type1,
      param2: Type2,
      child: SparkPlan): NativeYourOperatorBase
}
```

**File**: `spark-extension-shims-spark3/src/main/scala/org/apache/spark/sql/auron/ShimsImpl.scala`

```scala
class ShimsImpl extends Shims {
  // ... existing methods ...
  override def createNativeYourOperatorExec(
      param1: Type1,
      param2: Type2,
      child: SparkPlan): NativeYourOperatorBase =
    NativeYourOperatorExec(param1, param2, child)
}
```

### Step 5: Add Conversion Logic

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala`

1. Add feature flag:
   ```scala
   def enableYourOperator: Boolean =
     getBooleanConf("spark.auron.enable.yourOperator", defaultValue = true)
   ```

2. Add pattern matching:
   ```scala
   def convertToNative(exec: SparkPlan): SparkPlan = {
     exec match {
       // ... existing cases ...
       case e: YourOperatorExec if enableYourOperator =>
         tryConvert(e, convertYourOperatorExec)
       // ... other cases ...
     }
   }
   ```

3. Add conversion method:
   ```scala
   def convertYourOperatorExec(exec: YourOperatorExec): SparkPlan = {
     logDebugPlanConversion(exec, Seq("param1" -> exec.param1))
     Shims.get.createNativeYourOperatorExec(
       exec.param1,
       exec.param2,
       addRenameColumnsExec(convertToNative(exec.child)))
   }
   ```

4. Add "never convert" reason:
   ```scala
   private def addNeverConvertReasonTag(exec: SparkPlan) = {
     val neverConvertReason = exec match {
       // ... existing cases ...
       case _: YourOperatorExec if !enableYourOperator =>
         "Conversion disabled: spark.auron.enable.yourOperator=false."
       // ... other cases ...
     }
   }
   ```

### Step 6: Implement Rust Execution Plan

**File**: `native-engine/datafusion-ext-plans/src/your_operator_exec.rs`

```rust
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::{
        DisplayAs, ExecutionPlan, ExecutionPlanProperties,
        SendableRecordBatchStream,
    },
};

#[derive(Debug, Clone)]
pub struct YourOperatorExec {
    input: Arc<dyn ExecutionPlan>,
    // Add operator-specific fields
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl YourOperatorExec {
    pub fn try_new(
        /* parameters */,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        // Validate parameters
        Ok(Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }
}

impl ExecutionPlan for YourOperatorExec {
    fn name(&self) -> &str {
        "YourOperatorExec"
    }

    fn schema(&self) -> SchemaRef {
        // Return output schema
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            /* parameters */,
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Implement execution logic
        let exec_ctx = ExecutionContext::new(
            context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;

        // Process input stream
        let output = /* your processing logic */;

        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                /* output partitioning */,
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn statistics(&self) -> Result<Statistics> {
        // Return statistics or todo!()
        todo!()
    }
}
```

**Add to module**: `native-engine/datafusion-ext-plans/src/lib.rs`
```rust
pub mod your_operator_exec;
```

### Step 7: Add Protobuf Deserialization

**File**: `native-engine/auron-serde/src/from_proto.rs`

1. Import your operator:
   ```rust
   use datafusion_ext_plans::your_operator_exec::YourOperatorExec;
   ```

2. Add conversion in `TryFrom` implementation:
   ```rust
   impl TryFrom<&protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
       type Error = PlanSerDeError;

       fn try_from(node: &protobuf::PhysicalPlanNode) -> Result<Self, Self::Error> {
           use protobuf::physical_plan_node::PhysicalPlanType;
           match plan {
               // ... existing cases ...
               PhysicalPlanType::YourOperator(op) => {
                   let input: Arc<dyn ExecutionPlan> =
                       convert_box_required!(op.input)?;

                   // Parse operator-specific fields
                   let exprs = op.expressions
                       .iter()
                       .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                       .collect::<Result<_, Self::Error>>()?;

                   Ok(Arc::new(YourOperatorExec::try_new(
                       /* parameters */,
                       input
                   )?))
               }
               // ... other cases ...
           }
       }
   }
   ```

### Step 8: Write Tests

**File**: `spark-extension-shims-spark3/src/test/scala/org/apache/spark/sql/auron/AuronYourOperatorSuite.scala`

```scala
class AuronYourOperatorSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  test("basic functionality") {
    withTable("test_table") {
      sql("CREATE TABLE test_table USING parquet AS SELECT 1 as id, 'test' as name")

      val result = sql("/* query that uses your operator */")

      checkAnswer(result, Seq(Row(/* expected values */)))
    }
  }

  test("edge case: empty input") {
    withTable("empty_table") {
      sql("CREATE TABLE empty_table(id INT, name STRING) USING parquet")

      val result = sql("/* query on empty table */")

      checkAnswer(result, Seq.empty)
    }
  }

  test("complex scenario with other operators") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 USING parquet AS SELECT 1 as id, 100 as value")
      sql("CREATE TABLE t2 USING parquet AS SELECT 1 as id, 'name1' as name")

      val result = sql("""
        SELECT t1.id, t1.value, t2.name
        FROM t1 JOIN t2 ON t1.id = t2.id
        /* your operator logic */
      """)

      checkAnswer(result, Seq(Row(1, 100, "name1")))
    }
  }
}
```

### Step 9: Build and Verify

1. **Build the project**:
   ```bash
   ./auron-build.sh
   ```

2. **Run scalastyle checks**:
   ```bash
   ./build/sbt "spark-extension/scalastyle"
   ./build/sbt "spark-extension-shims-spark3/scalastyle"
   ```

3. **Run tests**:
   ```bash
   # Run specific test suite
   ./build/sbt "spark-extension-shims-spark3/testOnly *AuronYourOperatorSuite"

   # Run all tests
   ./build/sbt "spark-extension-shims-spark3/test"
   ```

4. **Verify operator conversion**:
   - Set logging to DEBUG
   - Check for "Converting" log messages
   - Verify native execution in Spark UI

## Testing Your Operator

### Test Organization

Tests are organized in the `spark-extension-shims-spark3` module:

```
spark-extension-shims-spark3/src/test/scala/org/apache/spark/sql/auron/
├── AuronQuerySuite.scala          # General integration tests
├── AuronCheckConvertSuite.scala   # Conversion verification
├── BaseAuronSQLSuite.scala        # Base test infrastructure
└── YourOperatorSuite.scala        # Operator-specific tests
```

### Test Structure

```scala
class YourTestSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  test("descriptive test name") {
    // Setup
    withTable("table1") {
      sql("CREATE TABLE table1 ...")

      // Execute
      val result = sql("SELECT ...")

      // Verify
      checkAnswer(result, Seq(Row(...)))
    }
  }
}
```

### Best Practices

1. **Use `withTable`**: Automatically drops tables after test
   ```scala
   withTable("t1", "t2") {
     // Tables will be dropped even if test fails
   }
   ```

2. **Use `checkAnswer`**: Compares results regardless of order
   ```scala
   checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
   ```

3. **Test edge cases**:
   - Empty inputs
   - NULL values
   - Large datasets
   - Complex data types (arrays, maps, structs)

4. **Test integration**:
   - Combine with joins
   - Combine with aggregations
   - Test with different file formats (Parquet, ORC)

5. **Verify native execution**:
   ```scala
   assert(df.queryExecution.executedPlan.find(_.isInstanceOf[NativeYourOperatorExec]).isDefined)
   ```

### Running Tests

```bash
# Run all tests in a module
./build/sbt "spark-extension-shims-spark3/test"

# Run specific test suite
./build/sbt "spark-extension-shims-spark3/testOnly *AuronQuerySuite"

# Run specific test
./build/sbt "spark-extension-shims-spark3/testOnly *AuronQuerySuite -- -z 'filter with year'"

# Run tests with logging
./build/sbt "spark-extension-shims-spark3/testOnly *AuronQuerySuite" \
  -Dlog4j.configuration=file:///path/to/log4j.properties
```

## Build and Verification

### Build Process

Auron uses a unified build script that handles both local and Docker builds:

```bash
# Show all options
./auron-build.sh --help

# Local build (requires Rust and JDK installed)
./auron-build.sh

# Docker build (CentOS 7 environment)
./auron-build.sh --docker

# Build specific modules
./build/sbt "spark-extension/compile"
./build/sbt "spark-extension-shims-spark3/compile"
./build/sbt "native-engine/compile"  # Builds Rust code

# Clean build
./auron-build.sh --clean
```

### Build Output

- **Local builds**: `target/auron-spark-*.jar`
- **Docker builds**: `target-docker/auron-spark-*.jar`

### Verification Steps

After making changes, verify your implementation:

#### 1. Scalastyle Check

```bash
# Check all modules
./build/sbt scalastyle

# Check specific module
./build/sbt "spark-extension/scalastyle"
./build/sbt "spark-extension-shims-spark3/scalastyle"
```

**Common violations**:
- Lines longer than 100 characters
- Missing newline at end of file
- Trailing whitespace
- Incorrect imports

#### 2. Compile Check

```bash
# Compile Scala code
./build/sbt compile

# Compile Rust code
cd native-engine
cargo build --release
```

#### 3. Unit Tests

```bash
# Run all tests
./build/sbt test

# Run specific test suite
./build/sbt "testOnly *YourOperatorSuite"
```

#### 4. Integration Tests

```bash
# Run TPC-DS queries (if applicable)
./build/sbt "testOnly *AuronQuerySuite"
```

#### 5. Manual Verification

Test with spark-shell:

```bash
# Start spark-shell with Auron
spark-shell \
  --jars target/auron-spark-*.jar \
  --conf spark.auron.enable=true \
  --conf spark.sql.extensions=org.apache.spark.sql.auron.AuronSparkSessionExtension \
  --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager
```

```scala
// In spark-shell
spark.sql("CREATE TABLE test USING parquet AS SELECT 1 as id, 'test' as name")
val df = spark.sql("SELECT * FROM test WHERE id > 0")

// Check execution plan
df.explain()
df.queryExecution.executedPlan.treeString

// Verify native execution
assert(df.queryExecution.executedPlan.treeString.contains("Native"))
```

## Code Quality Standards

### Scalastyle Rules

Auron follows Apache Spark's scalastyle rules. Key requirements:

#### Line Length
- **Maximum 100 characters per line**
- Break long lines at logical points

```scala
// Bad
def methodWithManyParams(param1: String, param2: Int, param3: Boolean, param4: Double): Result = {

// Good
def methodWithManyParams(
    param1: String,
    param2: Int,
    param3: Boolean,
    param4: Double): Result = {
```

#### File Format
- Files must end with a newline character
- No trailing whitespace
- Use 2 spaces for indentation (no tabs)

#### Imports
- Organize imports logically
- Remove unused imports
- Use qualified imports when needed

```scala
// Group related imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.auron.protobuf.PhysicalPlanNode

import scala.collection.JavaConverters._
```

### Commit Guidelines

Following the CLAUDE.md guidelines:

1. **Incremental commits**: Break work into small, logical commits
2. **Test before commit**: Run tests and ensure they pass
3. **One feature per commit**: Keep commits focused
4. **Clear messages**: Describe what and why

```bash
# Good commit messages
git commit -m "Add NativeDistinctExec operator

Implements native execution for Distinct operator:
- Add protobuf definition for DistinctExecNode
- Implement NativeDistinctBase and NativeDistinctExec
- Add Rust execution plan with hash-based deduplication
- Add integration tests

Tests pass: AuronDistinctSuite (5 tests)"
```

### Code Review Checklist

Before submitting a PR, verify:

- [ ] All scalastyle checks pass
- [ ] All tests pass
- [ ] Code follows existing patterns
- [ ] Protobuf message is well-defined
- [ ] Metrics are properly defined
- [ ] Error handling is comprehensive
- [ ] Documentation is updated
- [ ] Examples are provided

## Troubleshooting

### Common Issues

#### 1. Protobuf Compilation Errors

**Error**: `Cannot find PhysicalPlanNode.setYourOperator`

**Solution**: Rebuild protobuf definitions
```bash
cd native-engine/auron-serde
cargo clean
cargo build
```

#### 2. Expression Conversion Failures

**Error**: `NativeConverterException: Unsupported expression type`

**Solution**:
- Check if the expression is supported in `NativeConverters.scala`
- Add conversion logic if needed
- Fall back to Spark execution for unsupported expressions

```scala
// Check supported expressions
try {
  val nativeExpr = NativeConverters.convertExpr(sparkExpr)
} catch {
  case _: NativeConverterException =>
    // Expression not supported, keep Spark execution
    return exec
}
```

#### 3. Rust Compilation Errors

**Error**: `cannot find type XxxExec in this scope`

**Solution**: Check imports and module declarations
```rust
// In lib.rs
pub mod your_operator_exec;

// In your operator file
use datafusion_ext_plans::your_operator_exec::YourOperatorExec;
```

#### 4. Scalastyle Violations

**Error**: `File line length exceeds 100 characters`

**Solution**: Break long lines
```bash
# Find long lines
grep -n '.\{101,\}' YourFile.scala

# Fix trailing whitespace
sed -i '' 's/[[:space:]]*$//' YourFile.scala

# Add final newline
echo >> YourFile.scala
```

#### 5. Test Failures

**Error**: Tests fail with ClassNotFoundException

**Solution**: Rebuild and ensure all modules are compiled
```bash
./auron-build.sh --clean
./auron-build.sh
./build/sbt "spark-extension-shims-spark3/test"
```

#### 6. Metrics Not Showing

**Error**: Operator metrics are empty in Spark UI

**Solution**: Ensure metrics are properly registered
```scala
// In base class
override lazy val metrics: Map[String, SQLMetric] =
  SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper.getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq: _*)

// In doExecuteNative
val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
```

### Debugging Tips

#### Enable Debug Logging

```bash
# In spark-defaults.conf or spark-shell
--conf spark.auron.logLevel=DEBUG
```

```scala
// In test code
import org.apache.log4j.{Level, Logger}
Logger.getLogger("org.apache.spark.sql.auron").setLevel(Level.DEBUG)
```

#### Inspect Execution Plans

```scala
// Show physical plan
df.explain(extended = true)

// Show execution plan tree
df.queryExecution.executedPlan.treeString

// Check for native operators
val hasNative = df.queryExecution.executedPlan.treeString.contains("Native")
println(s"Uses native execution: $hasNative")
```

#### Verify Protobuf Serialization

Add logging in `doExecuteNative`:
```scala
override def doExecuteNative(): NativeRDD = {
  val nativeExec = YourOperatorExecNode.newBuilder().build()
  println(s"Protobuf message: ${nativeExec.toString}")
  // ... rest of implementation
}
```

#### Check Rust Execution

Add logging in Rust:
```rust
impl ExecutionPlan for YourOperatorExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream> {
        println!("Executing YourOperatorExec partition {}", partition);
        // ... implementation
    }
}
```

## Deep Dive: Expression Conversion Patterns

Understanding expression conversion is critical for implementing new operators. This
section details how Spark expressions are converted to protobuf messages that the Rust
engine can execute.

### Expression Conversion Architecture

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeConverters.scala`

The expression converter uses a **three-tier fallback strategy**:

1. **Full native conversion**: Expression and all children convert successfully
2. **Partial native conversion**: Some children fail, fallback only those children
3. **UDF wrapper fallback**: Entire expression wraps in SparkUDFWrapper for Spark evaluation

### Supported Expression Categories

#### 1. Column References and Literals

```scala
// Column reference - uses ExprId in normal mode
case ar: AttributeReference =>
  buildExprNode(_.setColumn(
    pb.PhysicalColumn.newBuilder()
      .setName(Util.getFieldNameByExprId(ar))
      .build()))

// Literal - serialized as Arrow IPC bytes
case e: Literal =>
  // Converts to Arrow RecordBatch with single value
  pb.PhysicalExprNode.newBuilder()
    .setLiteral(pb.ScalarValue.newBuilder()
      .setIpcBytes(ByteString.copyFrom(ipcBytes)))
```

**Key points**:
- Columns use ExprId-based naming (e.g., `c_0`, `c_1`) for unique identification
- Literals are serialized using Arrow IPC format for type safety
- Pruning expressions (for file scans) use column names instead of ExprIds

#### 2. Binary Operators

```scala
case EqualTo(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Eq")
case GreaterThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Gt")
case LessThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Lt")
case And(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "And")
case Or(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Or")
```

**Supported operators**:
- Comparison: `Eq`, `NotEq`, `Gt`, `Lt`, `GtEq`, `LtEq`
- Logical: `And`, `Or`, `Not`
- Arithmetic: `Plus`, `Minus`, `Multiply`, `Divide`, `Modulo`
- Bitwise: `BitwiseAnd`, `BitwiseOr`, `BitwiseShiftLeft`, `BitwiseShiftRight`

#### 3. Cast Expressions

```scala
// Native cast (except timestamp/date)
case cast: Cast
    if !Seq(cast.dataType, cast.child.dataType).exists(t =>
      t.isInstanceOf[TimestampType] || t.isInstanceOf[DateType]) =>
  buildExprNode(_.setTryCast(
    pb.PhysicalTryCastNode.newBuilder()
      .setExpr(convertExprWithFallback(cast.child, ...))
      .setArrowType(convertDataType(cast.dataType))
      .build()))
```

**Important notes**:
- Timestamp/Date casts fall back to Spark (different semantics)
- Use `TryCast` (returns null on failure) instead of `Cast` (throws exception)
- String-to-numeric casts can optionally trim whitespace via config

#### 4. Aggregate Expressions

```scala
case e: Max =>
  aggBuilder.setAggFunction(pb.AggFunction.MAX)
  aggBuilder.addChildren(convertExpr(e.child))

case Count(children) if !children.exists(_.nullable) =>
  aggBuilder.setAggFunction(pb.AggFunction.COUNT)
  aggBuilder.addChildren(convertExpr(Literal.apply(1)))
```

**Supported aggregates**:
- Basic: `MAX`, `MIN`, `SUM`, `AVG`, `COUNT`
- Collection: `COLLECT_LIST`, `COLLECT_SET`
- Special: `FIRST`, `FIRST_IGNORES_NULL`
- Brickhouse UDAFs: `BRICKHOUSE_COLLECT`, `BRICKHOUSE_COMBINE_UNIQUE`
- Fallback: Generic `UDAF` wrapper for unsupported aggregates

#### 5. String Functions

```scala
case StartsWith(expr, Literal(prefix, StringType)) =>
  buildExprNode(_.setStringStartsWithExpr(
    pb.StringStartsWithExprNode.newBuilder()
      .setExpr(convertExprWithFallback(expr, ...))
      .setPrefix(prefix.toString)))

case Substring(str, Literal(pos, IntegerType), Literal(len, IntegerType))
    if pos.asInstanceOf[Int] > 0 && len.asInstanceOf[Int] >= 0 =>
  buildScalarFunction(
    pb.ScalarFunction.Substr,
    str :: Literal(longPos) :: Literal(longLen) :: Nil,
    StringType)
```

**Supported string functions**:
- Search: `StartsWith`, `EndsWith`, `Contains`
- Manipulation: `Substring`, `Trim`, `Ltrim`, `Rtrim`, `Upper`, `Lower`
- Combination: `Concat`, `ConcatWs`, `StringRepeat`, `StringSpace`
- Hash: `MD5`, `SHA2` (224, 256, 384, 512), `Murmur3Hash`, `XxHash64`

#### 6. Case/If Expressions

```scala
case e @ CaseWhen(branches, elseValue) =>
  val caseExpr = pb.PhysicalCaseNode.newBuilder()
  val whenThens = branches.map { case (w, t) =>
    val casted = t match {
      case t if t.dataType != e.dataType => Cast(t, e.dataType)
      case t => t
    }
    pb.PhysicalWhenThen.newBuilder()
      .setWhenExpr(convertExprWithFallback(w, ...))
      .setThenExpr(convertExprWithFallback(casted, ...))
      .build()
  }
```

**Key features**:
- Automatic type casting of THEN and ELSE branches
- `If` expressions convert to `CaseWhen` internally
- Short-circuit evaluation for Hive UDF conditions

### Expression Conversion Patterns

#### Pattern 1: Type Checking Before Conversion

```scala
// Good: Check data types before converting
def scalarTypeSupported(dataType: DataType): Boolean = {
  dataType match {
    case NullType | BooleanType | ByteType | ... => true
    case t: DecimalType if DecimalType.is64BitDecimalType(t) => true
    case _: DecimalType => false  // Only 64-bit decimal supported
    case at: ArrayType => scalarTypeSupported(at.elementType)
    case _ => false
  }
}
```

#### Pattern 2: Pruning vs Normal Mode

```scala
// Pruning mode (for file scans) - use column names
case ar: AttributeReference if isPruningExpr =>
  buildExprNode(_.setColumn(pb.PhysicalColumn.newBuilder().setName(ar.name)))

// Normal mode - use ExprId
case ar: AttributeReference =>
  buildExprNode(_.setColumn(
    pb.PhysicalColumn.newBuilder()
      .setName(Util.getFieldNameByExprId(ar))
      .build()))
```

#### Pattern 3: Fallback Strategy

```scala
// Try to convert expression
try {
  convertExprWithFallback(sparkExpr, isPruningExpr = false, fallbackToError)
} catch {
  case e: NotImplementedError =>
    // Fallback: Wrap in SparkUDFWrapper
    // 1. Bind convertible children to BoundReferences
    // 2. Serialize remaining Spark expression
    // 3. Create UDF wrapper that executes in Spark
    pb.PhysicalExprNode.newBuilder()
      .setSparkUdfWrapperExpr(
        pb.PhysicalSparkUDFWrapperExprNode.newBuilder()
          .setSerialized(ByteString.copyFrom(serialized))
          .setReturnType(convertDataType(bound.dataType))
          .addAllParams(convertedChildren.keys.asJava))
      .build()
}
```

### Common Expression Conversion Issues

#### Issue 1: Decimal Arithmetic

```scala
// Decimal operations require special handling
case e: Add if e.dataType.isInstanceOf[DecimalType] =>
  // Must calculate result precision and scale
  val resultScale = max(s1, s2)
  val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
  val resultType = DecimalType.adjustPrecisionScale(resultPrecision, resultScale)

  // Cast operands to result type, then add
  buildExprNode {
    _.setCast(pb.PhysicalCastNode.newBuilder()
      .setArrowType(convertDataType(resultType))
      .setExpr(buildExprNode {
        _.setBinaryExpr(
          pb.PhysicalBinaryExprNode.newBuilder()
            .setL(convertExprWithFallback(Cast(lhs, resultType), ...))
            .setR(convertExprWithFallback(rhs, ...))
            .setOp("Plus"))
      }))
  }
```

**Enable with**: `spark.auron.decimal.arithOp.enabled=true`

#### Issue 2: Division by Zero

```scala
// Divide operation must handle zero divisor
case e: Divide =>
  buildExprNode {
    _.setBinaryExpr(
      pb.PhysicalBinaryExprNode.newBuilder()
        .setL(convertExprWithFallback(lhs, ...))
        // Wrap divisor with NullIfZero to return null instead of error
        .setR(buildExtScalarFunction("NullIfZero", rhs :: Nil, rhs.dataType))
        .setOp("Divide"))
  }
```

#### Issue 3: Subquery Expressions

```scala
case subquery: ExecSubqueryExpression =>
  prepareExecSubquery(subquery)  // Ensure subquery is executed
  val serialized = serializeExpression(subquery, StructType(Nil))
  buildExprNode {
    _.setSparkScalarSubqueryWrapperExpr(
      pb.PhysicalSparkScalarSubqueryWrapperExprNode.newBuilder()
        .setSerialized(ByteString.copyFrom(serialized))
        .setReturnType(convertDataType(subquery.dataType)))
  }
```

### Data Type Conversion Reference

#### Supported Types

| Spark Type | Arrow Type | Notes |
|------------|------------|-------|
| `NullType` | `NONE` | |
| `BooleanType` | `BOOL` | |
| `ByteType` | `INT8` | |
| `ShortType` | `INT16` | |
| `IntegerType` | `INT32` | |
| `LongType` | `INT64` | |
| `FloatType` | `FLOAT32` | |
| `DoubleType` | `FLOAT64` | |
| `StringType` | `UTF8` | |
| `BinaryType` | `BINARY` | |
| `DateType` | `DATE32` | Days since epoch |
| `TimestampType` | `TIMESTAMP` | Microseconds, no timezone |
| `DecimalType` | `DECIMAL` | Only 64-bit (precision ≤ 18) |
| `ArrayType` | `LIST` | Recursive element type |
| `MapType` | `MAP` | Recursive key/value types |
| `StructType` | `STRUCT` | Recursive field types |

#### Unsupported Types

- `CalendarIntervalType` - No Arrow equivalent
- `DayTimeIntervalType` - Not yet implemented
- `YearMonthIntervalType` - Not yet implemented
- Decimal types with precision > 18 (128-bit decimals)

### Best Practices for Expression Conversion

1. **Always validate types**: Use `scalarTypeSupported()` before conversion
2. **Handle nullability**: Ensure expressions handle null values correctly
3. **Test with nulls**: Add test cases with NULL values in all positions
4. **Use helper functions**: `buildExprNode`, `buildBinaryExprNode`, etc.
5. **Recursive conversion**: Always convert child expressions recursively
6. **Fallback gracefully**: Let UDF wrapper handle unsupported expressions

## Deep Dive: Partitioning and RDD Dependencies

Understanding partitioning and dependencies is crucial for implementing operators that
handle data distribution correctly.

### RDD Dependency Types

Auron uses two main dependency types that determine how data flows between partitions:

#### 1. OneToOneDependency (Narrow)

**When to use**:
- Each output partition depends on exactly one input partition
- No data shuffling required
- Examples: Filter, Project, LocalLimit

**Example** (`NativeFilterBase.scala`):

```scala
override def doExecuteNative(): NativeRDD = {
  val inputRDD = NativeHelper.executeNative(child)
  val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

  new NativeRDD(
    sparkContext,
    nativeMetrics,
    rddPartitions = inputRDD.partitions,  // Same partitions as input
    rddPartitioner = inputRDD.partitioner, // Preserve partitioner
    rddDependencies = new OneToOneDependency(inputRDD) :: Nil,  // 1:1 mapping
    inputRDD.isShuffleReadFull,
    (partition, taskContext) => {
      val inputPartition = inputRDD.partitions(partition.index)
      // Process partition in place
      PhysicalPlanNode.newBuilder().setFilter(...).build()
    },
    friendlyName = "NativeRDD.Filter")
}
```

**Key characteristics**:
- `rddPartitions = inputRDD.partitions` - Same partition structure
- `rddPartitioner = inputRDD.partitioner` - Preserve partitioning
- `new OneToOneDependency(inputRDD)` - Direct partition mapping
- No network I/O between tasks

#### 2. ShuffleDependency (Wide)

**When to use**:
- Each output partition depends on multiple input partitions
- Data must be redistributed across nodes
- Examples: ShuffleExchange, Aggregate (with grouping), Join

**Example** (`NativeShuffleExchangeBase.scala`):

```scala
@transient
lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
  prepareNativeShuffleDependency(
    inputRDD,
    child.output,
    outputPartitioning,  // New partitioning scheme
    serializer,
    metrics)
}

override def doExecuteNative(): NativeRDD = {
  val shuffleHandle = shuffleDependency.shuffleHandle
  val rdd = doExecuteNonNative()

  new NativeRDD(
    sparkContext,
    nativeMetrics,
    rddPartitions = rdd.partitions,      // New partition structure
    rddPartitioner = rdd.partitioner,     // New partitioner
    rddDependencies = shuffleDependency :: Nil,  // Shuffle dependency
    Shims.get.getRDDShuffleReadFull(rdd),
    (partition, taskContext) => {
      // Read shuffled data for this partition
      PhysicalPlanNode.newBuilder()
        .setIpcReader(buildShuffleReader(...))
        .build()
    },
    friendlyName = "NativeRDD.ShuffleExchange")
}
```

**Key characteristics**:
- `ShuffleDependency` - Network shuffle
- New partition structure based on partitioning scheme
- Shuffle read/write metrics tracked separately

### Partitioning Schemes

#### 1. SinglePartition

**Use case**: Aggregate without grouping, GlobalLimit

```scala
override def outputPartitioning: Partitioning = SinglePartition

// Creates 1 partition containing all data
PhysicalPlanNode.newBuilder()
  .setRepartition(
    PhysicalRepartition.newBuilder()
      .setSingle(PhysicalSingleRepartition.getDefaultInstance))
```

#### 2. HashPartitioning

**Use case**: Shuffle for hash aggregation, hash join

```scala
override def outputPartitioning: Partitioning =
  HashPartitioning(groupingExpressions, numPartitions)

// Partitions data by hash of grouping expressions
val nativeHashExprs = expressions.map(expr =>
  NativeConverters.convertExpr(expr))

PhysicalPlanNode.newBuilder()
  .setRepartition(
    PhysicalRepartition.newBuilder()
      .setHash(
        PhysicalHashRepartition.newBuilder()
          .addAllHashExpr(nativeHashExprs.asJava)
          .setPartitionCount(numPartitions)))
```

#### 3. RangePartitioning

**Use case**: Sort operations requiring global ordering

```scala
override def outputPartitioning: Partitioning =
  RangePartitioning(sortOrder, numPartitions)

// Partitions data by sort key ranges
val nativeSortExprs = expressions.map { sortOrder =>
  PhysicalExprNode.newBuilder()
    .setSort(
      PhysicalSortExprNode.newBuilder()
        .setExpr(NativeConverters.convertExpr(sortOrder.child))
        .setAsc(sortOrder.direction == Ascending)
        .setNullsFirst(sortOrder.nullOrdering == NullsFirst))
    .build()
}

PhysicalPlanNode.newBuilder()
  .setRepartition(
    PhysicalRepartition.newBuilder()
      .setRange(
        PhysicalRangeRepartition.newBuilder()
          .addAllExpr(nativeSortExprs.asJava)
          .setPartitionCount(numPartitions)))
```

#### 4. RoundRobinPartitioning

**Use case**: Rebalance data evenly without specific key

```scala
override def outputPartitioning: Partitioning =
  RoundRobinPartitioning(numPartitions)

// Distributes rows evenly in round-robin fashion
PhysicalPlanNode.newBuilder()
  .setRepartition(
    PhysicalRepartition.newBuilder()
      .setRoundRobin(
        PhysicalRoundRobinRepartition.newBuilder()
          .setPartitionCount(numPartitions)))
```

**Important**: Not all data types supported (e.g., MapType fails)

### Multi-Input Dependencies (Union)

**Example** (`NativeUnionBase.scala`):

```scala
override def doExecuteNative(): NativeRDD = {
  val rdds = children.map(c => NativeHelper.executeNative(c))
  val unionRDD = sparkContext.union(rdds)

  new NativeRDD(
    sparkContext,
    nativeMetrics,
    unionRDD.partitions,      // Combined partitions
    unionRDD.partitioner,     // May be None
    unionRDD.dependencies,    // Multiple RangeDependencies
    rdds.forall(_.isShuffleReadFull),
    (partition, taskContext) => {
      // Determine which child RDD this partition belongs to
      partition match {
        case p: UnionPartition[_] =>
          val nativeRDD = rdds(p.parentRddIndex)
          val input = nativeRDD.nativePlan(p.parentPartition, taskContext)
          // Other children get empty partitions
          UnionExecNode.newBuilder()
            .addAllInput(unionInputs.asJava)
            .build()
      }
    },
    friendlyName = "NativeRDD.Union")
}
```

### Partition Preservation Patterns

#### Pattern 1: Preserve Input Partitioning

```scala
// Filter, Project, LocalLimit - maintain input partitioning
override def outputPartitioning: Partitioning = child.outputPartitioning
override def outputOrdering: Seq[SortOrder] = child.outputOrdering
```

#### Pattern 2: Change Partitioning

```scala
// ShuffleExchange - create new partitioning
override val outputPartitioning: Partitioning  // From constructor parameter

// Aggregate - depends on grouping keys
override def outputPartitioning: Partitioning = child.outputPartitioning match {
  case h: HashPartitioning if h.expressions == groupingExpressions =>
    h  // Preserve if already hash partitioned on same keys
  case _ =>
    UnknownPartitioning(0)  // Otherwise unknown
}
```

#### Pattern 3: Require Child Distribution

```scala
// Aggregate with grouping - requires clustered distribution
override def requiredChildDistribution: List[Distribution] = {
  requiredChildDistributionExpressions match {
    case Some(exprs) if exprs.isEmpty =>
      AllTuples :: Nil  // Require all data in single partition
    case Some(exprs) =>
      ClusteredDistribution(exprs) :: Nil  // Require hash/range partitioning
    case None =>
      UnspecifiedDistribution :: Nil  // No requirement
  }
}
```

### Best Practices for Partitioning

1. **Preserve when possible**: Use `child.outputPartitioning` for narrow operations
2. **Validate partitioner**: Ensure partitioner matches partition count
3. **Document requirements**: Use `requiredChildDistribution` to specify needs
4. **Test edge cases**: Empty partitions, single partition, uneven distribution
5. **Monitor shuffle**: Excessive shuffling indicates partitioning issues

## Deep Dive: Common Patterns and Anti-Patterns

This section highlights proven patterns to follow and common mistakes to avoid when
implementing new operators.

### Common Patterns (Good Practices)

#### Pattern 1: Always Use `addRenameColumnsExec`

**Why**: Spark uses ExprId-based column names internally (e.g., `c_0`, `c_1`), but native
operators need consistent naming across plan boundaries.

**Good**:
```scala
def convertFilterExec(exec: FilterExec): SparkPlan = {
  Shims.get.createNativeFilterExec(
    exec.condition,
    addRenameColumnsExec(convertToNative(exec.child)))  // ✓ Rename before native op
}
```

**Bad**:
```scala
def convertFilterExec(exec: FilterExec): SparkPlan = {
  Shims.get.createNativeFilterExec(
    exec.condition,
    convertToNative(exec.child))  // ✗ Missing rename - may cause field mismatches
}
```

**When to skip**: Only skip for `NativeShuffleExchange` and operators where the child is
guaranteed to already have correct naming.

#### Pattern 2: Use `tryConvert` for Error Handling

**Why**: Provides consistent error handling, logging, and fallback to Spark execution.

**Good**:
```scala
case e: YourOperatorExec if enableYourOperator =>
  tryConvert(e, convertYourOperatorExec)  // ✓ Handles exceptions gracefully
```

**Bad**:
```scala
case e: YourOperatorExec if enableYourOperator =>
  convertYourOperatorExec(e)  // ✗ Exceptions propagate, no fallback
```

**What `tryConvert` does**:
- Catches `NotImplementedError` and other exceptions
- Sets `convertibleTag` for tracking
- Logs conversion attempts for debugging
- Returns original Spark plan on failure

#### Pattern 3: Validate Conversions Early

**Why**: Fail fast during plan conversion, not during execution.

**Good**:
```scala
abstract class NativeYourOperatorBase(...) extends ... {
  // Convert and validate during plan construction
  private def nativeExprs = {
    expressions.map(NativeConverters.convertExpr)
  }

  // Trigger validation by accessing the val
  nativeExprs

  override def doExecuteNative(): NativeRDD = {
    // Use pre-validated expressions
    val nativeExprs = this.nativeExprs
    ...
  }
}
```

**Bad**:
```scala
override def doExecuteNative(): NativeRDD = {
  // ✗ Conversion may fail during execution, not during planning
  val nativeExprs = expressions.map(NativeConverters.convertExpr)
  ...
}
```

#### Pattern 4: Preserve Output Schema

**Why**: Ensures operator output matches Spark's expectations.

**Good**:
```scala
// Filter preserves input schema
override def output: Seq[Attribute] = FilterExec(condition, child).output

// Project changes schema based on expressions
override def output: Seq[Attribute] = projectList.map(_.toAttribute)
```

**Bad**:
```scala
// ✗ Hardcoding schema may not match Spark's expectations
override def output: Seq[Attribute] = child.output.map(a =>
  a.copy(name = "new_name")(a.exprId, a.qualifier))
```

#### Pattern 5: Consistent Metrics Definition

**Why**: Provides visibility into operator performance.

**Good**:
```scala
override lazy val metrics: Map[String, SQLMetric] =
  SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set(
        "stage_id",          // Always include for debugging
        "output_rows",       // Always include for cardinality
        "elapsed_compute",   // Always include for performance
        "operator_specific_metric"))  // Add operator-specific metrics
      .toSeq: _*)
```

**Bad**:
```scala
// ✗ Missing standard metrics
override lazy val metrics: Map[String, SQLMetric] = Map(
  "custom_metric" -> SQLMetrics.createMetric(sparkContext, "custom"))
```

#### Pattern 6: Recursive Child Conversion

**Why**: Ensures entire plan tree is converted to native.

**Good**:
```scala
def convertFilterExec(exec: FilterExec): SparkPlan = {
  Shims.get.createNativeFilterExec(
    exec.condition,
    addRenameColumnsExec(convertToNative(exec.child)))  // ✓ Recursive
}
```

**Bad**:
```scala
def convertFilterExec(exec: FilterExec): SparkPlan = {
  Shims.get.createNativeFilterExec(
    exec.condition,
    exec.child)  // ✗ Child may not be native
}
```

### Anti-Patterns (Common Mistakes)

#### Anti-Pattern 1: Ignoring Spark Version Differences

**Bad**:
```scala
// ✗ Assumes all Spark versions have same method
case class NativeFilterExec(...)
    extends NativeFilterBase(...) {
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
  // Missing withNewChildInternal for Spark 3.2+
}
```

**Good**:
```scala
case class NativeFilterExec(...)
    extends NativeFilterBase(...) {
  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
```

#### Anti-Pattern 2: Forgetting Protobuf Field Numbers

**Bad**:
```protobuf
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    FilterExecNode filter = 8;
    YourOperatorExecNode your_operator = 8;  // ✗ Duplicate number!
  }
}
```

**Good**:
```protobuf
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    FilterExecNode filter = 8;
    YourOperatorExecNode your_operator = 30;  // ✓ Unique number
  }
}
```

#### Anti-Pattern 3: Incorrect Metrics Reporting

**Bad**:
```scala
// ✗ Creating new MetricNode without parent metrics
val nativeMetrics = MetricNode(metrics, Nil)
```

**Good**:
```scala
// ✓ Include child metrics for proper aggregation
val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
```

#### Anti-Pattern 4: Not Handling Empty Inputs

**Bad**:
```scala
// ✗ Assumes partitions are non-empty
override def doExecuteNative(): NativeRDD = {
  val inputRDD = NativeHelper.executeNative(child)
  val firstPartition = inputRDD.partitions(0)  // May not exist!
  ...
}
```

**Good**:
```scala
override def doExecuteNative(): NativeRDD = {
  val inputRDD = NativeHelper.executeNative(child)
  if (inputRDD.partitions.isEmpty) {
    // Handle empty input gracefully
    return createEmptyNativeRDD()
  }
  ...
}
```

#### Anti-Pattern 5: Hardcoding Configuration Values

**Bad**:
```scala
// ✗ Hardcoded configuration
val bufferSize = 8192
```

**Good**:
```scala
// ✓ Use configuration system
def bufferSize: Int =
  AuronConverters.getIntConf("spark.auron.bufferSize", defaultValue = 8192)
```

#### Anti-Pattern 6: Mixing Scala and Java Collections

**Bad**:
```scala
import scala.collection.JavaConverters._

// ✗ Forgetting .asJava conversion
val nativeExec = FilterExecNode.newBuilder()
  .addAllExpr(nativeExprs)  // Compile error: expects Java List
  .build()
```

**Good**:
```scala
import scala.collection.JavaConverters._

val nativeExec = FilterExecNode.newBuilder()
  .addAllExpr(nativeExprs.asJava)  // ✓ Convert to Java List
  .build()
```

### Code Review Checklist

Before submitting your operator implementation, verify:

**Scala Implementation**:
- [ ] Used `tryConvert` in AuronConverters
- [ ] Called `addRenameColumnsExec` on child plans
- [ ] Validated expressions in base class constructor
- [ ] Defined metrics using `NativeHelper.getDefaultNativeMetrics`
- [ ] Implemented both `withNewChildInternal` and `withNewChildren`
- [ ] Converted Scala collections to Java with `.asJava`
- [ ] Preserved `outputPartitioning` and `outputOrdering` when appropriate

**Protobuf Definition**:
- [ ] Used unique field number in `PhysicalPlanNode`
- [ ] Followed naming convention: `YourOperatorExecNode`
- [ ] Included all necessary fields (input, expressions, config)

**Rust Implementation**:
- [ ] Added module to `lib.rs`
- [ ] Implemented all required `ExecutionPlan` methods
- [ ] Used `ExecutionContext` for metrics and batch coalescing
- [ ] Validated inputs in `try_new` constructor
- [ ] Handled errors with `Result<T>`

**Testing**:
- [ ] Added test suite in `spark-extension-shims-spark3/src/test`
- [ ] Tested with empty inputs
- [ ] Tested with NULL values
- [ ] Tested integration with other operators
- [ ] Verified native execution in plan

## Deep Dive: Metrics and Performance Monitoring

Metrics provide visibility into operator performance and are essential for debugging and
optimization. This section explains Auron's metrics system in detail.

### Metrics Architecture

#### MetricNode Structure

Metrics flow through the execution plan tree using `MetricNode`:

```scala
case class MetricNode(
  metrics: Map[String, SQLMetric],           // Operator's own metrics
  children: Seq[MetricNode],                 // Child operator metrics
  specialFn: Option[(String, Long) => Unit]  // Optional custom metric handling
)
```

**Example usage**:
```scala
override def doExecuteNative(): NativeRDD = {
  val inputRDD = NativeHelper.executeNative(child)

  // Create metric node linking this operator's metrics to child's metrics
  val nativeMetrics = MetricNode(
    metrics,                    // This operator's metrics
    inputRDD.metrics :: Nil,    // Child metrics
    None)                       // No special handling

  new NativeRDD(..., nativeMetrics, ...)
}
```

### Standard Metrics

#### Core Metrics (All Operators)

```scala
NativeHelper.getDefaultNativeMetrics(sparkContext).filterKeys(Set(
  "stage_id",         // Spark stage ID (for correlation)
  "output_rows",      // Number of rows produced
  "elapsed_compute"   // Native computation time (nanoseconds)
))
```

**When to use**:
- `stage_id`: Always include (required for Spark UI integration)
- `output_rows`: Always include (shows cardinality)
- `elapsed_compute`: Always include (shows performance)

#### I/O Metrics (Scan Operators)

```scala
Map(
  "bytes_scanned" -> SQLMetrics.createSizeMetric(sc, "Native.bytes_scanned"),
  "io_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.io_time"),
  "io_time_getfs" -> SQLMetrics.createNanoTimingMetric(sc, "Native.io_time_getfs")
)
```

**Use in**: `NativeParquetScanBase`, `NativeOrcScanBase`, `NativeFileSourceScanBase`

#### Join Metrics (Join Operators)

```scala
Map(
  "build_hash_map_time" -> nanoTimingMetric("Native.build_hash_map_time"),
  "probed_side_hash_time" -> nanoTimingMetric("Native.probed_side_hash_time"),
  "probed_side_search_time" -> nanoTimingMetric("Native.probed_side_search_time"),
  "probed_side_compare_time" -> nanoTimingMetric("Native.probed_side_compare_time"),
  "build_output_time" -> nanoTimingMetric("Native.build_output_time"),
  "fallback_sort_merge_join_time" -> nanoTimingMetric("...")
)
```

**Use in**: `NativeBroadcastJoinBase`, `NativeShuffledHashJoinBase`,
`NativeSortMergeJoinBase`

#### Spill Metrics (Memory-Intensive Operators)

```scala
Map(
  "mem_spill_count" -> metric("Native.mem_spill_count"),
  "mem_spill_size" -> sizeMetric("Native.mem_spill_size"),
  "mem_spill_iotime" -> nanoTimingMetric("Native.mem_spill_iotime"),
  "disk_spill_size" -> sizeMetric("Native.disk_spill_size"),
  "disk_spill_iotime" -> nanoTimingMetric("Native.disk_spill_iotime")
)
```

**Use in**: `NativeAggBase`, `NativeSortBase`, Join operators

#### Shuffle Metrics (Exchange Operators)

```scala
Map(
  "shuffle_write_total_time" -> nanoTimingMetric("Native.shuffle_write_total_time"),
  "shuffle_read_total_time" -> nanoTimingMetric("Native.shuffle_read_total_time")
)
```

**Use in**: `NativeShuffleExchangeBase`

#### Aggregate Metrics (Aggregate Operators)

```scala
Map(
  "hashing_time" -> nanoTimingMetric("Native.hashing_time"),
  "merging_time" -> nanoTimingMetric("Native.merging_time"),
  "output_time" -> nanoTimingMetric("Native.output_time")
)
```

**Use in**: `NativeAggBase`

### Metric Types

#### 1. Counter Metrics

```scala
// Count occurrences
def metric(name: String) = SQLMetrics.createMetric(sc, name)

// Example: counting rows
metrics("output_rows") += numRows
```

#### 2. Timing Metrics

```scala
// Measure elapsed time in nanoseconds
def nanoTimingMetric(name: String) =
  SQLMetrics.createNanoTimingMetric(sc, name)

// Example: timing computation
val startTime = System.nanoTime()
// ... do work ...
metrics("elapsed_compute") += (System.nanoTime() - startTime)
```

#### 3. Size Metrics

```scala
// Measure data sizes in bytes (displays as KB/MB/GB in UI)
def sizeMetric(name: String) = SQLMetrics.createSizeMetric(sc, name)

// Example: tracking bytes read
metrics("bytes_scanned") += bytesRead
```

### Adding Custom Metrics

#### Example: Window Operator Metrics

```scala
override lazy val metrics: Map[String, SQLMetric] =
  SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set(
        "stage_id",
        "output_rows",
        "elapsed_compute",
        "mem_spill_count",
        "mem_spill_size",
        "mem_spill_iotime",
        "disk_spill_size",
        "disk_spill_iotime"))
      .toSeq: _*) ++
  Map(
    // Custom metrics for window operations
    "window_compute_time" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "Native.window_compute_time"),
    "window_rows_processed" ->
      SQLMetrics.createMetric(sparkContext, "Native.window_rows_processed"))
```

### Metric Reporting Patterns

#### Pattern 1: Direct Metric Updates (Scala)

```scala
override def doExecuteNative(): NativeRDD = {
  // Metrics updated directly in Scala code
  metrics("output_rows") += resultRows
  metrics("elapsed_compute") += computeTime
}
```

**Use when**: Scala code has visibility to the metric values

#### Pattern 2: Native Metric Reporting (Rust)

```rust
// Rust side: report metrics via ExecutionContext
let exec_ctx = ExecutionContext::new(
    context, partition, self.schema(), &self.metrics);

// Metrics automatically tracked:
// - elapsed_compute: Total execution time
// - output_rows: Number of output rows
// - mem_spill_*: Memory spilling statistics
```

**Use when**: Metrics are generated during native execution

#### Pattern 3: Custom Metric Mapping

```scala
val nativeMetrics = MetricNode(
  metrics,
  inputRDD.metrics :: Nil,
  Some({
    // Map native metric names to Spark metric names
    case ("output_rows", v) =>
      val shuffleReadMetrics = TaskContext.get.taskMetrics()
        .createTempShuffleReadMetrics()
      new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics)
        .incRecordsRead(v)
      TaskContext.get.taskMetrics().mergeShuffleReadMetrics()
    case ("elapsed_compute", v) =>
      metrics("shuffle_read_total_time") += v
    case _ =>
  }))
```

**Use when**: Native metrics need special handling or mapping

### Input Batch Statistics (Optional)

Enable detailed input statistics:

```scala
spark.conf.set("spark.auron.input.batch.statistics.enable", "true")
```

**Additional metrics**:
```scala
Map(
  "input_batch_count" -> metric("Native.input_batches"),
  "input_row_count" -> metric("Native.input_rows"),
  "input_batch_mem_size" -> sizeMetric("Native.input_mem_bytes")
)
```

**Use for**: Debugging batch size issues, understanding data distribution

### Parquet-Specific Metrics

For Parquet scan operations:

```scala
Map(
  "predicate_evaluation_errors" -> metric("Native.predicate_evaluation_errors"),
  "row_groups_matched_bloom_filter" -> metric("Native.row_groups_matched_bloom_filter"),
  "row_groups_pruned_bloom_filter" -> metric("Native.row_groups_pruned_bloom_filter"),
  "row_groups_matched_statistics" -> metric("Native.row_groups_matched_statistics"),
  "row_groups_pruned_statistics" -> metric("Native.row_groups_pruned_statistics"),
  "pushdown_rows_filtered" -> metric("Native.pushdown_rows_filtered"),
  "pushdown_eval_time" -> nanoTimingMetric("Native.pushdown_eval_time"),
  "page_index_rows_filtered" -> metric("Native.page_index_rows_filtered"),
  "page_index_eval_time" -> nanoTimingMetric("Native.page_index_eval_time")
)
```

**Use for**: Understanding filter pushdown effectiveness

### Viewing Metrics

#### Spark UI

1. Navigate to SQL tab in Spark UI (http://localhost:4040/SQL/)
2. Click on query to see execution plan
3. Expand operator nodes to see metrics
4. Native operators show with "Native." prefix

#### Programmatic Access

```scala
// Get metrics from DataFrame execution
val df = spark.sql("SELECT * FROM table WHERE id > 100")
df.collect()  // Execute query

val metrics = df.queryExecution.executedPlan.metrics
metrics.foreach { case (name, metric) =>
  println(s"$name: ${metric.value}")
}
```

### Performance Debugging with Metrics

#### Identify Bottlenecks

```scala
// High elapsed_compute but low output_rows suggests expensive computation per row
if (metrics("elapsed_compute").value > threshold &&
    metrics("output_rows").value < expectedRows) {
  // Check expression complexity, consider optimization
}

// High spill metrics suggest memory pressure
if (metrics("mem_spill_count").value > 0) {
  // Increase executor memory or reduce batch size
}

// High shuffle times suggest network bottleneck
if (metrics("shuffle_read_total_time").value > threshold) {
  // Check data skew, consider repartitioning
}
```

### Best Practices for Metrics

1. **Always include core metrics**: `stage_id`, `output_rows`, `elapsed_compute`
2. **Use SortedMap**: Ensures consistent metric ordering in UI
3. **Follow naming convention**: Prefix with "Native." for clarity
4. **Choose appropriate metric type**: Counter, timing, or size
5. **Include child metrics**: `MetricNode(metrics, childMetrics :: Nil)`
6. **Document custom metrics**: Add comments explaining purpose
7. **Test metric accuracy**: Verify metrics match expected values

## Additional Resources

### Documentation

- **Apache Auron**: https://auron.apache.org/
- **Apache DataFusion**: https://arrow.apache.org/datafusion/
- **Apache Arrow**: https://arrow.apache.org/
- **Protocol Buffers**: https://developers.google.com/protocol-buffers

### Code References

- **Spark Internals**: https://github.com/apache/spark
- **DataFusion Examples**: https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples
- **Auron Source**: https://github.com/apache/auron

### Operator Examples

Study these existing operators for patterns:

| Operator | Complexity | Key Features |
|----------|-----------|--------------|
| Filter | Simple | Expression evaluation, predicate splitting |
| Project | Simple | Column projection, expression evaluation |
| Sort | Medium | Ordering, memory management |
| Aggregate | Complex | Hash aggregation, grouping, accumulation |
| Join | Complex | Hash join, sort-merge join, broadcast |
| Window | Complex | Partitioning, ordering, window functions |

### File Locations Quick Reference

```
auron/
├── spark-extension/
│   └── src/main/scala/org/apache/spark/sql/
│       ├── auron/
│       │   ├── AuronConverters.scala       # Conversion logic
│       │   ├── NativeConverters.scala      # Expression conversion
│       │   ├── NativeRDD.scala             # Native RDD wrapper
│       │   └── Shims.scala                 # Version abstraction
│       └── execution/auron/plan/
│           └── Native*Base.scala           # Base operator classes
│
├── spark-extension-shims-spark3/
│   ├── src/main/scala/org/apache/spark/sql/
│   │   ├── auron/ShimsImpl.scala          # Shims implementation
│   │   └── execution/auron/plan/
│   │       └── Native*Exec.scala          # Concrete operators
│   └── src/test/scala/org/apache/spark/sql/auron/
│       └── Auron*Suite.scala              # Tests
│
└── native-engine/
    ├── auron-serde/
    │   ├── proto/auron.proto              # Protobuf definitions
    │   └── src/from_proto.rs              # Deserialization
    └── datafusion-ext-plans/src/
        └── *_exec.rs                       # Rust operators
```

### Community and Support

- **Mailing List**: dev@auron.apache.org
- **Issues**: https://github.com/apache/auron/issues
- **Pull Requests**: https://github.com/apache/auron/pulls

### Getting Help

When asking for help, include:

1. **Error message**: Full stack trace
2. **Code snippet**: Minimal reproducible example
3. **Environment**: Spark version, Auron version, build mode
4. **What you tried**: Steps taken to debug
5. **Expected vs actual**: What should happen vs what happens

---

## Summary

Adding a new operator to Auron involves:

1. **Define** the protobuf message
2. **Implement** Scala base and concrete classes
3. **Integrate** via Shims and AuronConverters
4. **Implement** Rust execution plan
5. **Add** protobuf deserialization
6. **Test** with SQL queries
7. **Verify** build and scalastyle

By following this guide and studying existing operators, you can successfully contribute new operator support to Apache Auron!

For questions or issues, reach out to the community via the mailing list or GitHub issues.

Happy contributing! 🚀
