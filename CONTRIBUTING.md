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

## Deep Dive: Native Execution Pipeline

Understanding how plans execute in the Rust engine is essential for debugging and
optimization. This section explains the complete execution flow from protobuf to results.

### Execution Pipeline Overview

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Scala: NativeRDD.compute(partition, context)            │
│    └─> Serializes plan to protobuf                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. JNI Bridge: Call into native code                       │
│    └─> Pass protobuf bytes across JNI boundary            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Rust: Deserialize protobuf → ExecutionPlan              │
│    └─> from_proto.rs converts to Rust structs             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Rust: ExecutionPlan.execute(partition, context)         │
│    └─> Creates SendableRecordBatchStream                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. Rust: Stream processing with ExecutionContext           │
│    ├─> execute_with_input_stats() - track input stats    │
│    ├─> Process batches through operators                 │
│    └─> coalesce_with_default_batch_size() - optimize     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. Return: Arrow RecordBatches → Spark InternalRows        │
│    └─> Convert back to Spark format                       │
└─────────────────────────────────────────────────────────────┘
```

### ExecutionContext: The Central Coordinator

**File**: `native-engine/datafusion-ext-plans/src/common/execution_context.rs`

`ExecutionContext` is the heart of native execution, providing:

```rust
pub struct ExecutionContext {
    task_ctx: Arc<TaskContext>,           // DataFusion task context
    partition_id: usize,                  // Current partition number
    output_schema: SchemaRef,             // Expected output schema
    metrics: ExecutionPlanMetricsSet,     // Metrics collection
    baseline_metrics: BaselineMetrics,    // Core metrics (elapsed, output_rows)
    spill_metrics: Arc<OnceCell<SpillMetrics>>,  // Memory spill tracking
    input_stat_metrics: Arc<OnceCell<Option<InputBatchStatistics>>>,  // Input stats
}
```

#### Key Methods

**1. execute_with_input_stats** - Execute with automatic statistics

```rust
pub fn execute_with_input_stats(
    self: &Arc<Self>,
    input: &Arc<dyn ExecutionPlan>,
) -> Result<SendableRecordBatchStream> {
    let executed = self.execute(input)?;
    Ok(self.stat_input(executed))  // Wraps stream to track input batches
}
```

**What it does**:
- Executes the input plan
- Wraps output stream to automatically track:
  - `input_batch_count`: Number of input batches
  - `input_batch_mem_size`: Memory size of input batches
  - `input_row_count`: Number of input rows

**2. coalesce_with_default_batch_size** - Optimize batch sizes

```rust
pub fn coalesce_with_default_batch_size(
    self: &Arc<Self>,
    input: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    // Coalesces small batches into optimal sizes
    // Target: batch_size() rows or suggested_batch_mem_size() bytes
}
```

**What it does**:
- Combines small batches to reduce overhead
- Splits large batches to fit memory
- Target batch size: configurable (default ~8192 rows or ~8MB)
- Fast path: Batches > 25% of target pass through unchanged

**Example usage pattern**:

```rust
impl ExecutionPlan for YourOperatorExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // 1. Create execution context
        let exec_ctx = ExecutionContext::new(
            context,
            partition,
            self.schema(),
            &self.metrics
        );

        // 2. Execute input with stats tracking
        let input = exec_ctx.execute_with_input_stats(&self.input)?;

        // 3. Process the stream
        let processed = self.process_stream(input, exec_ctx.clone())?;

        // 4. Coalesce output batches
        Ok(exec_ctx.coalesce_with_default_batch_size(processed))
    }
}
```

### Stream Processing Model

Auron uses Rust's async streams (`SendableRecordBatchStream`) for data flow:

```rust
pub type SendableRecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;
```

**Benefits**:
- **Lazy evaluation**: Data pulled on demand
- **Pipelining**: Operators process batches as they arrive
- **Memory efficiency**: No need to materialize entire datasets
- **Backpressure**: Downstream operators control flow rate

**Example: Filter operator stream processing**

```rust
pub fn execute_filter(
    input: SendableRecordBatchStream,
    predicates: Vec<PhysicalExprRef>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let schema = exec_ctx.output_schema();
    let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();

    let filtered_stream = input.map(move |batch_result| {
        batch_result.and_then(|batch| {
            let _timer = elapsed_compute.timer();  // Track computation time

            // Evaluate all predicates
            let mut filter_array = None;
            for predicate in &predicates {
                let pred_result = predicate
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;
                filter_array = Some(match filter_array {
                    Some(existing) => and(&existing, &pred_result)?,
                    None => pred_result,
                });
            }

            // Apply filter
            let filtered = arrow::compute::filter_record_batch(
                &batch,
                &filter_array.unwrap()
            )?;
            Ok(filtered)
        })
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, filtered_stream)))
}
```

### Metrics Collection

Metrics are automatically collected at multiple points:

#### Baseline Metrics (Automatic)

```rust
let baseline_metrics = exec_ctx.baseline_metrics();

// Automatically tracked:
baseline_metrics.elapsed_compute()  // Total compute time
baseline_metrics.output_rows()      // Number of output rows
```

**How it works**:
- Compute time tracked via `_timer` guards
- Output rows counted automatically by stream adapter
- No manual tracking needed for these metrics

#### Custom Metrics

```rust
// Register custom metrics
let custom_timer = exec_ctx.register_timer_metric("custom_operation_time");
let custom_counter = exec_ctx.register_counter_metric("custom_operation_count");

// Use them
{
    let _timer = custom_timer.timer();
    // ... do custom operation ...
    custom_counter.add(1);
}
```

#### Spill Metrics (Automatic for memory-intensive ops)

```rust
let spill_metrics = exec_ctx.spill_metrics();

// Automatically tracked during spilling:
spill_metrics.mem_spill_count()    // Number of memory spills
spill_metrics.mem_spill_size()     // Bytes spilled to disk
spill_metrics.mem_spill_iotime()   // Time spent on I/O
```

### Memory Management

Auron uses DataFusion's memory pool for tracking and limiting memory usage:

```rust
// Check memory availability
let mem_pool = context.runtime_env().memory_pool.clone();
let available = mem_pool.available();
let reserved = mem_pool.reserved();

// Reserve memory (returns error if insufficient)
mem_pool.grow_reserved(required_bytes)?;

// Release memory when done
mem_pool.shrink_reserved(released_bytes);
```

**Spilling behavior**:
- When memory pool is exhausted, spill to disk
- Spill metrics automatically tracked
- Graceful degradation vs OOM errors

### Error Handling Across Scala/Rust Boundary

#### Rust Side: Result<T>

```rust
pub fn try_new(
    predicates: Vec<PhysicalExprRef>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Self> {
    // Validate inputs
    if predicates.is_empty() {
        return df_execution_err!("Filter requires at least one predicate");
    }

    // Return Ok or Err
    Ok(Self { input, predicates, ... })
}
```

#### Stream Processing: Result in Items

```rust
// Stream items are Result<RecordBatch>
impl Stream for FilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Process batch, may return Err
                Poll::Ready(Some(self.filter_batch(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),  // Propagate
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
```

#### JNI Boundary: Exception Handling

```rust
// Errors crossing JNI become Java exceptions
#[no_mangle]
pub extern "C" fn Java_..._execute(
    env: JNIEnv,
    ...
) -> jobject {
    match execute_native_plan(...) {
        Ok(iterator) => iterator_to_jobject(iterator),
        Err(e) => {
            // Convert to Java exception
            env.throw_new("java/lang/RuntimeException", e.to_string())
                .expect("Failed to throw exception");
            std::ptr::null_mut()
        }
    }
}
```

### Performance Optimization Techniques

#### 1. Batch Coalescing

```rust
// Small batches cause overhead
// Coalesce to target size: batch_size() rows or suggested_batch_mem_size() bytes
let coalesced = exec_ctx.coalesce_with_default_batch_size(input);
```

#### 2. Column Pruning

```rust
// Only project needed columns early
pub fn execute_projected(
    self: &Arc<Self>,
    input: &Arc<dyn ExecutionPlan>,
    projection: &[usize],
) -> Result<SendableRecordBatchStream> {
    // Reads only specified columns
    input.execute_projected(self.partition_id, self.task_ctx.clone(), projection)
}
```

#### 3. Predicate Pushdown

```rust
// Push filters close to data source
let predicate = scan.pruning_predicates
    .iter()
    .filter_map(|p| try_parse_physical_expr(p, &schema).ok())
    .fold(lit(true), |a, b| Arc::new(BinaryExpr::new(a, Operator::And, b)));

let parquet_exec = ParquetExec::new(config, fs_id, Some(predicate));
```

#### 4. Parallel Execution

```rust
// Each partition executes in parallel
// No synchronization needed for narrow dependencies
let num_partitions = input.output_partitioning().partition_count();
```

### Common Execution Patterns

#### Pattern 1: Unary Operator (Filter, Project)

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream> {
    let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
    let input = exec_ctx.execute_with_input_stats(&self.input)?;
    let processed = self.transform_stream(input)?;
    Ok(exec_ctx.coalesce_with_default_batch_size(processed))
}
```

#### Pattern 2: Binary Operator (Join)

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream> {
    let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);

    // Execute both inputs
    let left = exec_ctx.execute_with_input_stats(&self.left)?;
    let right = exec_ctx.execute_with_input_stats(&self.right)?;

    // Process (e.g., hash join)
    let joined = self.join_streams(left, right, exec_ctx.clone())?;
    Ok(exec_ctx.coalesce_with_default_batch_size(joined))
}
```

#### Pattern 3: Stateful Operator (Aggregate)

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>)
    -> Result<SendableRecordBatchStream> {
    let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
    let input = exec_ctx.execute_with_input_stats(&self.input)?;

    // Accumulate state, may spill
    let aggregated = exec_ctx.output_with_sender("aggregate", |sender| async move {
        let mut agg_table = AggTable::new(...)?;

        // Process input batches
        while let Some(batch) = input.next().await {
            agg_table.update(batch?)?;

            // Check memory pressure, spill if needed
            if should_spill() {
                agg_table.spill()?;
            }
        }

        // Output results
        for output_batch in agg_table.finish()? {
            sender.send(output_batch).await;
        }
        Ok(())
    })?;

    Ok(exec_ctx.coalesce_with_default_batch_size(aggregated))
}
```

## Deep Dive: Protobuf Serialization

Protobuf is the bridge between Scala and Rust. Understanding serialization is crucial for
adding new operators and debugging plan issues.

### Serialization Flow

```
Scala SparkPlan
       ↓
NativeXxxBase.doExecuteNative()
       ↓
Build protobuf message (XxxExecNode)
       ↓
PhysicalPlanNode.newBuilder()
       ↓
Serialize to bytes
       ↓
[JNI Boundary]
       ↓
Deserialize from bytes
       ↓
from_proto.rs: TryFrom<&PhysicalPlanNode>
       ↓
Rust ExecutionPlan (XxxExec)
```

### Protobuf Message Structure

**File**: `native-engine/auron-serde/proto/auron.proto`

#### Top-Level Container

```protobuf
message PhysicalPlanNode {
  oneof PhysicalPlanType {
    ProjectExecNode projection = 1;
    FilterExecNode filter = 8;
    ParquetScanNode parquet_scan = 9;
    HashJoinNode hash_join = 11;
    AggExecNode aggregate = 18;
    // ... more operators
  }
}
```

**Key points**:
- `oneof` = exactly one operator type per node
- Field numbers must be unique and stable (never reuse!)
- Child plans nested recursively

#### Operator Message Example

```protobuf
message FilterExecNode {
  PhysicalPlanNode input = 1;           // Child plan (recursive)
  repeated PhysicalExprNode expr = 2;   // List of filter predicates
}

message ProjectExecNode {
  PhysicalPlanNode input = 1;
  repeated PhysicalExprNode expr = 2;
  repeated string expr_name = 3;        // Column names
  repeated ArrowType data_type = 4;     // Expected output types
}
```

#### Expression Messages

```protobuf
message PhysicalExprNode {
  oneof ExprType {
    PhysicalColumn column = 1;
    ScalarValue literal = 2;
    PhysicalBinaryExprNode binary_expr = 3;
    PhysicalCaseNode case = 10;
    PhysicalScalarFunctionNode scalar_function = 15;
    // ... more expression types
  }
}

message PhysicalBinaryExprNode {
  PhysicalExprNode l = 1;    // Left operand
  PhysicalExprNode r = 2;    // Right operand
  string op = 3;             // Operator name: "Eq", "Add", "And", etc.
}
```

### Scala→Protobuf (Serialization)

#### Example: Filter Operator

**Scala side** (`NativeFilterBase.scala`):

```scala
override def doExecuteNative(): NativeRDD = {
  val inputRDD = NativeHelper.executeNative(child)
  val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

  // Pre-convert expressions (validates they're convertible)
  val nativeFilterExprs = this.nativeFilterExprs

  new NativeRDD(
    sparkContext,
    nativeMetrics,
    rddPartitions = inputRDD.partitions,
    rddPartitioner = inputRDD.partitioner,
    rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
    inputRDD.isShuffleReadFull,
    (partition, taskContext) => {
      // Build protobuf message for this partition
      val inputPartition = inputRDD.partitions(partition.index)
      val nativeFilterExec = FilterExecNode
        .newBuilder()
        .setInput(inputRDD.nativePlan(inputPartition, taskContext))  // Recursive!
        .addAllExpr(nativeFilterExprs.asJava)  // Convert Scala → Java collection
        .build()

      // Wrap in top-level container
      PhysicalPlanNode.newBuilder()
        .setFilter(nativeFilterExec)
        .build()
    },
    friendlyName = "NativeRDD.Filter")
}
```

**Key techniques**:
1. **Recursive child serialization**: `inputRDD.nativePlan(...)` gets child's protobuf
2. **Expression pre-conversion**: Validate during plan construction, not execution
3. **Collection conversion**: `.asJava` converts Scala Seq to Java List
4. **Wrapping**: Every operator wrapped in `PhysicalPlanNode`

#### Expression Serialization

**NativeConverters.scala**:

```scala
def convertExpr(sparkExpr: Expression): pb.PhysicalExprNode = {
  sparkExpr match {
    // Literal → ScalarValue (Arrow IPC bytes)
    case e: Literal =>
      val schema = StructType(Seq(StructField("", e.dataType, e.nullable)))
      val row = InternalRow(e.eval(null))
      val ipcBytes = serializeToArrowIPC(schema, row)
      pb.PhysicalExprNode.newBuilder()
        .setLiteral(pb.ScalarValue.newBuilder().setIpcBytes(ByteString.copyFrom(ipcBytes)))
        .build()

    // Column → PhysicalColumn
    case ar: AttributeReference =>
      buildExprNode(_.setColumn(
        pb.PhysicalColumn.newBuilder()
          .setName(Util.getFieldNameByExprId(ar))
          .build()))

    // Binary operation → PhysicalBinaryExprNode
    case EqualTo(lhs, rhs) =>
      buildExprNode(_.setBinaryExpr(
        pb.PhysicalBinaryExprNode.newBuilder()
          .setL(convertExpr(lhs))     // Recursive!
          .setR(convertExpr(rhs))
          .setOp("Eq")))

    // ... more patterns
  }
}
```

### Protobuf→Rust (Deserialization)

**File**: `native-engine/auron-serde/src/from_proto.rs`

#### Top-Level Conversion

```rust
impl TryFrom<&protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = PlanSerDeError;

    fn try_from(node: &protobuf::PhysicalPlanNode) -> Result<Self, Self::Error> {
        let plan = node.physical_plan_type.as_ref()
            .ok_or_else(|| proto_error("PhysicalPlanNode::physical_plan_type is None"))?;

        use protobuf::physical_plan_node::PhysicalPlanType;
        match plan {
            PhysicalPlanType::Filter(filter) => {
                // 1. Convert child plan (recursive)
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;

                // 2. Convert expressions
                let predicates = filter.expr
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                    .collect::<Result<_, Self::Error>>()?;

                // 3. Create Rust operator
                Ok(Arc::new(FilterExec::try_new(predicates, input)?))
            }

            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(projection.input)?;
                let input_schema = input.schema();

                // Parse expressions with expected types
                let exprs = projection.expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .zip(projection.data_type.iter())
                    .map(|((expr, name), data_type)| {
                        let physical_expr = try_parse_physical_expr(expr, &input_schema)?;
                        let data_type: DataType = data_type.try_into()?;

                        // Cast if necessary
                        let casted = if physical_expr.data_type(&input_schema)? == data_type {
                            physical_expr
                        } else {
                            Arc::new(TryCastExpr::new(physical_expr, data_type))
                        };
                        Ok((casted, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, Self::Error>>()?;

                Ok(Arc::new(ProjectExec::try_new(exprs, input)?))
            }

            // ... more operators
        }
    }
}
```

**Macros for cleaner code**:

```rust
// convert_box_required! - unwrap Option<Box<T>> and convert
let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;

// Expands to:
let input: Arc<dyn ExecutionPlan> = filter.input
    .as_ref()
    .ok_or_else(|| proto_error("input is None"))?
    .as_ref()
    .try_into()?;
```

#### Expression Deserialization

```rust
pub fn try_parse_physical_expr(
    expr: &protobuf::PhysicalExprNode,
    schema: &SchemaRef,
) -> Result<PhysicalExprRef, PlanSerDeError> {
    let expr_type = expr.expr_type.as_ref()
        .ok_or_else(|| proto_error("PhysicalExprNode::expr_type is None"))?;

    use protobuf::physical_expr_node::ExprType;
    match expr_type {
        ExprType::Column(col) => {
            // Find column by name in schema
            let field = schema.field_with_name(&col.name)?;
            let index = schema.index_of(&col.name)?;
            Ok(Arc::new(Column::new(&field.name(), index)))
        }

        ExprType::Literal(lit) => {
            // Deserialize Arrow IPC bytes
            let scalar_value = deserialize_scalar_value(&lit.ipc_bytes)?;
            Ok(Arc::new(Literal::new(scalar_value)))
        }

        ExprType::BinaryExpr(binary) => {
            // Recursive expression parsing
            let left = try_parse_physical_expr(&binary.l.as_ref().unwrap(), schema)?;
            let right = try_parse_physical_expr(&binary.r.as_ref().unwrap(), schema)?;
            let op = parse_operator(&binary.op)?;
            Ok(Arc::new(BinaryExpr::new(left, op, right)))
        }

        // ... more expression types
    }
}
```

### Data Type Conversion

#### Scala→Protobuf

```scala
def convertDataType(sparkDataType: DataType): pb.ArrowType = {
  val builder = pb.ArrowType.newBuilder()
  sparkDataType match {
    case IntegerType => builder.setINT32(pb.EmptyMessage.getDefaultInstance)
    case LongType => builder.setINT64(pb.EmptyMessage.getDefaultInstance)
    case StringType => builder.setUTF8(pb.EmptyMessage.getDefaultInstance)
    case t: DecimalType =>
      builder.setDECIMAL(
        pb.Decimal.newBuilder()
          .setWhole(max(t.precision, 1))
          .setFractional(t.scale)
          .build())
    case a: ArrayType =>
      builder.setLIST(
        pb.List.newBuilder()
          .setFieldType(
            pb.Field.newBuilder()
              .setName("item")
              .setArrowType(convertDataType(a.elementType))
              .setNullable(a.containsNull))
          .build())
    // ... more types
  }
  builder.build()
}
```

#### Protobuf→Rust

```rust
impl TryFrom<&protobuf::ArrowType> for DataType {
    type Error = PlanSerDeError;

    fn try_from(arrow_type: &protobuf::ArrowType) -> Result<Self, Self::Error> {
        let arrow_type_enum = arrow_type.arrow_type_enum.as_ref()
            .ok_or_else(|| proto_error("ArrowType::arrow_type_enum is None"))?;

        use protobuf::arrow_type::ArrowTypeEnum;
        match arrow_type_enum {
            ArrowTypeEnum::Int32(_) => Ok(DataType::Int32),
            ArrowTypeEnum::Int64(_) => Ok(DataType::Int64),
            ArrowTypeEnum::Utf8(_) => Ok(DataType::Utf8),
            ArrowTypeEnum::Decimal(d) => {
                Ok(DataType::Decimal128(d.whole as u8, d.fractional as i8))
            }
            ArrowTypeEnum::List(list) => {
                let field: FieldRef = list.field_type.as_ref()
                    .ok_or_else(|| proto_error("List::field_type is None"))?
                    .try_into()?;
                Ok(DataType::List(field))
            }
            // ... more types
        }
    }
}
```

### Serialization Best Practices

1. **Version Compatibility**
   - Never change field numbers
   - Never reuse deleted field numbers
   - Add new fields with new numbers
   - Use `optional` for fields that may not exist in older versions

2. **Validation**
   - Validate in Scala during plan construction
   - Fail fast, not during execution
   - Use `try_new()` in Rust for additional validation

3. **Collection Handling**
   - Scala: Use `.asJava` to convert Seq → Java List
   - Protobuf: Use `repeated` for lists
   - Rust: Iterate and collect into Vec

4. **Recursive Structures**
   - Always handle child plans first
   - Build from leaves to root
   - Avoid cycles (not possible in execution plans)

5. **Error Messages**
   - Include context in error messages
   - Show which field/operator failed
   - Help users debug serialization issues

## Deep Dive: Operator Lifecycle

Understanding the complete lifecycle helps debug issues and understand performance.

### Lifecycle Stages

```
┌─────────────────────────────────────────────────────────────┐
│ Stage 1: SPARK LOGICAL PLAN                                │
│ ────────────────────────────────────────────────────────── │
│ Spark SQL → Catalyst logical plan                          │
│ Example: Filter(col("id") > 100, Relation("table"))       │
└─────────────────────────────────────────────────────────────┘
                            ↓ Catalyst Optimizer
┌─────────────────────────────────────────────────────────────┐
│ Stage 2: SPARK PHYSICAL PLAN                               │
│ ────────────────────────────────────────────────────────── │
│ Logical → Physical via strategies                          │
│ Example: FilterExec(col("id") > 100,                      │
│            FileSourceScanExec("table"))                    │
└─────────────────────────────────────────────────────────────┘
                            ↓ AuronSparkSessionExtension
┌─────────────────────────────────────────────────────────────┐
│ Stage 3: CONVERSION TO NATIVE (Scala)                      │
│ ────────────────────────────────────────────────────────── │
│ AuronConverters.convertToNative(plan)                     │
│ ├─> Pattern match on operator type                        │
│ ├─> tryConvert(exec, convertXxxExec)                     │
│ ├─> Validate expressions convertible                      │
│ └─> Create NativeXxxExec                                  │
│                                                            │
│ Example: FilterExec → NativeFilterExec                    │
└─────────────────────────────────────────────────────────────┘
                            ↓ Spark execution
┌─────────────────────────────────────────────────────────────┐
│ Stage 4: EXECUTION STARTS (Scala)                          │
│ ────────────────────────────────────────────────────────── │
│ NativeFilterExec.doExecuteNative()                        │
│ ├─> Get child NativeRDD                                   │
│ ├─> Create new NativeRDD with:                            │
│ │   ├─> Partitions (from child)                           │
│ │   ├─> Dependencies (OneToOneDependency)                 │
│ │   └─> nativePlan function (builds protobuf)            │
│ └─> Return NativeRDD                                       │
└─────────────────────────────────────────────────────────────┘
                            ↓ Spark scheduler
┌─────────────────────────────────────────────────────────────┐
│ Stage 5: PARTITION COMPUTATION (Scala)                     │
│ ────────────────────────────────────────────────────────── │
│ NativeRDD.compute(partition, taskContext)                 │
│ ├─> Call nativePlan function                              │
│ ├─> Build FilterExecNode protobuf                         │
│ │   ├─> setInput(childPlan) - recursive                   │
│ │   └─> addAllExpr(nativeExprs)                           │
│ ├─> Wrap in PhysicalPlanNode                              │
│ └─> Serialize to bytes                                     │
└─────────────────────────────────────────────────────────────┘
                            ↓ JNI call
┌─────────────────────────────────────────────────────────────┐
│ Stage 6: NATIVE EXECUTION (Rust)                           │
│ ────────────────────────────────────────────────────────── │
│ executeNativePlan(protobufBytes)                          │
│ ├─> Deserialize PhysicalPlanNode                          │
│ ├─> from_proto: match on PhysicalPlanType                 │
│ ├─> FilterExec::try_new(predicates, input)               │
│ └─> ExecutionPlan created                                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Stage 7: STREAM EXECUTION (Rust)                           │
│ ────────────────────────────────────────────────────────── │
│ FilterExec.execute(partition, context)                    │
│ ├─> Create ExecutionContext                               │
│ ├─> exec_ctx.execute_with_input_stats(input)             │
│ ├─> Process stream:                                        │
│ │   ├─> Poll input for next batch                         │
│ │   ├─> Evaluate predicates                               │
│ │   ├─> Filter rows                                        │
│ │   ├─> Update metrics                                     │
│ │   └─> Yield filtered batch                              │
│ └─> exec_ctx.coalesce_with_default_batch_size(output)    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Stage 8: RESULTS RETURN (Rust → Scala)                     │
│ ────────────────────────────────────────────────────────── │
│ Arrow RecordBatches → Iterator<InternalRow>               │
│ ├─> Convert Arrow arrays to Spark UnsafeRow               │
│ ├─> Return to Scala via JNI                               │
│ └─> Metrics reported back                                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Stage 9: RESULTS CONSUMPTION (Scala/User)                  │
│ ────────────────────────────────────────────────────────── │
│ df.collect() / df.write.save() / df.show()                │
│ └─> Consumes iterator, triggers computation                │
└─────────────────────────────────────────────────────────────┘
```

### Key Lifecycle Points

#### 1. Plan Conversion Decision Point

**Where**: `AuronConverters.convertToNative()`

**Decision factors**:
- Is operator type supported? (e.g., `enableFilter`)
- Are expressions convertible? (checked in `tryConvert`)
- Is conversion beneficial? (some ops slower in native)

**Fallback behavior**:
```scala
case e: FilterExec if enableFilter =>
  tryConvert(e, convertFilterExec)  // Try native, fallback to Spark on error
case e: FilterExec =>
  e  // Keep Spark execution if disabled
```

#### 2. Expression Validation Point

**Where**: Base class constructor

**Why early validation**:
- Fail during planning, not execution
- Clear error messages
- Avoid wasted work

```scala
abstract class NativeFilterBase(...) {
  // Validate expressions eagerly
  private def nativeFilterExprs = {
    expressions.map(NativeConverters.convertExpr)  // Throws if unsupported
  }
  nativeFilterExprs  // Trigger validation during construction
}
```

#### 3. Partition-Level Plan Building

**Where**: `NativeRDD.compute()`

**Why per-partition**:
- Different partitions may have different inputs (e.g., after shuffle)
- Allows partition-specific optimizations
- Serialization happens lazily

```scala
new NativeRDD(
  ...
  (partition, taskContext) => {
    // Called once per partition
    val inputPlan = inputRDD.nativePlan(partition, taskContext)
    PhysicalPlanNode.newBuilder()
      .setFilter(FilterExecNode.newBuilder().setInput(inputPlan))
      .build()
  }
)
```

#### 4. Stream Materialization Point

**Where**: `ExecutionPlan.execute()`

**Lazy evaluation**:
```rust
fn execute(...) -> Result<SendableRecordBatchStream> {
    // Returns a stream (lazy)
    // No computation happens until stream is polled
    Ok(Box::pin(FilterStream { ... }))
}
```

**Actual computation**:
```rust
// Happens when stream is polled
impl Stream for FilterStream {
    fn poll_next(...) -> Poll<Option<Result<RecordBatch>>> {
        // NOW the actual filtering happens
    }
}
```

### Lifecycle Debugging Techniques

#### Enable Debug Logging

```scala
// Scala side
Logger.getLogger("org.apache.spark.sql.auron").setLevel(Level.DEBUG)

// See conversion decisions
// [DEBUG] Converting FilterExec to NativeFilterExec
// [DEBUG] Converting expression: (id > 100)
```

#### Inspect Execution Plans

```scala
val df = spark.sql("SELECT * FROM table WHERE id > 100")

// Logical plan
df.queryExecution.logical.treeString

// Physical plan (before conversion)
df.queryExecution.sparkPlan.treeString

// Executed plan (after conversion)
df.queryExecution.executedPlan.treeString
// Should show NativeFilterExec if converted
```

#### Check Protobuf Serialization

Add logging in `doExecuteNative()`:
```scala
override def doExecuteNative(): NativeRDD = {
  new NativeRDD(
    ...
    (partition, taskContext) => {
      val plan = PhysicalPlanNode.newBuilder()...build()
      logInfo(s"Partition $partition plan: $plan")  // Log protobuf
      plan
    }
  )
}
```

#### Monitor Metrics

```scala
df.collect()  // Execute query

df.queryExecution.executedPlan.metrics.foreach { case (name, metric) =>
  println(s"$name: ${metric.value}")
}
// output_rows: 1000
// elapsed_compute: 123456789 (nanoseconds)
```

## Deep Dive: Shims System for Version Compatibility

Auron supports multiple Spark versions (3.2, 3.3, 3.4, 3.5) using a "shims" abstraction layer.
Understanding this system is critical when adding operators that interact with Spark APIs.

### Shims Architecture

```
┌───────────────────────────────────────────────────────────┐
│  Common Code (spark-extension)                            │
│  - NativeXxxBase abstract classes                         │
│  - Shims.scala interface                                  │
│  - Version-agnostic logic                                 │
└───────────────────────────────────────────────────────────┘
                          ↓ Uses shims interface
┌───────────────────────────────────────────────────────────┐
│  Version-Specific Code (spark-extension-shims-spark3*)    │
│  - ShimsImpl.scala (per version)                         │
│  - NativeXxxExec concrete classes                        │
│  - Version-specific API calls                            │
└───────────────────────────────────────────────────────────┘
```

### Shims Interface

**File**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/Shims.scala`

Defines version-agnostic interface for creating operators:

```scala
abstract class Shims {
  def shimVersion: String  // e.g., "3.3", "3.4", "3.5"

  // Factory methods for native operators
  def createNativeFilterExec(
      condition: Expression,
      child: SparkPlan): NativeFilterBase

  def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan): NativeProjectBase

  def createNativeAggExec(
      execMode: NativeAggBase.AggExecMode,
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      child: SparkPlan): NativeAggBase

  // ... more factory methods
}

object Shims {
  private lazy val _shims: Shims = {
    // Detect Spark version at runtime
    val version = org.apache.spark.SPARK_VERSION
    version match {
      case v if v.startsWith("3.2") => new org.apache.spark.sql.auron.ShimsImpl$()
      case v if v.startsWith("3.3") => new org.apache.spark.sql.auron.ShimsImpl$()
      case v if v.startsWith("3.4") => new org.apache.spark.sql.auron.ShimsImpl$()
      case v if v.startsWith("3.5") => new org.apache.spark.sql.auron.ShimsImpl$()
      case _ => throw new UnsupportedOperationException(s"Unsupported Spark version: $version")
    }
  }

  def get: Shims = _shims
}
```

### Using Shims in Common Code

**Pattern**: Always create operators through shims, never directly

```scala
// Good: Version-agnostic using shims
def convertFilterExec(exec: FilterExec): SparkPlan = {
  Shims.get.createNativeFilterExec(
    exec.condition,
    addRenameColumnsExec(convertToNative(exec.child)))
}

// Bad: Direct instantiation (won't work across versions)
def convertFilterExec(exec: FilterExec): SparkPlan = {
  new NativeFilterExec(  // ✗ Error: Class doesn't exist in common code
    exec.condition,
    addRenameColumnsExec(convertToNative(exec.child)))
}
```

### Implementing Version-Specific Code

**Directory structure**:
```
spark-extension-shims-spark32/  # For Spark 3.2
spark-extension-shims-spark33/  # For Spark 3.3
spark-extension-shims-spark34/  # For Spark 3.4
spark-extension-shims-spark35/  # For Spark 3.5
```

**File**: `spark-extension-shims-spark3*/src/main/scala/org/apache/spark/sql/auron/ShimsImpl.scala`

```scala
object ShimsImpl extends Shims {
  override def shimVersion: String = "3.5"  // Or appropriate version

  override def createNativeFilterExec(
      condition: Expression,
      child: SparkPlan): NativeFilterBase = {
    NativeFilterExec(condition, child)
  }

  // ... implement all abstract methods
}
```

**File**: `spark-extension-shims-spark3*/src/main/scala/.../plan/NativeFilterExec.scala`

```scala
case class NativeFilterExec(
    condition: Expression,
    child: SparkPlan)
    extends NativeFilterBase(condition, child) {

  // Spark 3.2+ method
  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  // Spark 3.0 / 3.1 method (if supporting older versions)
  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
```

### Handling API Differences Across Versions

#### Method Name Changes

```scala
// Spark 3.0-3.1 uses withNewChildren
// Spark 3.2+ uses withNewChildInternal

// Solution: Implement both, annotate with @sparkver
case class NativeFilterExec(...) extends NativeFilterBase(...) {
  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
```

#### Signature Changes

```scala
// Example: BroadcastExchangeExec signature changed in Spark 3.2

// Spark 3.0-3.1
class BroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)

// Spark 3.2+
class BroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan, shuffleOrigin: ...)

// Solution: Add version-specific parameters in shims
abstract class Shims {
  def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase

  // Spark 3.2+ overload
  def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan,
      shuffleOrigin: Option[Any]): NativeBroadcastExchangeBase =
    createNativeBroadcastExchangeExec(mode, child)  // Default implementation
}
```

#### Class Existence Checks

```scala
// Some classes don't exist in all versions
// Example: WindowGroupLimitExec only in Spark 3.5+

def convertWindowGroupLimitExec(exec: SparkPlan): SparkPlan = {
  // Use reflection to check class existence
  if (e.getClass.getSimpleName == "WindowGroupLimitExec" && enableWindowGroupLimit) {
    tryConvert(e, convertWindowGroupLimitExec)
  } else {
    e  // Keep Spark execution
  }
}
```

### `@sparkver` Annotation

**Purpose**: Document which Spark versions a code block supports

```scala
@sparkver("3.2 / 3.3 / 3.4 / 3.5")
override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ...
```

**Note**: This is primarily documentation; actual compilation is controlled by build configuration.

### Build Configuration

**File**: `pom.xml` (root)

Multiple profiles for different Spark versions:

```xml
<profiles>
  <profile>
    <id>spark-3.2</id>
    <modules>
      <module>spark-extension-shims-spark32</module>
    </modules>
  </profile>
  <profile>
    <id>spark-3.3</id>
    <modules>
      <module>spark-extension-shims-spark33</module>
    </modules>
  </profile>
  <!-- ... more versions -->
</profiles>
```

**Building for specific version**:
```bash
# Build for Spark 3.5 (default)
./build/sbt compile

# Build for Spark 3.3
./build/sbt -Pspark-3.3 compile

# Build for multiple versions
./build/sbt -Pspark-3.3,spark-3.4,spark-3.5 compile
```

### Best Practices for Shims

1. **Always use shims for operator creation**
   - Never instantiate `NativeXxxExec` directly in common code
   - Use `Shims.get.createNativeXxxExec(...)` instead

2. **Keep base classes version-agnostic**
   - Put all common logic in `NativeXxxBase`
   - Only version-specific overrides in `NativeXxxExec`

3. **Test across all supported versions**
   - CI runs tests on all Spark versions
   - Ensure backward compatibility

4. **Document version differences**
   - Use `@sparkver` annotations
   - Comment why different implementations exist

5. **Handle missing APIs gracefully**
   - Use reflection for optional features
   - Provide fallbacks when APIs unavailable

### Adding Support for New Spark Versions

**Steps**:

1. **Create new shims module**
   ```bash
   cp -r spark-extension-shims-spark35 spark-extension-shims-spark36
   ```

2. **Update module POM**
   - Change Spark version dependency
   - Update artifactId

3. **Update parent POM**
   - Add new profile
   - Add to modules list

4. **Implement/update ShimsImpl**
   - Handle new/changed APIs
   - Update factory methods

5. **Update operators**
   - Add `@sparkver` annotations
   - Implement new required methods

6. **Test thoroughly**
   - Run full test suite
   - Check for deprecation warnings

## Deep Dive: Testing Strategies

Comprehensive testing ensures operators work correctly and don't regress. This section
covers Auron's testing approach and best practices.

### Test Structure

```
spark-extension-shims-spark3*/
└── src/test/scala/org/apache/spark/sql/auron/
    ├── BaseAuronSQLSuite.scala       # Base test trait
    ├── AuronQuerySuite.scala         # SQL query tests
    ├── AuronFunctionSuite.scala      # Expression tests
    ├── NativeConvertersSuite.scala   # Conversion tests
    └── Auron<Operator>Suite.scala    # Operator-specific tests
```

### Base Test Suite

**File**: `BaseAuronSQLSuite.scala`

All Auron tests extend this trait:

```scala
trait BaseAuronSQLSuite extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions",
        "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set("spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")  // Enable Auron globally
  }
}
```

**What it provides**:
- Spark session with Auron enabled
- Default configurations
- Cleanup after tests

### Test Categories

#### 1. SQL Query Tests

**File**: `AuronQuerySuite.scala`

Test complete SQL queries end-to-end:

```scala
class AuronQuerySuite extends BaseAuronSQLSuite {
  test("filter with simple predicate") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT, name STRING) USING parquet")
      sql("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

      val result = sql("SELECT * FROM test_table WHERE id > 1")

      // Verify results
      checkAnswer(result, Seq(Row(2, "Bob"), Row(3, "Charlie")))

      // Verify native execution
      assert(result.queryExecution.executedPlan.find(_.isInstanceOf[NativeFilterExec]).isDefined,
        "Should use NativeFilterExec")
    }
  }

  test("aggregate with grouping") {
    withTable("sales") {
      sql("CREATE TABLE sales (category STRING, amount DOUBLE) USING parquet")
      sql("INSERT INTO sales VALUES ('A', 100.0), ('B', 200.0), ('A', 150.0)")

      val result = sql("SELECT category, SUM(amount) FROM sales GROUP BY category")

      checkAnswer(result, Seq(Row("A", 250.0), Row("B", 200.0)))

      // Verify native execution
      assert(result.queryExecution.executedPlan.find(_.isInstanceOf[NativeAggExec]).isDefined)
    }
  }
}
```

**Best practices**:
- Use `withTable` for automatic cleanup
- Use `checkAnswer` to verify results match expected
- Verify plan uses native operators
- Test edge cases (nulls, empty tables, etc.)

#### 2. Expression Tests

**File**: `AuronFunctionSuite.scala`

Test expression conversion and evaluation:

```scala
class AuronFunctionSuite extends BaseAuronSQLSuite {
  test("string functions") {
    val df = Seq(("  hello  ", "WORLD")).toDF("a", "b")

    // Test TRIM
    checkAnswer(df.selectExpr("TRIM(a)"), Row("hello") :: Nil)

    // Test LOWER/UPPER
    checkAnswer(df.selectExpr("LOWER(b)"), Row("world") :: Nil)
    checkAnswer(df.selectExpr("UPPER(a)"), Row("  HELLO  ") :: Nil)

    // Test CONCAT
    checkAnswer(df.selectExpr("CONCAT(TRIM(a), ' ', LOWER(b))"),
      Row("hello world") :: Nil)
  }

  test("aggregate functions") {
    val df = Seq(1, 2, 3, 4, 5).toDF("value")

    checkAnswer(df.selectExpr("MAX(value)"), Row(5) :: Nil)
    checkAnswer(df.selectExpr("MIN(value)"), Row(1) :: Nil)
    checkAnswer(df.selectExpr("SUM(value)"), Row(15) :: Nil)
    checkAnswer(df.selectExpr("AVG(value)"), Row(3.0) :: Nil)
    checkAnswer(df.selectExpr("COUNT(value)"), Row(5) :: Nil)
  }

  test("null handling") {
    val df = Seq(Some(1), None, Some(3)).toDF("value")

    // COUNT should not count nulls
    checkAnswer(df.selectExpr("COUNT(value)"), Row(2) :: Nil)

    // SUM should ignore nulls
    checkAnswer(df.selectExpr("SUM(value)"), Row(4) :: Nil)

    // MAX should ignore nulls
    checkAnswer(df.selectExpr("MAX(value)"), Row(3) :: Nil)
  }
}
```

#### 3. Conversion Tests

**File**: `NativeConvertersSuite.scala`

Test expression and operator conversion logic:

```scala
class NativeConvertersSuite extends BaseAuronSQLSuite {
  test("convert simple expressions") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)))

    // Test column reference
    val colRef = AttributeReference("id", IntegerType)()
    val nativeCol = NativeConverters.convertExpr(colRef)
    assert(nativeCol.hasColumn)

    // Test literal
    val lit = Literal(42, IntegerType)
    val nativeLit = NativeConverters.convertExpr(lit)
    assert(nativeLit.hasLiteral)

    // Test binary expression
    val binExpr = EqualTo(colRef, lit)
    val nativeBinExpr = NativeConverters.convertExpr(binExpr)
    assert(nativeBinExpr.hasBinaryExpr)
  }

  test("unsupported expression fallback") {
    // Some expressions should fallback to UDF wrapper
    val unsupportedExpr = SomeUnsupportedExpression(...)

    val nativeExpr = NativeConverters.convertExpr(unsupportedExpr)
    assert(nativeExpr.hasSparkUdfWrapperExpr,
      "Unsupported expression should wrap in UDF")
  }
}
```

#### 4. Operator-Specific Tests

Test individual operators in isolation:

```scala
class AuronFilterSuite extends BaseAuronSQLSuite {
  test("filter selectivity") {
    withTable("data") {
      // Create table with known data distribution
      sql("CREATE TABLE data (id INT) USING parquet")
      sql(s"INSERT INTO data VALUES ${(1 to 1000).map(i => s"($i)").mkString(",")}")

      // Test various selectivities
      checkAnswer(sql("SELECT COUNT(*) FROM data WHERE id < 10"), Row(9) :: Nil)
      checkAnswer(sql("SELECT COUNT(*) FROM data WHERE id > 990"), Row(10) :: Nil)
      checkAnswer(sql("SELECT COUNT(*) FROM data WHERE id = 500"), Row(1) :: Nil)
    }
  }

  test("filter with complex predicates") {
    val df = Seq((1, "A"), (2, "B"), (3, "A"), (4, "C")).toDF("id", "category")

    // AND predicate
    checkAnswer(
      df.filter("id > 1 AND category = 'A'"),
      Row(3, "A") :: Nil)

    // OR predicate
    checkAnswer(
      df.filter("id = 1 OR id = 4"),
      Row(1, "A") :: Row(4, "C") :: Nil)

    // NOT predicate
    checkAnswer(
      df.filter("NOT (category = 'B')"),
      Row(1, "A") :: Row(3, "A") :: Row(4, "C") :: Nil)
  }
}
```

### Testing Patterns

#### Pattern 1: Compare Native vs Spark Results

```scala
test("native matches Spark") {
  withTable("test") {
    sql("CREATE TABLE test (id INT, value DOUBLE) USING parquet")
    sql("INSERT INTO test VALUES (1, 1.5), (2, 2.5), (3, 3.5)")

    // Execute with Auron
    spark.conf.set("spark.auron.enable", "true")
    val auronResult = sql("SELECT SUM(value) FROM test WHERE id > 1")
      .collect().map(_.getDouble(0))

    // Execute with vanilla Spark
    spark.conf.set("spark.auron.enable", "false")
    val sparkResult = sql("SELECT SUM(value) FROM test WHERE id > 1")
      .collect().map(_.getDouble(0))

    // Results should match
    assert(auronResult === sparkResult)
  }
}
```

#### Pattern 2: Verify Plan Structure

```scala
test("verify native operator used") {
  val df = spark.range(100).filter($"id" > 50)

  val plan = df.queryExecution.executedPlan

  // Should contain NativeFilterExec
  assert(plan.find(_.isInstanceOf[NativeFilterExec]).isDefined,
    s"Plan should use NativeFilterExec:\n${plan.treeString}")

  // Should NOT contain vanilla FilterExec
  assert(plan.find(_.getClass.getSimpleName == "FilterExec").isEmpty,
    "Plan should not contain vanilla FilterExec")
}
```

#### Pattern 3: Test with Different Data Types

```scala
test("filter works with all types") {
  val data = Seq(
    (1, 1L, 1.0f, 1.0, "one", true, BigDecimal(1.0)),
    (2, 2L, 2.0f, 2.0, "two", false, BigDecimal(2.0)),
    (3, 3L, 3.0f, 3.0, "three", true, BigDecimal(3.0)))
    .toDF("int_col", "long_col", "float_col", "double_col",
      "string_col", "bool_col", "decimal_col")

  // Test each type
  checkAnswer(data.filter($"int_col" > 1), /* expected */)
  checkAnswer(data.filter($"long_col" > 1L), /* expected */)
  checkAnswer(data.filter($"float_col" > 1.0f), /* expected */)
  checkAnswer(data.filter($"double_col" > 1.0), /* expected */)
  checkAnswer(data.filter($"string_col" === "two"), /* expected */)
  checkAnswer(data.filter($"bool_col" === true), /* expected */)
  checkAnswer(data.filter($"decimal_col" > BigDecimal(1.0)), /* expected */)
}
```

#### Pattern 4: Test Edge Cases

```scala
test("filter edge cases") {
  // Empty table
  val empty = spark.emptyDataFrame.filter("1 = 1")
  assert(empty.count() === 0)

  // All nulls
  val allNulls = Seq[Option[Int]](None, None, None).toDF("value")
  checkAnswer(allNulls.filter($"value".isNotNull), Nil)

  // Mixed nulls and values
  val mixed = Seq(Some(1), None, Some(3)).toDF("value")
  checkAnswer(mixed.filter($"value" > 1), Row(3) :: Nil)

  // Very large values
  val large = Seq(Int.MaxValue, Int.MaxValue - 1).toDF("value")
  checkAnswer(large.filter($"value" === Int.MaxValue), Row(Int.MaxValue) :: Nil)
}
```

### Running Tests

#### Run all tests
```bash
./build/sbt test
```

#### Run specific test suite
```bash
./build/sbt "testOnly *AuronFilterSuite"
```

#### Run specific test
```bash
./build/sbt "testOnly *AuronFilterSuite -- -z 'filter selectivity'"
```

#### Run tests for specific Spark version
```bash
./build/sbt -Pspark-3.3 test
```

### Test Best Practices

1. **Always use `withTable`**
   - Ensures cleanup even if test fails
   - Prevents table name collisions

2. **Use `checkAnswer` for result verification**
   - Better error messages than direct comparison
   - Handles row ordering automatically

3. **Verify native execution**
   - Check plan contains expected native operators
   - Catch regressions where conversion fails

4. **Test nulls extensively**
   - Null handling is error-prone
   - Test NULL in all positions (predicates, aggregates, etc.)

5. **Test empty inputs**
   - Empty tables
   - Empty partitions
   - Filters that match no rows

6. **Test data type coverage**
   - All primitive types
   - Complex types (arrays, maps, structs)
   - Decimals with various precision/scale

7. **Compare with Spark results**
   - Ultimate correctness check
   - Run same query with Auron enabled/disabled

## Deep Dive: Configuration System

Auron provides extensive configuration options to control behavior, enable/disable
operators, and tune performance. Understanding these options helps optimize applications.

### Configuration Categories

#### 1. Global Enable/Disable

```scala
// Enable Auron extension
spark.conf.set("spark.auron.enable", "true")  // Default: true

// When false, all operators use vanilla Spark execution
```

#### 2. Operator-Specific Enable Flags

Each operator can be individually enabled/disabled:

```scala
// Scan operators
spark.conf.set("spark.auron.enable.scan", "true")
spark.conf.set("spark.auron.enable.scan.parquet", "true")
spark.conf.set("spark.auron.enable.scan.orc", "true")

// Filter operator
spark.conf.set("spark.auron.enable.filter", "true")

// Project operator
spark.conf.set("spark.auron.enable.project", "true")

// Aggregate operator
spark.conf.set("spark.auron.enable.aggr", "true")

// Join operators
spark.conf.set("spark.auron.enable.smj", "true")        // Sort-merge join
spark.conf.set("spark.auron.enable.shj", "true")        // Shuffled hash join
spark.conf.set("spark.auron.enable.bhj", "true")        // Broadcast hash join
spark.conf.set("spark.auron.enable.bnlj", "true")       // Broadcast nested loop join

// Exchange operators
spark.conf.set("spark.auron.enable.exchange", "true")
spark.conf.set("spark.auron.enable.shuffleExchange", "true")
spark.conf.set("spark.auron.enable.broadcastExchange", "true")

// Other operators
spark.conf.set("spark.auron.enable.sort", "true")
spark.conf.set("spark.auron.enable.limit", "true")
spark.conf.set("spark.auron.enable.union", "true")
spark.conf.set("spark.auron.enable.window", "true")
spark.conf.set("spark.auron.enable.expand", "true")
spark.conf.set("spark.auron.enable.generate", "true")
```

#### 3. Memory and Performance Tuning

```scala
// Batch size (number of rows per batch)
spark.conf.set("spark.auron.batchSize", "8192")  // Default: 8192

// Target batch memory size
spark.conf.set("spark.auron.batchSize.memSize", "8388608")  // Default: 8MB

// Memory fraction for native execution
spark.conf.set("spark.auron.memoryFraction", "0.7")  // Default: 0.7 (70%)

// Spill threshold
spark.conf.set("spark.auron.sort.spillThreshold", "1000000")  // Rows before spill
```

#### 4. Shuffle Configuration

```scala
// Native shuffle writer
spark.conf.set("spark.auron.shuffle.write.enabled", "true")

// Compression codec for shuffle
spark.conf.set("spark.auron.shuffle.compression.codec", "lz4")  // lz4, zstd, snappy

// Shuffle buffer size
spark.conf.set("spark.auron.shuffle.write.buffer.size", "32768")  // 32KB
```

#### 5. Debugging and Logging

```scala
// Enable debug logging
spark.conf.set("spark.auron.debug.enabled", "true")

// Log native plan
spark.conf.set("spark.auron.debug.logNativePlan", "true")

// Input batch statistics
spark.conf.set("spark.auron.input.batch.statistics.enable", "true")
```

#### 6. Expression-Specific Configuration

```scala
// Decimal arithmetic in native
spark.conf.set("spark.auron.decimal.arithOp.enabled", "true")

// Cast string to numeric with trim
spark.conf.set("spark.auron.cast.stringToNumeric.trim", "true")

// Fallback strategy for unsupported expressions
spark.conf.set("spark.auron.expression.fallback", "udf")  // udf, spark, error
```

### Accessing Configuration in Code

#### Scala Side

```scala
import org.apache.spark.sql.auron.AuronConverters

// Get boolean config
val enableFilter = AuronConverters.getBooleanConf(
  "spark.auron.enable.filter",
  defaultValue = true)

// Get integer config
val batchSize = AuronConverters.getIntConf(
  "spark.auron.batchSize",
  defaultValue = 8192)

// Get string config
val codec = AuronConverters.getStringConf(
  "spark.auron.shuffle.compression.codec",
  defaultValue = "lz4")
```

#### Rust Side

```rust
use auron_jni_bridge::conf;

// Get boolean config
let enabled = conf::SOME_FEATURE_ENABLE.value()
    .unwrap_or(true);  // Default value

// Get integer config
let batch_size = conf::BATCH_SIZE.value()
    .unwrap_or(8192);

// Get string config
let codec = conf::COMPRESSION_CODEC.value()
    .unwrap_or_else(|| "lz4".to_string());
```

### Configuration File

**File**: `conf/auron-defaults.conf` (optional)

```hocon
# Auron default configuration
spark.auron.enable = true
spark.auron.batchSize = 8192

# Enable all operators by default
spark.auron.enable.filter = true
spark.auron.enable.project = true
spark.auron.enable.aggr = true
# ... more operators

# Performance tuning
spark.auron.memoryFraction = 0.7
spark.auron.shuffle.compression.codec = lz4
```

### Environment Variables

Some configs can be set via environment:

```bash
# Enable Auron via environment
export SPARK_AURON_ENABLE=true

# Batch size via environment
export SPARK_AURON_BATCH_SIZE=16384
```

### Runtime Configuration

Change config at runtime:

```scala
// Initial config
spark.conf.set("spark.auron.enable.filter", "true")

val df1 = spark.sql("SELECT * FROM table WHERE id > 100")
// Uses NativeFilterExec

// Disable filter at runtime
spark.conf.set("spark.auron.enable.filter", "false")

val df2 = spark.sql("SELECT * FROM table WHERE id > 100")
// Uses vanilla FilterExec
```

### Configuration Best Practices

1. **Start with defaults**
   - Default configs work well for most workloads
   - Only tune when profiling shows bottlenecks

2. **Enable operators incrementally**
   - Start with simple operators (filter, project)
   - Add complex operators after validation

3. **Monitor metrics**
   - Use Spark UI to check operator performance
   - Compare native vs Spark execution times

4. **Test configuration changes**
   - Run representative queries
   - Verify correctness before production

5. **Document custom configs**
   - Keep track of non-default settings
   - Understand why each config was changed

### Common Configuration Scenarios

#### Scenario 1: Maximum Performance

```scala
// Enable all operators
spark.conf.set("spark.auron.enable", "true")
spark.conf.set("spark.auron.enable.scan", "true")
spark.conf.set("spark.auron.enable.filter", "true")
spark.conf.set("spark.auron.enable.project", "true")
spark.conf.set("spark.auron.enable.aggr", "true")
spark.conf.set("spark.auron.enable.smj", "true")
spark.conf.set("spark.auron.enable.shj", "true")
spark.conf.set("spark.auron.enable.bhj", "true")

// Large batch size for throughput
spark.conf.set("spark.auron.batchSize", "16384")
spark.conf.set("spark.auron.batchSize.memSize", "16777216")  // 16MB

// More memory for native execution
spark.conf.set("spark.auron.memoryFraction", "0.8")
```

#### Scenario 2: Conservative (Stability First)

```scala
// Enable only proven operators
spark.conf.set("spark.auron.enable.scan", "true")
spark.conf.set("spark.auron.enable.filter", "true")
spark.conf.set("spark.auron.enable.project", "true")

// Disable complex operators initially
spark.conf.set("spark.auron.enable.aggr", "false")
spark.conf.set("spark.auron.enable.smj", "false")

// Conservative batch size
spark.conf.set("spark.auron.batchSize", "4096")
```

#### Scenario 3: Debugging

```scala
// Enable verbose logging
spark.conf.set("spark.auron.debug.enabled", "true")
spark.conf.set("spark.auron.debug.logNativePlan", "true")

// Track input statistics
spark.conf.set("spark.auron.input.batch.statistics.enable", "true")

// Disable operators to isolate issues
spark.conf.set("spark.auron.enable.filter", "false")  // Test without native filter
```

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
