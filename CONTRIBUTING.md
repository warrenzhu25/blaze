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
