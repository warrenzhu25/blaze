# Auron Extension Guide

This guide covers how to extend Auron with new operators, expressions, data sources, and configuration options.

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
