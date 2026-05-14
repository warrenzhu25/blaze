# Critical Execution Paths

This document traces end-to-end code paths through the Auron system with specific file and line references.

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
File: spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronColumnarOverrides.scala

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

```
┌──────────────────────────────────────────────────────────────────┐
│                        Memory Configuration                       │
│                                                                    │
│  Executor Memory = spark.executor.memory                          │
│         │                                                          │
│         ├──▶ JVM Heap                                             │
│         │      └── OnHeapSpillManager (Spark task memory)         │
│         │                                                          │
│         └──▶ Native Memory (spark.auron.memory.fraction)          │
│                └── MemManager (Rust memory pool)                  │
│                      ├── Aggregation buffers                      │
│                      ├── Sort buffers                             │
│                      └── Join hash tables                         │
└──────────────────────────────────────────────────────────────────┘
```

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
