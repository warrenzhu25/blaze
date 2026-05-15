# Auron Memory Management

> Deep dive into how Auron manages native memory and integrates with Spark's unified memory system.

---

## Table of Contents

- [Overview](#overview)
- [Memory Architecture](#memory-architecture)
- [Native Memory Management](#native-memory-management)
- [Spark Integration](#spark-integration)
- [Two-Tier Spilling](#two-tier-spilling)
- [Spill Flow](#spill-flow)
- [Spill Examples](#spill-examples)
- [Key Components](#key-components)
- [Configuration](#configuration)
- [Metrics](#metrics)

---

## Overview

Auron executes queries in native (Rust) code, which requires careful coordination between:

1. **Native memory** - Rust heap for sort buffers, hash tables, Arrow batches
2. **JVM direct memory** - Off-heap buffers for Arrow FFI data transfer
3. **JVM on-heap memory** - Spark's execution memory pool for spill buffers

The memory management system ensures native operators don't exceed their budget while leveraging Spark's existing memory infrastructure for spilling.

---

## Memory Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           JVM Process                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────┐    ┌─────────────────────────────────────┐ │
│  │    Spark On-Heap        │    │         Off-Heap / Direct           │ │
│  │  (Execution Memory)     │    │                                     │ │
│  │                         │    │  ┌─────────────────────────────┐   │ │
│  │  OnHeapSpillManager     │    │  │   Native (Rust) Memory      │   │ │
│  │  extends MemoryConsumer │    │  │   - MemManager budget       │   │ │
│  │                         │    │  │   - Sort/Agg/Shuffle bufs   │   │ │
│  │  ┌─────────────────┐   │    │  │   - Arrow RecordBatches     │   │ │
│  │  │ OnHeapSpill     │   │    │  └─────────────────────────────┘   │ │
│  │  │ (MemBasedBuf)   │───┼────┼──→ Spill to JVM heap first         │ │
│  │  └────────┬────────┘   │    │                                     │ │
│  │           │            │    │  ┌─────────────────────────────┐   │ │
│  │           ▼ spill()    │    │  │   JVM Direct Memory         │   │ │
│  │  ┌─────────────────┐   │    │  │   (BufferPoolMXBean)        │   │ │
│  │  │ FileBasedBuf    │   │    │  │   - Arrow FFI buffers       │   │ │
│  │  │ (Disk)          │   │    │  └─────────────────────────────┘   │ │
│  │  └─────────────────┘   │    │                                     │ │
│  └─────────────────────────┘    └─────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Memory Budget Calculation

```
executor_memory_overhead × MEMORY_FRACTION (0.6 default)
         ↓
    Native Budget
         ↓
   Divided among consumers (sort, agg, shuffle)
```

The native budget is derived from Spark's executor memory overhead, ensuring Auron doesn't compete with Spark's own memory needs.

---

## Native Memory Management

### MemManager (Rust Singleton)

**File:** `native-engine/datafusion-ext-plans/src/memmgr/mod.rs`

The `MemManager` is a global singleton that tracks all memory-consuming operators:

```rust
pub struct MemManager {
    total: usize,                              // Total budget from JVM
    consumers: Mutex<Vec<Arc<MemConsumerInfo>>>,  // Registered operators
    status: Mutex<MemManagerStatus>,           // Current usage stats
    cv: Condvar,                               // Wait/notify for memory pressure
}

struct MemManagerStatus {
    num_consumers: usize,      // Total registered consumers
    total_used: usize,         // Total memory in use
    num_spillables: usize,     // Count of spillable consumers
    mem_spillables: usize,     // Memory used by spillable consumers
}
```

### MemConsumer Trait

Operators implement this trait to participate in memory management:

```rust
#[async_trait]
pub trait MemConsumer: Send + Sync {
    fn name(&self) -> &str;

    // Called when operator's memory usage changes
    async fn update_mem_used(&self, new_used: usize) -> Result<()>;

    // Called when operator must release memory
    async fn spill(&self) -> Result<()>;
}
```

### Per-Consumer Limits

Each spillable consumer gets a fair share of the managed memory:

```rust
let total_managed = total
    .saturating_sub(mem_jvm_direct_used)   // Reserve for JVM direct memory
    .saturating_sub(mem_unspillable);       // Reserve for unspillable consumers

let consumer_mem_max = total_managed / num_spillables;  // Fair share
let consumer_mem_min = consumer_mem_max / 8;            // Minimum before spill
```

### Spill Decision Logic

When an operator reports memory growth, `MemManager` decides whether to spill:

```rust
// Check three overflow conditions
let total_overflowed = total_used > total_managed;        // Native budget exceeded
let consumer_overflowed = new_used > consumer_mem_max;    // Per-consumer limit exceeded
let proc_overflowed = mem_proc_total_used > mem_proc_max; // Process RSS exceeded (Linux)

// Decide action
if (overflow detected) && new_used > MIN_TRIGGER_SIZE (16MB) && growing {
    if spillable && new_used > consumer_mem_min {
        Operation::Spill      // Spill this consumer immediately
    } else {
        Operation::Wait       // Wait for other consumers to free memory
    }
} else {
    Operation::Nothing        // Continue without action
}
```

### Wait with Timeout

If an operator can't spill immediately, it waits for others to free memory:

```rust
const WAIT_TIME: Duration = Duration::from_millis(10000);  // 10 seconds

let wait = mm.cv.wait_while_for(&mut mm_status,
    |s| total < s.total_used, WAIT_TIME);

if wait.timed_out() {
    operation = Operation::Spill;  // Force spill on timeout
}
```

---

## Spark Integration

### OnHeapSpillManager

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/OnHeapSpillManager.scala`

Auron's `OnHeapSpillManager` extends Spark's `MemoryConsumer`, allowing it to participate in Spark's unified memory management:

```scala
class OnHeapSpillManager(taskContext: TaskContext)
    extends MemoryConsumer(
      taskContext.taskMemoryManager,
      taskContext.taskMemoryManager.pageSizeBytes(),
      MemoryMode.ON_HEAP)  // Uses Spark's on-heap execution pool
```

This integration means:
- Auron can acquire memory from Spark's execution pool
- Spark can ask Auron to spill when other operators need memory
- Memory accounting is unified across Spark and Auron

### On-Heap Availability Check

Before spilling to JVM heap, Auron checks if there's sufficient headroom:

```scala
def isOnHeapAvailable: Boolean = {
  // Check Spark's on-heap execution memory pool
  val memoryPool = OnHeapSpillManagerHelper.getOnHeapExecutionMemoryPool
  val memoryUsed = memoryPool.memoryUsed
  val memoryFree = memoryPool.memoryFree
  val memoryUsedRatio = (memoryUsed + 1.0) / (memoryUsed + memoryFree + 1.0)

  // Also check JVM heap directly
  val jvmMemoryFree = Runtime.getRuntime.freeMemory()
  val jvmMemoryUsed = Runtime.getRuntime.totalMemory() - jvmMemoryFree
  val jvmMemoryUsedRatio = (jvmMemoryUsed + 1.0) / (jvmMemoryUsed + jvmMemoryFree + 1.0)

  // Need at least 10% free (configurable)
  val maxRatio = AuronConf.ON_HEAP_SPILL_MEM_FRACTION.doubleConf()
  memoryUsedRatio < maxRatio && jvmMemoryUsedRatio < maxRatio
}
```

### Direct Memory Tracking

**File:** `spark-extension/src/main/java/org/apache/spark/sql/auron/JniBridge.java`

Rust tracks JVM direct memory usage via JNI to avoid over-allocating:

```java
private static final List<BufferPoolMXBean> directMXBeans =
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

public static long getDirectMemoryUsed() {
    return directMXBeans.stream()
            .mapToLong(BufferPoolMXBean::getTotalCapacity)
            .sum();
}
```

This value is subtracted from the native budget to reserve space for Arrow FFI buffers.

### Spark's Spill Callback

When Spark needs memory, it calls `spill()` on registered consumers:

```scala
override def spill(size: Long, trigger: MemoryConsumer): Long = {
  // Don't spill if we're using less than half of task's memory
  if (trigger != this && memUsed * 2 < this.taskMemoryManager.getMemoryConsumptionForThisTask) {
    return 0L
  }

  // Spill largest buffers first to minimize file count
  val sortedSpills = spills.seq.sortBy(0 - _.map(_.memUsed).getOrElse(0L))
  sortedSpills.foreach {
    case Some(spill) if spill.memUsed > 0 =>
      totalFreed += spill.spill()  // MemBasedSpillBuf → FileBasedSpillBuf
      if (totalFreed >= size) return totalFreed
    case _ =>
  }
  totalFreed
}
```

---

## Two-Tier Spilling

Auron uses a two-tier spill strategy to balance speed and reliability:

| Tier | Storage | Speed | When Used |
|------|---------|-------|-----------|
| **Tier 1** | JVM on-heap | Fast | When heap < 90% full |
| **Tier 2** | Local disk | Slower | When heap exhausted or on driver |

### Spill Type Selection

**File:** `native-engine/datafusion-ext-plans/src/memmgr/spill.rs`

```rust
pub fn try_new_spill(spill_metrics: &SpillMetrics) -> Result<Box<dyn Spill>> {
    if !is_jni_bridge_inited() || jni_call_static!(JniBridge.isDriverSide())? {
        // Driver side: always use disk (no task memory manager)
        Ok(Box::new(FileSpill::try_new(spill_metrics)?))
    } else {
        // Executor side: check JVM heap availability
        let hsm = jni_call_static!(JniBridge.getTaskOnHeapSpillManager())?;
        if jni_call!(AuronOnHeapSpillManager(hsm).isOnHeapAvailable())? {
            Ok(Box::new(OnHeapSpill::try_new(hsm, spill_metrics)?))
        } else {
            Ok(Box::new(FileSpill::try_new(spill_metrics)?))
        }
    }
}
```

### SpillBuf Hierarchy

**File:** `spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/SpillBuf.scala`

```
SpillBuf (abstract)
    │
    ├── MemBasedSpillBuf    ← JVM heap storage (Netty ByteBuf)
    │         │
    │         └── spill() ──→ FileBasedSpillBuf
    │
    ├── FileBasedSpillBuf   ← Local disk storage
    │
    └── ReleasedSpillBuf    ← Stats-only wrapper after release
```

| Class | Storage | Memory | Disk |
|-------|---------|--------|------|
| `MemBasedSpillBuf` | JVM heap (Netty ByteBuf) | Yes | No |
| `FileBasedSpillBuf` | Local temp file | No | Yes |
| `ReleasedSpillBuf` | N/A (stats only) | No | No |

---

## Spill Flow

### Complete Spill Sequence

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. Native operator needs to spill (e.g., AggTable, ExternalSorter) │
│    Rust calls: try_new_spill()                                      │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. Check: is JVM on-heap available?                                 │
│    Rust JNI → JniBridge.getTaskOnHeapSpillManager()                │
│             → OnHeapSpillManager.isOnHeapAvailable()               │
└───────────────────────────────┬─────────────────────────────────────┘
                                ▼
            ┌───────────────────┴───────────────────┐
            │                                       │
     YES (< 90% used)                        NO (≥ 90% used)
            │                                       │
            ▼                                       ▼
┌───────────────────────┐               ┌───────────────────────┐
│ 3a. OnHeapSpill       │               │ 3b. FileSpill         │
│     MemBasedSpillBuf  │               │     Direct to disk    │
│     Uses Netty heap   │               │     Compressed (LZ4)  │
└───────────┬───────────┘               └───────────────────────┘
            │
            │ If Spark needs memory,
            │ calls spill() callback
            ▼
┌───────────────────────┐
│ 4. FileBasedSpillBuf  │
│    Secondary spill    │
│    to local disk      │
└───────────────────────┘
```

### Operator-Specific Spilling

Each operator type handles spilling differently:

**Sort (ExternalSorter):**
- Tracks in-memory sorted blocks
- Spill: Merge sorted blocks → write to disk
- Output: K-way merge of spill files + memory

**Aggregation (AggTable):**
- Tracks hash table + accumulators
- Spill: Freeze hash table → write to disk
- Output: Radix merge of spill buckets by hash

**Shuffle (BufferedData):**
- Tracks staged + sorted buffers
- Spill: Write sorted partition data
- Output: Merge all spills by partition ID

---

## Spill Examples

Concrete examples showing how memory spilling works in different scenarios.

### Example 1: Aggregation Spill

**Scenario:** Hash aggregation exceeds memory budget

```
Input: 100M rows, GROUP BY user_id (10M unique users)
Native budget: 512MB
Hash table size: 800MB (exceeds budget!)
```

**Flow:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  1. Process input batches, hash table grows                            │
│                                                                        │
│     AggTable.process_input_batch()                                     │
│       → HashingData.update_batch()                                     │
│       → self.update_mem_used(mem_used).await  ← Reports 600MB          │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  2. MemManager detects overflow                                        │
│                                                                        │
│     total_used (600MB) > consumer_mem_max (512MB/2 = 256MB)           │
│     AND mem_used > MIN_TRIGGER_SIZE (16MB)                             │
│     AND growing = true                                                 │
│     → Operation::Spill                                                 │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  3. AggTable.spill() called                                            │
│                                                                        │
│     // Freeze current hash table                                       │
│     let cur_in_mem = in_mem.renew(next_is_hashing)?;                  │
│                                                                        │
│     // Write to spill (JVM heap or disk)                              │
│     let spill = try_new_spill(&spill_metrics)?;                       │
│     cur_in_mem.try_into_spill(&mut spill, spill_idx)?;                │
│                                                                        │
│     // Reset memory counter                                            │
│     self.update_mem_used(0).await?;                                   │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  4. Continue processing with fresh hash table                          │
│                                                                        │
│     // New empty hash table                                            │
│     in_mem = InMemTable::try_new(id+1, ...)?;                         │
│                                                                        │
│     // Spill stored for later merge                                   │
│     spills.push(cur_spill);                                           │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  5. Output: Merge all spills + in-memory table                         │
│                                                                        │
│     // Radix merge by hash bucket                                      │
│     for bucket in 0..256 {                                             │
│         let cursor = SpillCursor::new(spill, bucket);                 │
│         while cursor.has_next() {                                      │
│             merge_record(&mut acc_table, cursor.next());              │
│         }                                                              │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
```

**Code:** `agg_table.rs:322-352`

```rust
async fn spill(&self) -> Result<()> {
    let mut in_mem = self.in_mem.lock().await;
    let mut spills = self.spills.lock().await;

    // Decide if next table should use hashing based on cardinality
    let mut next_is_hashing = false;
    if let InMemData::Hashing(hashing_data) = &in_mem.data {
        if hashing_data.cardinality_ratio() < 0.5 {
            next_is_hashing = true;  // Low cardinality, keep hashing
        }
    }

    // Swap in a fresh table
    let cur_in_mem = in_mem.renew(next_is_hashing)?;

    // Write old table to spill file
    let spill_metrics = self.exec_ctx.spill_metrics().clone();
    let cur_spill = tokio::task::spawn_blocking(move || {
        let mut spill = try_new_spill(&spill_metrics)?;  // JVM heap or disk
        cur_in_mem.try_into_spill(&mut spill, spill_idx)?;
        Ok(spill)
    }).await??;

    spills.push(cur_spill);
    self.update_mem_used(0).await?;  // Memory released
    Ok(())
}
```

### Example 2: Sort Spill

**Scenario:** Sorting large dataset with limited memory

```
Input: 50M rows to sort
Native budget: 256MB
Sorted blocks: 400MB total (exceeds budget!)
```

**Flow:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  1. Insert batches, create sorted in-memory blocks                     │
│                                                                        │
│     sorter.insert_batch(batch).await                                  │
│       → Sort batch by key                                              │
│       → in_mem_blocks.push(InMemSortedBlock { sorted_keys, batch })   │
│       → update_mem_used(total_mem_used)                               │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  2. Memory pressure triggers spill                                     │
│                                                                        │
│     ExternalSorter.spill() called                                     │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  3. Merge in-memory blocks → write to spill                           │
│                                                                        │
│     // Take all in-memory blocks                                       │
│     let blocks = std::mem::take(&mut *self.in_mem_blocks.lock());     │
│                                                                        │
│     // Merge using loser tree (k-way merge)                           │
│     let merged_block = merge_blocks(blocks, SpillSortedBlockBuilder); │
│                                                                        │
│     // Write to compressed spill file                                 │
│     spills.push(LevelSpill { block: merged_block, level: 0 });        │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  4. Hierarchical merging (if too many spills)                         │
│                                                                        │
│     Level 0: [spill1, spill2, ..., spill32]  → merge → Level 1        │
│     Level 1: [merged1, merged2, ...]         → merge → Level 2        │
│                                                                        │
│     // Keeps at most NUM_MAX_MERGING_BATCHES (32) per level           │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  5. Output: K-way merge of all spills + remaining in-memory           │
│                                                                        │
│     LoserTree merge:                                                   │
│       - Compare min key from each source                              │
│       - Output smallest, advance that source                          │
│       - Repeat until all sources exhausted                            │
└────────────────────────────────────────────────────────────────────────┘
```

**Code:** `sort_exec.rs:379-436`

```rust
async fn spill(&self) -> Result<()> {
    let spills = self.spills.clone();
    let blocks = std::mem::take(&mut *self.in_mem_blocks.lock());

    tokio::task::spawn_blocking(move || {
        let mut spills = spills.lock();

        // Create spill file (JVM heap or disk)
        let spill = try_new_spill(self.exec_ctx.spill_metrics())?;

        // Merge all in-memory blocks into single sorted spill
        let merged_block = merge_blocks::<_, SqueezeKeyCollector>(
            self.clone(),
            blocks,
            SpillSortedBlockBuilder::new(self.pruned_schema(), spill),
        )?;

        spills.push(LevelSpill { block: merged_block, level: 0 });

        // Hierarchical merge if too many spills at same level
        for level in 0..levels.len() {
            if levels[level].len() >= NUM_MAX_MERGING_BATCHES {  // 32
                let spill = try_new_spill(self.exec_ctx.spill_metrics())?;
                let merged = merge_blocks(
                    std::mem::take(&mut levels[level]),
                    SpillSortedBlockBuilder::new(schema, spill),
                )?;
                levels[level + 1].push(merged);  // Promote to next level
            }
        }
        Ok(())
    }).await??;

    self.update_mem_used(0).await?;
    Ok(())
}
```

### Example 3: Shuffle Spill

**Scenario:** Shuffle write with memory pressure

```
Input: 200 partitions output, 10GB data
Native budget: 1GB
Buffered data: 1.5GB (exceeds budget!)
```

**Flow:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  1. Insert batches, buffer by partition                                │
│                                                                        │
│     SortShuffleRepartitioner.insert_batch(batch)                      │
│       → Evaluate partition ID for each row                            │
│       → Add to BufferedData (sorted by partition)                     │
│       → update_mem_used(mem_used)                                     │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  2. Proactive spill at 80% memory usage                               │
│                                                                        │
│     if mem_used_percent > 0.8 {                                       │
│         log::info!("memory usage: {}, spilling...", mem_used);        │
│         self.force_spill().await?;                                    │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  3. SortShuffleRepartitioner.spill()                                  │
│                                                                        │
│     // Drain current buffered data                                    │
│     let data = self.data.lock().await.drain();                        │
│                                                                        │
│     // Write to spill with partition offsets                          │
│     let spill = try_new_spill(&spill_metrics)?;                       │
│     let offsets = data.write(spill.get_buf_writer())?;                │
│                                                                        │
│     // Store spill with offset index                                  │
│     spills.push(Offsetted::new(offsets, spill));                      │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  4. Final shuffle_write: merge all spills by partition                │
│                                                                        │
│     // Merge iterator reads partition-by-partition from all spills    │
│     let merge_iter = OffsettedMergeIterator::new(                     │
│         num_output_partitions,                                         │
│         spills.into_iter().map(...)                                   │
│     );                                                                 │
│                                                                        │
│     // Write each partition's data contiguously                       │
│     while let Some((partition_id, reader, range)) = merge_iter.next() │
│         std::io::copy(&mut reader, &mut output_data)?;                │
│     }                                                                  │
│                                                                        │
│     // Write index file with partition offsets                        │
│     output_index.write_all(&offsets_data)?;                           │
└────────────────────────────────────────────────────────────────────────┘
```

**Code:** `sort_repartitioner.rs:98-112`

```rust
async fn spill(&self) -> Result<()> {
    // Drain buffered data
    let data = self.data.lock().await.drain();
    let spill_metrics = self.exec_ctx.spill_metrics().clone();

    // Write to spill in background thread
    let spill = tokio::task::spawn_blocking(move || {
        let mut spill = try_new_spill(&spill_metrics)?;  // JVM heap or disk
        let offsets = data.write(spill.get_buf_writer())?;  // Compressed
        Ok(Offsetted::new(offsets, spill))  // Keep partition offsets
    }).await??;

    self.spills.lock().await.push(spill);
    self.update_mem_used(0).await?;
    Ok(())
}
```

### Example 4: JVM On-Heap → Disk Cascade

**Scenario:** JVM heap fills up during native spill

```
Native spill triggered → tries JVM on-heap
JVM on-heap at 95% → cascades to disk
```

**Flow:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  1. Native operator needs to spill                                     │
│                                                                        │
│     // Rust code                                                       │
│     let spill = try_new_spill(&spill_metrics)?;                       │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  2. Check JVM heap availability via JNI                               │
│                                                                        │
│     // Rust calls Java                                                 │
│     let hsm = jni_call_static!(JniBridge.getTaskOnHeapSpillManager())?;│
│     if jni_call!(AuronOnHeapSpillManager(hsm).isOnHeapAvailable())? { │
│         // Use JVM heap                                                │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  3. OnHeapSpillManager.isOnHeapAvailable() (Scala)                    │
│                                                                        │
│     val memoryPool = OnHeapSpillManagerHelper.getOnHeapExecutionPool  │
│     val memoryUsedRatio = memoryUsed / (memoryUsed + memoryFree)      │
│     val jvmMemoryUsedRatio = jvmUsed / (jvmUsed + jvmFree)            │
│                                                                        │
│     // Need < 90% usage on both                                        │
│     return memoryUsedRatio < 0.9 && jvmMemoryUsedRatio < 0.9          │
│                                                                        │
│     // Returns FALSE (95% used) → fall back to disk                   │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  4. Create FileSpill instead                                          │
│                                                                        │
│     // Rust code in spill.rs                                          │
│     pub fn try_new_spill(...) -> Result<Box<dyn Spill>> {             │
│         if hsm.isOnHeapAvailable() {                                  │
│             Ok(Box::new(OnHeapSpill::try_new(hsm)?))  // NOT taken    │
│         } else {                                                       │
│             Ok(Box::new(FileSpill::try_new()?))       // ← TAKEN      │
│         }                                                              │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  5. FileSpill writes compressed data to temp file                     │
│                                                                        │
│     let file_name = JniBridge.getDirectWriteSpillToDiskFile();        │
│     let file = OpenOptions::new().create(true).write(true).open()?;   │
│                                                                        │
│     // LZ4 compressed writes                                          │
│     IoCompressionWriter::try_new("lz4", BufWriter::new(file))         │
└────────────────────────────────────────────────────────────────────────┘
```

### Example 5: On-Heap Spill with Spark Memory Pressure

**Scenario:** Spark needs memory while Auron is spilling to heap

```
Auron spilling to JVM heap (MemBasedSpillBuf)
Spark operator requests memory
Spark calls OnHeapSpillManager.spill()
```

**Flow:**

```
┌────────────────────────────────────────────────────────────────────────┐
│  1. Auron data in JVM heap via OnHeapSpill                            │
│                                                                        │
│     OnHeapSpill                                                        │
│       └── MemBasedSpillBuf (Netty ByteBuf, 500MB in heap)             │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  2. Another Spark operator needs memory                               │
│                                                                        │
│     TaskMemoryManager.acquireExecutionMemory(200MB)                   │
│       → Not enough free memory                                         │
│       → Calls spill() on registered MemoryConsumers                   │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  3. OnHeapSpillManager.spill() called by Spark                        │
│                                                                        │
│     override def spill(size: Long, trigger: MemoryConsumer): Long = { │
│       // Don't spill if we're < 50% of task memory                    │
│       if (trigger != this &&                                           │
│           memUsed * 2 < taskMemoryManager.getMemoryConsumption) {     │
│         return 0L                                                      │
│       }                                                                │
│                                                                        │
│       // Sort by size descending, spill largest first                 │
│       val sortedSpills = spills.sortBy(-_.memUsed)                    │
│       sortedSpills.foreach { spill =>                                 │
│         totalFreed += spill.spill()  // MemBased → FileBased          │
│         if (totalFreed >= size) return totalFreed                     │
│       }                                                                │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  4. MemBasedSpillBuf.spill() → FileBasedSpillBuf                      │
│                                                                        │
│     def spill(): FileBasedSpillBuf = {                                │
│       val file = blockManager.diskBlockManager.createTempLocalBlock() │
│       val channel = new RandomAccessFile(file, "rw").getChannel       │
│                                                                        │
│       // Write all Netty buffers to disk                              │
│       while (!bufs.isEmpty) {                                          │
│         channel.write(bufs.removeFirst().nioBuffer())                 │
│       }                                                                │
│                                                                        │
│       new FileBasedSpillBuf(numWrittenBytes, file, channel)           │
│     }                                                                  │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  5. Memory freed, Spark operator proceeds                             │
│                                                                        │
│     // OnHeapSpillManager returns freed memory to Spark               │
│     hsm.freeMemory(releasingMemory)                                   │
│                                                                        │
│     // Auron reads continue from disk-backed spill                    │
│     // (transparent to native code)                                   │
└────────────────────────────────────────────────────────────────────────┘
```

### Spill Trigger Summary

| Trigger | Condition | Action |
|---------|-----------|--------|
| Native budget exceeded | `total_used > total_managed` | Spill largest consumer |
| Per-consumer limit | `consumer_used > consumer_max` | Spill this consumer |
| Process RSS (Linux) | `proc_rss > proc_max` | Spill largest consumer |
| Shuffle proactive | `mem_used_percent > 0.8` | Force spill shuffle buffer |
| JVM heap full | `heap_ratio > 0.9` | Use disk instead of heap |
| Spark memory pressure | `MemoryConsumer.spill()` callback | Spill heap to disk |

---

## Key Components

### Memory Tracking Summary

| Memory Type | Tracker | Method |
|-------------|---------|--------|
| Native heap | Rust `MemManager` | Per-consumer `update_mem_used()` |
| JVM direct | Rust via JNI | `BufferPoolMXBean.getTotalCapacity()` |
| JVM on-heap (spill) | Spark `TaskMemoryManager` | `MemoryConsumer.acquireMemory()` |
| Spark execution pool | Spark `MemoryManager` | `onHeapExecutionMemoryPool` |

### File Locations

| Component | File |
|-----------|------|
| MemManager | `native-engine/datafusion-ext-plans/src/memmgr/mod.rs` |
| Spill types | `native-engine/datafusion-ext-plans/src/memmgr/spill.rs` |
| Spill metrics | `native-engine/datafusion-ext-plans/src/memmgr/metrics.rs` |
| OnHeapSpillManager | `spark-extension/.../memory/OnHeapSpillManager.scala` |
| OnHeapSpill | `spark-extension/.../memory/OnHeapSpill.scala` |
| SpillBuf | `spark-extension/.../memory/SpillBuf.scala` |
| JniBridge | `spark-extension/.../auron/JniBridge.java` |

---

## Configuration

### Core Memory Settings

| Config | Default | Description |
|--------|---------|-------------|
| `spark.auron.memory.fraction` | 0.6 | Native memory as fraction of executor memory overhead |
| `spark.auron.onheap.spill.mem.fraction` | 0.9 | Max on-heap usage ratio before disk spill |
| `spark.auron.spill.compression.codec` | lz4 | Compression for spill files (lz4, zstd, none) |
| `spark.auron.process.memory.fraction` | 1.0 | Max process RSS as fraction of limit |

### Thresholds

| Constant | Value | Purpose |
|----------|-------|---------|
| `MIN_TRIGGER_SIZE` | 16 MB | Don't spill consumers smaller than this |
| `consumer_mem_max` | `total_managed / num_spillables` | Fair share per consumer |
| `consumer_mem_min` | `consumer_mem_max / 8` | Minimum size before forcing spill |
| `WAIT_TIME` | 10 sec | Timeout before forced spill |

---

## Metrics

### SpillMetrics

**File:** `native-engine/datafusion-ext-plans/src/memmgr/metrics.rs`

```rust
pub struct SpillMetrics {
    pub mem_spill_count: Count,     // Number of spill events
    pub mem_spill_size: Gauge,      // Bytes spilled to on-heap
    pub mem_spill_iotime: Time,     // On-heap I/O time
    pub disk_spill_size: Gauge,     // Bytes spilled to disk
    pub disk_spill_iotime: Time,    // Disk I/O time
}
```

### Monitoring

These metrics are reported to Spark UI and can be used to:
- Identify memory pressure hotspots
- Tune memory configuration
- Debug performance issues

Example log output during spill:
```
mem manager spilling AggTable (consumer: 128.5 MB),
  total_consumer: 512.0 MB/768.0 MB,
  unspillable: 32.0 MB,
  jvm_direct: 64.0 MB,
  proc resident: 2.1 GB
```

---

## Best Practices

1. **Size executor memory overhead appropriately** - Auron's native budget comes from this

2. **Monitor spill metrics** - Frequent spills indicate memory pressure

3. **Use LZ4 compression** - Good balance of speed and compression ratio

4. **Watch for JVM direct memory** - Arrow FFI uses direct buffers

5. **Consider process RSS on Linux** - `MemManager` monitors this to avoid OOM kills
