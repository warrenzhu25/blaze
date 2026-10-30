<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# Apache Auron Deep Dive: Shuffle, Memory Management, and the Road to Production

Auron accelerates Apache Spark (and Flink) by running physical operators in a native Rust
engine built on Apache Arrow and DataFusion. This document explains how three subsystems work
together in that engine: shuffle write, shuffle read, and memory management. It ends with a
plan for making Auron production ready.

**Audience:** contributors and platform engineers who need to debug, tune, or extend Auron.

**Contents**

1. [Execution model in brief](#1-execution-model-in-brief)
2. [Shuffle write](#2-shuffle-write)
3. [Shuffle read](#3-shuffle-read)
4. [Memory management](#4-memory-management)
5. [Configuration and tuning](#5-configuration-and-tuning)
6. [Observability and troubleshooting](#6-observability-and-troubleshooting)
7. [Known limitations and risks](#7-known-limitations-and-risks)
8. [Roadmap to production readiness](#8-roadmap-to-production-readiness)

---

## 1. Execution model in brief

Here is what happens to a Spark query:

1. `AuronSparkSessionExtension` adds planner rules. `AuronConvertStrategy` tags each physical
   node as convertible or not and records why a node can't be converted
   (`auron.never.convert.reason`).
2. Convertible subtrees are rewritten as `Native*Exec` nodes and serialized into a protobuf
   `PhysicalPlanNode`.
3. On the executor, each task calls into Rust over JNI. The native side
   ([rt.rs](native-engine/auron/src/rt.rs)) builds a dedicated **Tokio runtime per task**,
   with threads named `auron-native-stage-<s>-part-<p>-tid-<t>`. It then executes the
   DataFusion plan and streams Arrow batches back through the Arrow C data interface (FFI).
4. If a subtree can't be converted, it runs in the JVM. Data moves into native code through
   `ConvertToNativeExec` (rows to Arrow) and back out through Spark's columnar-to-row path.
   Each crossing costs CPU and memory.

Shuffle and memory management are the two places where the native engine has to cooperate most
closely with Spark's own runtime. The rest of this document covers them.

---

## 2. Shuffle write

### 2.1 Entry point

To use Auron shuffle, set `spark.shuffle.manager` to
[AuronShuffleManager](spark-extension-shims-spark/src/main/scala/org/apache/spark/sql/execution/auron/shuffle/AuronShuffleManager.scala).
The manager wraps Spark's `SortShuffleManager`, reuses its `IndexShuffleBlockResolver`, and
falls back to it for shuffles that aren't native. For remote shuffle services, use the
subclasses of `AuronRssShuffleManagerBase`:

| Shuffle backend | Manager | Native repartitioner |
| :--- | :--- | :--- |
| Local disk (default) | `AuronShuffleManager` | `SortShuffleRepartitioner`, `SingleShuffleRepartitioner` |
| Apache Celeborn 0.5 / 0.6 | `AuronCelebornShuffleManager` (`thirdparty/auron-celeborn-*`) | `RssSortShuffleRepartitioner`, `RssSingleShuffleRepartitioner` |
| Apache Uniffle | `AuronUniffleShuffleManager` (`thirdparty/auron-uniffle`) | same RSS repartitioners |

When the manager is constructed, it also sets several JVM system properties:
`spark.memory.offHeap.enabled=false`, `io.netty.maxDirectMemory=0`,
`io.netty.noPreferDirect=true`, and `io.netty.noUnsafe=true`. The goal is to leave direct
(off-heap) memory to the native engine. These properties only affect components that read them
*after* this point, so it's safest to also set them explicitly (see [section 5](#5-configuration-and-tuning)).

### 2.2 Handing the write to native code

For each map task, `getWriter()` returns an
[AuronShuffleWriter](spark-extension-shims-spark/src/main/scala/org/apache/spark/sql/execution/auron/shuffle/AuronShuffleWriter.scala).
Instead of writing rows from the JVM, `AuronShuffleWriterBase` builds a `ShuffleWriterExecNode`
with temporary `.data` and `.index` paths and runs it through `NativeHelper.executeNativePlan`.

### 2.3 Buffering, partitioning, and sorting

[ShuffleWriterExec](native-engine/datafusion-ext-plans/src/shuffle_writer_exec.rs) picks a
repartitioner. If the shuffle has a single output partition, it uses `SingleShuffleRepartitioner`.
Otherwise it uses
[SortShuffleRepartitioner](native-engine/datafusion-ext-plans/src/shuffle/sort_repartitioner.rs).

1. **Staging.** Incoming `RecordBatch`es are appended to
   [BufferedData](native-engine/datafusion-ext-plans/src/shuffle/buffered_data.rs). Before each
   insert, the repartitioner reserves `current + 2 × batch_size` from the memory manager, which
   leaves room for the sort's temporary copy.
2. **Partition IDs.** Rows get partition IDs from Spark-compatible hash (Murmur3, seed 42),
   range, or round-robin partitioning. Hashing must match Spark exactly. If it doesn't, rows from native
   and JVM writers land in different partitions and joins return wrong results (see
   [section 7](#7-known-limitations-and-risks)).
3. **Radix sort.** `radix_sort_by_key` groups rows by partition ID. The rows are then
   interleaved into contiguous per-partition chunks, compressed (IPC + `lz4`/`zstd`), and
   tracked with offsets.

### 2.4 Spilling and the final merge

* **Spill early.** `SortShuffleRepartitioner` is a spillable `MemConsumer`. After every
  insert, it checks `mem_used_percent()`, which is its usage divided by its *fair share* (not
  by total memory). Above **0.8**, it spills on its own. Re-reading a shuffle spill is cheap
  compared with re-reading a hash-join or aggregation spill, so shuffle buffers give memory
  back first.
* **Spill target.** `spill()` drains `BufferedData` into a spill created by `try_new_spill()`.
  The spill goes to the JVM heap when possible and to a local file otherwise (see
  [section 4.4](#44-tier-2-spilling-to-the-jvm-heap)).
* **Merge.** When the task finishes, `shuffle_write()` marks the consumer unspillable. It
  merges the in-memory data and all spills with `OffsettedMergeIterator`, writing partition by
  partition into the final `.data` file and recording the offsets in the `.index` file. The JVM
  then commits both files through `IndexShuffleBlockResolver.writeMetadataFileAndCommit`.

RSS repartitioners don't write local files. They push per-partition chunks to the remote
service through a JNI-wrapped `RssPartitionWriter`.

---

## 3. Shuffle read

### 3.1 Block abstraction

On the reduce side, `getReader()` returns
[AuronBlockStoreShuffleReader](spark-extension-shims-spark/src/main/scala/org/apache/spark/sql/execution/auron/shuffle/AuronBlockStoreShuffleReader.scala).
The reader keeps Spark's `ShuffleBlockFetcherIterator`, which handles fetching, retries, and
`spark.reducer.maxSizeInFlight`. Instead of deserializing rows, `readIpc()` wraps each fetched
block in a `BlockObject` that exposes one of three access paths:

| Access path | When it's used | Native reader |
| :--- | :--- | :--- |
| `hasFileSegment` | Local block (same host) | File reader (path + offset + length) |
| `hasByteBuffer` | Fetched block held in memory | Direct or heap `ByteBuffer` reader |
| `getChannel` | Streamed remote block | `ReadableByteChannel` reader |

The block iterator is registered with `JniBridge.putResource` under a UUID. The native plan
refers to it by that UUID.

### 3.2 Native decoding

[IpcReaderExec](native-engine/datafusion-ext-plans/src/ipc_reader_exec.rs) gets the iterator
over JNI. It pulls blocks inside `tokio::task::spawn_blocking`, so JNI and IO calls don't stall
the async workers, and wraps each block in `IpcCompressionReader` to decompress and decode Arrow
IPC. Small batches are combined with `coalesce_arrays_unchecked` until they reach
`spark.auron.suggested.batch.memSize` (8 MiB by default).

---

## 4. Memory management

Native memory is managed by
[auron-memmgr](native-engine/auron-memmgr/src/lib.rs). It has three tiers:
**native memory → JVM heap (on-heap spill) → disk**.

### 4.1 Sizing the native pool

[NativeHelper](spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeHelper.scala)
computes the native pool size on the executor:

```
totalMemory     = spark.executor.memory + spark.executor.memoryOverhead
                  (overhead defaults to max(0.10 * executorMemory, 384 MiB) if unset)
nativeMemory    = totalMemory - Runtime.getRuntime().maxMemory()      # roughly the overhead
MemManager.total = nativeMemory * spark.auron.memoryFraction          # default 0.6
```

`MemManager::init` is called once per executor process in
[exec.rs](native-engine/auron/src/exec.rs). All tasks in that executor share the pool.

### 4.2 Consumers and fair share

Memory-heavy operators (sort, aggregation, joins, shuffle repartitioners, window, and so on)
implement `MemConsumer` and register as either **spillable** or **unspillable**. Whenever a
consumer's usage changes, `update_consumer_mem_used_with_custom_updater` recomputes its limits:

```
total_managed    = MemManager.total - jvm_direct_used - mem_unspillable
consumer_mem_max = total_managed / num_spillables
consumer_mem_min = consumer_mem_max / 8
mem_proc_max     = totalMemory * spark.auron.process.vmrss.memoryFraction   # default 0.9
```

`jvm_direct_used` comes from `JniBridge.getDirectMemoryUsed()`. Process RSS is read from
`/proc/self/status` (VmRSS) through `procfs`.

### 4.3 Tier 1: wait or spill

The manager takes action only if **all** of the following are true:

* the consumer is growing (`new_used > old_used`),
* the consumer uses more than `MIN_TRIGGER_SIZE` (16 MiB), and
* at least one overflow condition holds: `total_used > total_managed`,
  `new_used > consumer_mem_max`, or `process RSS > mem_proc_max`.

When it does act, there are two cases:

| Condition | Action |
| :--- | :--- |
| Consumer is spillable and `new_used > consumer_mem_min` | **Spill** this consumer right away. |
| Otherwise (small or unspillable consumer) | **Wait** on a condition variable for up to 10 s for other consumers to free memory. If the wait times out, spill anyway. |

The minimum (1/8 of the fair share) keeps a small consumer from spilling over and over just
because a large neighbor is using the shared budget.

### 4.4 Tier 2: spilling to the JVM heap

[try_new_spill](native-engine/auron-memmgr/src/spill.rs) asks the JVM whether it has heap to
spare by calling `AuronOnHeapSpillManager.isOnHeapAvailable()`. The answer comes from
[SparkOnHeapSpillManager](spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/SparkOnHeapSpillManager.scala),
and it's yes only when **both** of these ratios are below
`spark.auron.onHeapSpill.memoryFraction` (default 0.9):

* Spark's on-heap execution pool usage: `used / (used + free)`
* JVM committed heap usage: `(Runtime.totalMemory - Runtime.freeMemory) / Runtime.totalMemory`

If heap is available, the native buffers are copied into JVM `ByteBuffer`s (`OnHeapSpill`)
and no disk IO happens. If not, the data goes to a temporary file (`FileSpill`) in one of
Spark's local directories. On the driver, where there's no task context, spills always go to a
file.

### 4.5 Tier 3: Spark evicts heap spills to disk

`SparkOnHeapSpillManager` is a regular Spark `MemoryConsumer` in `MemoryMode.ON_HEAP`. When
another consumer in the task needs heap, `TaskMemoryManager` calls `spill(size, trigger)`. The
manager then writes its largest on-heap spills to disk through `BlockManager` until it has
freed enough memory. Because of this, data that native code parks on the heap can't cause a
JVM OOM. In the worst case, it becomes a disk write.

### 4.6 Summary

| Tier | Owner | Trigger | Cost |
| :--- | :--- | :--- | :--- |
| Native memory | `MemManager` | Default | None |
| JVM heap | `SparkOnHeapSpillManager` | Native spill while heap usage < 90% | One memcpy plus GC pressure |
| Disk | `FileSpill` / `BlockManager` | Heap busy, or Spark evicts | Compression plus disk IO |

---

## 5. Configuration and tuning

### 5.1 Minimal production configuration

```properties
spark.sql.extensions            org.apache.spark.sql.auron.AuronSparkSessionExtension
spark.shuffle.manager           org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager
spark.auron.enable              true

# Memory: give native execution an explicit budget
spark.executor.memory           8g
spark.executor.memoryOverhead   6g
spark.memory.offHeap.enabled    false
```

### 5.2 Key settings

| Setting | Default | Guidance |
| :--- | :--- | :--- |
| `spark.executor.memoryOverhead` | `max(10% heap, 384 MiB)` | **Set it explicitly.** This is the native budget. Start at 50–100% of the heap for join- or aggregation-heavy jobs. |
| `spark.auron.memoryFraction` | `0.6` | Fraction of overhead that `MemManager` manages. The rest covers allocator fragmentation, Arrow FFI buffers, and Netty. |
| `spark.auron.process.vmrss.memoryFraction` | `0.9` | Start spilling when process RSS passes this fraction of heap + overhead. Lower it if YARN or Kubernetes kills containers. |
| `spark.auron.onHeapSpill.memoryFraction` | `0.9` | Upper limit on heap usage for on-heap spills. Lower it if GC time increases while jobs spill. |
| `spark.auron.suggested.batch.memSize` | `8 MiB` | Target batch size. Larger batches are faster per row but use more peak memory. |
| `spark.auron.tokio.worker.threads.per.cpu` | `0` (auto) | `0` lets Tokio use **all host cores per task runtime**. On executors that run many tasks, set it to `1` so the thread count matches `spark.task.cpus`. |
| `spark.memory.offHeap.enabled` | forced `false` | Auron owns off-heap memory. Leave this disabled. |

### 5.3 Why the overhead default is risky

When `memoryOverhead` is unset, `NativeHelper` uses a fixed 10% / 384 MiB formula. It doesn't
read `spark.executor.memoryOverheadFactor` (Spark 3.3+) or Kubernetes' non-JVM factor, and it
doesn't subtract `spark.executor.pyspark.memory`. With the default of 10% overhead and a 0.6
fraction, an 8 GiB executor gets only about 490 MiB of managed native memory. Native operators
then spill constantly, or the process RSS goes past the container limit.

---

## 6. Observability and troubleshooting

| Symptom | Where to look | Likely cause or fix |
| :--- | :--- | :--- |
| Container killed by YARN or Kubernetes (exit 137/143) | Executor log: `mem manager spilling ... proc resident:` | RSS above the container limit. Raise `memoryOverhead` or lower `vmrss.memoryFraction`. |
| Long task times with many spills | Log: `mem manager spilling <consumer>`. Spill metrics in the Auron UI tab. | Native pool too small. Raise overhead or `memoryFraction`. |
| `timeout waiting for resources` | Executor log (WARN) | Unspillable consumers, such as a broadcast hash map, are holding the pool. Check for broadcast-join size misestimates. |
| High GC time during spills | GC logs, `current jvm memory usage` log lines | On-heap spill is competing with the JVM. Lower `onHeapSpill.memoryFraction`. |
| Operators not running natively | Driver log: `convertible=false`, `auron.never.convert.reason`. Auron SQL UI tab. | Unsupported expression or type. File an issue with the reason string. |
| Too many threads, CPU thrash | `jstack` / `top -H`: `auron-native-stage-*` threads | Set `tokio.worker.threads.per.cpu=1`. |

The [auron-spark-ui](auron-spark-ui) module adds an **Auron** SQL tab and a History Server
plugin. They show each query's native coverage.

---

## 7. Known limitations and risks

These come from reading the code and from recent fixes:

1. **Hash compatibility is required for correctness.** Native hash partitioning must match
   Spark's Murmur3 bit for bit. That includes decimals of each precision, NaN, and `-0.0` for
   float and double. Any difference sends rows to the wrong partition and silently corrupts join
   and bucketing results when native and JVM writers are mixed.
2. **Scan pushdown has to be conservative.** Predicate pushdown in ORC and Parquet must never
   skip a row group that could match. NULL handling in statistics is the classic trap.
3. **Session semantics.** Time functions (`date_trunc`, casts, `from_utc_timestamp`) must use
   `spark.sql.session.timeZone`. ANSI mode and overflow behavior must match Spark.
4. **Memory safety at the FFI and IO edges.** Unsafe code around zero-length reads, Arrow FFI
   ownership, and JNI global references can crash the executor, which is worse than a failed
   task.
5. **Threading defaults.** Each task gets its own Tokio runtime. With auto sizing, an executor
   running N tasks on a C-core host can create about N×C worker threads.
6. **Shuffle compression ratio.** Rows are regrouped by partition. Without stable ordering
   within a partition, the shuffle output may compress worse than vanilla Spark's.
7. **Fallback cost is invisible to users.** Plans that switch between native and JVM execution
   repeatedly pay conversion costs and can end up slower than vanilla Spark. Nothing warns about
   this today.

---

## 8. Roadmap to production readiness

"Production ready" here means an operator can turn Auron on for a large share of a mixed fleet
and expect:

1. **Correctness.** Results are identical to vanilla Spark, or the query falls back.
2. **Stability.** Auron never crashes an executor and never causes more OOM kills than vanilla
   Spark.
3. **Predictable performance.** Jobs get faster or stay the same, and regressions are caught
   before release.
4. **Operability.** Users can see what ran natively and why, and can turn Auron off per query,
   operator, or expression.

The work is split into four phases. Phases 1 and 2 block a general-availability recommendation.
Phases 3 and 4 make Auron the default.

### Phase 1: Correctness parity (blocking)

| # | Work item | Exit criteria |
| :--- | :--- | :--- |
| 1.1 | **Spark-compatible hashing everywhere.** Audit `hash`, `xxhash64`, and shuffle partitioning for every type: decimal (compact and large), float/double (NaN, `-0.0`), nested types, and binary. | Property tests compare native hashes with Spark's `Murmur3Hash` / `XxHash64` for random values of every type. |
| 1.2 | **Run the Spark SQL test suite natively.** Grow `auron-spark-tests` (Spark 3.1–4.1) and burn down the roughly 120 current `ignore`/`exclude` entries. Track the pass rate for each release. | 95% or more of the relevant Spark SQL suites pass, and every remaining exclusion links to an issue. |
| 1.3 | **Differential fuzzing.** Run random queries and data on Spark and Spark + Auron and compare results, covering NULLs, extreme decimals, timezones, and ANSI mode. Run it nightly in CI. | Zero unresolved mismatches for 30 consecutive nights. |
| 1.4 | **Conservative pushdown.** Review ORC and Parquet predicate pushdown for NULLs, NaN, `IN` lists, and type promotion. When unsure, don't prune. | Pushdown test matrix per format, with and without NULL statistics. |
| 1.5 | **Session semantics.** Session timezone, ANSI mode, `spark.sql.legacy.*` flags, and case sensitivity are passed to native code and respected there. If a semantic isn't supported, the query **falls back** instead of approximating. | The config-sensitivity test suite passes. Unsupported flags trigger fallback with a logged reason. |

### Phase 2: Stability and resource safety (blocking)

| # | Work item | Exit criteria |
| :--- | :--- | :--- |
| 2.1 | **Memory sizing that works without tuning.** Respect `memoryOverheadFactor`, PySpark memory, and Kubernetes overhead. Warn at startup when the native pool is below a safe minimum, such as 1 GiB. Consider defaulting the overhead to a larger fraction when Auron is enabled. | Default configs on standard benchmarks show no container OOM kills. A startup warning is logged when the pool is under-provisioned. |
| 2.2 | **Bounded threading.** Default `tokio.worker.threads.per.cpu=1`, sized by `spark.task.cpus`. Bound the `spawn_blocking` pool. | The executor thread count is bounded by `cores × constant`. No regression on TPC-DS. |
| 2.3 | **Memory-safety hardening.** Run Miri or AddressSanitizer on the IO, FFI, and JNI layers. Audit every `unsafe` block, especially zero-length buffers and FFI release callbacks. Turn panics into Spark task failures with context. | ASan CI job on the native test suite. No `abort()` path from a recoverable error. |
| 2.4 | **Spill robustness.** Test memory-pressure scenarios: many concurrent consumers, big broadcast maps, skewed partitions, and full disks. Make the 10 s wait timeout configurable and record a metric each time it expires. | Chaos tests with small native budgets finish correctly, slower but without OOM. |
| 2.5 | **Graceful fallback when native code fails.** Optionally retry a failed native task attempt on the JVM path, for example on panic or unsupported-feature errors. | Behind a config flag. Retry rate is reported in metrics. |
| 2.6 | **RSS parity.** Run the same correctness and chaos suites on Celeborn and Uniffle, including stage retry and speculative execution. | All RSS backends pass the Phase 1 and 2.4 suites. |

### Phase 3: Performance predictability

| # | Work item | Exit criteria |
| :--- | :--- | :--- |
| 3.1 | **Continuous benchmarks.** Run TPC-DS and TPC-H at SF1000 nightly, comparing against vanilla Spark and the previous release. Results go to `benchmark-results/`. | Dashboard exists, and a regression of 5% or more on any query blocks the release. |
| 3.2 | **Fallback cost model.** Estimate how many native/JVM transitions a plan has. Skip native execution for subtrees where the conversion overhead outweighs the gain. | No query in TPC-DS is slower than vanilla because of fallback. |
| 3.3 | **Better shuffle compression.** Keep input order within partitions (stable radix sort). Tune the codec and buffer size defaults. | Shuffle bytes are at most vanilla Spark's on TPC-DS. |
| 3.4 | **Expression coverage.** Use the `never.convert.reason` statistics from production event logs to rank the missing expressions and types to implement next. | 95% or more of operators in fleet workloads run natively. |
| 3.5 | **Adaptive memory.** Replace the static `memoryFraction` with feedback from RSS and allocator statistics, for example jemalloc or mimalloc introspection. | Fewer spills at the same RSS envelope. |

### Phase 4: Operability and rollout

| # | Work item | Exit criteria |
| :--- | :--- | :--- |
| 4.1 | **Metrics.** Export native metrics to Spark's metrics system and the event log: spill bytes per tier, wait timeouts, native pool peak, RSS peak, fallback count, and conversion time. | Metrics appear in the Spark UI, the History Server, and Prometheus or Dropwizard sinks. |
| 4.2 | **Fine-grained kill switches.** About 30 `spark.auron.enable.*` operator flags exist already. Document them, add a deny-list config for expressions, and make sure every flag is honored at runtime. | A runbook can disable any operator without a rebuild. |
| 4.3 | **Fallback explainability.** Add an `EXPLAIN AURON` style report and a per-query native coverage percentage in the UI. | Users can find out why a query didn't accelerate without reading logs. |
| 4.4 | **Rollout playbook.** Shadow mode (run both, compare checksums on a sample), canary by queue or tenant, automatic disable on mismatch or OOM spike. | The playbook is documented and tried on at least one large workload. |
| 4.5 | **Compatibility and release policy.** Publish a support matrix (Spark, Scala, JDK, OS, glibc), a deprecation policy, and a backport policy for correctness fixes. | Matrix appears in the README and is enforced by CI. |

### Suggested sequencing

| Quarter | Focus | Milestone |
| :--- | :--- | :--- |
| Q1 | 1.1, 1.4, 1.5, 2.1, 2.2 | Fix known correctness and resource bugs. Make default configs safe. |
| Q2 | 1.2, 1.3, 2.3, 2.4 | Correctness gates in CI. Chaos and ASan suites. **Beta.** |
| Q3 | 2.5, 2.6, 3.1, 3.2, 4.1 | Benchmark gate, RSS parity, metrics. **GA candidate.** |
| Q4 | 3.3–3.5, 4.2–4.5 | Performance tuning, rollout tooling. **GA / default-on.** |
