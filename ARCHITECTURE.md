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

# Apache Auron Deep Dive: Shuffle, Memory, and the Road to Production

This guide explains how Auron moves data between stages (shuffle) and how it
manages memory, two areas where Auron's native engine has to cooperate closely
with Spark. It ends with a plan for making Auron production ready.

Each section explains the idea in plain language first. Class and file names
appear only in the **Where to look** table at the end of each section.

**Contents**

1. [Auron in one page](#1-auron-in-one-page)
2. [Shuffle write: how map tasks produce output](#2-shuffle-write-how-map-tasks-produce-output)
3. [Shuffle read: how reduce tasks consume it](#3-shuffle-read-how-reduce-tasks-consume-it)
4. [Memory management](#4-memory-management)
5. [Configuring and tuning](#5-configuring-and-tuning)
6. [Troubleshooting](#6-troubleshooting)
7. [Known risks](#7-known-risks)
8. [Roadmap to production readiness](#8-roadmap-to-production-readiness)

---

## 1. Auron in One Page

Auron makes Spark SQL faster by running the heavy parts of a query in a
**native engine** written in Rust, built on Apache Arrow and DataFusion,
instead of in the JVM. Spark still plans the query, schedules tasks, and
handles failures. Auron replaces the work done *inside* each task.

Here's what happens to a query:

1. **Planning.** Auron adds rules to Spark's planner. For each operator, Auron
   decides whether it can run natively. If it can't, Auron records why (for
   example, "unsupported expression").
2. **Conversion.** Operators that can run natively are replaced with native
   versions. The native part of the plan is serialized and sent along with
   each task.
3. **Execution.** On the executor, each task calls into the Rust engine through
   JNI. Each task gets its own small thread pool (a Tokio runtime). Results flow
   back as Arrow columnar batches.
4. **Fallback.** Any part that can't run natively stays in the JVM. Data is
   converted between Spark rows and Arrow columns at each boundary, and every
   conversion costs CPU and memory.

### Key Terms

| Term | Meaning |
|------|---------|
| Native engine | Auron's Rust execution engine, running inside the executor process but outside the JVM heap. |
| JNI | The bridge that lets JVM code and Rust code call each other. |
| Map and reduce side | In a shuffle, map tasks write data partitioned by key, and reduce tasks read one partition each. |
| Spill | Moving in-memory data somewhere else (the JVM heap or disk) to free memory. |
| Memory consumer | A native operator that uses a lot of memory and registers with Auron's memory manager, such as a sort, aggregation, join, or shuffle buffer. |
| Spillable | A consumer that can give memory back by spilling. Some, such as a broadcast hash table, can't. |
| Fair share | The amount of native memory one spillable consumer is allowed to use right now. |
| Memory overhead | Executor memory outside the JVM heap (`spark.executor.memoryOverhead`). Auron's native engine lives here. |

---

## 2. Shuffle Write: How Map Tasks Produce Output

### Plugging into Spark

Spark lets you replace its shuffle implementation. Auron provides its own
**shuffle manager**, which you enable with the `spark.shuffle.manager` setting.
The manager is a thin layer over Spark's standard sort-based shuffle manager. It
reuses Spark's file layout and bookkeeping, so external shuffle services, block
fetching, and stage retries keep working. For shuffles that aren't native, it
hands work back to Spark's standard implementation.

There are versions for three back ends:

| Shuffle back end | How data is stored |
|------------------|--------------------|
| Local disk (default) | Each map task writes one data file and one index file on the executor's disk, the same as Spark. |
| Apache Celeborn 0.5 and 0.6 | Map tasks push partition data to a remote shuffle service. |
| Apache Uniffle | Map tasks push partition data to a remote shuffle service. |

When the shuffle manager starts, it also tells Spark and Netty (Spark's
networking library) to **avoid off-heap (direct) memory**. Auron wants the
memory outside the heap for its native engine. Because these are JVM system
properties, they only affect components that read them later, so it's safest to
also set them yourself (see [section 5](#5-configuring-and-tuning)).

### What a Map Task Does

Instead of serializing rows in the JVM, a map task hands the whole job to the
native engine and tells it where to write the output files. Inside the native
engine:

1. **Buffer.** Incoming batches are collected in memory. Before adding a batch,
   the writer reserves twice its size from the memory manager, which leaves
   room for the temporary copy made while sorting.
2. **Assign partitions.** Each row is assigned a target partition, using the
   same hash function Spark uses (Murmur3 with seed 42), or range or round-robin
   partitioning. The hash has to match Spark's **bit for bit**. If it doesn't,
   rows written by native tasks and by JVM tasks land in different partitions
   and joins silently return wrong results.
3. **Group by partition.** Rows are sorted by partition number with a fast radix
   sort, so each partition's rows are next to each other. Each partition's
   chunk is then compressed (lz4 or zstd).
4. **Spill when needed.** If the buffer grows too large, it's written out as a
   spill (see below) and buffering starts again.
5. **Merge and commit.** When the input is finished, the in-memory data and all
   spills are merged partition by partition into one final data file. An index
   file records where each partition starts. The JVM then commits both files
   through Spark's normal mechanism, so the rest of Spark sees an ordinary
   shuffle output.

With a remote shuffle service, steps 1–4 are the same, but chunks are pushed to
the service instead of written to local files.

### Why Shuffle Spills Early

The shuffle writer spills **on its own** once it reaches **80% of its fair
share** of memory, without waiting for the memory manager to force it. Shuffle
data is cheap to spill: it's written once and read back once, in order. A
hash join or aggregation, by contrast, has to re-read and rebuild its spilled
state. By giving memory back first, shuffle buffers leave more room for the
operators that need it most.

### Where to look

| Topic | Location |
|-------|----------|
| Shuffle manager | `spark-extension-shims-spark/.../shuffle/AuronShuffleManager.scala` |
| Remote shuffle managers | `thirdparty/auron-celeborn-*`, `thirdparty/auron-uniffle` |
| Map-side entry (JVM) | `AuronShuffleWriter.scala`, `AuronShuffleWriterBase` |
| Native writer | `native-engine/datafusion-ext-plans/src/shuffle_writer_exec.rs` |
| Buffering, sorting, spilling | `native-engine/datafusion-ext-plans/src/shuffle/` (`sort_repartitioner.rs`, `buffered_data.rs`) |

---

## 3. Shuffle Read: How Reduce Tasks Consume It

### Fetching Stays in Spark

On the reduce side, Auron keeps Spark's own block fetcher, which handles
finding blocks, fetching them over the network, retrying, and limiting how much
data is in flight. Auron changes what happens *after* a block arrives: instead
of turning it back into JVM rows, Auron hands the raw bytes to the native
engine.

Each fetched block arrives in one of three forms, and the native engine has a
reader for each:

| Block form | When it happens |
|------------|-----------------|
| A slice of a local file | The block lives on the same machine. It's read straight from disk. |
| A memory buffer | The block was fetched and is held in memory. |
| A network stream | A remote block is streamed in. |

The JVM puts the list of blocks into a shared registry under a unique ID, and
the native plan refers to that ID. That's how the native engine finds its
input without copying anything through the JVM.

### Decoding in the Native Engine

The native reader pulls blocks one at a time. It does this on a separate
blocking thread so that waiting on JNI or I/O doesn't stall the engine's other
work. It decompresses each block, decodes the Arrow data, and combines small
batches until they reach about 8 MiB (set by `spark.auron.suggested.batch.memSize`).
Larger batches are processed more efficiently by the operators downstream.

### Where to look

| Topic | Location |
|-------|----------|
| Reduce-side entry (JVM) | `AuronBlockStoreShuffleReader.scala`, `AuronBlockStoreShuffleReaderBase.scala` |
| Native reader | `native-engine/datafusion-ext-plans/src/ipc_reader_exec.rs` |
| Decompression | `native-engine/datafusion-ext-commons/src/io/ipc_compression.rs` |

---

## 4. Memory Management

### The Big Picture

The native engine allocates memory directly from the operating system, outside
the JVM heap. That memory has to fit inside the executor's container
alongside the JVM, or the cluster manager (YARN or Kubernetes) kills the
container. Auron handles this with **three tiers**, used in order:

1. **Native memory**, the fastest. Operators work here by default.
2. **The JVM heap**, used for spills when the heap has room. This avoids disk
   I/O at the cost of one memory copy.
3. **Disk**, the slowest, used when neither of the first two has room.

### How Big Is the Native Memory Pool?

Auron sizes its pool from your executor settings:

1. **Total container memory** is the heap (`spark.executor.memory`) plus the
   overhead (`spark.executor.memoryOverhead`). If you don't set the overhead,
   Auron assumes Spark's classic default of 10% of the heap, with a minimum of
   384 MiB.
2. **Native memory** is whatever's left after the JVM heap. In practice that's
   about the same as the overhead.
3. **The managed pool** is 60% of native memory (`spark.auron.memoryFraction`).
   The remaining 40% is a cushion for allocator fragmentation, Arrow buffers
   passed between the JVM and Rust, and networking.

The pool is created once per executor and shared by all tasks running in it.

**Example.** An executor with an 8 GiB heap and 6 GiB of overhead has 14 GiB in
total, about 6 GiB of native memory, and a **managed pool of about 3.6 GiB**.

### Sharing the Pool: Fair Shares

Every memory-heavy native operator registers with the memory manager as a
**consumer**, either spillable or not. Each time a consumer's usage changes,
the manager recalculates the limits:

- **Managed space** is the pool, minus direct memory the JVM is using (for
  example, for networking), minus memory held by unspillable consumers.
- **Fair share** is the managed space divided evenly among the spillable
  consumers.
- **Minimum share** is one-eighth of the fair share.
- **Process limit** is 90% of the whole container
  (`spark.auron.process.vmrss.memoryFraction`), checked against the process's
  actual resident memory as reported by the operating system.

**Example, continued.** With the 3.6 GiB pool, nothing unspillable, and four
spillable consumers, each gets a fair share of about 900 MiB and a minimum share
of about 115 MiB. The process limit is 90% of 14 GiB, or about 12.6 GiB.

Checking real process memory, not just Auron's own bookkeeping, matters because
it catches memory Auron doesn't track directly: allocator overhead, JVM direct
buffers, and native libraries.

### When the Manager Steps In

The manager does nothing unless a consumer is **growing**, is already using
more than **16 MiB**, and **one** of these is true:

- all consumers together are over the managed space,
- this consumer is over its fair share, or
- the whole process is over its memory limit.

When it does step in, it either spills or waits:

- **Spill.** If the consumer is spillable and above its minimum share, the
  manager tells it to spill right away.
- **Wait.** Otherwise (the consumer is small or can't spill), the manager pauses
  it for up to **10 seconds**, hoping other consumers free memory. If nothing
  frees up in time, it spills anyway.

The minimum share keeps a small consumer from spilling over and over just
because a big neighbor is using most of the pool.

### Where Spills Go: Heap First, Then Disk

When a native operator spills, Auron first asks the JVM whether it has spare
heap. The answer is yes only when **both** of these are below 90%
(`spark.auron.onHeapSpill.memoryFraction`):

- how full Spark's on-heap execution memory is, and
- how full the JVM heap is overall.

If the heap has room, the spilled data is copied into JVM byte buffers and no
disk I/O happens. If not, it's written to a temporary file in Spark's local
directories. On the driver, spills always go to disk.

### Keeping Heap Spills Safe

Putting native data on the heap could push the JVM toward running out of
memory. Auron prevents that by registering its heap spills with **Spark's own
memory manager**, like any other Spark memory user. If another part of the task
later needs heap memory, Spark asks Auron to free some. Auron then writes its
largest heap spills to disk until enough memory is freed. In the worst case,
native data on the heap turns into a disk write. It never causes a JVM
out-of-memory error.

### Summary

| Tier | Managed by | Used when | Cost |
|------|-----------|-----------|------|
| Native memory | Auron's memory manager | Always, first | None |
| JVM heap | Auron's heap spill manager, inside Spark's memory manager | A native spill happens and the heap is less than 90% full | One memory copy, plus some garbage collection pressure |
| Disk | Temporary files, or Spark's block manager | The heap is busy, or Spark asks Auron to give heap back | Compression plus disk I/O |

### Where to look

| Topic | Location |
|-------|----------|
| Pool sizing | `NativeHelper.scala` (`totalMemory`, `nativeMemory`), `native-engine/auron/src/exec.rs` |
| Fair share, spill, and wait logic | `native-engine/auron-memmgr/src/lib.rs` |
| Choosing heap or disk | `native-engine/auron-memmgr/src/spill.rs` |
| Heap spill manager | `spark-extension/.../memory/SparkOnHeapSpillManager.scala` |
| Settings | `AuronConfiguration.java`, `SparkAuronConfiguration.java` |

---

## 5. Configuring and Tuning

### Minimal Setup

| Setting | Value |
|---------|-------|
| `spark.sql.extensions` | `org.apache.spark.sql.auron.AuronSparkSessionExtension` |
| `spark.shuffle.manager` | `org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager` |
| `spark.auron.enable` | `true` |
| `spark.executor.memory` | for example, `8g` |
| `spark.executor.memoryOverhead` | for example, `6g` (**set this explicitly**) |
| `spark.memory.offHeap.enabled` | `false` |

### What Each Memory Setting Does

| Setting | Default | What it controls, and when to change it |
|---------|---------|------------------------------------------|
| `spark.executor.memoryOverhead` | 10% of the heap (minimum 384 MiB) | The native engine's budget. **Always set it.** For join- or aggregation-heavy jobs, start at 50–100% of the heap. |
| `spark.auron.memoryFraction` | 0.6 | The share of the overhead Auron actively manages. Raise it for more native memory, or lower it if containers are killed even though Auron reports little usage. |
| `spark.auron.process.vmrss.memoryFraction` | 0.9 | When process memory crosses this share of the container, Auron starts spilling. Lower it if YARN or Kubernetes kills containers. |
| `spark.auron.onHeapSpill.memoryFraction` | 0.9 | How full the heap may be before spills go to disk instead. Lower it if garbage collection time rises during spills. |
| `spark.auron.suggested.batch.memSize` | 8 MiB | Target batch size. Bigger batches are faster per row but use more memory at peak. |
| `spark.auron.tokio.worker.threads.per.cpu` | 0 (automatic) | Threads per task. Automatic means **one thread per host core for every task**. On busy executors, set it to 1. |

### Why You Should Always Set the Overhead

If you leave `spark.executor.memoryOverhead` unset, Auron falls back to a
fixed 10% formula. It doesn't take into account Spark 3.3's
`memoryOverheadFactor`, Kubernetes' larger default for non-JVM workloads, or
memory set aside for PySpark.

**Example.** An executor with an 8 GiB heap and no overhead set gets about
820 MiB of overhead and a managed native pool of only about **490 MiB**. Native
operators then spill all the time, or the process grows past the container
limit and is killed.

---

## 6. Troubleshooting

| Symptom | What to check | Likely fix |
|---------|---------------|------------|
| Container killed by YARN or Kubernetes (exit code 137 or 143) | Executor log lines starting with "mem manager spilling" show the process's resident memory | Raise the overhead, or lower `spark.auron.process.vmrss.memoryFraction`. |
| Tasks are slow and spill constantly | Repeated "mem manager spilling" lines, and spill metrics in the Auron tab of the Spark UI | The native pool is too small. Raise the overhead or `spark.auron.memoryFraction`. |
| Warning "timeout waiting for resources" | Executor log | Memory is tied up by something that can't spill, often a broadcast hash table that's bigger than expected. Check broadcast join size estimates. |
| Garbage collection time rises during spills | GC logs, and log lines starting with "current jvm memory usage" | Heap spills are competing with the JVM. Lower `spark.auron.onHeapSpill.memoryFraction`. |
| Operators aren't running natively | Driver log lines showing "convertible=false" and the reason, and the Auron tab of the SQL UI | An unsupported expression or type. Report the reason string. |
| Too many threads, CPU thrashing | Thread dumps show many threads named "auron-native-stage-…" | Set `spark.auron.tokio.worker.threads.per.cpu` to 1. |

The Auron UI module adds an **Auron** tab to the Spark SQL UI and a History
Server plugin. They show how much of each query ran natively.

---

## 7. Known Risks

These risks come from reading the code and from recent bug fixes:

1. **Hashing must match Spark exactly.** That includes decimals of every
   precision, NaN, and negative zero for floats and doubles. Any difference
   sends rows to the wrong partition and silently corrupts join and bucketing
   results when native and JVM tasks are mixed.
2. **Scan filters must be conservative.** When skipping parts of ORC or Parquet
   files based on their statistics, Auron must never skip data that could
   match. Handling of NULLs in statistics is the classic mistake.
3. **Session settings must be respected.** Date and time functions must use the
   session time zone, and ANSI mode and overflow behavior must match Spark.
4. **Memory-safety bugs crash the executor.** Unsafe code at the edges (empty
   reads, buffers passed between the JVM and Rust, JNI references) can bring
   down the whole executor instead of failing a single task.
5. **Too many threads by default.** Each task gets its own thread pool. With
   automatic sizing, an executor running N tasks on a C-core machine can start
   about N × C threads.
6. **Weaker shuffle compression.** Rows are regrouped by partition. If their
   original order isn't preserved, the data may compress worse than with
   standard Spark.
7. **Fallback cost is invisible.** A plan that switches between native and JVM
   execution many times pays for each conversion and can end up slower than
   standard Spark. Nothing warns the user.

---

## 8. Roadmap to Production Readiness

### What "Production Ready" Means

Operators can turn Auron on across a large share of a mixed fleet and expect:

1. **Correct results**, identical to standard Spark. If Auron can't guarantee
   that, the query falls back to Spark.
2. **Stability.** Auron never crashes an executor and never causes more
   out-of-memory kills than standard Spark.
3. **Predictable performance.** Jobs get faster or stay the same, and
   regressions are caught before release.
4. **Operability.** Users can see what ran natively and why, and can turn Auron
   off per query, operator, or expression.

Phases 1 and 2 must be done before recommending Auron for general use. Phases 3
and 4 make it the default.

### Phase 1: Correctness (blocking)

| Work | Done when |
|------|-----------|
| **Spark-compatible hashing everywhere.** Check `hash`, `xxhash64`, and shuffle partitioning for every data type, including all decimal sizes, NaN and negative zero, nested types, and binary. | Automated tests compare native hashes with Spark's for random values of every type. |
| **Run Spark's own SQL tests natively.** Expand the Spark test module (Spark 3.1 to 4.1) and work through the roughly 120 tests currently excluded. | At least 95% of relevant Spark SQL test suites pass, and every remaining exclusion links to an issue. |
| **Compare against Spark automatically.** Run random queries on random data with both standard Spark and Spark + Auron every night, and compare results. Cover NULLs, extreme decimals, time zones, and ANSI mode. | 30 consecutive nights with no unexplained mismatch. |
| **Safe file skipping.** Review ORC and Parquet filter pushdown for NULLs, NaN, `IN` lists, and type changes. When unsure, don't skip. | A test matrix for each format, with and without NULL statistics. |
| **Respect session settings.** Pass the session time zone, ANSI mode, legacy compatibility flags, and case sensitivity to the native engine. If a setting isn't supported, fall back to Spark instead of guessing. | Unsupported settings trigger a fallback with a logged reason. |

### Phase 2: Stability (blocking)

| Work | Done when |
|------|-----------|
| **Memory sizing that works without tuning.** Take Spark's overhead factor, Kubernetes defaults, and PySpark memory into account. Warn at startup if the native pool is under 1 GiB. | Benchmarks at default settings show no container killed for memory, and a warning appears when the pool is too small. |
| **Bounded threads.** Default to one thread per task CPU, and limit the blocking-thread pool. | Thread count per executor is proportional to its cores, with no TPC-DS slowdown. |
| **Memory-safety checks.** Run the native tests under AddressSanitizer, review every unsafe block at the JVM, Rust, and I/O boundaries, and turn panics into ordinary task failures with context. | An AddressSanitizer CI job, and no recoverable error that aborts the process. |
| **Spill stress tests.** Many concurrent consumers, oversized broadcast tables, skewed partitions, and full disks. Make the 10-second wait configurable and count timeouts. | Jobs with tiny native budgets finish correctly, just slower. |
| **Retry on the JVM.** Optionally retry a task that failed in the native engine using the standard JVM path. | Available behind a setting, with the retry rate reported in metrics. |
| **Remote shuffle parity.** Run the same tests on Celeborn and Uniffle, including stage retries and speculative tasks. | All remote shuffle back ends pass the correctness and stress suites. |

### Phase 3: Performance

| Work | Done when |
|------|-----------|
| **Nightly benchmarks.** TPC-DS and TPC-H at scale factor 1000, compared with standard Spark and with the previous release. | A dashboard exists, and a slowdown of 5% or more on any query blocks the release. |
| **Fallback cost model.** Estimate how many native/JVM switches a plan needs, and keep a section in the JVM when switching would cost more than it saves. | No TPC-DS query is slower than standard Spark because of fallback. |
| **Better shuffle compression.** Keep the original row order within each partition, and tune the compression defaults. | Shuffle size is no larger than standard Spark's on TPC-DS. |
| **More native coverage.** Use the fallback reasons recorded in production to decide which expressions and types to add next. | At least 95% of operators in fleet workloads run natively. |
| **Adaptive memory.** Replace the fixed 60% fraction with feedback from actual process and allocator memory use. | Fewer spills with the same container size. |

### Phase 4: Operability

| Work | Done when |
|------|-----------|
| **Metrics.** Export native metrics to Spark's metrics system and event log: spill bytes per tier, wait timeouts, peak native memory, peak process memory, fallback counts, and conversion time. | Metrics appear in the Spark UI, the History Server, and Prometheus or Dropwizard. |
| **Fine-grained off switches.** About 30 per-operator enable settings already exist. Document them, add a block list for individual expressions, and make sure each one is honored at runtime. | Any operator can be turned off from a runbook without a rebuild. |
| **Explain fallback.** A per-query report of what ran natively, what didn't, and why, plus a native coverage percentage in the UI. | Users can find out why a query wasn't accelerated without reading logs. |
| **Rollout playbook.** Shadow runs (run both engines and compare checksums on a sample), canaries by queue or tenant, and automatic disable on a mismatch or a spike in memory kills. | Documented and tried on at least one large workload. |
| **Support policy.** Publish supported Spark, Scala, JDK, and OS versions, plus deprecation and backport rules for correctness fixes. | The support matrix is in the README and checked in CI. |

### Suggested Order

| Quarter | Focus | Milestone |
|---------|-------|-----------|
| Q1 | Hashing, file skipping, session settings, memory sizing, threads | Known correctness and resource bugs fixed. Defaults are safe. |
| Q2 | Spark SQL tests, nightly comparison, AddressSanitizer, spill stress tests | Correctness checks run in CI. **Beta.** |
| Q3 | JVM retry, remote shuffle parity, benchmarks, fallback cost model, metrics | Performance gate and metrics in place. **GA candidate.** |
| Q4 | Compression, coverage, adaptive memory, operability items | **GA, on by default.** |
