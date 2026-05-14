# Merged Files Reference

This document describes the pre-merged code files in `merged-output/` and how to generate them using the `dev/merge-files` tool.

## Purpose

The merged files consolidate related source code into single files, making it easier to:

1. **Feed to LLMs**: Provide complete context in a single file for AI-assisted code analysis
2. **Code Review**: See related code together without jumping between files
3. **Learning**: Understand how components work together
4. **Documentation**: Reference code structure without cloning the repo

## Pre-Merged Files

The `merged-output/` directory contains consolidated views of the codebase:

### Scala Files

| File | Description | Key Contents |
|------|-------------|--------------|
| `auron.scala.txt` | Core Spark extension | AuronSparkSessionExtension, AuronConverters, NativeConverters, NativeRDD, Shims |
| `plan.scala.txt` | Base operator classes | All NativeXxxBase classes, NativeSupports trait |
| `columnar.scala.txt` | Arrow/columnar handling | AuronColumnVector, ArrowUtils, ColumnarBatch conversion |
| `shuffle.scala.txt` | Shuffle implementation | ShuffleWriter, ShuffleReader, partition handling |
| `memory.scala.txt` | Memory management | OnHeapSpillManager, memory tracking |
| `util.scala.txt` | Utility classes | Helpers, converters, common functions |
| `arrowio.scala.txt` | Arrow I/O | FFI bridge, Arrow schema/batch handling |

### Rust Files

| File | Description | Key Contents |
|------|-------------|--------------|
| `src.rs.txt` | Main engine code | JNI entry points, runtime, execution coordination |
| `agg.rs.txt` | Aggregation operators | Hash/Sort aggregation, accumulators, group-by |
| `joins.rs.txt` | Join implementations | Sort-merge join, broadcast join, hash join |
| `smj.rs.txt` | Sort-merge join details | SMJ algorithm, merge logic |
| `bhj.rs.txt` | Broadcast hash join | Build/probe phases, hash table |
| `shuffle.rs.txt` | Shuffle operators | Shuffle writer, partition assignment |
| `window.rs.txt` | Window functions | Row number, rank, frame handling |
| `scan.rs.txt` | File scan operators | Parquet, ORC reading |
| `memmgr.rs.txt` | Memory management | MemManager, spill logic |
| `common.rs.txt` | Common utilities | Shared functions, helpers |
| `generate.rs.txt` | Generate operator | explode, posexplode |
| `brickhouse.rs.txt` | Brickhouse UDFs | Custom UDF implementations |
| `http.rs.txt` | HTTP service | Debug/metrics endpoints |
| `processors.rs.txt` | Batch processors | RecordBatch transformations |

## Using the Merge Tool

### Basic Usage

```bash
# Merge a single directory
./dev/merge-files --input spark-extension/src/main/scala/org/apache/spark/sql/auron \
                  --output merged-output/

# Merge recursively
./dev/merge-files --input native-engine/datafusion-ext-plans/src \
                  --output merged-output/ \
                  --recursive

# Filter by language
./dev/merge-files --input spark-extension/src/main/scala \
                  --output merged-output/ \
                  --lang scala \
                  --recursive
```

### Command Options

| Option | Short | Description |
|--------|-------|-------------|
| `--input` | `-i` | Input directory containing source files |
| `--output` | `-o` | Output directory for merged files |
| `--lang` | `-l` | Filter by language: `scala`, `rust`, `java`, `all` |
| `--recursive` | `-r` | Process subdirectories recursively |

### What Gets Processed

The tool:
- **Removes** Apache license headers (16 lines for Scala/Java, 14 for Rust)
- **Removes** import/use statements
- **Preserves** package declarations (Scala/Java)
- **Orders** files intelligently (lib.rs, mod.rs, *Base.scala first)
- **Adds** file separators for traceability
- **Skips** test directories

### Output Format

Each merged file includes:
1. Header with source info and timestamp
2. Package declaration (if applicable)
3. File contents separated by `// ===... Source: filename ===...`

Example:
```scala
// Merged from directory: auron
// Source files: 15 files
// Generated: 2025-11-20 10:30:00

package org.apache.spark.sql.auron

// ======================================================================
// Source: AuronSparkSessionExtension.scala
// ======================================================================
class AuronSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  // ...
}

// ======================================================================
// Source: AuronConverters.scala
// ======================================================================
object AuronConverters {
  // ...
}
```

## Regenerating Merged Files

To regenerate all merged files:

```bash
# Scala - Core Auron extension
./dev/merge-files -i spark-extension/src/main/scala/org/apache/spark/sql/auron \
                  -o merged-output/ -l scala

# Scala - Base plan operators
./dev/merge-files -i spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan \
                  -o merged-output/ -l scala

# Scala - Columnar handling
./dev/merge-files -i spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/columnar \
                  -o merged-output/ -l scala

# Rust - Main engine
./dev/merge-files -i native-engine/auron/src \
                  -o merged-output/ -l rust

# Rust - DataFusion plans (recursive)
./dev/merge-files -i native-engine/datafusion-ext-plans/src \
                  -o merged-output/ -l rust -r

# Rust - Expressions
./dev/merge-files -i native-engine/datafusion-ext-exprs/src \
                  -o merged-output/ -l rust

# Rust - Commons
./dev/merge-files -i native-engine/datafusion-ext-commons/src \
                  -o merged-output/ -l rust
```

## Use Cases

### 1. LLM Code Analysis

Feed merged files to Claude, GPT, or other LLMs for:
- Architecture questions
- Code review
- Bug analysis
- Documentation generation

```bash
# Example: Analyze aggregation implementation
cat merged-output/agg.rs.txt | pbcopy
# Paste into LLM with your question
```

### 2. Code Search

Search across related files without grep gymnastics:

```bash
# Find all uses of MemManager
grep -n "MemManager" merged-output/src.rs.txt

# Find aggregation patterns
grep -A5 "impl Accumulator" merged-output/agg.rs.txt
```

### 3. Learning Path

Recommended reading order for understanding Auron:

1. `auron.scala.txt` - How Spark integrates with Auron
2. `plan.scala.txt` - How operators are structured
3. `src.rs.txt` - How native execution works
4. `agg.rs.txt` or `joins.rs.txt` - Deep dive into specific operators

### 4. Offline Reference

Clone merged files for offline access:

```bash
# Copy to another location
cp -r merged-output/ ~/auron-reference/
```

## File Sizes

Typical merged file sizes (for reference):

| Category | Files | Total Size |
|----------|-------|------------|
| Scala files | 7 | ~320 KB |
| Rust files | 14 | ~800 KB |
| **Total** | 21 | ~1.1 MB |

These sizes are optimized for LLM context windows (typically 100-200K tokens).

## Best Practices

1. **Regenerate after major changes**: Run merge tool after significant code changes
2. **Don't commit merged files**: They're in `.gitignore` for a reason
3. **Use for analysis, not editing**: Edit source files, not merged output
4. **Check file order**: lib.rs and *Base.scala are intentionally first
