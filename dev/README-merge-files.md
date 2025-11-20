# File Merge Tool

## Overview

The `merge-files` tool merges code files from directories into single files, automatically removing Apache license headers and import/use statements while preserving package declarations and code structure.

## Features

✅ **Multi-language support**: Scala, Rust, Java
✅ **License removal**: Automatically removes Apache 2.0 license headers
✅ **Import cleanup**: Removes import/use/mod statements
✅ **Package preservation**: Keeps package declarations in merged files
✅ **File traceability**: Adds separators showing source file boundaries
✅ **Recursive processing**: Can process nested directory structures
✅ **Smart sorting**: Processes lib.rs, mod.rs, *Base.scala files first

## Installation

The script is ready to use from the `dev/` directory:

```bash
cd /path/to/auron
./dev/merge-files --help
```

## Usage

### Basic Usage

Merge all files from a single directory:

```bash
./dev/merge-files --input <directory> --output <output-dir>
```

### Examples

#### Merge Scala files from a specific directory

```bash
./dev/merge-files \
  --input spark-extension/src/main/scala/org/apache/spark/sql/auron/plan \
  --output merged-output/
```

**Output**: `merged-output/plan.scala.txt`

#### Merge all Scala files recursively

```bash
./dev/merge-files \
  --input spark-extension/src/main/scala \
  --output merged-output/ \
  --lang scala \
  --recursive
```

**Output**: One merged file per directory in the structure

#### Merge Rust files

```bash
./dev/merge-files \
  --input native-engine/datafusion-ext-plans/src \
  --output merged-output/ \
  --lang rust \
  --recursive
```

#### Merge all languages

```bash
./dev/merge-files \
  --input spark-extension/src/main \
  --output merged-output/ \
  --lang all \
  --recursive
```

## Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input directory containing source files | **Required** |
| `--output` | `-o` | Output directory for merged files | **Required** |
| `--lang` | `-l` | Language filter: `scala`, `rust`, `java`, `all` | `all` |
| `--recursive` | `-r` | Process subdirectories recursively | `false` |

## Output Format

Merged files have the following structure:

```scala
// Merged from directory: <directory-name>
// Source files: <count> files
// Generated: <timestamp>

package <package.name>  // Preserved from source

// ======================================================================
// Source: <filename1>
// ======================================================================

<code from file1 without license and imports>

// ======================================================================
// Source: <filename2>
// ======================================================================

<code from file2 without license and imports>

...
```

## What Gets Removed

### License Headers
- **Scala/Java**: First 16 lines (/* ... */ block comment)
- **Rust**: First 14 lines (// ... line comments)
- Pattern: "Licensed to the Apache Software Foundation..."

### Import Statements
- **Scala/Java**: All lines starting with `import `
- **Rust**: All lines starting with `use `, `mod `, `pub mod `

## What Gets Preserved

✅ **Package declarations**: First `package` statement (Scala/Java)
✅ **Code comments**: Non-license comments are preserved
✅ **Annotations**: `@sparkver`, `@tailrec`, etc.
✅ **Doc comments**: `/** ... */`, `///`
✅ **Code structure**: Classes, functions, traits, implementations

## File Processing Order

Files are processed in priority order:

1. **Rust**: `lib.rs`, `mod.rs`
2. **Scala**: `*Base.scala`
3. **Others**: Alphabetical order

This ensures that foundational code appears first in merged files.

## Special Handling

### Test Directories
Directories containing `/test/` in their path are automatically skipped.

### Subdirectories (with --recursive)
Each subdirectory creates its own merged file named after the directory:
- `foo/bar/` → `foo/bar.scala.txt`
- `baz/qux/` → `baz/qux.rs.txt`

### Multiple Languages
If a directory contains multiple languages without `--lang` filter, separate merged files are created:
- `mydir/` with `.scala` and `.java` files → `mydir.scala.txt` and `mydir.java.txt`

## Example Output

### Input Structure
```
spark-extension/src/main/scala/org/apache/spark/sql/auron/plan/
├── NativeAggBase.scala
├── NativeFilterBase.scala
├── NativeProjectBase.scala
└── NativeSortBase.scala
```

### Command
```bash
./dev/merge-files \
  --input spark-extension/src/main/scala/org/apache/spark/sql/auron/plan \
  --output merged/
```

### Output
```
merged/plan.scala.txt  (all 4 files merged, ~200KB)
```

## Use Cases

### 1. Code Analysis
Merge related files for easier code review or analysis:
```bash
./dev/merge-files --input <module> --output analysis/
```

### 2. Documentation
Create single-file references for documentation:
```bash
./dev/merge-files --input <core-module> --output docs/reference/
```

### 3. Code Generation Input
Use merged files as context for LLM-based code generation:
```bash
./dev/merge-files --input <module> --output llm-context/ --recursive
```

### 4. Diff Analysis
Compare module versions by merging and diffing:
```bash
git checkout v1.0
./dev/merge-files --input <module> --output v1/

git checkout v2.0
./dev/merge-files --input <module> --output v2/

diff -u v1/ v2/
```

## Limitations

- **No semantic analysis**: Code is concatenated, not restructured
- **No dependency resolution**: Import removal may break compilation
- **No deduplication**: Duplicate code from multiple files is preserved
- **Output not compilable**: Merged files are for reference, not compilation

## Troubleshooting

### Empty output file
- Check that input directory contains files with correct extensions
- Verify `--lang` filter matches file types

### Missing code
- Check if files have standard Apache license header format
- Verify line counts match expected (16 for Scala/Java, 14 for Rust)

### Package conflicts
- If multiple files have different `package` declarations, only the first is used
- Tool will warn about conflicts (future enhancement)

## Version

- **Version**: 1.0
- **Created**: November 2025
- **Language**: Python 3.6+
- **Dependencies**: Standard library only

## License

Apache License 2.0 (same as Auron project)
