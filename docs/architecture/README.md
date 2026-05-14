# Apache Auron Architecture Documentation

This directory contains comprehensive documentation for learning and understanding Apache Auron's architecture.

## Documents

| Document | Description | Audience |
|----------|-------------|----------|
| [OVERVIEW.md](OVERVIEW.md) | System overview, high-level architecture, and data flow | Beginners |
| [MODULE-GUIDE.md](MODULE-GUIDE.md) | Deep dives into each module with key files and abstractions | Intermediate |
| [CRITICAL-PATHS.md](CRITICAL-PATHS.md) | End-to-end execution paths with code references | Advanced |
| [EXTENSION-GUIDE.md](EXTENSION-GUIDE.md) | How to extend Auron with new operators and expressions | Contributors |
| [GLOSSARY.md](GLOSSARY.md) | Key terms and concepts | All levels |

## Quick Start

1. **New to Auron?** Start with [OVERVIEW.md](OVERVIEW.md) to understand what Auron is and how it works
2. **Want to understand a specific module?** Check [MODULE-GUIDE.md](MODULE-GUIDE.md)
3. **Need to trace code execution?** See [CRITICAL-PATHS.md](CRITICAL-PATHS.md)
4. **Ready to contribute?** Read [EXTENSION-GUIDE.md](EXTENSION-GUIDE.md) and [../CONTRIBUTING.md](../../CONTRIBUTING.md)

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Apache Spark                                    │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│  │ SparkPlan   │───▶│  Catalyst   │───▶│ Physical    │                 │
│  │ (Logical)   │    │  Optimizer  │    │ Plan        │                 │
│  └─────────────┘    └─────────────┘    └──────┬──────┘                 │
└───────────────────────────────────────────────┼─────────────────────────┘
                                                │
                    ┌───────────────────────────▼───────────────────────┐
                    │              Auron Spark Extension                 │
                    │  ┌─────────────────────────────────────────────┐  │
                    │  │ AuronSparkSessionExtension                   │  │
                    │  │   └─▶ AuronColumnarOverrides                 │  │
                    │  │         └─▶ AuronConverters                  │  │
                    │  │               └─▶ NativeXxxExec operators    │  │
                    │  └─────────────────────────────────────────────┘  │
                    │                         │                          │
                    │                    Protobuf                        │
                    │                    Serialization                   │
                    │                         │                          │
                    └─────────────────────────┼──────────────────────────┘
                                              │ JNI
                    ┌─────────────────────────▼──────────────────────────┐
                    │               Native Rust Engine                    │
                    │  ┌─────────────────────────────────────────────┐  │
                    │  │ auron-jni-bridge                             │  │
                    │  │   └─▶ auron (runtime)                        │  │
                    │  │         └─▶ datafusion-ext-plans              │  │
                    │  │               └─▶ DataFusion execution        │  │
                    │  └─────────────────────────────────────────────┘  │
                    │                         │                          │
                    │                    Arrow FFI                       │
                    │                         │                          │
                    └─────────────────────────┼──────────────────────────┘
                                              │
                    ┌─────────────────────────▼──────────────────────────┐
                    │                 Results to Spark                    │
                    │           Arrow RecordBatch → InternalRow           │
                    └─────────────────────────────────────────────────────┘
```

## Key Insight

Auron accelerates Spark by:
1. **Converting** Spark physical plans to native equivalents
2. **Serializing** plans using Protocol Buffers
3. **Executing** natively in Rust using DataFusion's vectorized engine
4. **Returning** results as Arrow record batches via FFI
