# EMR Serverless Spark Config Advisor MCP Server

[![MCP](https://img.shields.io/badge/MCP-1.0-blue)](https://modelcontextprotocol.io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](../../LICENSE)

> **🤖 Connect AI agents to EMR Serverless for intelligent Spark job analysis, configuration recommendations, and performance optimization**

An MCP (Model Context Protocol) server that analyzes Apache Spark event logs from Amazon EMR Serverless, identifies performance bottlenecks, and generates optimized Spark configurations. Works with any MCP-compatible client including [Kiro](https://kiro.dev), Claude Desktop, Amazon Q CLI, and LangGraph agents.

## 🎯 What is This?

This MCP server bridges AI agents with your EMR Serverless Spark workloads, enabling:

- 🔍 **Analyze Spark event logs** from S3 and extract detailed metrics
- ⚡ **Generate optimized configurations** — cost-optimized or performance-optimized worker sizing
- 🚨 **Identify bottlenecks** — CPU waste, memory pressure, spill, idle executors
- 🔄 **Compare applications** — performance metrics and Spark config diffs
- 📊 **Stage-level analysis** — find the slowest stages and their root causes
- 📈 **Resource timeline** — visualize executor scaling behavior over time
- 🔎 **SQL plan analysis** — compare execution plans between runs

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Clients                              │
│  ┌──────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │   Kiro   │  │ Claude       │  │ Amazon Q │  │ LangGraph  │  │
│  │   CLI    │  │ Desktop      │  │ CLI      │  │ Agents     │  │
│  └────┬─────┘  └──────┬───────┘  └────┬─────┘  └─────┬──────┘  │
│       │               │               │              │          │
│       └───────────────┼───────────────┼──────────────┘          │
│                       │               │                         │
│                  ┌────▼───────────────▼────┐                    │
│                  │   MCP Protocol (stdio)  │                    │
│                  └────────────┬────────────┘                    │
└───────────────────────────────┼──────────────────────────────────┘
                                │
                   ┌────────────▼────────────┐
                   │  Spark Config Advisor   │
                   │     MCP Server          │
                   │                         │
                   │  12 Tools:              │
                   │  • analyze_spark_logs   │
                   │  • list_applications    │
                   │  • get_application      │
                   │  • get_bottlenecks      │
                   │  • compare_performance  │
                   │  • compare_environments │
                   │  • list_slowest_stages  │
                   │  • get_stage_details    │
                   │  • get_resource_timeline│
                   │  • list_sql_executions  │
                   │  • compare_sql_plans    │
                   │  • list_event_logs      │
                   └────────────┬────────────┘
                                │ SSH
                   ┌────────────▼────────────┐
                   │   EC2 Processing Node   │
                   │                         │
                   │  ┌───────────────────┐  │
                   │  │ Spark Extractor   │  │
                   │  │ (PySpark)         │  │
                   │  │                   │  │
                   │  │ Phase A: Parallel  │  │
                   │  │ S3 decompress     │  │
                   │  │                   │  │
                   │  │ Phase B: Spark    │  │
                   │  │ metric extraction │  │
                   │  └────────┬──────────┘  │
                   │           │              │
                   │  ┌────────▼──────────┐  │
                   │  │ EMR Recommender   │  │
                   │  │                   │  │
                   │  │ Worker sizing     │  │
                   │  │ Executor tuning   │  │
                   │  │ Shuffle partitions│  │
                   │  └───────────────────┘  │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │     Amazon S3           │
                   │                         │
                   │  Spark Event Logs       │
                   │  (EMR Serverless)       │
                   └─────────────────────────┘
```

### Processing Pipeline

```
S3 Event Logs ──► Phase A: Decompress ──► Phase B: Spark Extract ──► Recommender
                  (50 parallel threads)    (PySpark local[*])        (Python)
                  bz2/zstd → jsonl         Per-app aggregation       Worker sizing
                                           Stage/executor/SQL        Spark configs
                                           metrics extraction        Shuffle tuning
```

**Phase A** downloads and decompresses event log files from S3 in parallel (50 threads). **Phase B** uses PySpark to aggregate task metrics, executor utilization, stage details, and SQL plans per application. The **Recommender** generates cost-optimized or performance-optimized EMR Serverless configurations.

## 🛠️ Available Tools

The MCP server provides **12 specialized tools** organized by analysis pattern:

### 📊 Extraction & Recommendations

| Tool | Description |
|---|---|
| `analyze_spark_logs` | Full pipeline: extract metrics from S3 event logs and generate EMR Serverless configuration recommendations (cost or performance optimized) |
| `list_event_log_prefixes` | Browse available Spark event log application prefixes in S3 |

### 🔍 Application Querying

| Tool | Description |
|---|---|
| `list_applications` | List all extracted applications with summary metrics (sorted by cost factor) |
| `get_application` | Get detailed metrics for a specific application — executor summary, IO, spill, Spark config |

### 🚨 Bottleneck Analysis

| Tool | Description |
|---|---|
| `get_bottlenecks` | Identify performance bottlenecks with severity-ranked, actionable recommendations. Analyzes CPU, memory, executors, spill, shuffle, stages, and failures |

### 🔄 Comparative Analysis

| Tool | Description |
|---|---|
| `compare_job_performance` | Side-by-side performance metrics between two applications with percentage deltas |
| `compare_job_environments` | Diff Spark configurations between two applications — different values, unique configs |

### ⚡ Stage Analysis

| Tool | Description |
|---|---|
| `list_slowest_stages` | Get the N slowest stages sorted by duration with IO/shuffle/spill per stage |
| `get_stage_details` | Deep dive into a specific stage's metrics |

### 📈 Resource & SQL Analysis

| Tool | Description |
|---|---|
| `get_resource_timeline` | Chronological executor add/remove events with running count — shows scaling behavior |
| `list_sql_executions` | List all SQL queries with duration and plan size |
| `compare_sql_execution_plans` | Compare physical execution plans between two SQL queries across applications |

### 🤖 How LLMs Use These Tools

| User Query | Tools Selected |
|---|---|
| *"Analyze my Spark logs and recommend configs"* | `analyze_spark_logs` |
| *"Why is my job slow?"* | `get_bottlenecks` + `list_slowest_stages` |
| *"Compare today's run with yesterday's"* | `compare_job_performance` + `compare_job_environments` |
| *"What's wrong with stage 5?"* | `get_stage_details` |
| *"Show me resource usage over time"* | `get_resource_timeline` |
| *"Find my slowest SQL queries"* | `list_sql_executions` + `compare_sql_execution_plans` |
| *"List all applications sorted by cost"* | `list_applications` |

## ⚡ Quick Start

### Prerequisites

- Python 3.10+
- An EC2 instance with:
  - Apache Spark installed (for PySpark extraction)
  - AWS CLI configured with S3 access to your event logs
  - The pipeline scripts deployed (`spark_extractor.py`, `pipeline_wrapper.py`, `emr_recommender.py`)
- SSH key access to the EC2 instance
- `mcp` Python package: `pip install mcp`

### 1. Configure the MCP Server

Edit `spark_advisor_mcp.py` and set your EC2 connection details:

```python
EC2_HOST = "hadoop@<your-ec2-public-dns>"
SSH_KEY = "~/<your-ssh-key>.pem"
```

### 2. Deploy Pipeline Scripts to EC2

```bash
# Copy the extraction and recommendation scripts
scp -i <your-key>.pem \
  spark_extractor.py pipeline_wrapper.py emr_recommender.py \
  hadoop@<your-ec2>:~/
```

### 3. Register with Your MCP Client

#### Kiro CLI

Add to `~/.kiro/settings/mcp.json`:

```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"],
      "env": {
        "AWS_DEFAULT_REGION": "us-east-1"
      }
    }
  }
}
```

#### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"]
    }
  }
}
```

#### Amazon Q CLI

Add to `~/.aws/amazonq/mcp.json`:

```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"]
    }
  }
}
```

### 4. Start Using

```
You: "Analyze Spark logs at s3://my-bucket/event-logs/ and recommend cost-optimized configs"

AI: [calls analyze_spark_logs] → Extracts metrics from 10 applications, generates recommendations...

You: "Which app has the worst bottlenecks?"

AI: [calls list_applications, then get_bottlenecks] →
    App 00g0jaejehl1980b has 3 HIGH severity issues:
    - 57% of executors were idle (203/354 never ran tasks)
    - 99.6% idle core percentage
    - 2 TB disk spill on 0.16 GB input

You: "Compare that app's config with the healthy one"

AI: [calls compare_job_environments] → 57 config differences found...
```

## 📊 Example Output

### Bottleneck Analysis

```json
{
  "application_id": "00g0jaejehl1980b",
  "bottleneck_count": 4,
  "findings": [
    {
      "severity": "HIGH",
      "category": "Executors",
      "finding": "203/354 executors (57.3%) were allocated but never ran tasks",
      "recommendation": "Tune spark.dynamicAllocation.maxExecutors or increase idle timeout"
    },
    {
      "severity": "HIGH",
      "category": "Cores",
      "finding": "Idle core percentage is 99.61%",
      "recommendation": "Most core-hours are wasted. Reduce total cores or improve parallelism"
    },
    {
      "severity": "HIGH",
      "category": "Spill",
      "finding": "Disk spill: 2088.31 GB, Memory spill: 7517.98 GB",
      "recommendation": "Increase executor memory or spark.sql.shuffle.partitions"
    }
  ]
}
```

### Configuration Recommendation

```json
{
  "worker": {
    "type": "Large",
    "vcpu": 16,
    "memory_gb": 108,
    "max_executors": 191,
    "min_executors": 95
  },
  "spark_configs": {
    "spark.executor.cores": "16",
    "spark.executor.memory": "108g",
    "spark.executor.instances": "191",
    "spark.sql.shuffle.partitions": "4938",
    "spark.emr-serverless.executor.disk": "500G",
    "spark.dynamicAllocation.enabled": "true"
  }
}
```

## 🔧 EC2 Instance Setup

The processing node needs sufficient memory for large Spark event logs. Recommended:

| Workload | Instance Type | Memory | Apps |
|---|---|---|---|
| Small (1-10 apps) | r5.4xlarge | 128 GB | Up to 10 apps |
| Medium (10-50 apps) | r5.8xlarge | 256 GB | Up to 50 apps |
| Large (50-100+ apps) | r5.24xlarge | 768 GB | 100+ apps |

### Performance Benchmarks (r5.24xlarge, 10 apps)

| Phase | Time | Peak Memory |
|---|---|---|
| Phase A: Decompress (1,676 files) | 85s | 48 GB |
| Phase B: Spark Extract (3M+ tasks) | 62s | 59 GB |
| Recommender | 1.4s | < 1 GB |
| **Total Pipeline** | **151s** | **59 GB** |

### Install Dependencies on EC2

```bash
# Install Python dependencies
pip install boto3 pandas numpy

# Spark should already be available on EMR EC2 instances
# Verify:
spark-submit --version
```

## 🏗️ Project Structure

```
emr-serverless-spark-advisor/
├── spark_advisor_mcp.py          # MCP server (12 tools)
├── spark_extractor.py            # PySpark-based metric extraction
├── pipeline_wrapper.py           # Orchestrates extract → recommend pipeline
├── emr_recommender.py            # EMR Serverless configuration recommender
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🔬 Extraction Accuracy

The Spark extractor has been validated against ground truth (raw event log parsing) and outperforms the legacy Python extractor:

| Metric | Ground Truth | Spark Extractor | Python Extractor |
|---|---|---|---|
| total_executors | 126 | 126 ✓ | 126 ✓ |
| active_executors | 121 | 121 ✓ | 124 ✗ |
| dead_executors | 5 | 5 ✓ | 2 ✗ |
| total_uptime_hours | 10.15 | 10.15 ✓ | 10.08 ✗ |
| cost_factor | 6.7513 | 6.7513 ✓ | 6.7032 ✗ |
| IO metrics | exact | exact ✓ | exact ✓ |

Recommendations generated from both extractors are **identical** — the same worker types, executor counts, memory, and shuffle partitions.

## 🤝 Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## 📄 License

Apache License 2.0 — see [LICENSE](../../LICENSE) for details.

## 🔗 Related Projects

- [EMR Serverless Config Advisor](../utilities/EMR-Serverless-Config-Advisor/) — The underlying extraction and recommendation engine
- [Kubeflow Spark History MCP Server](https://github.com/kubeflow/mcp-apache-spark-history-server) — MCP server for Apache Spark History Server (queries live SHS REST API)
