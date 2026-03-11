# EMR Serverless Spark Config Advisor — MCP Server

[![MCP](https://img.shields.io/badge/MCP-Compatible-blue)](https://modelcontextprotocol.io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

An [MCP](https://modelcontextprotocol.io) server that analyzes Spark event logs and generates optimized EMR Serverless configurations. Uses EMR Serverless for extraction (no EMR EC2 cluster needed) and reads results from S3.

Works with [Kiro](https://kiro.dev), Claude Desktop, Amazon Q CLI, and any MCP-compatible client.

## How It Works

```
You: "Analyze my Spark logs at s3://my-bucket/event-logs/"

AI → [analyze_spark_logs] → submits parallel EMR Serverless jobs → extracts metrics → returns:

  12 apps analyzed in 153s
  Top cost: eg-user-attribute-store (cost_factor: 96.8, idle: 86%)
  Peak shuffle: 1278 GB/stage (exceeds 200GB Serverless storage limit)

You: "What are the bottlenecks for app 00g0jaejehl1980b?"

AI → [get_bottlenecks] →
  HIGH: 52% idle core-hours
  HIGH: 2 TB disk spill on 0.16 GB input
  HIGH: Peak stage shuffle 1278 GB — not eligible for Serverless managed storage
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Clients                              │
│  ┌──────────┐  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │   Kiro   │  │    Claude    │  │ Amazon Q │  │ LangGraph  │  │
│  │   CLI    │  │   Desktop    │  │   CLI    │  │  Agents    │  │
│  └────┬─────┘  └──────┬───────┘  └────┬─────┘  └─────┬──────┘  │
│       └───────────────┼───────────────┼──────────────┘          │
│                  ┌────▼───────────────▼────┐                    │
│                  │   MCP Protocol (stdio)  │                    │
└──────────────────┼────────────────────────┼─────────────────────┘
                   │                        │
      ┌────────────▼────────────┐           │
      │  Spark Config Advisor   │           │
      │  MCP Server (local)     │           │
      │                         │           │
      │  12 Tools:              │           │
      │  • analyze_spark_logs   │           │
      │  • get_bottlenecks      │           │
      │  • compare_performance  │           │
      │  • list_applications    │           │
      │  • ...8 more            │           │
      └────────────┬────────────┘           │
                   │ boto3 API              │
      ┌────────────▼──────────────────────────────────────┐
      │                    AWS Cloud                       │
      │                                                    │
      │  ┌─────────────────────────────────────────────┐   │
      │  │         EMR Serverless Application           │   │
      │  │                                              │   │
      │  │  ┌──────────────┐  ┌──────────────┐         │   │
      │  │  │spark_extractor│  │spark_extractor│  ...N  │   │
      │  │  │  (App 1)     │  │  (App 2)     │         │   │
      │  │  └──────┬───────┘  └──────┬───────┘         │   │
      │  └─────────┼─────────────────┼─────────────────┘   │
      │            ▼                 ▼                      │
      │  ┌──────────────────────────────────┐               │
      │  │            Amazon S3             │               │
      │  │  /event-logs/     (input)        │               │
      │  │  /task_stage_summary/ (extract)  │               │
      │  │  /spark_config/      (configs)   │               │
      │  └──────────────────────────────────┘               │
      └────────────────────────────────────────────────────┘
```

**Flow:**
1. MCP client sends `analyze_spark_logs` with S3 path
2. MCP server discovers apps, submits 1 EMR Serverless job per app (parallel)
3. Each job runs `spark_extractor.py` — reads compressed event logs, extracts 80+ metrics
4. MCP server reads results from S3, returns recommendations
5. All other tools (`get_bottlenecks`, `compare_*`, etc.) read cached results from S3

**No SSH, no EMR EC2 cluster, no infrastructure to manage.**

## Tools

| Tool | Description |
|------|-------------|
| **`analyze_spark_logs`** | Extract metrics via EMR Serverless and generate recommendations |
| `list_event_log_prefixes` | Browse S3 for available event log apps |
| `list_applications` | List extracted apps with summary metrics |
| `get_application` | Full metrics + Spark config for one app |
| `get_bottlenecks` | Severity-ranked findings: CPU, memory, spill, idle cores, shuffle storage |
| `compare_job_performance` | Side-by-side metrics with % deltas |
| `compare_job_environments` | Diff Spark configs between two apps |
| `list_slowest_stages` | Top N stages by duration with IO/shuffle/spill |
| `get_stage_details` | Deep dive into one stage |
| `get_resource_timeline` | Executor add/remove events over time |
| `list_sql_executions` | SQL queries with duration |
| `compare_sql_execution_plans` | Diff physical plans between two SQL queries |

## Setup

### Prerequisites

- Python 3.10+
- AWS credentials with access to EMR Serverless, S3
- An EMR Serverless application (Spark)
- `spark_extractor.py` uploaded to S3

### 1. Create EMR Serverless Application

```bash
aws emr-serverless create-application \
  --name spark-advisor \
  --release-label emr-7.7.0 \
  --type SPARK \
  --region us-east-1
```

Note the `applicationId` from the output.

### 2. Upload Extraction Script to S3

```bash
# From the EMR-Serverless-Config-Advisor directory
aws s3 cp spark_extractor.py s3://your-bucket/scripts/spark_extractor.py

# If using zstd-compressed event logs, also upload the zstandard archive
aws s3 cp zstandard.zip s3://your-bucket/scripts/zstandard.zip
```

### 3. Install & Configure MCP Client

```bash
pip install mcp boto3
```

**Kiro CLI** (`~/.kiro/settings/mcp.json`):

```json
{
  "mcpServers": {
    "spark-config-advisor": {
      "command": "python3",
      "args": ["/path/to/spark_advisor_mcp.py"],
      "env": {
        "EMR_SERVERLESS_APP_ID": "00g1234567890",
        "EMR_EXECUTION_ROLE": "arn:aws:iam::ACCOUNT:role/EMRServerlessRole",
        "SCRIPT_S3_PATH": "s3://your-bucket/scripts/spark_extractor.py",
        "ARCHIVES_S3_PATH": "s3://your-bucket/scripts/zstandard.zip",
        "OUTPUT_S3_PATH": "s3://your-bucket/advisor-output",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`): same format.

**Amazon Q CLI** (`~/.aws/amazonq/mcp.json`): same format.

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `EMR_SERVERLESS_APP_ID` | Yes | EMR Serverless application ID |
| `EMR_EXECUTION_ROLE` | Yes | IAM role ARN for job execution |
| `SCRIPT_S3_PATH` | Yes | S3 path to `spark_extractor.py` |
| `ARCHIVES_S3_PATH` | No | S3 path to `zstandard.zip` (for zstd logs) |
| `OUTPUT_S3_PATH` | Yes | S3 base path for extracted output |
| `AWS_REGION` | No | AWS region (default: us-east-1) |

## Example Conversation

```
User: Analyze the Spark event logs at s3://my-bucket/event-logs/ and tell me
      which jobs are wasting the most resources.

AI: I'll analyze those logs now.
    [calls analyze_spark_logs(input_path="s3://my-bucket/event-logs/")]

    Extracted 12 applications in 153 seconds. Here are the top resource wasters:

    1. eg-user-attribute-store (00g0nlpd9atac00b)
       - Cost factor: 96.8 | Idle cores: 86% | Duration: 37 min
       - 1,225 GB memory spill on 20.8 TB input

    2. eg-user-attribute-store (00g0dtj5r0om5o0b)
       - Cost factor: 60.6 | Idle cores: 78% | Duration: 46 min

User: What are the bottlenecks for the first one?

AI: [calls get_bottlenecks(application_id="00g0nlpd9atac00b")]

    HIGH: 86% idle core-hours — most allocated cores sit unused
    HIGH: 1,225 GB memory spill — executor memory too small for data volume
    MEDIUM: Low CPU utilization (14%) — over-provisioned for compute

    Recommendations:
    - Increase executor memory to reduce spill
    - Reduce max executors from current level
    - Increase shuffle partitions to spread data more evenly
```

## Project Structure

```
emr-serverless-spark-advisor/
├── spark_advisor_mcp.py      # MCP server (12 tools, uses EMR Serverless + S3)
├── requirements.txt          # Dependencies (mcp, boto3)
├── README.md
└── legacy/                   # Previous SSH-based implementation
    ├── spark_advisor_mcp.py
    ├── spark_extractor.py
    ├── pipeline_wrapper.py
    ├── emr_recommender.py
    ├── format_to_job_config.py
    └── write_to_iceberg.py
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `Missing config: EMR_SERVERLESS_APP_ID` | Set all required env vars in MCP client config |
| `No event log apps found` | Check S3 path — should contain `eventlog_v2_*` or `application_*` subdirs |
| EMR Serverless job FAILED | Check CloudWatch logs for the job run |
| `Application not found` | Run `analyze_spark_logs` first to extract data |
| Slow extraction | Normal — first run takes ~150s for 12 apps. Subsequent queries use cached S3 data |

## License

Apache License 2.0 — see [LICENSE](../../LICENSE).
