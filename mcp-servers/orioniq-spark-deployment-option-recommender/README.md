# 🚀 OrionIQ — AI-Powered Data Platform Advisor

> Analyzes your real workloads, clusters, and costs — combined with a curated expert knowledge base of AWS best practices, blogs, and deployment guides — to recommend the optimal data platform deployment backed by actual metrics, not just keyword matching.

**Current focus:** AWS EMR (EC2, EKS, Serverless) and AWS Glue
**Roadmap:** Databricks, Snowflake, and cross-platform migration recommendations

## 🎬 Demo

[📹 Watch the demo](demo1.mp4) — Full walkthrough of OrionIQ analyzing a live EMR cluster and producing an evidence-based deployment recommendation.

![OrionIQ Demo](demo1.mp4)

## Why OrionIQ?

An LLM with cloud docs can say *"EMR Serverless is good for batch."* OrionIQ says:

> *"Your cluster j-ABC123 is 92% idle, your Spark job is CPU-intensive with 30-min runtime, and EMR Serverless costs $0.45 vs $15.70 on EC2 — 97% savings with no performance trade-off because your workload isn't IO-bound."*

That requires three things no general-purpose LLM has:
1. **Real metrics** from your actual cluster and Spark jobs
2. **Live pricing** from the AWS Pricing API
3. **Curated expert knowledge** — best practices docs, AWS blog posts, and deployment model guides indexed and searchable at analysis time

## Getting Started

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- AWS credentials configured (`aws configure` or `AWS_PROFILE`)
- Access to an EMR cluster you want to analyze

### Installation

```bash
git clone git@ssh.gitlab.aws.dev:2025-data-and-ai-hackathon/hack-151-orioniq-emr-deployment-option-recommender.git
cd hack-151-orioniq-emr-deployment-option-recommender
uv sync
```

### Usage Option 1: As an MCP Server (Kiro CLI / Claude Desktop)

OrionIQ runs as an MCP server that any MCP-compatible client can connect to. Add it to your MCP client config:

**Kiro CLI** (`~/.kiro/settings/tools.json`):
```json
{
  "mcpServers": {
    "orioniq": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/hack-151-orioniq-emr-deployment-option-recommender", "src/combined_emr_advisor_mcp.py"],
      "env": {
        "FASTMCP_LOG_LEVEL": "WARNING",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "orioniq": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/hack-151-orioniq-emr-deployment-option-recommender", "src/combined_emr_advisor_mcp.py"],
      "env": {
        "FASTMCP_LOG_LEVEL": "WARNING",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

Then ask your AI assistant:
> *"Analyze EMR cluster j-1TPYYUPQ7DMK8 in us-east-1 and recommend the best deployment option"*

The agent will use OrionIQ's 6 tools to gather data, analyze patterns, fetch pricing, and produce an evidence-based recommendation.

### Usage Option 2: Standalone Agent (Strands)

Run OrionIQ as an interactive agent with a conversation loop:

```bash
# Edit config.yaml with your cluster details, then:
uv run src/emr_advisor_agent_v2.py
```

This starts a Strands agent that connects to OrionIQ MCP + external MCP servers (AWS Data Processing, AWS Pricing, Spark History) and lets you chat interactively.

### Configuration

Edit `config.yaml` or use environment variables:

```bash
export AWS_REGION="us-east-1"
export EMR_CLUSTER_ID="j-YOUR-CLUSTER-ID"
export EMR_CLUSTER_ARN="arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-YOUR-CLUSTER-ID"
```

See [mcp_configuration_examples.md](mcp_configuration_examples.md) for advanced configuration options including custom knowledge sources, Docker deployment, and programmatic integration.

## Architecture

```
User Request
    │
    ▼
┌─────────────────────────────────────┐
│   Single OrionIQ Agent              │
│   (Claude + System Prompt)          │
│                                     │
│   Reasons about:                    │
│   - What data is available          │
│   - Which tools to call             │
│   - When to stop gathering          │
│   - How to structure report         │
└──────────┬──────────────────────────┘
           │ calls tools as needed
    ┌──────┼──────┬──────────┐
    ▼      ▼      ▼          ▼
┌──────┐┌──────┐┌──────┐┌──────┐
│EMR   ││AWS   ││AWS   ││Spark │
│Advisor││DP   ││Price ││Hist. │
│MCP   ││MCP   ││MCP   ││MCP   │
└──────┘└──────┘└──────┘└──────┘
```

### MCP Tools (6)

| Tool | Step | What it does |
|------|------|-------------|
| `load_knowledge_sources` | — | Indexes curated blogs, PDFs, and best practices docs |
| `search_knowledge_base` | — | Semantic search over expert content |
| `collect_workload_context` | 1 | Gathers user constraints + knowledge insights. No scoring — just context. |
| `analyze_spark_job_config` | 2 | Extracts Spark config from event logs or History Server |
| `analyze_cluster_patterns` | 3 | Analyzes EMR cluster via direct boto3 (idle time, spot usage, autoscaling) |
| `get_emr_pricing` | 4 | Fetches live AWS pricing + produces weighted evidence-based recommendation |

### Evidence-Based Scoring

The final recommendation weighs four factors from real data:

| Factor | Weight | Source |
|--------|--------|--------|
| Cost | 40% | Real-time AWS Pricing API |
| Workload fit | 30% | Spark metrics + cluster patterns + expert knowledge |
| Operational fit | 20% | Cluster utilization analysis |
| Constraints | 10% | User-provided preferences |

## What Changed (v1 → v2)

| Aspect | v1 | v2 |
|--------|----|----|
| Agents | 7 separate Agent() instances | 1 agent with all tools |
| Orchestrator | 901 lines, manual sequential pipeline | ~180 lines, agent reasons about workflow |
| MCP server | 3619 lines, 8 tools | 2897 lines, 6 tools |
| Context passing | Raw string dumps between steps | Agent maintains its own context |
| Tool behavior | 3 tools returned instructions for agent to relay | Tools call AWS APIs directly (boto3) |
| Scoring | 3 disagreeing implementations | 1 unified + 1 config-driven scorer |
| Recommendation | Keyword-based → pick cheapest | Metrics-first, weighted evidence from real data + expert knowledge |
| Determinism | Different results each run | Deterministic (sweep-line executor count, fixed pricing) |
| Dead code | 3 unused functions | 0 |

### Key Refactoring Decisions

- **Tools do work, not delegate it back.** `analyze_cluster_patterns` and `get_emr_pricing` now call boto3 directly instead of returning 500+ lines of instructions for the agent to relay.
- **Single agent beats multi-agent** when each "agent" is just calling 1 tool. Insight from Cognition's "Don't Build Multi-Agents" philosophy.
- **Deterministic executor count** via sweep-line algorithm on executor add/remove timeline — no more LLM interpretation of raw Spark data.

See [REFACTORING.md](REFACTORING.md) for the full refactoring journal with detailed issue analysis and fixes.

## Testing

Automated determinism test — runs the same analysis N times and compares results:

```bash
AWS_PROFILE=acc4-admin uv run python3 test_deterministic.py
```

Verified across 4 consecutive runs:

| Run | EMR on EC2 | EMR on EKS | EMR Serverless | Peak Executors |
|-----|-----------|-----------|---------------|---------------|
| 1 | $4.40 | $5.16 | $6.94 | 29 |
| 2 | $4.40 | $5.16 | $6.94 | 29 |
| 3 | $4.40 | $5.16 | $6.94 | 29 |
| 4 | $4.40 | $5.16 | $6.94 | 29 |

## Project Structure

```
src/
├── emr_advisor_agent_v2.py          # v2 single-agent orchestrator (~180 lines)
├── emr_advisor_strands_agent.py     # v1 7-agent orchestrator (preserved for comparison)
├── combined_emr_advisor_mcp.py      # MCP server with 6 tools (2897 lines)
├── emr_serverless_estimator.py      # Serverless cost estimation
├── scoring_config.yaml              # Config-driven scoring weights
└── region_mapping.yaml              # AWS region mappings
curated_pdfs/                        # Expert knowledge base (deployment guides, best practices)
templates/                           # Report templates (HTML + Markdown)
test_deterministic.py                # Automated determinism test
config.yaml                          # Default configuration
mcp_configuration_examples.md        # Advanced MCP configuration guide
REFACTORING.md                       # Detailed refactoring journal
```

## Known Limitations

- `analyze_spark_job_config` still uses the MCP relay pattern for Spark History Server (deferred — auth complexity justifies it)
- `combined_emr_advisor_mcp.py` (2897 lines) could be further split into focused MCP servers
- Knowledge base is static — consider embedding snapshots for faster loading
- Tool responses can be verbose, contributing to context window pressure on long analyses

## License

See [LICENSE](LICENSE).
