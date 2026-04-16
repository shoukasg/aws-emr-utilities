# Refactoring a Multi-Agent MCP System: From 3600-Line God Server to Clean Architecture

> A hands-on learning journal of refactoring OrionIQ — an AI-powered EMR deployment advisor — applying lessons from the Multi-Agent Architecture course, Anthropic's context engineering principles, and Cognition's "Don't Build Multi-Agents" philosophy.

## Background

OrionIQ is an MCP-based tool that analyzes AWS EMR clusters and recommends optimal deployment options (EMR on EC2, EMR on EKS, EMR Serverless). The original implementation:
- 7 separate Agent() instances in a sequential pipeline (901-line orchestrator)
- 1 monolithic MCP server with 8 tools (3619 lines)
- 4 external MCP servers (AWS Data Processing, AWS Pricing, Spark History)

### What We Already Fixed (v2 Orchestrator)

Collapsed 7 agents into a single well-tooled agent (~180 lines). Key insight from the course: each "agent" was just calling 1 tool — a single agent with all tools reasons better about workflow order and manages its own context.

### What We're Fixing Now: The MCP Server

The `combined_emr_advisor_mcp.py` (3619 lines) has deeper architectural issues that we'll fix one at a time.

---

## Issue #1: Tools That Return Instructions Instead of Results

### The Problem

Look at `analyze_cluster_patterns()` and `analyze_spark_job_config()` in the MCP server. When you call them with cluster IDs or application IDs, they don't actually analyze anything. Instead, they return massive JSON blobs like this:

```python
return {
    'status': 'WORKFLOW_BLOCKED',
    'REQUIRED_CALLS': [
        'manage_aws_emr_clusters(operation="describe-cluster", ...)',
        'manage_aws_emr_ec2_instances(operation="list-instances", ...)',
        'manage_aws_emr_ec2_steps(operation="list-steps", ...)',
    ],
    'EXTRACT_THESE': [
        'cluster_runtime_hours = (EndDateTime - CreationDateTime) / 3600',
        'bootstrapping_minutes = (ReadyDateTime - CreationDateTime) / 60',
        ...
    ],
    'DATA_STRUCTURE_REQUIRED': { ... },
    'THEN_CALL': 'analyze_cluster_patterns(extracted_cluster_data=results)'
}
```

The tool is telling the agent: "I can't do this myself — here's what YOU need to do, then call me back with the results."

### Why This Is Bad

1. **Tools should do work, not delegate it back.** A tool's job is to encapsulate complexity. If the agent has to make 4 AWS calls, extract specific fields, structure the data, and call the tool again — the tool isn't providing value.

2. **3-pass calling pattern wastes tokens.** Each tool gets called 3 times: (1) returns instructions → (2) agent makes AWS calls → (3) agent calls tool again with extracted data. That's 3x the token cost and 3x the failure points.

3. **Instruction JSON is ~1000 lines of the server.** Nearly a third of the MCP server is just instruction payloads — not logic, not analysis, just prompts disguised as tool responses.

4. **It's fragile.** The agent has to follow exact instructions, extract exact fields, and structure data in an exact format. If it misses one field or structures it differently, the tool fails or returns wrong results.

### Your Task

Open `combined_emr_advisor_mcp.py` and find:
- How many lines are instruction payloads vs actual computation?
- What does the tool actually DO when it receives `extracted_cluster_data` back? (the "success" path)
- Could the tool just call AWS APIs directly instead of asking the agent to do it?

Study this and tell me what you think the fix should be. Then we'll implement it together.

### The Fix

Replaced the 3-pass instruction relay with direct boto3 calls inside the tool:

```python
# BEFORE: 140 lines returning instructions for the agent to relay
return {
    'status': 'WORKFLOW_BLOCKED',
    'REQUIRED_CALLS': ['manage_aws_emr_clusters(...)'],
    'EXTRACT_THESE': ['cluster_runtime_hours = ...'],
    'THEN_CALL': 'analyze_cluster_patterns(extracted_cluster_data=results)'
}

# AFTER: Tool does the work itself
emr_client = boto3.client('emr', region_name=region)
cluster_info = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']
steps = emr_client.list_steps(ClusterId=cluster_id)['Steps']
instances = emr_client.list_instances(ClusterId=cluster_id)['Instances']

# Feed into EMRPatternAnalyzer (previously dead code!)
pattern = unified_engine.pattern_analyzer.analyze_cluster_patterns(cluster_data)
```

**Result:**
- 140 lines of instruction JSON → ~100 lines of actual computation
- 3 tool calls → 1 tool call
- `EMRPatternAnalyzer` class is now actually used (was dead code before)
- Agent gets real analysis results, not homework assignments

**Trade-off acknowledged:** We duplicated 3 boto3 calls that the AWS DP MCP server also provides. But 3 lines of boto3 is simpler than 1000 lines of instruction relay. Sometimes a little duplication beats a complex abstraction.

---

## Issue #2: `analyze_spark_job_config` — Same Relay Pattern (Deferred)

Same 3-pass instruction relay as Issue #1, but for Spark History Server MCP. Unlike EMR (pure boto3), the Spark History Server requires either a REST URL or EMR persistent UI auth — the MCP server handles this complexity.

**Decision:** Defer this fix. The MCP relay is more justified here because:
- Spark History Server auth varies (EMR managed vs self-managed)
- The MCP server handles URL resolution and connection setup
- Direct REST calls would duplicate that auth logic

**Future fix:** Trim the ~200 lines of instruction JSON down to a minimal "call these tools and pass results back" message. Or add direct REST support when the History Server URL is known.

## Issue #3: Duplicate Scoring Logic

### The Problem

Three separate places calculated deployment recommendations with overlapping logic:

| Location | Method | Used by | Approach |
|----------|--------|---------|----------|
| 1 | `SparkJobAnalyzer._generate_serverless_recommendation()` | Event log analysis | Hardcoded, serverless-only |
| 2 | `UnifiedRecommendationEngine._calculate_unified_scores()` | `get_emr_recomm` tool | Config-driven (scoring_config.yaml), all 3 options |
| 3 | `_get_deployment_recommendations_from_job_analysis()` | `analyze_spark_job_from_event_log` tool | Hardcoded, all 3 options |

Locations 1 and 3 had nearly identical serverless scoring (duration thresholds, workload type, failure rate). Location 2 was config-driven and the most complete.

### The Fix

Created `score_deployment_options()` — a single function that accepts structured metrics and returns scored recommendations for all 3 deployment options. Locations 1 and 3 now delegate to it.

Location 2 (`_calculate_unified_scores`) kept as-is — it's config-driven with YAML-based scoring weights, which is a different (and more sophisticated) approach. It's the primary scorer for the recommendation tool.

**Result:**
- ~80 lines of duplicate hardcoded scoring → 6-line delegations to unified function
- Scoring logic changes only need to happen in one place
- Location 2's config-driven approach preserved for advanced use cases

## Issue #4: Redundant Tools Removed

### The Problem

8 tools with overlapping responsibilities:
- `get_deployment_comparison` duplicated `get_emr_recomm` with less context
- `analyze_spark_job_from_event_log` duplicated `analyze_spark_job_config`'s event log path + added redundant scoring

### The Fix

Removed both redundant tools. 8 tools → 6 tools:

```
load_knowledge_sources        → Load blogs/PDFs
search_knowledge_base         → Search loaded content
get_emr_recomm                → Step 1: Initial recommendation from description
analyze_spark_job_config      → Step 2: Extract Spark config (event log OR history server)
analyze_cluster_patterns      → Step 3: Cluster analysis (boto3 direct)
get_emr_pricing               → Step 4: Cost comparison
```

Also removed `_generate_serverless_recommendation` (biased toward serverless) and `_get_deployment_recommendations_from_job_analysis` (duplicate scorer). `SparkJobAnalyzer.analyze_spark_job()` now calls `score_deployment_options()` directly and stores unbiased `deployment_scores` in the dataclass.

## Issue #5: `get_emr_pricing` Relay Pattern

### The Problem

Same 3-pass relay as `analyze_cluster_patterns`:
1. Agent calls with spark + cluster data → tool returns ~500 lines of instructions telling agent to call AWS Pricing API 5 times
2. Agent makes 5 `use_aws` pricing calls, extracts rates
3. Agent calls tool again with `extracted_pricing_data` → tool calculates costs

Plus `extract_pricing_rates_from_cli` parsed CLI output format (`exit_status`, `stdout`) — but the agent was supposed to call it as a Python function, which it can't since it's inside the MCP server.

### The Fix

Created `fetch_pricing_rates(region)` — calls `boto3.client('pricing')` directly with 5 API calls:
- EMR Serverless (vCPU + memory rates)
- EMR on EKS (vCPU + memory rates)
- EC2 m1.small (normalized instance pricing)
- EMR EC2 uplift (service charge)
- EKS cluster (hourly rate)

Collapsed 3-path tool into 1 path: has spark + cluster data → fetch pricing → calculate costs → return results. One call.

**Result:**
- ~500 lines of instruction JSON + dead `parse_aws_pricing_response` → ~80 lines of direct computation
- 3 tool calls → 1 tool call
- `extract_pricing_rates_from_cli` (dead code) → `fetch_pricing_rates` (actually works)
- MCP server: 3619 → 2897 lines (20% reduction)

---

## Issue #6: Metrics-First Recommendation (The Real v2)

### The Problem (Mohit's Challenge)

Service team feedback: "Does it ever recommend anything other than the cheapest option? Can't a Knowledge MCP + LLM latent knowledge do this without application metrics?"

Valid point. The old flow:
1. `get_emr_recomm` parsed keywords from description → scored based on keywords
2. `get_emr_pricing` calculated costs → picked cheapest

This is no better than an LLM reading AWS docs. The scoring was keyword-based, and the final pick was just `min(cost)`.

### The Fix

**Renamed `get_emr_recomm` → `collect_workload_context`** — Step 1 no longer makes any recommendation. It collects user constraints (k8s experience, compliance, preferences) and knowledge base insights.

**Added `_build_evidence_based_recommendation`** — called at the end of `get_emr_pricing` (Step 4), combines ALL data:

| Factor | Weight | Source | Example evidence |
|--------|--------|--------|-----------------|
| Cost | 40% | Real AWS pricing API | "$0.45 on Serverless vs $15.70 on EC2" |
| Workload fit | 30% | Spark metrics + cluster patterns | "IO-intensive with 2.1GB shuffle — needs local SSDs" |
| Operational fit | 20% | Cluster analysis | "92% idle time — massive waste on persistent clusters" |
| Constraints | 10% | User input | "No k8s experience — EKS not viable" |

**The answer to Mohit:** An LLM with docs can say "EMR-S is good for batch." OrionIQ says "Your specific cluster j-ABC123 is 92% idle, your Spark job is CPU-intensive with 30-min runtime, and EMR-S costs $0.45 vs $15.70 on EC2 — 97% savings with no performance trade-off because your workload isn't IO-bound." That requires real data.

### New Tool Flow

```
collect_workload_context    → constraints + knowledge (NO scoring)
analyze_spark_job_config    → Spark metrics
analyze_cluster_patterns    → cluster metrics (boto3 direct)
get_emr_pricing             → costs + FINAL evidence-based recommendation
```

| Metric | Before | After |
|--------|--------|-------|
| MCP server lines | 3619 | 2897 |
| MCP tools | 8 | 6 |
| Orchestrator agents | 7 | 1 |
| Orchestrator lines | 901 | 180 |
| Scoring implementations | 3 (disagreeing) | 1 unified + 1 config-driven |
| Relay pattern tools | 3 | 1 (analyze_spark_job_config — deferred) |
| Dead code functions | 3 | 0 |

---

## Key Principles We're Applying

| Principle | Source | Application |
|-----------|--------|-------------|
| "Start with a single agent" | Cognition blog | Collapsed 7 agents → 1 |
| "Context engineering > architecture" | Anthropic blog | Structured context, not raw dumps |
| "Tools should encapsulate complexity" | MCP design principles | Tools do work, not return instructions |
| "Simplest pattern that works" | Multi-Agent Course | Sequential pipeline → single agent reasoning |

---

## Progress Log

| Date | Change | Impact |
|------|--------|--------|
| 2026-04-09 | Collapsed 7-agent orchestrator to single agent | 901 → 180 lines, better context management |
| 2026-04-09 | Started MCP server refactoring — Issue #1 analysis | Understanding instruction-returning anti-pattern |
| 2026-04-09 | Fixed Issue #1: `analyze_cluster_patterns` now uses direct boto3 | 140 lines instructions → ~100 lines real computation, 3 calls → 1 |
| 2026-04-09 | Fixed Issue #3: Consolidated duplicate scoring into `score_deployment_options()` | ~80 lines duplicate logic → 6-line delegations |
| 2026-04-09 | Fixed Issue #4: Removed redundant tools + serverless bias | 8 tools → 6 tools, unbiased scoring |
| 2026-04-09 | Fixed Issue #5: `get_emr_pricing` now uses direct boto3 pricing | ~500 lines instructions → ~80 lines computation, 3619 → 2897 total lines |
| 2026-04-09 | Fixed Issue #6: Metrics-first evidence-based recommendation | Keyword scoring → weighted scoring from real data (cost 40% + workload 30% + ops 20% + constraints 10%) |
| 2026-04-09 | Added Phase 1 analysis plan with user priority selection | Agent asks user priorities before analysis, adjusts scoring weights accordingly |
| 2026-04-10 | Fixed Issue #8: EMR uplift fallback rate 0.11 → 0.011 | Pricing validated against real boto3 API |
| 2026-04-10 | Fixed Issue #9: Deterministic executor count via sweep-line | 4 runs identical: EC2 $4.40, EKS $5.16, Serverless $6.94 |
| 2026-04-10 | Fixed Issue #10: Stages call timeout on large apps | with_summaries=false + list_slowest_stages(n=5) |
| 2026-04-10 | Added planning gate enforcement | collect_workload_context rejects without preferences, get_emr_pricing rejects without planning |
| 2026-04-10 | Added automated Strands test (test_deterministic.py) | Verifies determinism across multiple runs |

## Issue #7: Context Window Bloat (TODO)

### The Problem

Tool responses are too verbose — Kiro CLI hits 95% context usage during a full 4-step analysis. The main culprits:
- `collect_workload_context` returns full knowledge base search results (blog excerpts, PDF chunks)
- `analyze_spark_job_config` returns the massive instruction payload (Issue #2 deferred)
- Tool responses include debug/metadata fields the agent doesn't need

### The Fix (Anthropic Context Engineering Lesson)

In `collect_workload_context`, return knowledge insight summaries (1-2 lines each) instead of full search results with blog excerpts. Send structured summaries, not raw data dumps.

Apply the same principle to all tool responses — return only what the agent needs to make decisions, not everything the tool computed.

## Issue #8: Pricing Accuracy Validation (RESOLVED)

### The Problem

Output showed EMR on EC2 at $4.40/run, but manual calculation gave ~$15.40. Suspected agent was fabricating numbers.

### Root Cause

Our manual calculation used the wrong EMR uplift fallback rate: `$0.11` instead of the real rate `$0.011`. The tool was fetching real pricing via boto3 and returning correct numbers all along.

### Fix

- Fixed fallback rate from `0.11` to `0.011`
- Added `calculation_breakdown` embedded in cost_analysis values for transparency
- Added `IMPORTANT` instruction to use exact tool numbers

## Issue #9: Non-Deterministic Executor Count (RESOLVED)

### The Problem

Same Spark app produced different executor counts across runs: 26, 38, 16 cores/64GB (instance spec confused with Spark config). The agent interpreted Spark History data differently each time because of the relay pattern.

### Root Cause

The agent was extracting `spark_executors`, `spark_executor_cores`, and `spark_executor_memory_gb` from raw Spark History data — an LLM interpretation task that produced different results each run.

### Fix

1. **Hard gate**: `analyze_spark_job_config` rejects `extracted_spark_data` without `raw_executors` — agent MUST pass the entire executor list
2. **Sweep-line algorithm**: `_calculate_peak_concurrent_executors()` calculates peak concurrent executors from `addTime`/`removeTime` timeline — deterministic math, not LLM interpretation
3. **Server-side extraction**: Tool extracts `totalCores` and `maxMemory` from first non-driver executor — agent cannot set these
4. **Null removeTime fix**: Active executors (no removeTime) use latest removeTime as end boundary

### Verification

4 consecutive runs with same input:

| Run | EC2 | EKS | Serverless | Peak Executors |
|-----|-----|-----|------------|---------------|
| 1 | $4.40 | $5.16 | $6.94 | 29 |
| 2 | $4.40 | $5.16 | $6.94 | 29 |
| 3 | $4.40 | $5.16 | $6.94 | 29 |
| 4 | $4.40 | $5.16 | $6.94 | 29 |

## Issue #10: Stages Call Timeout on Large Apps (RESOLVED)

### The Problem

`list_stages(app_id, with_summaries=true)` caused parsing errors on apps with 280+ jobs (hundreds of stages with full metrics overwhelmed MCP transport).

### Fix

Changed instruction to use `list_stages(app_id, with_summaries=false)` for stage list and `list_slowest_stages(app_id, n=5)` for detailed metrics on key stages only.

## Automated Testing

### Deterministic Test Script

`test_deterministic.py` — runs the same prompt N times through a Strands agent with all MCP servers and compares results.

```bash
cd /Users/shoukasg/Desktop/work/git/orion-hack1/hack-151-orioniq-emr-deployment-option-recommender-v2
AWS_PROFILE=acc4-admin uv run python3 test_deterministic.py
```

The script:
1. Connects to OrionIQ MCP + Spark History MCP
2. Creates a Strands agent with all tools
3. Runs the same EMR analysis prompt N times (default: 2)
4. Extracts dollar amounts from each run
5. Compares for determinism

### Test Results

```
============================================================
  RUN 1
============================================================
  Tools: 24
  EMR Serverless: $0.55 | EMR on EC2: $4.40 | EMR on EKS: $3.69
  Recommendation: EMR Serverless (score: 83/100)

============================================================
  RUN 2
============================================================
  Tools: 24
  EMR Serverless: $0.55 | EMR on EC2: $4.40 | EMR on EKS: $3.69
  Recommendation: EMR Serverless (score: 83/100)

============================================================
  COMPARISON
============================================================
  Run 1 dollar amounts: ['$0.55', '$3.69', '$4.40', '$0.55']
  Run 2 dollar amounts: ['$0.55', '$4.40', '$3.69', '$0.55']
```

Note: Strands test used Haiku model (faster/cheaper for testing). Kiro CLI uses a larger model which produces more detailed output but same pricing numbers.

### Key Config for Test

```python
# MCP Servers
orioniq_dir = "/Users/shoukasg/Desktop/work/git/orion-hack1/hack-151-orioniq-emr-deployment-option-recommender-v2"
spark_history_dir = "/Users/shoukasg/Desktop/work/git/sparkhistory-mcp/mcp-apache-spark-history-server"

# Model (cross-region inference profile)
model_id = "us.anthropic.claude-3-5-haiku-20241022-v1:0"
region = "us-east-1"
temperature = 0.0  # deterministic

# Test data
CLUSTER_ID = "j-1TPYYUPQ7DMK8"
APP_ID = "application_1754454353720_2477"
REGION = "us-east-1"
```

### Running from Command Line

```bash
# Kill any existing MCP processes first
ps aux | grep combined_emr_advisor_mcp | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null

# Run test
cd /Users/shoukasg/Desktop/work/git/orion-hack1/hack-151-orioniq-emr-deployment-option-recommender-v2
AWS_PROFILE=acc4-admin uv run python3 test_deterministic.py

# Run the interactive agent (with conversation loop)
AWS_PROFILE=acc4-admin uv run python3 src/emr_advisor_agent_v2.py
```
