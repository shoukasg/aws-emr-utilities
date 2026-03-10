#!/usr/bin/env python3
"""
Spark Config Advisor MCP Server
Runs analysis on a remote EC2 instance via SSH and returns recommendations.
"""

import json
import subprocess
import time
from mcp.server.fastmcp import FastMCP

EC2_HOST = "hadoop@ec2-3-91-248-12.compute-1.amazonaws.com"
SSH_KEY = "~/suthan-bda.pem"
SSH_CMD = f"ssh -i {SSH_KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10 {EC2_HOST}"

mcp = FastMCP("spark-config-advisor")


def _ssh(cmd, timeout=900):
    """Run command on EC2 via SSH."""
    result = subprocess.run(
        f'{SSH_CMD} \'{cmd}\'',
        shell=True, capture_output=True, text=True, timeout=timeout
    )
    return result.stdout, result.stderr, result.returncode


@mcp.tool()
def analyze_spark_logs(
    input_path: str,
    mode: str = "cost-optimized",
    limit: int = 100,
) -> str:
    """Analyze Spark event logs from S3 and generate EMR Serverless configuration recommendations.

    Runs on a high-memory EC2 instance (747 GB RAM) to process Spark event logs
    and produce optimized worker sizing, executor counts, and Spark configs.

    Args:
        input_path: S3 path to event logs (e.g. s3://bucket/prefix/)
        mode: 'cost-optimized' (fewer resources, lower cost) or 'performance-optimized' (more resources, faster)
        limit: Max applications to analyze (default 100)

    Returns:
        JSON with recommendations per application including worker type, executor counts, and full Spark configs.
    """
    if not input_path.startswith("s3://"):
        return json.dumps({"error": "input_path must be an S3 path (s3://bucket/prefix/)"})

    parts = input_path.replace("s3://", "").split("/", 1)
    input_bucket = parts[0]
    input_prefix = parts[1] if len(parts) > 1 else ""

    run_id = str(int(time.time()))
    staging_prefix = f"mcp-staging/{run_id}/"
    output_file = f"/tmp/recs_{run_id}.json"

    # Build pipeline command
    pipeline_cmd = (
        f"cd ~ && python3 pipeline_wrapper.py"
        f" --input-bucket {input_bucket}"
        f" --input-prefix {input_prefix}"
        f" --staging-prefix {staging_prefix}"
        f" --output {output_file}"
        f" --limit {limit}"
    )
    if mode == "cost-optimized":
        pipeline_cmd += " --cost-optimized"
    elif mode == "performance-optimized":
        pipeline_cmd += " --performance-optimized"

    # Run pipeline
    stdout, stderr, rc = _ssh(pipeline_cmd)

    if rc != 0:
        return json.dumps({"error": f"Pipeline failed (exit {rc})", "stderr": stderr[-2000:], "stdout": stdout[-2000:]})

    # Read results
    cost_file = output_file.replace(".json", "_cost.json")
    perf_file = output_file.replace(".json", "_perf.json")
    target = cost_file if mode == "cost-optimized" else perf_file

    out, err, rc = _ssh(f"cat {target}")
    if rc != 0:
        # Try the other file
        out, err, rc = _ssh(f"cat {cost_file} 2>/dev/null || cat {perf_file}")

    if rc != 0:
        return json.dumps({"error": "Could not read results", "stderr": err})

    try:
        results = json.loads(out)
        return json.dumps({
            "mode": mode,
            "application_count": len(results),
            "recommendations": results,
        }, indent=2)
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON in results", "raw": out[:2000]})


@mcp.tool()
def list_event_log_prefixes(bucket: str = "suthan-event-logs", prefix: str = "") -> str:
    """List available Spark event log application prefixes in S3.

    Args:
        bucket: S3 bucket name (default: suthan-event-logs)
        prefix: S3 prefix to search under

    Returns:
        List of application prefixes found.
    """
    out, _, rc = _ssh(f"aws s3 ls s3://{bucket}/{prefix} --region us-east-1")
    if rc != 0:
        return json.dumps({"error": "Failed to list S3"})
    prefixes = [line.strip().split()[-1] for line in out.strip().split("\n") if line.strip().startswith("PRE")]
    return json.dumps(prefixes, indent=2)


if __name__ == "__main__":
    mcp.run()
