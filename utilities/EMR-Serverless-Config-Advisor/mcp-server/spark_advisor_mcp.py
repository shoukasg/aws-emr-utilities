#!/usr/bin/env python3
"""
Spark Config Advisor MCP Server
Submits analysis jobs to AWS Batch and returns recommendations.
"""

import json
import time
import boto3
from mcp.server.fastmcp import FastMCP

REGION = "us-east-1"
JOB_QUEUE = "spark-advisor-queue"
JOB_DEFINITION = "spark-config-advisor"
OUTPUT_BUCKET = "suthan-event-logs"
RESULTS_PREFIX = "mcp-results"

mcp = FastMCP("spark-config-advisor")


def _get_clients():
    return boto3.client("batch", region_name=REGION), boto3.client("s3", region_name=REGION)


def _wait_for_job(batch, job_id, timeout=3600):
    """Poll Batch job until terminal state."""
    start = time.time()
    while time.time() - start < timeout:
        job = batch.describe_jobs(jobs=[job_id])["jobs"][0]
        status = job["status"]
        if status in ("SUCCEEDED", "FAILED"):
            return job
        time.sleep(15)
    return {"status": "TIMEOUT", "jobId": job_id}


@mcp.tool()
def analyze_spark_logs(
    input_path: str,
    mode: str = "cost-optimized",
    limit: int = 100,
) -> str:
    """Analyze Spark event logs from S3 and generate EMR Serverless configuration recommendations.

    Runs a high-memory AWS Batch job (256 GB RAM) to process Spark event logs
    and produce optimized worker sizing, executor counts, and Spark configs.

    Args:
        input_path: S3 path to event logs (e.g. s3://bucket/prefix/)
        mode: 'cost-optimized' (fewer resources, lower cost) or 'performance-optimized' (more resources, faster)
        limit: Max applications to analyze (default 100)

    Returns:
        JSON with recommendations per application including worker type, executor counts, and full Spark configs.
    """
    batch, s3 = _get_clients()

    if not input_path.startswith("s3://"):
        return json.dumps({"error": "input_path must be an S3 path (s3://bucket/prefix/)"})

    parts = input_path.replace("s3://", "").split("/", 1)
    input_bucket = parts[0]
    input_prefix = parts[1] if len(parts) > 1 else ""

    run_id = str(int(time.time()))
    staging_prefix = f"{RESULTS_PREFIX}/{run_id}/staging/"
    output_file = f"/tmp/recs_{run_id}.json"

    # The pipeline writes local files. We add a post-processing step in the
    # container command to upload results to S3.
    cost_file = output_file.replace(".json", "_cost.json")
    perf_file = output_file.replace(".json", "_perf.json")
    s3_cost_key = f"{RESULTS_PREFIX}/{run_id}/recommendations_cost.json"
    s3_perf_key = f"{RESULTS_PREFIX}/{run_id}/recommendations_perf.json"

    # Build shell command: run pipeline then upload results to S3
    pipeline_cmd = (
        f"python3 pipeline_wrapper.py"
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

    upload_cmd = (
        f"aws s3 cp {cost_file} s3://{OUTPUT_BUCKET}/{s3_cost_key} 2>/dev/null;"
        f" aws s3 cp {perf_file} s3://{OUTPUT_BUCKET}/{s3_perf_key} 2>/dev/null"
    )

    full_cmd = f"{pipeline_cmd} && {upload_cmd}"

    response = batch.submit_job(
        jobName=f"spark-advisor-{run_id}",
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        containerOverrides={
            "command": ["bash", "-c", full_cmd],
        },
    )
    job_id = response["jobId"]

    # Poll for completion
    job = _wait_for_job(batch, job_id)

    if job["status"] != "SUCCEEDED":
        reason = job.get("statusReason", "unknown")
        return json.dumps({"error": f"Job {job['status']}: {reason}", "job_id": job_id})

    # Fetch results
    target_key = s3_cost_key if mode == "cost-optimized" else s3_perf_key
    try:
        obj = s3.get_object(Bucket=OUTPUT_BUCKET, Key=target_key)
        results = json.loads(obj["Body"].read().decode("utf-8"))
        return json.dumps({
            "job_id": job_id,
            "mode": mode,
            "application_count": len(results),
            "recommendations": results,
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch results: {str(e)}", "job_id": job_id})


@mcp.tool()
def get_job_status(job_id: str) -> str:
    """Check the status of a Spark Config Advisor batch job.

    Args:
        job_id: AWS Batch job ID.

    Returns:
        Job status and timing details.
    """
    batch, _ = _get_clients()
    job = batch.describe_jobs(jobs=[job_id])["jobs"][0]
    result = {
        "job_id": job_id,
        "status": job["status"],
        "status_reason": job.get("statusReason", ""),
    }
    for field in ("createdAt", "startedAt", "stoppedAt"):
        if field in job:
            result[field] = str(job[field])
    return json.dumps(result, indent=2)


@mcp.tool()
def list_event_log_prefixes(bucket: str = "suthan-event-logs", prefix: str = "") -> str:
    """List available Spark event log application prefixes in S3.

    Args:
        bucket: S3 bucket name (default: suthan-event-logs)
        prefix: S3 prefix to search under

    Returns:
        List of application prefixes found.
    """
    _, s3 = _get_clients()
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.add(cp["Prefix"])
    return json.dumps(sorted(prefixes), indent=2)


if __name__ == "__main__":
    mcp.run()
