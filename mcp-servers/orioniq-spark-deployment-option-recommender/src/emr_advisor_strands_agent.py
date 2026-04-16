#!/usr/bin/env python3
"""
EMR Deployment Options Advisor - Merged Implementation
Combines the robust error handling and chunked analysis from main-v1.py 
with the structured 6-step workflow from emr_advisor_strands_agent.py
"""

import logging
import boto3
import os
import time
import json
import re
import yaml
from datetime import datetime
from contextlib import ExitStack
from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
from mcp import stdio_client, StdioServerParameters
from strands_tools import http_request, file_write, file_read, editor

# Configure beautiful console logging
import sys

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🚨'
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        emoji = self.EMOJIS.get(record.levelname, '📝')
        reset = self.COLORS['RESET']
        
        # Simplify logger names
        name = record.name
        if 'strands' in name:
            name = name.replace('strands.', '').replace('multiagent.', '')
        if 'awslabs' in name:
            name = 'AWS-MCP'
        if 'mcp.server' in name:
            name = 'MCP'
            
        formatted = f"{emoji} {color}{record.levelname}{reset} | {name} | {record.getMessage()}"
        return formatted

# Set up beautiful logging
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColoredFormatter())

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[console_handler],
    format="%(message)s"
)

# Load configuration
def load_config(config_path="config.yaml"):
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        print(f"✅ Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file {config_path} not found. Using default values.")
        return {}
    except yaml.YAMLError as e:
        print(f"❌ Error parsing YAML configuration: {e}")
        return {}

def print_config_summary():
    """Print a summary of loaded configuration"""
    print_section("⚙️ Configuration Summary", "⚙️")
    
    aws_config = CONFIG.get('aws', {})
    llm_config = CONFIG.get('llm', {})
    paths_config = CONFIG.get('paths', {})
    
    print_step("AWS", f"Region: {aws_config.get('region', 'us-west-2')}", "🌍")
    print_step("AWS", f"Cluster ID: {aws_config.get('emr_cluster_id', 'j-2G6747K4S23GY')}", "🔗")
    print_step("LLM", f"Model: {llm_config.get('model_id', 'anthropic.claude-3-5-sonnet-20241022-v2:0').split('.')[-1]}", "🤖")
    print_step("LLM", f"Temperature: {llm_config.get('temperature', 0.1)}, Max Tokens: {llm_config.get('max_tokens', 4000)}", "⚙️")
    print_step("Paths", f"Output: {paths_config.get('output_directory', 'output')}", "📁")
    
    # Show configured knowledge sources
    knowledge_sources = CONFIG.get('knowledge_sources', {})
    blog_count = len(knowledge_sources.get('blogs', []))
    pdf_count = len(knowledge_sources.get('pdfs', []))
    print_step("Knowledge", f"Blogs: {blog_count}, PDFs: {pdf_count}", "📚")

# Load global configuration
CONFIG = load_config()

# Suppress noisy loggers
log_level = getattr(logging, CONFIG.get('app', {}).get('log_level', 'INFO').upper())
mcp_log_level = getattr(logging, CONFIG.get('app', {}).get('mcp_log_level', 'INFO').upper())

logging.getLogger("strands").setLevel(logging.WARNING)
logging.getLogger("strands.multiagent").setLevel(logging.WARNING)
logging.getLogger("mcp").setLevel(mcp_log_level)
logging.getLogger("awslabs").setLevel(mcp_log_level)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def run_agent_with_retry(agent, prompt, max_retries=None, initial_delay=None):
    """Run individual agent with retry logic for throttling and other recoverable errors"""
    if max_retries is None:
        max_retries = CONFIG.get('app', {}).get('max_retries', 3)
    if initial_delay is None:
        initial_delay = CONFIG.get('app', {}).get('initial_retry_delay', 10)
    delay = initial_delay
    
    for attempt in range(max_retries):
        try:
            print(f"  Attempt {attempt + 1}/{max_retries}: Running {agent.name if hasattr(agent, 'name') else 'agent'}...")
            return agent(prompt)
        except Exception as e:
            error_str = str(e).lower()
            
            # Check for recoverable errors including max_tokens and timeouts
            recoverable_errors = [
                "throttling", "too many requests", "rate limit", "service unavailable",
                "timeout", "connection", "temporary", "retry", "503", "429", "502",
                "max_tokens", "maxtokensreachedexception", "token limit", "read timed out",
                "awshttpsconnectionpool", "bedrock-runtime"
            ]
            
            is_recoverable = any(error_type in error_str for error_type in recoverable_errors)
            
            # Special handling for max_tokens errors
            if "max_tokens" in error_str or "maxtokensreachedexception" in error_str:
                print(f"  ⚠️  Token limit reached - this indicates the agent tried to process too much data")
                print(f"  💡 Consider using chunked data fetching strategy")
                # Don't retry max_tokens errors - they need different approach
                raise e
            
            if is_recoverable and attempt < max_retries - 1:
                print(f"  ⚠️  Recoverable error: {str(e)[:100]}...")
                print(f"  ⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                continue
            else:
                print(f"  ❌ Non-recoverable error or max retries reached: {str(e)}")
                raise e
    
    return None

def print_banner():
    """Print a beautiful banner"""
    banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                    🚀 AWS EMR Deployment Analyzer 🚀                        ║
║                                                                              ║
║              Powered by 6-Step AI Workflow & Knowledge Validation           ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_section(title, emoji="📋"):
    """Print a beautiful section header"""
    print(f"\n{emoji} {title}")
    print("─" * (len(title) + 4))

def print_step(step, description, status="⏳"):
    """Print a step with status"""
    print(f"{status} {step}: {description}")

def get_tool_name_safe(tool) -> str:
    """Safely extract tool name from MCP tool object"""
    # Try different possible attribute names in order of preference
    name_attributes = [
        'name',           # Standard name attribute
        'function_name',  # MCPAgentTool likely uses this
        '_name',          # Private name attribute
        '__name__',       # Python built-in name
        'tool_name',      # Alternative naming
        'id',             # Some tools might use id
        'identifier'      # Another possible identifier
    ]
    
    for attr in name_attributes:
        if hasattr(tool, attr):
            value = getattr(tool, attr)
            if value and isinstance(value, str):
                return value
    
    # Fallback to string representation
    tool_str = str(tool)
    
    # Try to extract name from string representation
    import re
    patterns = [
        r"function_name='([^']+)'",
        r"name='([^']+)'",
        r"'([^']+)'",
        r"\(([^)]+)\)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, tool_str)
        if match:
            return match.group(1)
    
    # Final fallback - use the string representation itself
    return tool_str

def diagnose_knowledge_base_issues(emr_tools):
    """Diagnose and report knowledge base connectivity issues - FIXED VERSION"""
    print_step("Debug", "Diagnosing knowledge base connectivity...", "🔍")
    
    try:
        # Check available EMR tools - FIXED: Use safe name extraction
        tool_names = []
        for tool in emr_tools:
            tool_name = get_tool_name_safe(tool)
            tool_names.append(tool_name)
        
        print(f"  Available EMR tools: {', '.join(tool_names)}")
        
        # Check for knowledge-related tools
        knowledge_tools = [name for name in tool_names if 'knowledge' in name.lower() or 'search' in name.lower()]
        if knowledge_tools:
            print(f"  ✅ Knowledge tools found: {', '.join(knowledge_tools)}")
        else:
            print(f"  ⚠️  No knowledge tools found in EMR MCP server")
            
        return knowledge_tools
    except Exception as e:
        print(f"  ❌ Error diagnosing knowledge base: {str(e)}")
        return []

def update_agent_findings(agent_name: str, step: str, findings, status: str = "✅", filename: str = 'analysis_findings.md'):
    """Update the analysis findings markdown file with raw agent results"""
    try:
        # Extract text content from AgentResult if needed
        if hasattr(findings, 'content'):
            findings_text = findings.content
        elif hasattr(findings, 'text'):
            findings_text = findings.text
        elif hasattr(findings, '__str__'):
            findings_text = str(findings)
        else:
            findings_text = findings
        
        # Read current content
        with open(filename, 'r') as f:
            content = f.read()
        
        # Find the agent section
        section_header = f"## {agent_name}"
        
        if section_header in content:
            # Replace existing content
            lines = content.split('\n')
            start_idx = None
            end_idx = None
            
            for i, line in enumerate(lines):
                if line.strip() == section_header:
                    start_idx = i
                elif start_idx is not None and line.startswith('## ') and line.strip() != section_header:
                    end_idx = i
                    break
            
            if start_idx is not None:
                if end_idx is None:
                    end_idx = len(lines)
                
                # Replace the section content with raw findings dump
                new_lines = lines[:start_idx+1] + ['', findings_text, ''] + lines[end_idx:]
                content = '\n'.join(new_lines)
        else:
            # Add new section at the end with raw findings dump
            content += f"\n\n{section_header}\n\n{findings_text}\n"
        
        # Write back to file
        with open(filename, 'w') as f:
            f.write(content)
            
        print(f"✓ Raw findings updated for {agent_name} (step {step})")
        
    except Exception as e:
        print(f"Error updating findings for {agent_name}: {e}")

def initialize_analysis_findings(user_request, filename):
    """Initialize the analysis findings markdown file with fresh template data"""
    try:
        # Extract cluster ID and region from user request if available
        cluster_match = re.search(r'\[([j-][A-Z0-9]+)\]', user_request)
        region_match = re.search(r'\[([a-z]+-[a-z]+-\d+)\]', user_request)
        
        cluster_id = cluster_match.group(1) if cluster_match else "To be determined"
        region = region_match.group(1) if region_match else "To be determined"
        
        # Create fresh template content - simple format
        fresh_content = f"""# EMR Deployment Analysis Findings - 6-Step Workflow

This document contains the collaborative findings from the 6-step EMR analysis workflow.

## Step 0: Knowledge Loading Agent

*Agent findings will be updated here automatically*

## Step 1: Workload Analysis Agent

*Agent findings will be updated here automatically*

## Step 2: Spark Configuration Analysis Agent

*Agent findings will be updated here automatically*

## Step 3: Cluster Pattern Analysis Agent

*Agent findings will be updated here automatically*

## Step 4: Pricing Analysis Agent

*Agent findings will be updated here automatically*

## Step 4.5: Knowledge Validation Agent

*Agent findings will be updated here automatically*

## Step 5: Markdown Report Generation Agent

*Agent findings will be updated here automatically*"""
        
        # Write the fresh content to the timestamped file
        with open(filename, 'w') as f:
            f.write(fresh_content)
        
        print_step("0", f"Analysis findings document created: {filename}", "✅")
    except Exception as e:
        print_step("0", f"Failed to initialize analysis findings: {str(e)}", "⚠️")

def main():
    print_banner()
    
    # Print configuration summary
    print_config_summary()
    
    print_section("🔧 Initialization", "🔧")
    
    # Set environment variable to bypass tool consent for automatic file operations
    bypass_consent = CONFIG.get('mcp_servers', {}).get('bypass_tool_consent', True)
    if bypass_consent:
        os.environ['BYPASS_TOOL_CONSENT'] = 'true'
        print_step("1", "Auto-approval enabled for file operations", "✅")
    else:
        print_step("1", "Manual approval required for file operations", "⚠️")
    
    # Clear expired AWS environment variables to use default profile
    aws_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']
    cleared_vars = []
    for var in aws_env_vars:
        if var in os.environ:
            del os.environ[var]
            cleared_vars.append(var)
    
    if cleared_vars:
        print_step("2", f"Cleared expired AWS environment variables: {', '.join(cleared_vars)}", "✅")
    else:
        print_step("2", "Using default AWS credentials profile", "✅")
    
    try:
        print_section("🔌 MCP Server Connections", "🔌")
        
        # Initialize EMR Advisor MCP Client
        print_step("1", "Initializing EMR Advisor MCP Server...", "⏳")
        uv_command = CONFIG.get('mcp_servers', {}).get('uv_command', 'uv')
        mcp_log_level = CONFIG.get('app', {}).get('mcp_log_level', 'INFO')
        
        # Build environment variables for MCP server configuration
        mcp_env = {
            "FASTMCP_LOG_LEVEL": mcp_log_level,
            # AWS Configuration
            "AWS_REGION": CONFIG.get('aws', {}).get('region', 'us-west-2'),
            "EMR_CLUSTER_ID": CONFIG.get('aws', {}).get('emr_cluster_id', ''),
            "EMR_CLUSTER_ARN": CONFIG.get('aws', {}).get('emr_cluster_arn', ''),
            # LLM Configuration
            "LLM_MODEL_ID": CONFIG.get('llm', {}).get('model_id', 'anthropic.claude-3-5-sonnet-20241022-v2:0'),
            "LLM_TEMPERATURE": str(CONFIG.get('llm', {}).get('temperature', 0.1)),
            "LLM_MAX_TOKENS": str(CONFIG.get('llm', {}).get('max_tokens', 4000)),
            # App Configuration
            "LOG_LEVEL": CONFIG.get('app', {}).get('log_level', 'INFO'),
            "MAX_RETRIES": str(CONFIG.get('app', {}).get('max_retries', 3)),
            # Semantic Search Configuration
            "SEMANTIC_MODEL_NAME": CONFIG.get('semantic_search', {}).get('model_name', 'all-MiniLM-L6-v2'),
            "SIMILARITY_THRESHOLD": str(CONFIG.get('semantic_search', {}).get('similarity_threshold', 0.1))
        }
        
        # Add knowledge sources as JSON if configured
        knowledge_sources = CONFIG.get('knowledge_sources', {})
        if knowledge_sources.get('blogs'):
            import json
            mcp_env["KNOWLEDGE_BLOGS_JSON"] = json.dumps(knowledge_sources['blogs'])
        if knowledge_sources.get('pdfs'):
            import json
            mcp_env["KNOWLEDGE_PDFS_JSON"] = json.dumps(knowledge_sources['pdfs'])
        
        # Optional: Pass custom config file path
        config_path = CONFIG.get('mcp_servers', {}).get('config_path')
        if config_path:
            mcp_env["EMR_ADVISOR_CONFIG_PATH"] = config_path
        
        emr_advisor_mcp_client = MCPClient(
            lambda: stdio_client(StdioServerParameters(
                command=uv_command,
                args=[
                    "run",
                    "src/combined_emr_advisor_mcp.py"
                ],
                env=mcp_env
            ))
        )
        
        # Initialize AWS Data Processing MCP Client
        print_step("2", "Initializing AWS Data Processing MCP Server...", "⏳")
        uvx_command = CONFIG.get('mcp_servers', {}).get('uvx_command', 'uvx')
        aws_dp_package = CONFIG.get('mcp_servers', {}).get('aws_dataprocessing_package', 'awslabs.aws-dataprocessing-mcp-server@latest')
        aws_region = CONFIG.get('aws', {}).get('region', 'us-west-2')
        allow_write = CONFIG.get('mcp_servers', {}).get('allow_write', True)
        
        args = [aws_dp_package]
        if allow_write:
            args.append("--allow-write")
            
        aws_dataprocessing_mcp_client = MCPClient(
            lambda: stdio_client(StdioServerParameters(
                command=uvx_command,
                args=args,
                env={
                    "FASTMCP_LOG_LEVEL": mcp_log_level,
                    "AWS_REGION": aws_region
                }
            ))
        )
        
        # Initialize AWS Pricing MCP Client
        print_step("3", "Initializing AWS Pricing MCP Server...", "⏳")
        aws_pricing_package = CONFIG.get('mcp_servers', {}).get('aws_pricing_package', 'awslabs.aws-pricing-mcp-server@latest')
        
        aws_pricing_mcp_client = MCPClient(
            lambda: stdio_client(StdioServerParameters(
                command=uvx_command,
                args=[aws_pricing_package],
                env={"FASTMCP_LOG_LEVEL": mcp_log_level}
            ))
        )
        
        # Initialize Spark History Server MCP (required)
        print_step("4", "Initializing Spark History Server MCP...", "⏳")
        try:
            spark_mcp_path = CONFIG.get('spark_history_mcp', {}).get('path', '/Users/syedair/Documents/All Docs/AWS_Work/Development/mcp-apache-spark-history-server')
            startup_timeout = CONFIG.get('spark_history_mcp', {}).get('startup_timeout', 120)
            emr_cluster_arn = CONFIG.get('aws', {}).get('emr_cluster_arn', 'arn:aws:elasticmapreduce:us-west-2:964888361164:cluster/j-2G6747K4S23GY')
            
            spark_history_mcp_client = MCPClient(
                lambda: stdio_client(StdioServerParameters(
                    command=uv_command,
                    args=[
                        "--directory",
                        spark_mcp_path,
                        "run",
                        "src/spark_history_mcp/core/main.py"
                    ],
                    env={
                        "FASTMCP_LOG_LEVEL": mcp_log_level,
                        "SHS_SERVERS_EMR_EMRCLUSTERARN": emr_cluster_arn,
                        "SHS_MCP_TRANSPORT": "stdio",
                        "SHS_SERVERS_EMR_DEFAULT": "true"
                    }
                )),
                startup_timeout=startup_timeout
            )
            print_step("4", "Spark History Server MCP initialized successfully", "✅")
        except Exception as e:
            print_step("4", f"Spark History Server MCP failed: {str(e)[:100]}...", "❌")
            raise e
        
        
        
        # print_step("4", "Initializing Spark History Server MCP...", "⏳")
        # try:
        #     spark_history_mcp_client = MCPClient(
        #         lambda: stdio_client(StdioServerParameters(
        #             command="uvx",
        #             args=[
        #                 "--from",
        #                 "mcp-apache-spark-history-server",
        #                 "spark-mcp"
        #             ],
        #             env={
        #                 "FASTMCP_LOG_LEVEL": "INFO"
        #                 # "SHS_SERVERS_EMR_EMR_CLUSTER_ARN": "arn:aws:elasticmapreduce:us-west-2:964888361164:cluster/j-2G6747K4S23GY",
        #                 # "SHS_MCP_TRANSPORT": "stdio",
        #                 # "SHS_SERVERS_EMR_DEFAULT": "true"
        #             }
        #         )),
        #         startup_timeout=120  # Increase timeout to 2 minutes for EMR Persistent UI startup
        #     )
        #     print_step("4", "Spark History Server MCP initialized successfully", "✅")
        # except Exception as e:
        #     print_step("4", f"Spark History Server MCP failed: {str(e)[:100]}...", "❌")
        #     raise e
        

        # Model setup - Using configuration values
        llm_config = CONFIG.get('llm', {})
        model = BedrockModel(
            model_id=llm_config.get('model_id', 'anthropic.claude-3-5-sonnet-20241022-v2:0'),
            region_name=llm_config.get('region', 'us-west-2'),
            temperature=llm_config.get('temperature', 0.1),
            max_tokens=llm_config.get('max_tokens', 4000)
        )

        print_section("🛠️ Tool Discovery", "🛠️")
        
        # Prepare MCP clients list
        clients_to_use = [emr_advisor_mcp_client, aws_dataprocessing_mcp_client, aws_pricing_mcp_client]
        if spark_history_mcp_client:
            clients_to_use.append(spark_history_mcp_client)
        
        # Keep ALL MCP clients open during agent execution
        with ExitStack() as stack:
            for client in clients_to_use:
                stack.enter_context(client)
            
            # Get tools from all MCP servers
            print_step("1", "Discovering EMR Advisor tools...", "⏳")
            emr_tools = emr_advisor_mcp_client.list_tools_sync()
            print_step("1", f"Found {len(emr_tools)} EMR Advisor tools", "✅")
            
            print_step("2", "Discovering AWS Data Processing tools...", "⏳")
            aws_dp_tools = aws_dataprocessing_mcp_client.list_tools_sync()
            print_step("2", f"Found {len(aws_dp_tools)} AWS Data Processing tools", "✅")
            
            print_step("3", "Discovering AWS Pricing tools...", "⏳")
            pricing_tools = aws_pricing_mcp_client.list_tools_sync()
            print_step("3", f"Found {len(pricing_tools)} AWS Pricing tools", "✅")
            
            spark_tools = []
            if spark_history_mcp_client:
                print_step("4", "Discovering Spark History Server tools...", "⏳")
                spark_tools = spark_history_mcp_client.list_tools_sync()
                print_step("4", f"Found {len(spark_tools)} Spark History Server tools", "✅")

            print_section("📝 Analysis Request", "📝")
            
            # Get user input
            user_input = input("\n🔍 Please describe your EMR analysis request\n   (e.g., 'Analyze my EMR clusters [j-2G6747K4S23GY] in [us-west-2]. My application id is: application_1756285766062_0001 and recommend the right deployment option'): ")
            
            if not user_input.strip():
                default_cluster_id = CONFIG.get('aws', {}).get('emr_cluster_id', 'j-2G6747K4S23GY')
                default_region = CONFIG.get('aws', {}).get('region', 'us-west-2').upper()
                default_app_id = CONFIG.get('app', {}).get('default_application_id', 'application_1756285766062_0001')
                
                user_input = f"""
                Below are my inputs for EMR analysis:
                - EMR cluster IDs: {default_cluster_id}
                - Kubernetes experience: NO
                - Region: {default_region}
                - APP_ID: {default_app_id}
                Analyze the cluster and recommend which deployment options we should use.
                """
                print_step("1", f"Using default analysis request", "✅")
            else:
                print_step("1", f"Custom analysis request received", "✅")

            print_section("🤖 6-Step Agent Configuration", "🤖")
            
            # Create output directory and timestamped filenames for this analysis run
            timestamp_format = CONFIG.get('app', {}).get('timestamp_format', '%Y%m%d_%H%M%S')
            timestamp = datetime.now().strftime(timestamp_format)
            
            output_dir = CONFIG.get('paths', {}).get('output_directory', 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            findings_pattern = CONFIG.get('app', {}).get('findings_filename_pattern', 'analysis_findings_{timestamp}.md')
            report_pattern = CONFIG.get('app', {}).get('report_filename_pattern', 'EMR_Analysis_Report_{timestamp}.md')
            
            findings_filename = os.path.join(output_dir, findings_pattern.format(timestamp=timestamp))
            markdown_report_filename = os.path.join(output_dir, report_pattern.format(timestamp=timestamp))
            
            # STEP 0 AGENT: Knowledge Loading (CRITICAL REQUIREMENT)
            knowledge_loader_agent = Agent(
                name="knowledge_loader",
                model=model,
                system_prompt=(
                    "You are a knowledge loading specialist. "
                    "CRITICAL: Use load_knowledge_sources() tool to load curated blogs and PDFs. "
                    "This knowledge will be used to validate recommendations in later steps. "
                    "Ensure all knowledge sources are loaded successfully before proceeding."
                ),
                tools=[http_request] + emr_tools
            )
            
            # STEP 1 AGENT: Initial Workload Analysis
            knowledge_based_advisor_agent = Agent(
                name="workload_analyzer",
                model=model,
                system_prompt=(
                    "You are a Step 1 EMR workload analysis specialist. "
                    "CRITICAL: Use get_emr_recomm() tool to perform initial workload analysis with knowledge validation. "
                    "Extract workload characteristics, team expertise, and generate initial deployment scores. "
                    "Pass cluster_ids, spark application ids and/or event_log_paths for later steps. "
                    "This is STEP 1 of 6 - ensure next step is analyze_spark_job_config."
                ),
                tools=[http_request] + emr_tools
            )
            
            # STEP 2 AGENT: Spark Configuration Analysis  
            spark_configs_analyzer_agent = Agent(
                name="spark_config_analyzer",
                model=model,
                system_prompt=(
                    "You are a Step 2 Spark configuration analysis specialist. "
                    "CRITICAL: Use analyze_spark_job_config() tool to extract Spark job parameters. "
                    "The EMR advisor instructs to use Spark History Server MCP you should use it pull the required metrics from the different spark applications provided by user. "
                    "Extract executor config, timing data, and workload characterization. "
                    "This is STEP 2 of 6 - ensure next step is analyze_cluster_patterns."
                ),
                tools=[http_request, file_read] + emr_tools + spark_tools
            )
            
            # STEP 3 AGENT: Cluster Pattern Analysis
            cluster_pattern_analyzer_agent = Agent(
                name="cluster_pattern_analyzer",
                model=model,
                system_prompt=(
                    "You are a Step 3 cluster analysis specialist. "
                    "CRITICAL: Use analyze_cluster_patterns() from EMR advisor MCP AND AWS DP MCP tools directly. "
                    "When EMR advisor instructs to use AWS DP MCP, call manage_aws_emr_clusters, manage_aws_emr_ec2_instances, manage_aws_emr_ec2_steps directly. "
                    "Extract timing data, spot usage, autoscaling efficiency, and utilization metrics. "
                    "This is STEP 3 of 6 - ensure next step is get_emr_pricing."
                ),
                tools=[http_request] + emr_tools + aws_dp_tools + spark_tools
            )
            
            # STEP 4 AGENT: Pricing Analysis
            pricing_analyzer_agent = Agent(
                name="pricing_analyzer",
                model=model,
                system_prompt=(
                    "You are a Step 4 pricing analysis specialist. "
                    "CRITICAL: Use get_emr_pricing() from EMR advisor MCP AND AWS Pricing MCP tools directly. "
                    "When EMR advisor instructs to use AWS Pricing MCP, call get_pricing tools directly. "
                    "Calculate EMR Serverless, EMR on EC2, and EMR on EKS costs with spot/autoscaling considerations. "
                    "This is STEP 4 of 6 - ensure next step is knowledge validation."
                ),
                tools=[http_request] + emr_tools + pricing_tools
            )
            
            # STEP 4.5 AGENT: Knowledge Validation (Enhanced)
            validation_agent = Agent(
                name="knowledge_validator",
                model=model,
                system_prompt=(
                    "You are a knowledge validation specialist. "
                    "CRITICAL: First try search_knowledge_base() tool. If that fails, use alternative validation methods: "
                    "1. Use load_knowledge_sources() to re-check available sources "
                    "2. Use http_request to fetch AWS documentation directly "
                    "3. Validate against known AWS best practices for EMR deployments "
                    "4. Cross-reference with AWS pricing and performance guidelines "
                    "If search fails, provide validation based on AWS official documentation patterns. "
                    "Always provide specific recommendations even if direct quotes aren't available."
                ),
                tools=[http_request] + emr_tools
            )
            
            # STEP 5 AGENT: Markdown Report Generation with Enhanced Knowledge Validation
            markdown_report_agent = Agent(
                name="markdown_report_generator",
                model=model,
                system_prompt=(
                    "You are a comprehensive markdown report generation specialist with enhanced knowledge validation. "
                    "CRITICAL: Create a well-structured, professional markdown report that consolidates all analysis findings. "
                    "KNOWLEDGE VALIDATION STRATEGY: "
                    "1. First try search_knowledge_base() tool for specific quotes "
                    "2. If search fails, use http_request to fetch current AWS documentation "
                    "3. Include AWS best practices from official sources "
                    "4. Reference AWS pricing documentation and performance guides "
                    "5. If no direct quotes available, clearly state validation methodology used "
                    "ALWAYS include an Expert Knowledge section with validation details. "
                    
                    "REPORT STRUCTURE REQUIREMENTS: "
                    "1. Executive Summary with key findings and primary recommendation "
                    "2. Current State Analysis (cluster config, utilization, costs) "
                    "3. Spark Application Analysis (performance metrics, resource usage) "
                    "4. Deployment Options Comparison (EMR Serverless vs EC2 vs EKS) "
                    "5. Cost Analysis with detailed breakdowns and savings potential "
                    "6. Expert Knowledge Validation with quotes from knowledge base "
                    "7. Implementation Roadmap with specific next steps "
                    "8. Risk Assessment and mitigation strategies "
                    
                    "FORMATTING REQUIREMENTS: "
                    "- Use proper markdown headers (##, ###, ####) "
                    "- Include tables for cost comparisons and metrics "
                    "- Use bullet points and numbered lists for clarity "
                    "- Add code blocks for configuration examples "
                    "- Include badges/indicators for recommendations (🟢 Recommended, 🟡 Alternative, 🔴 Not Recommended) "
                    "- Use emojis and visual indicators for better readability "
                    
                    f"Save the comprehensive report to {markdown_report_filename} "
                    "Include all metrics, costs, recommendations, and workflow details from the 6-step analysis. "
                    "This is STEP 5 of 6 - final comprehensive report generation."
                ),
                tools=[file_read, file_write] + emr_tools
            )
            
            print_section("🚀 6-Step Sequential Workflow Execution", "🚀")
            
            # Initialize the analysis findings document
            initialize_analysis_findings(user_input, findings_filename)
            
            print("\n" + "="*60)
            print("EXECUTING 6-STEP EMR ANALYSIS WORKFLOW WITH KNOWLEDGE VALIDATION")
            print("="*60)
            
            # STEP 0: Load Knowledge Sources with Diagnostics (CRITICAL REQUIREMENT)
            print_step("0", "Knowledge Loading: Diagnosing and loading knowledge sources...", "⏳")
            
            # Diagnose knowledge base connectivity first
            available_knowledge_tools = diagnose_knowledge_base_issues(emr_tools)
            
            try:
                knowledge_response = run_agent_with_retry(
                    knowledge_loader_agent,
                    f"CRITICAL: Load all curated knowledge sources using load_knowledge_sources() tool. "
                    f"Available knowledge tools: {available_knowledge_tools}. "
                    "This includes blogs and PDFs that will validate EMR deployment recommendations. "
                    "If load_knowledge_sources fails, try alternative approaches to gather AWS EMR documentation."
                )
                knowledge_findings = str(knowledge_response) if knowledge_response else "Knowledge loading failed"
                print_step("0", "Knowledge Loading completed successfully", "✅")
                update_agent_findings("Step 0: Knowledge Loading Agent", "0", knowledge_findings, "✅", findings_filename)
            except Exception as e:
                print_step("0", f"Knowledge Loading failed: {str(e)[:100]}...", "❌")
                knowledge_findings = f"Knowledge loading failed: {str(e)}. Available tools: {available_knowledge_tools}"
                update_agent_findings("Step 0: Knowledge Loading Agent", "0", knowledge_findings, "❌ Failed", findings_filename)
            
            # STEP 1: Initial Workload Analysis
            print_step("1", "Workload Analysis: Analyzing workload with knowledge validation...", "⏳")
            try:
                step1_response = run_agent_with_retry(
                    knowledge_based_advisor_agent,
                    f"Execute STEP 1 analysis for: {user_input}. "
                    f"Knowledge sources loaded: {knowledge_findings}. "
                    "Use get_emr_recomm() tool with workload_description, cluster_ids, has_kubernetes_experience=True."
                )
                step1_findings = str(step1_response) if step1_response else "Workload analysis failed"
                print_step("1", "Workload Analysis completed successfully", "✅")
                update_agent_findings("Step 1: Workload Analysis Agent", "1", step1_findings, "✅", findings_filename)
            except Exception as e:
                print_step("1", f"Workload Analysis failed: {str(e)[:100]}...", "❌")
                step1_findings = f"Workload analysis failed: {str(e)}"
                update_agent_findings("Step 1: Workload Analysis Agent", "1", step1_findings, "❌ Failed", findings_filename)
            
            # STEP 2: Spark Configuration Analysis
            print_step("2", "Spark Config Analysis: Analyzing Spark job configurations...", "⏳")
            try:
                step2_response = run_agent_with_retry(
                    spark_configs_analyzer_agent,
                    f"Execute STEP 2 analysis. "
                    f"CONTEXT FROM STEP 1: {step1_findings}. "
                    f"Original request: {user_input}. "
                    "Use analyze_spark_job_config() tool with event_log_paths or application_ids from Step 1."
                )
                step2_findings = str(step2_response) if step2_response else "Spark config analysis failed"
                print_step("2", "Spark Config Analysis completed successfully", "✅")
                update_agent_findings("Step 2: Spark Configuration Analysis Agent", "2", step2_findings, "✅", findings_filename)
            except Exception as e:
                print_step("2", f"Spark Config Analysis failed: {str(e)[:100]}...", "❌")
                step2_findings = f"Spark config analysis failed: {str(e)}"
                update_agent_findings("Step 2: Spark Configuration Analysis Agent", "2", step2_findings, "❌ Failed", findings_filename)
            
            # STEP 3: Cluster Pattern Analysis
            print_step("3", "Cluster Pattern Analysis: Analyzing real cluster data...", "⏳")
            try:
                step3_response = run_agent_with_retry(
                    cluster_pattern_analyzer_agent,
                    f"Execute STEP 3 analysis. "
                    f"CONTEXT FROM STEP 1: {step1_findings}. "
                    f"CONTEXT FROM STEP 2: {step2_findings}. "
                    f"Original request: {user_input}. "
                    "Use analyze_cluster_patterns() AND AWS DP MCP tools for real cluster data extraction."
                )
                step3_findings = str(step3_response) if step3_response else "Cluster pattern analysis failed"
                print_step("3", "Cluster Pattern Analysis completed successfully", "✅")
                update_agent_findings("Step 3: Cluster Pattern Analysis Agent", "3", step3_findings, "✅", findings_filename)
            except Exception as e:
                print_step("3", f"Cluster Pattern Analysis failed: {str(e)[:100]}...", "❌")
                step3_findings = f"Cluster pattern analysis failed: {str(e)}"
                update_agent_findings("Step 3: Cluster Pattern Analysis Agent", "3", step3_findings, "❌ Failed", findings_filename)
            
            # STEP 4: Comprehensive Pricing Analysis
            print_step("4", "Pricing Analysis: Calculating costs with real-time pricing...", "⏳")
            try:
                step4_response = run_agent_with_retry(
                    pricing_analyzer_agent,
                    f"Execute STEP 4 final analysis. "
                    f"CONTEXT FROM STEP 1: {step1_findings}. "
                    f"CONTEXT FROM STEP 2: {step2_findings}. "
                    f"CONTEXT FROM STEP 3: {step3_findings}. "
                    f"Original request: {user_input}. "
                    "Use get_emr_pricing() for resource requirements, then AWS Pricing MCP for real-time costs."
                )
                step4_findings = str(step4_response) if step4_response else "Pricing analysis failed"
                print_step("4", "Pricing Analysis completed successfully", "✅")
                update_agent_findings("Step 4: Pricing Analysis Agent", "4", step4_findings, "✅", findings_filename)
            except Exception as e:
                print_step("4", f"Pricing Analysis failed: {str(e)[:100]}...", "❌")
                step4_findings = f"Pricing analysis failed: {str(e)}"
                update_agent_findings("Step 4: Pricing Analysis Agent", "4", step4_findings, "❌ Failed", findings_filename)
            
            # STEP 4.5: Knowledge Validation (EXPLICIT VALIDATION STEP)
            print_step("4.5", "Knowledge Validation: Validating recommendations against knowledge base...", "⏳")
            try:
                validation_response = run_agent_with_retry(
                    validation_agent,
                    f"CRITICAL: Validate the EMR deployment recommendations against loaded knowledge base. "
                    f"RECOMMENDATIONS TO VALIDATE: {step4_findings}. "
                    f"Use search_knowledge_base() tool to find supporting evidence from curated blogs and PDFs. "
                    f"Provide specific quotes and citations that support or contradict the recommendations."
                )
                validation_findings = str(validation_response) if validation_response else "Knowledge validation failed"
                print_step("4.5", "Knowledge Validation completed successfully", "✅")
                update_agent_findings("Step 4.5: Knowledge Validation Agent", "4.5", validation_findings, "✅", findings_filename)
            except Exception as e:
                print_step("4.5", f"Knowledge Validation failed: {str(e)[:100]}...", "❌")
                validation_findings = f"Knowledge validation failed: {str(e)}"
                update_agent_findings("Step 4.5: Knowledge Validation Agent", "4.5", validation_findings, "❌ Failed", findings_filename)
            
            # STEP 5: Markdown Report Generation with All Context
            print_step("5", "Markdown Report Generation: Creating comprehensive report...", "⏳")
            try:
                markdown_report = run_agent_with_retry(
                    markdown_report_agent,
                    f"CRITICAL: Create comprehensive markdown report consolidating all analysis findings\n\n"
                    f"COMPLETE WORKFLOW CONTEXT:\n"
                    f"KNOWLEDGE SOURCES LOADED: {knowledge_findings}\n\n"
                    f"STEP 1 - WORKLOAD ANALYSIS: {step1_findings}\n\n"
                    f"STEP 2 - SPARK CONFIG ANALYSIS: {step2_findings}\n\n" 
                    f"STEP 3 - CLUSTER ANALYSIS: {step3_findings}\n\n"
                    f"STEP 4 - PRICING ANALYSIS: {step4_findings}\n\n"
                    f"STEP 4.5 - KNOWLEDGE VALIDATION: {validation_findings}\n\n"
                    f"ORIGINAL REQUEST: {user_input}\n\n"
                    f"INSTRUCTIONS:\n"
                    f"1. Create a comprehensive markdown report with professional formatting\n"
                    f"2. Use ALL context from steps 0-4.5 to populate report sections\n"
                    f"3. Include validated quotes from knowledge base in Expert Knowledge section\n"
                    f"4. Follow the structured format specified in system prompt\n"
                    f"5. Save to {markdown_report_filename}\n"
                    f"6. Include timestamp: {timestamp}\n"
                    f"7. Use tables, bullet points, and visual indicators for clarity\n"
                    f"8. Highlight key recommendations with appropriate badges/emojis"
                )
                if markdown_report:
                    print_step("5", "Markdown Report Generation completed successfully", "✅")
                    update_agent_findings("Step 5: Markdown Report Generation Agent", "5", str(markdown_report), "✅", findings_filename)
                else:
                    print_step("5", "Markdown Report Generation failed", "❌")
                    update_agent_findings("Step 5: Markdown Report Generation Agent", "5", "Markdown report generation failed", "❌ Failed", findings_filename)
            except Exception as e:
                print_step("5", f"Markdown Report Generation failed: {str(e)[:100]}...", "❌")
                update_agent_findings("Step 5: Markdown Report Generation Agent", "5", f"Error: {str(e)}", "❌ Failed", findings_filename)
            
            print_section("🎉 6-Step Analysis Complete", "🎉")
            print("✅ 6-Step workflow execution completed!")
            print(f"📋 Check '{findings_filename}' for detailed agent findings")
            print(f"📄 Check '{markdown_report_filename}' for the comprehensive markdown report")
            print(f"📁 All outputs saved in '{output_dir}/' directory")

    except Exception as e:
        print(f"\n❌ Critical error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()