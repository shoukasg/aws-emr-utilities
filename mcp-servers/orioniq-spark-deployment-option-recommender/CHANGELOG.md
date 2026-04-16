# Changelog

All notable changes to OrionIQ will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of OrionIQ EMR Deployment Advisor
- Multi-source intelligence combining live data and expert knowledge
- Custom EMR Serverless cost estimator
- Integration with AWS Data Processing MCP server
- Spark History Server MCP integration
- Curated knowledge base with AWS best practices
- 6-step AI workflow for comprehensive analysis

### Features
- **Live Cluster Analysis**: Analyze existing EMR clusters for usage patterns
- **Event Log Processing**: Deep-dive Spark event log analysis for precise cost calculations
- **Expert Knowledge Integration**: Curated AWS best practices and case studies
- **Multi-Dimensional Comparison**: Side-by-side analysis of all EMR deployment options
- **Production-Validated Insights**: Real-world case studies with quantified cost savings

### Technical
- MCP (Model Context Protocol) architecture
- FastMCP server implementation
- Strands AI agent framework integration
- Comprehensive configuration management
- Robust error handling and retry logic

## [0.1.0] - 2024-08-28

### Added
- Initial project structure
- Core EMR advisor MCP server
- Basic cost estimation capabilities
- Configuration management system
- Documentation and examples

### Fixed
- MCPAgentTool attribute error in diagnose_knowledge_base_issues function
- Tool name extraction for MCP agent tools

### Security
- Added .gitignore to protect sensitive configuration files
- AWS credentials handling best practices

---

## Release Notes

### Version 0.1.0 - Initial Release

This is the first release of OrionIQ, providing intelligent EMR deployment recommendations through:

- **Data-Driven Analysis**: Leverages actual production cluster data and Spark event logs
- **Expert Knowledge**: Integrates curated AWS best practices and case studies  
- **Accurate Cost Estimation**: Custom estimator for precise cost projections across deployment options
- **Actionable Insights**: Specific, implementable recommendations with clear business impact

#### Key Capabilities
- Analyze EMR clusters for resource utilization and cost optimization
- Process Spark event logs for workload characterization
- Generate comprehensive deployment recommendations
- Provide cost comparisons between EMR-EC2, EMR-EKS, and EMR Serverless
- Validate recommendations against expert knowledge base

#### Supported Deployment Options
- **EMR on EC2**: Traditional deployment with full control
- **EMR on EKS**: Kubernetes-native with superior spot instance management
- **EMR Serverless**: Minimal operations with pay-per-use pricing
- **AWS Glue**: Fully managed serverless option

#### Getting Started
See the [README.md](README.md) for installation and usage instructions.

#### Known Limitations
- Requires AWS CLI configuration and appropriate permissions
- MCP server dependencies must be properly configured
- Limited to supported AWS regions

#### Feedback and Support
Please report issues and provide feedback through GitHub Issues.