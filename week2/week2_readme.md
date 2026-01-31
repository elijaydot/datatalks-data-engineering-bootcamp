# Week 2: Data Pipeline Orchestration & Environment Configuration

> **Getting Started with Kestra | Workflow Automation | Development Environment Setup**

A comprehensive guide to setting up and configuring the development environment for data pipeline orchestration using Kestra and other data engineering tools.

---

## Table of Contents

- [Overview](#overview)
- [What We Did Today](#what-we-did-today)
- [Development Environment Setup](#development-environment-setup)
- [Key Configuration Updates](#key-configuration-updates)
- [Available Tools & Services](#available-tools--services)
- [Next Steps](#next-steps)
- [Important Notes](#important-notes)

---

## Overview

Week 2 focuses on setting up a comprehensive development environment for data pipeline orchestration. We are working with Kestra, a modern workflow orchestration platform, alongside supporting tools and services. The week involves configuring the development container, managing extensions, and preparing infrastructure for data engineering workflows.

**Focus Areas**:
- Development container configuration
- Visual Studio Code extension management
- Infrastructure port forwarding
- Credential management
- Workflow YAML configuration

---

## The Journey: How We Built This Foundation

### Chapter 1: The Extension Discovery
The journey began with a simple realization: the development container had four extensions configured, but Visual Studio Code had three additional powerful tools already installed. Rather than leaving these tools unconnected to the container configuration, we made the decision to bridge that gap.

We discovered that GitHub Copilot, GitHub Copilot Chat, and GitHub Pull Request Management were running but not persisting in the container setup. By adding these three extensions to the `.devcontainer/devcontainer.json` file, we ensured that anyone who clones this project and starts the development container will automatically get access to the same artificial intelligence-powered coding assistance and collaborative tools.

This wasn't just about having more extensions—it was about democratizing the development experience. Whether someone starts this project tomorrow or six months from now, they'll have the complete toolkit ready to go, reducing setup friction and ensuring consistency across the entire team.

### Chapter 2: The Security Checkpoint
With development tools properly configured, attention turned to one of the most critical aspects of any project: security. We identified that Google Cloud Platform credentials were living in the project repository—a potential vulnerability waiting to happen.

We added `week2/secrets/gcp-credentials.json` to the `.gitignore` file, creating an invisible shield around sensitive authentication tokens. This single line of code now prevents any accidental exposure of credentials to public repositories or unauthorized access.

This is professional-grade security practice. By preventing credentials from being committed to version control, we're following industry standards and protecting the infrastructure from unauthorized access. It's a small change that prevents potentially catastrophic security breaches.

### Chapter 3: The Documentation Renaissance
With the technical work complete, we recognized that knowledge needs to be shared and preserved. The week one folder had excellent documentation, and week two deserved the same treatment.

We created a comprehensive guide that tells the story of what week two is about—workflow orchestration with Kestra. The document flows like a conversation, guiding readers through the big picture, the practical setup, the services available, and the next steps. Instead of technical jargon, we used plain language. Instead of abbreviations, we spelled things out. Instead of cold bullet points, we created context and connection.

### Chapter 4: The Assignment Bridge
Finally, we linked the week two assignment to this guide, creating a bridge between theory and practice. We specifically called out the `week2_assignment.yaml` file as the starting point for implementing the homework, giving future developers (including ourselves) a clear entry point into the practical work.

### The Bigger Picture
What we accomplished today might seem like small changes to a few files, but together they represent a complete professional development workflow:

- **Consistency**: Developers on this project will have the same tools and experience
- **Security**: Sensitive information is protected from accidental exposure
- **Clarity**: Anyone picking up this project understands what has been done and what comes next
- **Scalability**: The foundation is set for more complex work in subsequent weeks

We transformed a partially configured workspace into a well-structured, secure, documented development environment—the kind of environment where teams can collaborate effectively and knowledge accumulates over time.

This is the kind of foundational work that separates amateur projects from professional ones. Everything is now in place. The development container is optimized, credentials are protected, and the journey through workflow orchestration is documented and ready to begin.

---

## What We Did Today

### 1. Extended Visual Studio Code Extensions

We identified and added all currently installed extensions to the development container configuration to ensure they persist across container rebuilds:

**Previously Configured Extensions:**
- Python Language Support
- Pylance (Python Language Server)
- Makefile Tools
- Remote Containers Support

**Newly Added Extensions:**
- GitHub Copilot (Artificial Intelligence pair programmer)
- GitHub Copilot Chat (Interactive coding assistant)
- GitHub Pull Request Management

These extensions are now automatically installed when the development container starts up.

### 2. Secured Sensitive Credentials

We added Google Cloud Platform credentials to the project gitignore file to prevent accidental exposure of sensitive authentication information:

```
week2/secrets/gcp-credentials.json
```

This ensures that private authentication tokens and credentials are never committed to version control, maintaining security best practices.

### 3. Project Structure Overview

The week2 directory contains workflow definitions and configuration files:

```
week2/
├── docker-compose.yaml          # Docker service orchestration
├── flows/                        # Workflow definitions
│   ├── 01_hello_world.yaml       # Simple starter workflow
│   ├── 02_python.yaml            # Python-based workflow
│   ├── 03_getting_started_data_pipeline.yaml
│   ├── 04_postgres_taxi.yaml
│   └── week2_assignment.yaml     # Assignment workflow
├── secrets/                      # Credentials directory
│   └── gcp-credentials.json      # (ignored from git)
└── week2_readme.md              # This file
```

---

## Development Environment Setup

### Container Configuration

The development environment is defined in `.devcontainer/devcontainer.json`:

**Base Image**: Ubuntu 24.04 (Microsoft Dev Containers)

**Key Features**:
- Docker-in-Docker support for running containers within the development environment
- Automatic docker-compose installation on startup
- Port forwarding for multiple services

### Port Forwarding

The development container forwards the following ports for easy access to services:

| Port | Service | Purpose |
|------|---------|---------|
| 8080 | Kestra User Interface | Web interface for workflow management |
| 8081 | Kestra Management | Administrative interface for Kestra |
| 8085 | pgAdmin | PostgreSQL database administration |
| 5432 | PostgreSQL | Database connection |

### User Configuration

- **Remote User**: Root
- **Post-Create Command**: Updates package manager and installs docker-compose

---

## Key Configuration Updates

### Extension Installation Update

The `.devcontainer/devcontainer.json` file has been updated to include all seven extensions:

```json
"customizations": {
  "vscode": {
    "extensions": [
      "ms-python.python",
      "ms-python.vscode-pylance",
      "ms-vscode.makefile-tools",
      "ms-vscode-remote.remote-containers",
      "github.copilot",
      "github.copilot-chat",
      "github.vscode-pull-request-github"
    ]
  }
}
```

### Gitignore Updates

Added security protections to `.gitignore`:

```
# Google Cloud Platform credentials
week2/secrets/gcp-credentials.json
```

---

## Available Tools & Services

### Kestra
An open-source workflow orchestration platform for building and managing data pipelines. Access via:
- **User Interface**: http://localhost:8080
- **Management Interface**: http://localhost:8081

### PostgreSQL
Relational database for storing and querying data:
- **Port**: 5432
- **Admin Tool**: pgAdmin at http://localhost:8085

### Workflow Files
The flows directory contains YAML-based workflow definitions:
- `01_hello_world.yaml` - Basic workflow example
- `02_python.yaml` - Python script execution
- `03_getting_started_data_pipeline.yaml` - Data pipeline fundamentals
- `04_postgres_taxi.yaml` - Taxi data pipeline
- `week2_assignment.yaml` - Assignment-specific workflow

---

## Next Steps

To continue working in this environment:

### 1. Rebuild the Development Container
If you want to apply the extension updates:
- Press Ctrl+Shift+P (or Cmd+Shift+P on macOS)
- Search for "Rebuild Container"
- Select and execute

### 2. Start Services
```bash
cd week2
docker-compose up -d
```

### 3. Access Services
- Kestra UI: http://localhost:8080
- pgAdmin: http://localhost:8085

### 4. Explore Workflows
Review the YAML workflow files in the `flows/` directory to understand pipeline syntax and structure.

### 5. Version Control
When ready to commit changes:
```bash
git add .devcontainer/devcontainer.json .gitignore
git commit -m "Update dev environment with extensions and security configuration"
```

---

## Important Notes

### Credential Management
- The `week2/secrets/gcp-credentials.json` file is intentionally ignored by git
- Never commit authentication tokens or private keys
- Store credentials securely outside of version control
- Use environment variables or secure credential management systems for production

### Container Persistence
- Configuration changes in `.devcontainer/devcontainer.json` require a container rebuild
- Use "Rebuild Container" command in Visual Studio Code for changes to take effect
- Extensions will be automatically installed on container startup

### Extension Updates
- GitHub Copilot requires authentication with your GitHub account
- Pylance provides advanced Python language features and type checking
- Remote Containers extension enables seamless development in containerized environments

---

## Week 2 Assignment

Complete the following assignment to practice your workflow orchestration skills:

[Data Engineering Zoomcamp - Week 2 Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/02-workflow-orchestration/homework.md)

**Assignment Workflow File**: `flows/week2_assignment.yaml`

This assignment covers practical exercises in building and deploying data pipelines using Kestra and related technologies. Use the provided workflow file as a starting point for implementing your solutions.

---

## Resources & Documentation

- [Kestra Documentation](https://kestra.io/docs)
- [Visual Studio Code Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers)
- [PostgreSQL Documentation](https://www.postgresql.org/docs)
- [Docker Compose Documentation](https://docs.docker.com/compose)

---

**Last Updated**: January 31, 2026
**Environment**: Ubuntu 24.04 LTS in Visual Studio Code Dev Container
