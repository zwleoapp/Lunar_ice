# Project: Lunar Ice explorer


## Databricks Identity
- Host: https://dbc-483cf37d-fbee.cloud.databricks.com
- Profile: zwapp@protonmail.com
- Mode: Serverless (No Cluster ID required) 

## Environment
- Platform: Databricks (Unity Catalog enabled)
- Execution Tool: databricks-agent-notebooks
- Primary Language: PySpark / SQL
- The Databriack account is Free tier with UC, managed storage, limited Serverless compute for notebooks, and lake
- One SQL warehouse, limited to a 2X-Small cluster size.
- Max of 5 concurrent job tasks per account.
- Lakeflow Spark Declarative Pipelines: One active pipeline per pipeline type.
- Model serving endpoints: Limits on the number of active endpoints,No GPU serving endpoints,No provisioned throughput,No custom models on GPU or batch inference,Certain models not available
- One Vector Search endpoint, limited to one Vector Search unit. Additionally, Direct Vector Access in Vector Search is not supported.
- One Databricks App per account. Apps run for up to 24 hours after being started, updated, or redeployed. After that, the app is automatically stopped to help manage resource usage. You can restart the app at any time.
- One workspace and one metastore per account.
- No access to the account console or account-level APIs.
- No compliance enforcement, security customization, or private networking configurations. 
- Authentication is limited to email OTP, Sign in with Google, and Sign in with Microsoft. No SSO, or SCIM support.
- Databricks may delete Free Edition accounts that are inactive for a prolonged period.
- R and Scala unsupported
- As at 10 May 2026


## Rules of Engagement
- Always Haiku as subagent to explore and read, Sonnet 4.5 plan in `plan_notes.md` before writing production code.
- Always Haiku to put down details as why code created or changed in `code_study_notes.md` after codes are written down.
- All logic execution and workspace interactions must be performed via the agent-notebook CLI using --profile zwapp@protonmail.com --serverless
- Always include `--profile zwapp@protonmail.com` and `--serverless` in commands.
- Always write down `logic_notes.md` for dataflow and logic calculation.
- Prefer Markdown-based notebooks (.md) for persistence.


## Standards
- Follow Medallion Architecture (Bronze -> Silver -> Gold).
- All tables must have a 'created_by_agent' tag.
- Always separate config and scripts, ensure no hardcoded in logic scripts or UI view or Input
- Always keep codes lean no more than 300 lines in each script file

## Project folder structure
Lunar_ice/
├── .runs/               # Execution artifacts & logs
├── config/              # targets.yaml, calibration_constants.yaml
├── notes/               # The "Project Brain"
│   ├── logic_notes.md   # Mathematical formulas (RPI)
│   ├── plan_notes.md    # Strategy & Medallion roadmap
│   ├── code_study_notes.md # Agent rationale & chronicling
│   └── workspace_status.txt # Connectivity & environment logs
├── src/                 # Executable Notebooks/Scripts
│   ├── smoke_test.md
│   └── bronze_ingestion.md
├── CLAUDE.md            # System instructions
└── action_v0.01.md      # Active directive

## Resilience & Persistence
- **Source of Truth:** Local files are the Master. Never treat the Databricks Workspace as permanent storage.
- **Git Sync:** Before ending a session, ensure all changed files in `/src`, `/config`, and `/notes` are staged for Git.
- **Data Safety:** Gold-layer results must be exported to local `.csv` periodically to mitigate Free-Tier deletion risks.