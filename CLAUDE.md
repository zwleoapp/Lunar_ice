# Project: Lunar Ice explorer

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

## Rule of enegaement
- refer to notes/playbook.md

## Standards
- Follow Medallion Architecture (Bronze -> Silver -> Gold).
- All tables must have a 'created_by_agent' tag.
- Always separate config and scripts, ensure no hardcoded in logic scripts or UI view or Input
- Always keep codes lean no more than 300 lines in each script file

