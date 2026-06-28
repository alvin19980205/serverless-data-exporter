# ☁️ Serverless Data Exporter (Snowflake ➡️ Google Sheets)

An AWS Lambda-based backend service that manages the secure extraction, transformation, and automated delivery of data from a Data Warehouse into Google Sheets.

### 🏗️ Architecture & Technical Highlights
* **Memory Management:** Utilizes Snowpark's `to_local_iterator()` to stream SQL query results row-by-row, preventing Out-Of-Memory (OOM) errors in AWS Lambda when processing large datasets.
* **Pre-flight Validation:** Dynamically compiles the query and calculates cell constraints *before* fetching data, preventing the job from hitting Google Sheet's 10-million cell hard limit mid-execution.
* **Cloud Security:** Authenticates to Snowflake and Google Workspace securely using API keys retrieved via **AWS Secrets Manager** at runtime. No hardcoded credentials.
* **Event-Driven Orchestration:** Handles both synchronous API Gateway calls (for user UI triggers) and asynchronous EventBridge Cron events (for scheduled, recurring reports).

*Note: Proprietary database schemas, AWS account numbers, and Snowflake instances have been parameterized to environment variables for this public repository.*

## 📦 Deployment
Designed to be containerized using Docker and deployed to AWS ECR/Lambda, or packaged via AWS SAM/Serverless Framework.