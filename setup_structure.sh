#!/bin/bash

# Create the root directory
mkdir -p identity-risk-analytics-platform
cd identity-risk-analytics-platform

# Top-level files
touch README.md LICENSE CONTRIBUTING.md CHANGELOG.md

# Docs
mkdir -p docs
touch docs/architecture.md docs/patent_formulas.md docs/api_guide.md docs/developer_guide.md

# Global config
mkdir -p config
touch config/config.dev.yml config/config.test.yml config/config.prod.yml

# Tenants
mkdir -p tenants/tenant_A/data tenants/tenant_B/data
touch tenants/tenant_A/config.yml tenants/tenant_B/config.yml

# Services
mkdir -p services

## Discovery Service
mkdir -p services/discovery-service/{config,src/connectors/saas_connectors/plugins/saas,tests}
touch services/discovery-service/Dockerfile services/discovery-service/requirements.txt
touch services/discovery-service/config/discovery_config.yml
touch services/discovery-service/src/main.py
touch services/discovery-service/src/kafka_producer.py
touch services/discovery-service/src/connectors/__init__.py
touch services/discovery-service/src/connectors/ad_connector.py
touch services/discovery-service/src/connectors/aws_connector.py
touch services/discovery-service/src/connectors/gcp_connector.py

# SaaS Connectors for Identity Data
touch services/discovery-service/src/connectors/saas_connectors/__init__.py
touch services/discovery-service/src/connectors/saas_connectors/generic_saas_connector.py
touch services/discovery-service/src/connectors/saas_connectors/saas_connector_orchestrator.py
touch services/discovery-service/src/connectors/saas_connectors/plugins/saas/m365_connector.py
touch services/discovery-service/src/connectors/saas_connectors/plugins/saas/github_connector.py
touch services/discovery-service/src/connectors/saas_connectors/plugins/saas/servicenow_connector.py
touch services/discovery-service/src/connectors/saas_connectors/plugins/saas/salesforce_connector.py

# Tests for Discovery Service
touch services/discovery-service/tests/test_ad_connector.py
touch services/discovery-service/tests/test_aws_connector.py
touch services/discovery-service/tests/test_gcp_connector.py
touch services/discovery-service/tests/test_saas_connectors.py

## Access Log Service
mkdir -p services/access-log-service/{config,src/connectors/saas_access_log_connectors,tests}
touch services/access-log-service/Dockerfile services/access-log-service/requirements.txt
touch services/access-log-service/config/access_log_config.yml
touch services/access-log-service/src/main.py
touch services/access-log-service/src/kafka_producer.py
touch services/access-log-service/src/connectors/__init__.py
touch services/access-log-service/src/connectors/ad_access_log_connector.py
touch services/access-log-service/src/connectors/aws_access_log_connector.py
touch services/access-log-service/src/connectors/gcp_access_log_connector.py

# SaaS Access Log Connectors
mkdir -p services/access-log-service/src/connectors/saas_access_log_connectors/plugins/saas_access_logs
touch services/access-log-service/src/connectors/saas_access_log_connectors/__init__.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/generic_saas_access_log_connector.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/saas_access_log_orchestrator.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/plugins/saas_access_logs/m365_access_log_connector.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/plugins/saas_access_logs/github_access_log_connector.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/plugins/saas_access_logs/servicenow_access_log_connector.py
touch services/access-log-service/src/connectors/saas_access_log_connectors/plugins/saas_access_logs/salesforce_access_log_connector.py

# Tests for Access Log Service
touch services/access-log-service/tests/test_ad_access_log_connector.py
touch services/access-log-service/tests/test_saas_access_log_orchestrator.py

## Normalization Service
mkdir -p services/normalization-service/{config,src/models,src/processing,tests}
touch services/normalization-service/Dockerfile services/normalization-service/requirements.txt
touch services/normalization-service/config/normalization_config.yml
touch services/normalization-service/src/main.py
touch services/normalization-service/src/kafka_consumer.py
touch services/normalization-service/src/output_to_kafka.py
touch services/normalization-service/src/config.py
# AI Models for Normalization
touch services/normalization-service/src/models/identity_correlation_model.py
touch services/normalization-service/src/models/privilege_overlap_model.py
touch services/normalization-service/src/models/temporal_sequence_anomaly.py
touch services/normalization-service/src/models/log_event_correlation.py
touch services/normalization-service/src/models/lateral_movement_prediction.py
# Processing functions
touch services/normalization-service/src/processing/normalize_identity.py
touch services/normalization-service/src/processing/normalize_privilege.py
touch services/normalization-service/src/processing/normalize_resource.py
# Tests for Normalization
touch services/normalization-service/tests/test_normalize_identity.py
touch services/normalization-service/tests/test_models.py

## Graph-Correlation Service
mkdir -p services/graph-correlation-service/{src/{schema,queries},tests}
touch services/graph-correlation-service/Dockerfile services/graph-correlation-service/requirements.txt
touch services/graph-correlation-service/src/main.py
touch services/graph-correlation-service/src/neo4j_consumer.py
touch services/graph-correlation-service/src/neo4j_service.py
# Schema files
touch services/graph-correlation-service/src/schema/identity_schema.cypher
touch services/graph-correlation-service/src/schema/privilege_schema.cypher
# Query files
touch services/graph-correlation-service/src/queries/lateral_movement_query.cypher
touch services/graph-correlation-service/src/queries/correlation_query.cypher
# Tests for Graph Service
touch services/graph-correlation-service/tests/test_neo4j_consumer.py

## Risk-Scoring-Analytics Service
mkdir -p services/risk-scoring-analytics-service/{config,src/{models,query_engine},tests}
touch services/risk-scoring-analytics-service/Dockerfile services/risk-scoring-analytics-service/requirements.txt
touch services/risk-scoring-analytics-service/config/risk_scoring_config.yml
touch services/risk-scoring-analytics-service/src/main.py
# AI models for risk scoring
touch services/risk-scoring-analytics-service/src/models/risk_score_model.py
touch services/risk-scoring-analytics-service/src/models/anomaly_detection_model.py
# Scoring engine and federated query engine
touch services/risk-scoring-analytics-service/src/scoring_engine.py
mkdir -p services/risk-scoring-analytics-service/src/query_engine
touch services/risk-scoring-analytics-service/src/query_engine/federated_query_engine.py
touch services/risk-scoring-analytics-service/src/config.py
# Tests for Risk Scoring
touch services/risk-scoring-analytics-service/tests/test_scoring_engine.py

## Alerting & API Service
mkdir -p services/alerting-api-service/{src/{api,alerting},tests}
touch services/alerting-api-service/Dockerfile services/alerting-api-service/requirements.txt
touch services/alerting-api-service/src/main.py
# API endpoints
mkdir -p services/alerting-api-service/src/api
touch services/alerting-api-service/src/api/__init__.py
touch services/alerting-api-service/src/api/identity_api.py
touch services/alerting-api-service/src/api/alert_api.py
touch services/alerting-api-service/src/api/query_api.py
# Alerting logic
mkdir -p services/alerting-api-service/src/alerting/channels
touch services/alerting-api-service/src/alerting/alert_manager.py
touch services/alerting-api-service/src/alerting/channels/email_alert.py
touch services/alerting-api-service/src/alerting/channels/siem_alert.py
touch services/alerting-api-service/src/alerting/rules.py
touch services/alerting-api-service/src/config.py
# Tests for Alerting API Service
touch services/alerting-api-service/tests/test_identity_api.py
touch services/alerting-api-service/tests/test_alert_manager.py

## Common Services
mkdir -p services/common-services/{logging-service,auth-service,utils}
# Logging Service
mkdir -p services/common-services/logging-service/src
touch services/common-services/logging-service/Dockerfile
touch services/common-services/logging-service/src/logger.py
touch services/common-services/logging-service/src/log_formatter.py
mkdir -p services/common-services/logging-service/tests
touch services/common-services/logging-service/tests/test_logger.py
# Auth Service
mkdir -p services/common-services/auth-service/src
touch services/common-services/auth-service/Dockerfile
touch services/common-services/auth-service/requirements.txt
touch services/common-services/auth-service/src/auth.py
touch services/common-services/auth-service/src/rbac.py
mkdir -p services/common-services/auth-service/tests
touch services/common-services/auth-service/tests/test_auth.py
# Utils
mkdir -p services/common-services/utils
touch services/common-services/utils/config_loader.py
touch services/common-services/utils/validators.py
touch services/common-services/utils/helpers.py

## Front-End Placeholder
mkdir -p front-end/{public,src}
touch front-end/Dockerfile
touch front-end/package.json
touch front-end/public/index.html
touch front-end/src/README.md

## Deployments
mkdir -p deployments/{k8s,ci-cd}
touch deployments/docker-compose.yml
touch deployments/k8s/deployment.yaml
touch deployments/k8s/service.yaml
touch deployments/k8s/ingress.yaml
touch deployments/ci-cd/github-actions.yml
touch deployments/ci-cd/Jenkinsfile

## Tests (Top-level)
mkdir -p tests/{integration,performance}
touch tests/integration/test_full_pipeline.py
touch tests/performance/load_test.py

## .gitignore
touch .gitignore
