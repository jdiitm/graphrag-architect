DROP CONSTRAINT service_id IF EXISTS;
DROP CONSTRAINT database_id IF EXISTS;
DROP CONSTRAINT topic_name IF EXISTS;
DROP CONSTRAINT k8s_deploy_id IF EXISTS;
DROP CONSTRAINT k8s_pod_id IF EXISTS;
DROP CONSTRAINT service_tenant IF EXISTS;
DROP CONSTRAINT database_tenant IF EXISTS;
DROP CONSTRAINT topic_tenant IF EXISTS;
DROP CONSTRAINT deployment_tenant IF EXISTS;

CREATE CONSTRAINT service_tenant_id IF NOT EXISTS FOR (s:Service) REQUIRE (s.tenant_id, s.id) IS NODE KEY;
CREATE CONSTRAINT database_tenant_id IF NOT EXISTS FOR (d:Database) REQUIRE (d.tenant_id, d.id) IS NODE KEY;
CREATE CONSTRAINT topic_tenant_name IF NOT EXISTS FOR (t:KafkaTopic) REQUIRE (t.tenant_id, t.name) IS NODE KEY;
CREATE CONSTRAINT k8s_deploy_tenant_id IF NOT EXISTS FOR (k:K8sDeployment) REQUIRE (k.tenant_id, k.id) IS NODE KEY;
CREATE CONSTRAINT k8s_pod_tenant_id IF NOT EXISTS FOR (p:K8sPod) REQUIRE (p.tenant_id, p.id) IS NODE KEY;

CREATE INDEX service_tenant_idx IF NOT EXISTS FOR (s:Service) ON (s.tenant_id);
CREATE INDEX database_tenant_idx IF NOT EXISTS FOR (d:Database) ON (d.tenant_id);
CREATE INDEX topic_tenant_idx IF NOT EXISTS FOR (t:KafkaTopic) ON (t.tenant_id);
CREATE INDEX deployment_tenant_idx IF NOT EXISTS FOR (k:K8sDeployment) ON (k.tenant_id);

CREATE INDEX service_lang_idx IF NOT EXISTS FOR (s:Service) ON (s.language);
CREATE INDEX service_framework_idx IF NOT EXISTS FOR (s:Service) ON (s.framework);
CREATE FULLTEXT INDEX service_name_index IF NOT EXISTS FOR (n:Service) ON EACH [n.name];

CREATE INDEX rel_ingestion_id_idx IF NOT EXISTS FOR ()-[r:CALLS]-() ON (r.ingestion_id);
CREATE INDEX rel_produces_ingestion_id_idx IF NOT EXISTS FOR ()-[r:PRODUCES]-() ON (r.ingestion_id);
CREATE INDEX rel_consumes_ingestion_id_idx IF NOT EXISTS FOR ()-[r:CONSUMES]-() ON (r.ingestion_id);
CREATE INDEX rel_deployed_in_ingestion_id_idx IF NOT EXISTS FOR ()-[r:DEPLOYED_IN]-() ON (r.ingestion_id);
