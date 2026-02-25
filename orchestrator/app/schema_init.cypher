CREATE CONSTRAINT service_id IF NOT EXISTS FOR (s:Service) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT database_id IF NOT EXISTS FOR (d:Database) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT topic_name IF NOT EXISTS FOR (t:KafkaTopic) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT k8s_deploy_id IF NOT EXISTS FOR (k:K8sDeployment) REQUIRE k.id IS UNIQUE;
CREATE CONSTRAINT k8s_pod_id IF NOT EXISTS FOR (p:K8sPod) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT service_tenant IF NOT EXISTS FOR (s:Service) REQUIRE s.tenant_id IS NOT NULL;
CREATE CONSTRAINT database_tenant IF NOT EXISTS FOR (d:Database) REQUIRE d.tenant_id IS NOT NULL;
CREATE CONSTRAINT topic_tenant IF NOT EXISTS FOR (t:KafkaTopic) REQUIRE t.tenant_id IS NOT NULL;
CREATE CONSTRAINT deployment_tenant IF NOT EXISTS FOR (k:K8sDeployment) REQUIRE k.tenant_id IS NOT NULL;

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