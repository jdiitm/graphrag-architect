CREATE CONSTRAINT service_id IF NOT EXISTS FOR (s:Service) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT database_id IF NOT EXISTS FOR (d:Database) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT topic_name IF NOT EXISTS FOR (t:KafkaTopic) REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT k8s_deploy_id IF NOT EXISTS FOR (k:K8sDeployment) REQUIRE k.id IS UNIQUE;
CREATE CONSTRAINT k8s_pod_id IF NOT EXISTS FOR (p:K8sPod) REQUIRE p.id IS UNIQUE;

CREATE INDEX service_lang_idx IF NOT EXISTS FOR (s:Service) ON (s.language);
CREATE INDEX service_framework_idx IF NOT EXISTS FOR (s:Service) ON (s.framework);
CREATE FULLTEXT INDEX service_name_index IF NOT EXISTS FOR (n:Service) ON EACH [n.name];