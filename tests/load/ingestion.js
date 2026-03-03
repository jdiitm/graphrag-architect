import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const ingestionErrors = new Counter("ingestion_errors");
const ingestionDuration = new Trend("ingestion_duration", true);

export const options = {
  thresholds: {
    http_req_duration: [
      { threshold: "p(50)<500", abortOnFail: false },
      { threshold: "p(99)<2000", abortOnFail: true },
    ],
    ingestion_errors: ["count<10"],
  },
  stages: [
    { duration: "1m", target: 10 },
    { duration: "5m", target: 50 },
    { duration: "2m", target: 100 },
    { duration: "5m", target: 50 },
    { duration: "1m", target: 0 },
  ],
};

const PAYLOADS = {
  small: {
    source_type: "k8s_manifest",
    content: "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: test-svc",
    tenant_id: "load-test-tenant",
  },
  medium: {
    source_type: "source_code",
    content: "func main() {\n" + "  fmt.Println(\"hello\")\n".repeat(100) + "}",
    tenant_id: "load-test-tenant",
  },
  large: {
    source_type: "kafka_config",
    content: JSON.stringify({
      topics: Array.from({ length: 50 }, (_, i) => ({
        name: `topic-${i}`,
        partitions: 12,
        replication_factor: 3,
      })),
    }),
    tenant_id: "load-test-tenant",
  },
};

function pickPayload() {
  const roll = Math.random();
  if (roll < 0.6) return PAYLOADS.small;
  if (roll < 0.9) return PAYLOADS.medium;
  return PAYLOADS.large;
}

export default function () {
  const payload = pickPayload();
  const params = {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /ingest" },
  };

  const res = http.post(`${BASE_URL}/ingest`, JSON.stringify(payload), params);

  ingestionDuration.add(res.timings.duration);

  const ok = check(res, {
    "status is 200 or 202": (r) => r.status === 200 || r.status === 202,
    "response has job_id": (r) => {
      try {
        return JSON.parse(r.body).job_id !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!ok) {
    ingestionErrors.add(1);
  }

  sleep(0.1 + Math.random() * 0.4);
}
