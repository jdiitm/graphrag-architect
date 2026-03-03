import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const soakQueryDuration = new Trend("soak_query_duration", true);
const soakErrors = new Counter("soak_errors");
const memoryTrend = new Trend("memory_usage_trend", true);
const iterationCounter = new Counter("soak_iterations");

export const options = {
  thresholds: {
    soak_query_duration: [
      { threshold: "p(50)<1000", abortOnFail: false },
      { threshold: "p(99)<3000", abortOnFail: false },
    ],
    soak_errors: ["rate<0.01"],
    http_req_failed: ["rate<0.01"],
  },
  stages: [
    { duration: "5m", target: 100 },
    { duration: "24h", target: 100 },
    { duration: "5m", target: 0 },
  ],
};

const QUERIES = [
  "What is the auth-service?",
  "If the auth-service fails, which Kafka topics experience backpressure?",
  "Trace all downstream dependencies of the API gateway",
  "List all services in the production namespace",
  "Which databases does the payment-service connect to?",
  "Show the full dependency chain from ingress to database",
  "What consumer groups are affected by orders-topic outage?",
  "Describe the deployment topology of the checkout flow",
];

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const payload = {
    query: pickRandom(QUERIES),
    tenant_id: "load-test-tenant",
  };
  const params = {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /query (soak)" },
  };

  const res = http.post(`${BASE_URL}/query`, JSON.stringify(payload), params);

  soakQueryDuration.add(res.timings.duration);
  iterationCounter.add(1);

  const healthRes = http.get(`${BASE_URL}/health`, {
    tags: { name: "GET /health (soak)" },
  });

  if (healthRes.status === 200) {
    try {
      const body = JSON.parse(healthRes.body);
      if (body.memory_mb !== undefined) {
        memoryTrend.add(body.memory_mb);
      }
    } catch (_) {
      // health endpoint may not expose memory; that is acceptable
    }
  }

  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "response time under 5s": (r) => r.timings.duration < 5000,
  });

  if (!ok) {
    soakErrors.add(1);
  }

  sleep(1 + Math.random() * 2);
}
