import http from "k6/http";
import { check, sleep, group } from "k6";
import { Counter, Trend } from "k6/metrics";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const vectorQueryDuration = new Trend("vector_query_duration", true);
const graphQueryDuration = new Trend("graph_query_duration", true);
const queryErrors = new Counter("query_errors");

export const options = {
  thresholds: {
    vector_query_duration: [
      { threshold: "p(50)<200", abortOnFail: false },
      { threshold: "p(99)<500", abortOnFail: true },
    ],
    graph_query_duration: [
      { threshold: "p(50)<1000", abortOnFail: false },
      { threshold: "p(99)<3000", abortOnFail: true },
    ],
    query_errors: ["count<5"],
  },
  stages: [
    { duration: "30s", target: 10 },
    { duration: "3m", target: 50 },
    { duration: "5m", target: 100 },
    { duration: "3m", target: 50 },
    { duration: "30s", target: 0 },
  ],
};

const VECTOR_QUERIES = [
  "What is the auth-service?",
  "Describe the payment-gateway deployment",
  "Which databases does the user-service connect to?",
  "List all Kafka topics",
  "What namespace is the order-service deployed in?",
];

const GRAPH_QUERIES = [
  "If the auth-service fails, which Kafka topics experience backpressure?",
  "Trace all downstream dependencies of the API gateway",
  "What is the blast radius if the PostgreSQL primary goes down?",
  "Which services consume from the orders-topic and what are their failure modes?",
  "Show the full dependency chain from ingress to database for the checkout flow",
];

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const params = {
    headers: { "Content-Type": "application/json" },
  };

  group("vector_query", function () {
    const payload = {
      query: pickRandom(VECTOR_QUERIES),
      tenant_id: "load-test-tenant",
      strategy: "vector",
    };
    const res = http.post(
      `${BASE_URL}/query`,
      JSON.stringify(payload),
      Object.assign({}, params, { tags: { name: "POST /query (vector)" } })
    );

    vectorQueryDuration.add(res.timings.duration);

    const ok = check(res, {
      "vector status 200": (r) => r.status === 200,
      "vector has answer": (r) => {
        try {
          return JSON.parse(r.body).answer !== undefined;
        } catch (_) {
          return false;
        }
      },
    });
    if (!ok) queryErrors.add(1);
  });

  sleep(0.5 + Math.random());

  group("graph_query", function () {
    const payload = {
      query: pickRandom(GRAPH_QUERIES),
      tenant_id: "load-test-tenant",
      strategy: "hybrid",
    };
    const res = http.post(
      `${BASE_URL}/query`,
      JSON.stringify(payload),
      Object.assign({}, params, { tags: { name: "POST /query (graph)" } })
    );

    graphQueryDuration.add(res.timings.duration);

    const ok = check(res, {
      "graph status 200": (r) => r.status === 200,
      "graph has answer": (r) => {
        try {
          return JSON.parse(r.body).answer !== undefined;
        } catch (_) {
          return false;
        }
      },
    });
    if (!ok) queryErrors.add(1);
  });

  sleep(0.5 + Math.random());
}
