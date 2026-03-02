import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const vectorQueryDuration = new Trend("vector_query_duration", true);
const graphQueryDuration = new Trend("graph_query_duration", true);
const queryFailRate = new Rate("query_failures");

export const options = {
  scenarios: {
    vector_queries: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 20 },
        { duration: "2m", target: 50 },
        { duration: "2m", target: 100 },
        { duration: "1m", target: 50 },
        { duration: "30s", target: 0 },
      ],
      exec: "vectorQuery",
    },
    graph_queries: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 10 },
        { duration: "2m", target: 25 },
        { duration: "2m", target: 50 },
        { duration: "1m", target: 25 },
        { duration: "30s", target: 0 },
      ],
      exec: "graphQuery",
    },
  },
  thresholds: {
    vector_query_duration: [
      "p(50)<200",
      "p(99)<500",
    ],
    graph_query_duration: [
      "p(50)<1000",
      "p(99)<3000",
    ],
    query_failures: ["rate<0.01"],
  },
};

const VECTOR_QUERIES = [
  "What is the auth-service?",
  "Describe the payment gateway deployment",
  "What database does the user service connect to?",
  "List all Kafka topics",
  "What is the ingestion worker?",
];

const GRAPH_QUERIES = [
  "If the auth-service fails, which Kafka topics experience backpressure?",
  "What is the blast radius of a Redis outage?",
  "Trace the data flow from API gateway to Neo4j",
  "Which services are affected if kafka-broker-0 goes down?",
  "Show all services that transitively depend on the user database",
];

function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

const params = {
  headers: { "Content-Type": "application/json" },
};

export function vectorQuery() {
  const query = randomFrom(VECTOR_QUERIES);
  const payload = JSON.stringify({ query: query, max_results: 5 });

  const res = http.post(`${BASE_URL}/query`, payload, {
    ...params,
    tags: { name: "POST /query (vector)" },
  });

  vectorQueryDuration.add(res.timings.duration);

  const passed = check(res, {
    "vector query status 200": (r) => r.status === 200,
    "vector query has answer": (r) => {
      try {
        return JSON.parse(r.body).answer !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!passed) {
    queryFailRate.add(1);
  } else {
    queryFailRate.add(0);
  }

  sleep(Math.random() * 1 + 0.5);
}

export function graphQuery() {
  const query = randomFrom(GRAPH_QUERIES);
  const payload = JSON.stringify({ query: query, max_results: 10 });

  const res = http.post(`${BASE_URL}/query`, payload, {
    ...params,
    tags: { name: "POST /query (graph)" },
  });

  graphQueryDuration.add(res.timings.duration);

  const passed = check(res, {
    "graph query status 200": (r) => r.status === 200,
    "graph query has answer": (r) => {
      try {
        return JSON.parse(r.body).answer !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!passed) {
    queryFailRate.add(1);
  } else {
    queryFailRate.add(0);
  }

  sleep(Math.random() * 2 + 1);
}
