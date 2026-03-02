import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const soakQueryDuration = new Trend("soak_query_duration", true);
const soakFailRate = new Rate("soak_failures");
const memoryCheckFailures = new Counter("memory_stability_failures");

export const options = {
  stages: [
    { duration: "5m", target: 100 },
    { duration: "24h", target: 100 },
    { duration: "5m", target: 0 },
  ],
  thresholds: {
    soak_query_duration: [
      "p(50)<1000",
      "p(99)<3000",
    ],
    soak_failures: ["rate<0.02"],
    memory_stability_failures: ["count<10"],
  },
};

const QUERIES = [
  "What is the auth-service?",
  "List all Kafka topics in the platform",
  "Which services call the payment gateway?",
  "If the auth-service fails, which Kafka topics experience backpressure?",
  "What is the blast radius if Redis goes down?",
  "Show all K8s deployments in the graphrag namespace",
  "What database does the user service use?",
  "Trace data flow from ingestion worker to Neo4j",
];

function randomQuery() {
  return QUERIES[Math.floor(Math.random() * QUERIES.length)];
}

let previousHeapUsage = null;
const HEAP_GROWTH_THRESHOLD_MB = 500;

function checkMemoryStability(res) {
  try {
    const healthRes = http.get(`${BASE_URL}/metrics`);
    if (healthRes.status !== 200) {
      return;
    }

    const body = healthRes.body;
    const heapMatch = body.match(/process_resident_memory_bytes\s+(\d+)/);
    if (!heapMatch) {
      return;
    }

    const currentHeapMB = parseInt(heapMatch[1], 10) / (1024 * 1024);

    if (previousHeapUsage !== null) {
      const growth = currentHeapMB - previousHeapUsage;
      if (growth > HEAP_GROWTH_THRESHOLD_MB) {
        memoryCheckFailures.add(1);
        console.warn(
          `Memory growth detected: ${growth.toFixed(1)}MB ` +
          `(from ${previousHeapUsage.toFixed(1)}MB to ${currentHeapMB.toFixed(1)}MB)`
        );
      }
    }
    previousHeapUsage = currentHeapMB;
  } catch (_) {
    // metrics endpoint may not always be available
  }
}

export default function () {
  const query = randomQuery();
  const payload = JSON.stringify({ query: query, max_results: 5 });
  const params = {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /query (soak)" },
  };

  const res = http.post(`${BASE_URL}/query`, payload, params);

  soakQueryDuration.add(res.timings.duration);

  const passed = check(res, {
    "soak query status 200": (r) => r.status === 200,
    "soak query has answer": (r) => {
      try {
        return JSON.parse(r.body).answer !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!passed) {
    soakFailRate.add(1);
  } else {
    soakFailRate.add(0);
  }

  if (__ITER % 100 === 0) {
    checkMemoryStability(res);
  }

  sleep(Math.random() * 2 + 0.5);
}
