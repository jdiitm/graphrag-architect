import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

const config = JSON.parse(open("./config.json"));
const env = __ENV.TARGET_ENV || "dev";
const BASE_URL = __ENV.BASE_URL || config[env].base_url;

const ingestionDuration = new Trend("ingestion_duration", true);
const ingestionFailRate = new Rate("ingestion_failures");

export const options = {
  stages: [
    { duration: "1m", target: 10 },
    { duration: "3m", target: 50 },
    { duration: "2m", target: 100 },
    { duration: "1m", target: 150 },
    { duration: "1m", target: 50 },
    { duration: "30s", target: 0 },
  ],
  thresholds: {
    http_req_duration: [
      "p(50)<500",
      "p(99)<2000",
    ],
    ingestion_failures: ["rate<0.01"],
  },
};

const PAYLOADS = [
  {
    documents: [
      {
        file_path: "services/auth/main.go",
        content: btoa("package main\n\nfunc main() {}"),
      },
    ],
  },
  {
    documents: [
      {
        file_path: "k8s/deployment.yaml",
        content: btoa("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: test"),
      },
      {
        file_path: "k8s/service.yaml",
        content: btoa("apiVersion: v1\nkind: Service\nmetadata:\n  name: test"),
      },
    ],
  },
  {
    documents: [
      {
        file_path: "services/gateway/routes.py",
        content: btoa('from fastapi import FastAPI\napp = FastAPI()\n@app.get("/health")\ndef health(): return {"ok": True}'),
      },
      {
        file_path: "services/gateway/kafka.py",
        content: btoa('TOPIC = "events"\nBROKERS = "kafka:9092"'),
      },
      {
        file_path: "services/gateway/Dockerfile",
        content: btoa("FROM python:3.12-slim\nWORKDIR /app\nCOPY . .\nCMD [\"uvicorn\", \"routes:app\"]"),
      },
    ],
  },
];

function randomPayload() {
  return PAYLOADS[Math.floor(Math.random() * PAYLOADS.length)];
}

export default function () {
  const payload = JSON.stringify(randomPayload());
  const params = {
    headers: {
      "Content-Type": "application/json",
    },
    tags: { name: "POST /ingest" },
  };

  const res = http.post(`${BASE_URL}/ingest?sync=true`, payload, params);

  ingestionDuration.add(res.timings.duration);

  const passed = check(res, {
    "status is 200 or 202": (r) => r.status === 200 || r.status === 202,
    "response has status field": (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.status !== undefined || body.job_id !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!passed) {
    ingestionFailRate.add(1);
  } else {
    ingestionFailRate.add(0);
  }

  sleep(Math.random() * 0.5 + 0.1);
}
