# Incident Response Playbook

## Severity Levels

| Level | Name | Description | Response Time | Example |
|---|---|---|---|---|
| **P0** | Critical Outage | Complete service unavailable. Data loss occurring. | < 15 min | All Neo4j nodes down, DLQ sink failure (messages lost) |
| **P1** | Major Degradation | Core functionality impaired for all tenants. | < 30 min | Ingestion pipeline halted, query error rate > 50% |
| **P2** | Partial Degradation | Non-critical feature impaired, or single tenant affected. | < 2 hours | LLM provider failover active, single tenant rate limited |
| **P3** | Minor Issue | Degraded performance, no data loss, workaround available. | < 8 hours | Slow query response (p99 > 3s), elevated consumer lag |
| **P4** | Informational | Cosmetic issues, minor bugs, improvement opportunities. | Next sprint | Dashboard rendering issue, non-critical log noise |

### Response Time Expectations

- **P0/P1:** On-call engineer acknowledges within response time. War room opened immediately.
- **P2:** Assigned engineer begins investigation within response time.
- **P3:** Triaged and prioritized in next standup.
- **P4:** Added to backlog.

---

## Escalation Matrix

| Role | Contact Method | When to Engage |
|---|---|---|
| **On-Call Engineer** | PagerDuty (auto-page on P0/P1) | First responder for all P0/P1 incidents |
| **Backend Tech Lead** | Slack `#graphrag-incidents` + PagerDuty override | P0 incidents, or P1 not resolved in 30 min |
| **SRE Lead** | Slack `#graphrag-incidents` + phone | Infrastructure failures (Neo4j cluster, Kafka, K8s) |
| **Engineering Manager** | Slack DM + email | P0 lasting > 1 hour, or any data loss confirmed |
| **VP Engineering** | Phone | P0 lasting > 2 hours, or customer-facing impact confirmed |
| **Security Lead** | Slack `#security-alerts` + PagerDuty | Any suspected data breach or unauthorized access |

### Escalation Timeline (P0)

| Time | Action |
|---|---|
| T+0 min | PagerDuty pages on-call engineer |
| T+5 min | On-call acknowledges, begins diagnosis |
| T+15 min | If not acknowledged, escalate to backup on-call |
| T+30 min | Backend Tech Lead engaged if not resolved |
| T+60 min | Engineering Manager notified, status page updated |
| T+120 min | VP Engineering engaged, customer communication sent |

---

## Communication Templates

### Status Page Update (Initial)

```
Title: [P{severity}] {service} — {brief description}
Status: Investigating

We are aware of an issue affecting {affected functionality}.
Our engineering team is actively investigating.

Impact: {description of user-visible impact}
Started: {timestamp UTC}
Next update: Within {30 min for P0/P1, 2 hours for P2/P3}
```

### Status Page Update (Identified)

```
Title: [P{severity}] {service} — {brief description}
Status: Identified

We have identified the root cause: {brief root cause}.
We are implementing a fix.

Impact: {description of user-visible impact}
Started: {timestamp UTC}
ETA for resolution: {estimated time}
Next update: Within {15 min for P0, 30 min for P1}
```

### Status Page Update (Resolved)

```
Title: [P{severity}] {service} — {brief description}
Status: Resolved

The issue has been resolved. {brief description of fix}.

Impact: {description of user-visible impact}
Duration: {start time} to {end time} ({total duration})
Root cause: {1-2 sentence root cause}

A post-mortem will be published within 5 business days.
```

### Stakeholder Email (P0/P1)

```
Subject: [Incident {ID}] {Service} — {Status}

Team,

We are currently experiencing a {P0/P1} incident affecting {service}.

Timeline:
- {timestamp}: Issue detected via {alert/customer report}
- {timestamp}: Investigation started
- {timestamp}: Root cause identified — {brief description}
- {timestamp}: Fix deployed / Mitigation in place

Current status: {Investigating/Identified/Monitoring/Resolved}
Customer impact: {Yes/No — description}
Data loss: {Yes/No — scope if yes}

Next steps:
- {action item 1}
- {action item 2}

Next update: {time}

— {Incident Commander name}
```

---

## Post-Mortem Template

### Header

| Field | Value |
|---|---|
| **Incident ID** | INC-YYYY-NNN |
| **Date** | YYYY-MM-DD |
| **Duration** | HH:MM |
| **Severity** | P0 / P1 / P2 |
| **Incident Commander** | {name} |
| **Post-Mortem Author** | {name} |
| **Status** | Draft / Reviewed / Complete |

### Summary

_One paragraph describing what happened, the impact, and how it was resolved._

### Impact

| Metric | Value |
|---|---|
| Duration | {total downtime or degradation} |
| Affected tenants | {count or "all"} |
| Queries failed | {count} |
| Documents lost | {count, ideally 0} |
| Error budget consumed | {percentage of monthly budget} |

### Timeline

| Time (UTC) | Event |
|---|---|
| HH:MM | {Alert fired / issue detected} |
| HH:MM | {On-call acknowledged} |
| HH:MM | {Root cause identified} |
| HH:MM | {Fix deployed} |
| HH:MM | {Service restored} |
| HH:MM | {All-clear declared} |

### Root Cause

_Detailed technical explanation of what went wrong. Include relevant metrics, logs, and
configuration that contributed to the failure._

### Contributing Factors

- _Factor 1: {description}_
- _Factor 2: {description}_

### What Went Well

- _Thing 1_
- _Thing 2_

### What Went Poorly

- _Thing 1_
- _Thing 2_

### Action Items

| ID | Action | Owner | Priority | Due Date | Status |
|---|---|---|---|---|---|
| AI-01 | {description} | {name} | P0/P1/P2 | YYYY-MM-DD | Open |
| AI-02 | {description} | {name} | P0/P1/P2 | YYYY-MM-DD | Open |

### Lessons Learned

_Key takeaways that should inform future architecture, process, or tooling decisions._

---

## Incident Workflow

```
1. Alert fires → PagerDuty notifies on-call
2. On-call acknowledges → opens #incident-YYYY-NNN Slack channel
3. Triage → assign severity (P0–P4)
4. If P0/P1 → open war room, assign Incident Commander
5. Investigate → use component runbooks (docs/runbooks/)
6. Communicate → post status page updates per templates above
7. Mitigate → apply fix or workaround
8. Verify → confirm metrics return to normal
9. Resolve → update status page, close incident channel
10. Post-mortem → complete within 5 business days (P0/P1 mandatory)
```
