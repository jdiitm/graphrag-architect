# Data Processing Agreement (DPA)

## 1. Definitions

- **Controller**: The entity that determines the purposes and means of
  processing Personal Data (the Customer).
- **Processor**: GraphRAG platform operator, processing Personal Data on
  behalf of the Controller.
- **Personal Data**: Any information relating to an identified or identifiable
  natural person, as defined by GDPR Article 4(1).
- **Sub-processor**: A third party engaged by the Processor to assist in
  fulfilling its obligations under this Agreement.

---

## 2. Scope of Data Processing

The Processor shall process Personal Data only as necessary to provide the
GraphRAG knowledge-graph and retrieval services as described in the Service
Agreement. Processing activities include:

- Ingestion and indexing of infrastructure manifests, code, and documentation.
- Entity extraction, embedding generation, and knowledge-graph construction.
- Query processing and response generation.

The Processor shall not process Personal Data for any purpose other than those
specified by the Controller.

---

## 3. Security Measures

The Processor shall implement appropriate technical and organizational measures
to protect Personal Data, including but not limited to:

- Encryption at rest (AES-256) and in transit (TLS 1.2+).
- Role-based access control with principle of least privilege.
- Audit logging of all data access events.
- Regular vulnerability assessments and penetration testing.
- Incident response plan with 72-hour breach notification.

---

## 4. Sub-Processors

The Processor shall not engage any Sub-processor without prior written
authorization from the Controller. Current authorized Sub-processors:

| Sub-processor   | Purpose                   | Location     |
|-----------------|---------------------------|--------------|
| AWS             | Cloud infrastructure      | US / EU      |
| Neo4j Aura      | Graph database hosting    | Configurable |
| Anthropic       | LLM inference (Claude)    | US           |
| Google Cloud    | LLM inference (Gemini)    | US / EU      |

The Processor shall ensure that each Sub-processor is bound by data protection
obligations no less protective than those set out in this Agreement.

---

## 5. Data Subject Rights

The Processor shall assist the Controller in responding to requests from data
subjects exercising their rights under applicable data protection law,
including rights of access, rectification, erasure, and portability.

---

## 6. Data Retention and Deletion

Upon termination of the Service Agreement or upon the Controller's request,
the Processor shall delete or return all Personal Data within 30 days, unless
retention is required by applicable law.

---

## 7. International Transfers

Where Personal Data is transferred outside the EEA, the Processor shall
ensure appropriate safeguards are in place, including Standard Contractual
Clauses (SCCs) as approved by the European Commission.

---

## 8. Audit Rights

The Controller shall have the right to audit the Processor's compliance with
this Agreement, subject to reasonable notice and confidentiality obligations.

---

## 9. Term and Termination

This DPA shall remain in effect for the duration of the Service Agreement. The
obligations regarding data protection shall survive termination.
