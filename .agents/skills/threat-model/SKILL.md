---
name: threat-model
description: Lightweight STRIDE threat model for a feature touching authentication, money movement, PII, or external integrations; outputs threats with mitigations to add as acceptance criteria. Use during /spec, /design, or when asked "is this secure", "threat model", or "security review of the design".
---

# Threat Model (lightweight STRIDE)

1. Draw the data flow: actors, entry points, services, data stores, external systems, trust boundaries.
2. For each element crossing a trust boundary, check:

| STRIDE | Ask | Typical mitigation |
|---|---|---|
| Spoofing | Can someone act as another user/service? | AuthN, mTLS, signed webhooks |
| Tampering | Can data/amount be altered in transit or at rest? | Server-side recompute, signatures, checksums |
| Repudiation | Can a user deny an action? | Immutable audit log with actor + timestamp |
| Information disclosure | Can PII leak via API, logs, errors? | Masking, field-level authZ, encryption |
| Denial of service | Can it be flooded or made to hang? | Rate limit, timeouts, queue backpressure |
| Elevation of privilege | Can a user reach others' resources? | Ownership checks, least privilege, deny by default |

3. Add bank-specific checks: double-spend / replay (idempotency key), race conditions on balance, reconciliation with external party, regulatory retention.
4. Output a table `Threat | Likelihood | Impact | Mitigation | AC id` and add each mitigation as an AC in the spec.
