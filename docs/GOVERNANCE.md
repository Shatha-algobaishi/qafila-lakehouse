# Governance and personal-data handling

**Data in this repository is synthetic only.** Actual information classification requires an authorized owner and the applicable NDMO classification process; the labels below are illustrative, not official decisions.

| Data column | Illustrative classification | Location and controls |
|---|---|---|
| customer_id | Personal identifier / restricted | Bronze only; derive salted customer_token for Silver |
| customer_token | Pseudonymous / restricted | Silver only; still potentially personal, restrict access |
| order_id, payment_id, event_id | Internal operational | Bronze/Silver for reconciliation; prohibit unrestricted joins |
| city, amount, status, event_type | Internal operational | Aggregate into Gold; consider small-group disclosure |
| national_id, mobile, iban | Highly sensitive when present | Not ingested by this schema; remove from any submitted data |

The `quarantine` store excludes customer IDs from diagnostic payloads. **Bronze remains identifiable** and must be access controlled and encrypted at rest in a real deployment; SQLite does not provide these measures automatically. Source JSON/CSV files must not contain real customer data in a public repository. Set `QAFILA_TOKEN_SALT` through the environment; do not put actual secrets in files or commits. The default demo salt is intentionally insecure and is only for synthetic examples.

### Erasure and retention workflow

1. Authenticate deletion request and identify the source identifiers by controlled mapping.
2. Apply erasure in source systems, Bronze records, Silver tokens and generated Gold summaries as applicable; reaggregate affected dates.
3. Apply approved rules to CDC tombstones, exports, snapshots, backups and legal retention exceptions.
4. Keep minimal audit evidence without exposing the subject's personal information.
5. Reconcile restored backups and downstream extracts to avoid accidental reintroduction.

### Risk acceptance and sign-off

This demo has not undergone privacy assessment, key-management review, source access review, or production security testing. An approved governance assessment is required before connecting real data.