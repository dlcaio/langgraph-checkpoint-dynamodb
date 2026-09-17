# Security

This package processes persisted graph state, which may contain private prompts, credentials accidentally returned by tools, and user documents. Keep secrets out of graph state where possible. Do not include real checkpoint payloads or credentials in public bug reports.

AWS transport and storage encryption are separate from application encryption. To encrypt payloads before they reach DynamoDB or S3, supply a `PayloadCipher` with `id`, `encrypt(bytes, context)`, and `decrypt(bytes, context)`. Use authenticated encryption and bind the supplied `context` as authenticated associated data. Keep keys outside checkpoints and implement key lookup/rotation in your cipher. The identifier describes the cipher envelope format; key identifiers can be stored inside that envelope. Do not store encryption keys in `id`.

Compression occurs before the cipher. If you instead supply a serializer that already encrypts data, gzip may not save space. A saver configured with a cipher refuses plaintext payloads. A missing or mismatched cipher fails explicitly. This package does not supply a KMS integration or encryption key-management service.

Payload encryption covers checkpoint state, metadata, channel values, and pending-write contents. It does not hide table keys, namespace strings, checkpoint IDs, sizes, expiration times, cipher identifiers, or access patterns. Thread IDs are hashed in partition keys; hashing does not prevent guessing predictable IDs.

Thread deletion requires stopped and awaited writers. Small hashed control records persist to prevent accidental thread recreation. Deletion does not remove backups, audit logs, caller-managed artifacts, or objects uploaded later by a writer that was not stopped.

Only load checkpoints from a store you control. Treat custom serializers and cipher implementations as trusted code. Enforce application authorization before accepting a caller-provided thread ID; the saver has no user identity or authorization model.

For a suspected vulnerability, use [GitHub's private vulnerability reporting](https://github.com/dlcaio/langgraph-checkpoint-dynamodb/security/advisories/new) when available. If private reporting is unavailable, open an issue requesting a private contact without publishing exploit details or sensitive data. Only the latest release is supported during the 0.x series.
