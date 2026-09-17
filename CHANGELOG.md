# Changelog

## 0.1.0

Initial independent TypeScript implementation:

- LangGraph checkpoint, pending-write, namespace, history, and interrupt/resume support.
- Reuse of unchanged channel versions and gzip compression before optional payload encryption.
- Optional S3 offload with scoped immutable objects and explicit missing-payload errors.
- Fixed thread retention, logical expiry, retryable whole-thread deletion, and maintenance sweeping.
- Official LangGraph validation coverage and targeted storage/recovery tests.

The initial release supports a single region and unversioned S3 buckets. It does not include selective history pruning, a scheduler, a distributed run lock, a KMS cipher, or migration from another saver. See the README for operating constraints and validation scope.
