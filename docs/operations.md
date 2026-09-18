# Operating the saver

## Infrastructure

Use a table with string keys `PK` and `SK`, no secondary indexes, and TTL enabled on `ttl`. Start with on-demand billing and measure your workload. The [CloudFormation example](../examples/infrastructure.json) also creates a private, unversioned S3 bucket and retains both resources when the stack is deleted. Deploying infrastructure incurs AWS charges.

The saver does not create tables, enable TTL, schedule maintenance, configure bucket lifecycle rules, or provision IAM. Use one region for the table, bucket, and application. Keep bucket versioning disabled: versioned object deletion is deliberately unsupported, and the saver checks the bucket's versioning state before offload and cleanup.

The retention check takes effect when a request observes expiry. Physical DynamoDB TTL deletion is asynchronous and can lag by days. See [AWS TTL documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html). Schedule `sweepExpired()` as well: TTL alone leaves S3 objects and thread control records untouched.

## Runtime IAM

Scope permissions to the actual table and dedicated bucket prefix. This example uses placeholder ARNs; substitute your account, region, table, and bucket. The runtime does not need table-creation permissions.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:ConditionCheckItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/TABLE"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetBucketVersioning",
      "Resource": "arn:aws:s3:::BUCKET"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::BUCKET",
      "Condition": { "StringLike": { "s3:prefix": "checkpoints/*" } }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::BUCKET/checkpoints/*"
    }
  ]
}
```

DynamoDB transaction authorization uses the participating item actions, including `ConditionCheckItem`. See [AWS transaction permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html). Omit S3 statements if offload is disabled. A customer-managed encryption key requires its corresponding KMS permissions separately. `Scan` is needed for all-thread listing and expiration sweeping; those operations can use a separately authorized maintenance process.

`keyPrefix` separates data logically, but it is not an authorization boundary. Enforce access to thread IDs in your application. For stronger isolation, use distinct tables, buckets or prefixes, roles, and encryption keys according to your threat model.

## Bound history size

1. Choose a thread lifetime based on how long a user can reasonably resume a run. Start a new thread for a new workflow; do not use one permanent thread for every run by a user.
2. Schedule cleanup. Before sweeping, ensure expired runs and any in-flight uploads have finished or been cancelled and awaited. A rejected DynamoDB commit cannot cancel an already-running S3 upload.
3. Keep stable large values in separate channels. A stable image inside an ever-growing messages array is serialized again whenever that array's version changes.
4. Prefer artifact IDs or object references over embedded screenshots, documents, and repeated raw tool responses. Your application must manage access and lifecycle for these external artifacts. See the [image workflow example](images.md). Store artifacts outside the saver's checkpoint object prefix; whole-thread cleanup deletes that prefix, including objects with no database pointer.
5. Summarize or trim messages using supported LangGraph state updates. Trimming the current state reduces future checkpoints; it does not erase already stored history.
6. After a completed thread's recovery window closes, delete the whole thread. Arbitrary deletion of old checkpoint rows is unsafe because channel blobs and delta-channel ancestors can still be referenced.

Storage savings do not imply lower costs in every workload. Transactions, strongly consistent reads, S3 requests, and table scans have costs. Measure serialized bytes, objects, requests, and latency with representative runs before choosing a retention window. This release makes no price or throughput guarantee.

## Failures and recovery

- **Network timeout during save:** do not delete objects to compensate. Retry the same checkpoint operation; the write may already exist.
- **`CORRUPT_CHECKPOINT`:** stop the run and investigate missing or damaged data. Do not catch this error and start from scratch automatically.
- **`DELETE_INCOMPLETE`:** keep writers stopped and retry `deleteThread(threadId)`. The control record remains so cleanup can continue after a process restart.
- **`THREAD_EXPIRED`:** communicate the expired recovery window. Starting another workflow is an explicit application decision and requires a new thread ID.
- **Changed S3 configuration:** restore the original bucket and prefix. A fingerprint on the control record prevents cleanup from silently targeting a different store.

Back up DynamoDB and S3 under a coordinated policy. Restoring only DynamoDB can leave pointers to objects already removed by retention. Backups, provider retention, logs, and externally managed artifacts have lifecycles separate from `deleteThread`.

External images must remain available for as long as retained checkpoints need them for resume or replay. Store stable object keys or artifact IDs in state and generate signed URLs when needed, rather than persisting an expiring URL as the only reference. Thread deletion does not delete externally referenced images; account for other threads that may still use them before application-level cleanup.

## Scale and monitoring

The maintenance sweep performs a full-table scan until it finds its limit of eligible roots or reaches the end. It can become expensive as permanent tombstones accumulate; this first release has no expiry index or resumable sweep cursor. Large deployments should benchmark this path before adopting it.

Monitor DynamoDB throttles and transaction conflicts, S3 errors, expired-thread cleanup backlog, root counts, and total storage. A single thread with thousands of namespaces or channels needs particular scrutiny. This package intentionally avoids silently discarding old checkpoints to relieve storage pressure.
