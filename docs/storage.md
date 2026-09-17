# Storage format

Format version 1 uses one DynamoDB partition per thread. The partition key is `<keyPrefix>#<sha256(thread_id)>`. Thread IDs are hashed to keep keys bounded; hashing is not encryption or tenant authorization.

| Sort-key prefix | Contents                                                                             |
| --------------- | ------------------------------------------------------------------------------------ |
| `ROOT`          | Generation, state, configured object-store fingerprint, expiration, namespace hashes |
| `H#`            | Latest checkpoint ID for a namespace                                                 |
| `C#`            | Serialized checkpoint structure, metadata, thread ID, channel references             |
| `B#`            | One serialized channel value per namespace/channel/version                           |
| `W#`            | Pending write, addressed by checkpoint/task/index                                    |

The checkpoint payload omits `channel_values`; it references immutable channel-version rows. New versions use conditional writes. Checkpoint and head updates commit in a transaction guarded by the thread's active generation and expiration. An older checkpoint ID does not move the head backward.

Checkpoint IDs sort by UTF-8 byte order, as LangGraph's ordered IDs do. IDs and namespaces are encoded in sort keys without separator ambiguity. Normal pending writes use first-write-wins semantics; LangGraph's special write indexes support replacement.

## Payloads

Each payload contains a serializer type, format version, sizes, compression flag, cipher identifier, and SHA-256 checksum. It contains either inline binary bytes or a reference to an immutable S3 object. Encoding is:

```text
LangGraph serialization → optional gzip → optional cipher → DynamoDB or S3
```

The checksum detects damaged bytes; it is not authentication against an attacker able to rewrite database records. Use an authenticated `PayloadCipher` for that threat model. The decode path bounds both encoded and decompressed data and checks the result's size.

S3 keys use `<configured-prefix>/<hashed-partition>/<generation>/<random-id>`. The actual configured prefix already ends in `/`. Reads reject object references outside that thread generation. Whole-thread deletion removes the thread's object prefix, including uploads that never acquired a database pointer. A database timeout never triggers deletion of an uploaded object because the write may have committed.

Retries and overwritten checkpoints or special pending writes can leave obsolete S3 objects. Version-based reuse reduces duplicate channel writes, but there is no online mark-and-sweep garbage collector for a live thread. These objects are removed by whole-thread deletion or expiry maintenance.

## Expiration

`ROOT.expiresAt` is assigned once, on the first write. Every data row receives the same `ttl` value. The control record itself has no TTL. This prevents an older shared channel blob expiring while a later checkpoint still needs it.

An expired root rejects reads and mutations before normal resume. History listing skips expired and deleted threads. The maintenance sweep deletes their objects and rows, then leaves a `deleted` control record. No automatic reuse or retention extension is supported.

Deleting the control record manually removes the protection against recreating a previously deleted thread. Administrators can remove tombstones only when a separate system guarantees that old thread IDs will never be submitted again. Retained hashes and operational metadata should be included in your data retention assessment.

## Limits and performance

- `thread_id`, `checkpoint_ns`, `checkpoint_id`, and task IDs are limited to 256 UTF-8 bytes each. The combined encoded DynamoDB sort key must fit 1,024 bytes; long nested namespaces can hit this limit earlier.
- One thread's namespace registry must fit in a DynamoDB item. Do not use a single thread as an unlimited namespace container.
- The payload cap applies to each serialized channel, checkpoint structure, or pending write. Serializer overhead counts. Use `Uint8Array` for binary values with LangGraph's default serializer; Node `Buffer` is not guaranteed to round-trip as a `Buffer`.
- `maxInlineBytes` leaves headroom below DynamoDB's 400 KiB item limit. Custom serializer type strings should be short.
- Checkpoint reads currently fetch channel rows sequentially. Each new channel version and pending write uses its own guarded transaction. This prioritizes simple recovery semantics; many tiny channels can increase request count and latency.
- `list` for one thread queries its partition. Listing all threads and `sweepExpired` scan the table. Metadata filters run after decoding. A result limit does not cap scan cost.
- Listing within one namespace is newest-first. No globally chronological order is promised across namespaces or threads. A list is not a snapshot of concurrent changes.
- Point reads are strongly consistent, but a whole checkpoint read is not one transaction. Stop writers before deletion and use application-level coordination for runs.
- AWS SDK retries cover retryable service errors. Application retries may leave uploaded objects that only whole-thread cleanup removes. Monitor retry rates and storage, especially for long-lived threads.

The package does not compact LangGraph messages, change reducers, manage artifacts, coordinate application side effects, or implement cross-region replication.
