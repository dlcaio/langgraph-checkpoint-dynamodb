# LangGraph checkpoints in DynamoDB

A standalone TypeScript checkpoint saver for LangGraph. Stores checkpoint history in DynamoDB, reuses unchanged channel versions, compresses payloads, and optionally offloads large values to S3.

**Early release, version 0.1.** Tested against DynamoDB Local and LangGraph's official checkpoint validation suite. S3 failure tests use an in-memory test double; this release has not been validated against live AWS services. Run a staging soak test with your IAM policy and workload before production use.

## Install

Requires Node.js 22 or later and an ESM project.

```sh
npm install @dlcaio/langgraph-checkpoint-dynamodb @langchain/langgraph @langchain/langgraph-checkpoint @langchain/core
```

Tested with LangGraph 1.4.15, checkpoint 1.1.5, and core 1.2.11. The package is independently maintained and is not an official AWS or LangChain product.

## Quick start

Create a DynamoDB table with string partition key `PK` and string sort key `SK`. No secondary indexes are needed. An optional [CloudFormation example](examples/infrastructure.json) provisions a table and an unversioned, private S3 bucket. The saver does not create infrastructure.

```ts
import { randomUUID } from 'node:crypto';
import { Annotation, END, START, StateGraph } from '@langchain/langgraph';
import { DynamoDBSaver } from '@dlcaio/langgraph-checkpoint-dynamodb';

const saver = new DynamoDBSaver({
  tableName: 'agent-checkpoints',
  clientConfig: { region: 'eu-west-1' },
  retentionSeconds: 7 * 24 * 60 * 60,
  // Optional checkpoint payload overflow; does not manage image files.
  s3: { bucketName: 'your-checkpoint-bucket', keyPrefix: 'checkpoints/' },
});

const State = Annotation.Root({ answer: Annotation<string> });
const graph = new StateGraph(State)
  .addNode('answer', () => ({ answer: 'Hello!' }))
  .addEdge(START, 'answer')
  .addEdge('answer', END)
  .compile({ checkpointer: saver });

const config = { configurable: { thread_id: randomUUID() } };
try {
  console.log(await graph.invoke({ answer: '' }, config));
  console.log((await graph.getState(config)).values);
} finally {
  saver.destroy();
}
```

AWS clients use the standard AWS SDK credential provider chain. Use an IAM role in production; do not put credentials in application source.

## What reduces storage?

- **Channel version reuse:** if a value has the same channel version across ten checkpoints, its channel payload is stored once. This is version-based reuse, not content-based deduplication.
- **Compression:** gzip runs before optional application encryption and only stores compressed bytes when they are smaller.
- **Explicit retention:** a thread can have a fixed lifetime, followed by cleanup of its DynamoDB and S3 data.

A growing messages array changes on each step. Each new version is still stored in full, so this package alone does not make an indefinitely growing conversation small. Keep tool responses concise, store large artifacts outside graph state, and summarize messages in your graph. See [history and retention](docs/operations.md).

## Images and other external files

Upload images in your application or tool before adding their references to graph state. Checkpoint a stable artifact ID or object key, then resolve it into a fresh model-accessible URL or supported image input inside the model node. See the [image workflow example](docs/images.md).

The saver's optional `s3` setting stores **serialized checkpoint payloads** that exceed `maxInlineBytes` after compression and optional encryption (300 KiB by default). It does not detect images, extract them from messages, or fetch URLs. Without S3 configured, an oversized encoded payload raises `PAYLOAD_TOO_LARGE`. Other state can still need this overflow storage even when images are stored separately.

Your application owns external image access and retention. `deleteThread` and `sweepExpired` remove saver-managed payloads; they do not follow image references or delete your external images. Keep application artifacts in a separate bucket or prefix from checkpoint payloads.

## Retention and deletion

There is **no expiration by default**. Set `retentionSeconds` when creating the saver if you want one. The lifetime starts at the thread's first write and is shared by all its namespaces. Further writes do not extend it, and changing configuration does not change existing threads.

Enable DynamoDB TTL on the attribute named `ttl`. Also schedule maintenance in your application:

```ts
// Run periodically, after stopping and awaiting writers for expired threads.
const removed = await saver.sweepExpired({ limit: 100 });

// Or remove a particular completed thread, after awaiting its writers.
await saver.deleteThread(threadId);
```

The package does not create a scheduler. DynamoDB TTL does not delete S3 objects. `sweepExpired` performs that cleanup and retries incomplete deletions. It scans the table; see the [operating guide](docs/operations.md) before using it at scale.

Expired threads reject resume and writes. Deleted threads return no checkpoint and reject new writes. Use a new `thread_id` for a new run. A small permanent control record remains for each thread so delayed work cannot silently recreate it. Payload storage is bounded by your retention and cleanup policy; the control records still accumulate.

## Options

| Option             | Default           | Purpose                                                      |
| ------------------ | ----------------- | ------------------------------------------------------------ |
| `tableName`        | Required          | Existing DynamoDB table with `PK` / `SK` keys                |
| `clientConfig`     | SDK defaults      | Settings for a saver-owned DynamoDB client                   |
| `client`           | None              | Supply a `DynamoDBDocumentClient` instead of `clientConfig`  |
| `keyPrefix`        | `lg`              | Separate independent applications in one table               |
| `retentionSeconds` | None              | Fixed thread lifetime, 1 second to 5 years                   |
| `compression`      | `true`            | Gzip before encryption, when beneficial                      |
| `maxInlineBytes`   | 300 KiB           | Encoded payload threshold for S3; maximum 350 KiB            |
| `maxPayloadBytes`  | 32 MiB            | Maximum serialized/decompressed size per payload             |
| `s3`               | None              | `{ bucketName, keyPrefix, client? }`; prefix must end in `/` |
| `cipher`           | None              | Application-provided authenticated encryption hook           |
| `serde`            | LangGraph default | Custom LangGraph serializer                                  |

`destroy()` closes only clients created by the saver. If you inject clients, you own their lifecycle. When injecting a document client, configure `marshallOptions.removeUndefinedValues: true`. When injecting clients for custom endpoints or credentials, provide the S3 client separately as well.

## Guarantees and limits

- Supports checkpoint reads, history listing, pending writes, interrupt/resume, namespaces, and LangGraph delta-channel history through the base saver implementation.
- Reads referenced data strictly. Missing latest checkpoints, channel blobs, or S3 objects raise an error rather than silently restarting a graph. A completely missing thread and an unknown historical checkpoint return `undefined`.
- Never deletes an uploaded payload merely because a database response was lost. Such uploads may remain until whole-thread cleanup.
- Deletion failures are surfaced and retryable. Stop and await in-flight graph runs before deletion; S3 and DynamoDB do not share a transaction.
- Serialize independent graph runs for the same thread. This is a checkpoint store, not a distributed run lock or a guarantee that external actions happen exactly once.
- Use one AWS region and an **unversioned** S3 bucket. Previously versioned buckets with versioning suspended are also rejected. Multi-region active writers and selective checkpoint pruning are unsupported.
- Keep the table, key prefix, S3 bucket, and S3 prefix stable for existing threads. This storage format is not compatible with another DynamoDB saver or a Postgres saver; migration requires a separate, explicit export/import process.

See [storage format and constraints](docs/storage.md), [IAM and operations](docs/operations.md), and [security](SECURITY.md) for details.

## Errors

```ts
import { CheckpointStorageError } from '@dlcaio/langgraph-checkpoint-dynamodb';

try {
  await saver.getTuple(config);
} catch (error) {
  if (error instanceof CheckpointStorageError && error.code === 'THREAD_EXPIRED') {
    // Report expiry. Start a new run only as an explicit application decision.
  } else {
    throw error;
  }
}
```

Error codes: `INVALID_ARGUMENT`, `CORRUPT_CHECKPOINT`, `THREAD_EXPIRED`, `THREAD_DELETED`, `CONCURRENT_MODIFICATION`, `PAYLOAD_TOO_LARGE`, `CIPHER_REQUIRED`, and `DELETE_INCOMPLETE`. AWS permission, throttling, and network errors generally propagate unchanged. Do not treat a storage error as an empty checkpoint.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). The integration suite runs LangGraph's official validation tests plus recovery tests against DynamoDB Local. It never uses a cloud endpoint.

MIT licensed. Contributions and reproducible bug reports are welcome.
