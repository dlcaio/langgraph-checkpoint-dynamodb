import { isDeepStrictEqual } from 'node:util';
import {
  BaseCheckpointSaver,
  WRITES_IDX_MAP,
  TASKS,
  maxChannelVersion,
} from '@langchain/langgraph-checkpoint';
import type {
  ChannelVersions,
  Checkpoint,
  CheckpointListOptions,
  CheckpointMetadata,
  CheckpointPendingWrite,
  CheckpointTuple,
  PendingWrite,
} from '@langchain/langgraph-checkpoint';
import type { RunnableConfig } from '@langchain/core/runnables';
import { UpdateCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { Database } from './database.js';
import { PayloadStore } from './payload.js';
import {
  address,
  blobKey,
  configFor,
  cpKey,
  cpPrefix,
  hash,
  headKey,
  identifier,
  writeKey,
  writePrefix,
} from './keys.js';
import { CheckpointStorageError, corrupt, invalid, isConditionalFailure } from './errors.js';
import type { DynamoDBSaverOptions, Row, StoredPayload, ThreadRecord } from './types.js';

interface Snapshot {
  thread: string;
  checkpoint: Omit<Checkpoint, 'channel_values'>;
  metadata: CheckpointMetadata;
  refs: Record<string, string>;
}

/** LangGraph checkpoint storage. Callers must serialize graph runs for each thread. */
export class DynamoDBSaver extends BaseCheckpointSaver {
  private readonly db: Database;
  private readonly payloads: PayloadStore;
  constructor(options: DynamoDBSaverOptions) {
    super(options.serde);
    this.db = new Database(options);
    this.payloads = new PayloadStore(options, this.serde);
  }
  private scope(root: ThreadRecord, sk: string): string {
    return `${root.PK}/${root.generation}/${sk}`;
  }
  private async encode(root: ThreadRecord, sk: string, value: unknown): Promise<StoredPayload> {
    return this.payloads.encode(
      value,
      this.scope(root, sk),
      this.payloads.prefix(root.PK, root.generation),
    );
  }
  private async decode(root: ThreadRecord, row: Row): Promise<unknown> {
    if (row.format !== 1 || row.generation !== root.generation)
      corrupt('Invalid record format or generation');
    return this.payloads.decode(
      row.payload as StoredPayload,
      this.scope(root, row.SK),
      this.payloads.prefix(root.PK, root.generation),
    );
  }
  private row(root: ThreadRecord, sk: string, payload: StoredPayload): Row {
    if (Buffer.byteLength(sk) > 1024)
      invalid('Combined identifiers exceed the DynamoDB sort-key limit');
    return {
      PK: root.PK,
      SK: sk,
      format: 1,
      generation: root.generation,
      ...this.db.ttl(root),
      payload,
    };
  }
  private checkSignal(config: RunnableConfig): void {
    config.signal?.throwIfAborted();
  }
  private async snapshot(root: ThreadRecord, row: Row): Promise<Snapshot> {
    const value = (await this.decode(root, row)) as Snapshot;
    if (
      !value?.checkpoint ||
      typeof value.checkpoint.id !== 'string' ||
      typeof value.checkpoint.v !== 'number' ||
      !value.refs ||
      typeof value.refs !== 'object' ||
      !value.metadata ||
      typeof value.metadata !== 'object' ||
      typeof value.thread !== 'string'
    )
      corrupt('Invalid checkpoint contents');
    return value;
  }
  private async pending(
    root: ThreadRecord,
    ns: string,
    id: string,
  ): Promise<CheckpointPendingWrite[]> {
    const result: Array<{ index: number; tuple: CheckpointPendingWrite }> = [];
    for await (const row of this.db.query(root.PK, writePrefix(ns, id))) {
      const value = (await this.decode(root, row)) as {
        task: string;
        channel: string;
        value: unknown;
        index: number;
      };
      if (
        !value ||
        typeof value.task !== 'string' ||
        typeof value.channel !== 'string' ||
        !Number.isInteger(value.index)
      )
        corrupt('Invalid pending write');
      result.push({ index: value.index, tuple: [value.task, value.channel, value.value] });
    }
    result.sort((a, b) => a.tuple[0].localeCompare(b.tuple[0]) || a.index - b.index);
    return result.map((item) => item.tuple);
  }
  private async tuple(
    root: ThreadRecord,
    thread: string,
    row: Row,
    decoded?: Snapshot,
  ): Promise<CheckpointTuple> {
    const ns = identifier(row.ns, 'stored namespace', true);
    const value = decoded ?? (await this.snapshot(root, row));
    const entries: Array<[string, unknown]> = [];
    for (const [channel, ref] of Object.entries(value.refs)) {
      if (typeof ref !== 'string' || !/^B#[0-9a-f]{64}$/.test(ref))
        corrupt('Invalid channel reference');
      const blob = await this.db.get(root.PK, ref);
      if (!blob) corrupt('Referenced channel data is missing');
      const decoded = (await this.decode(root, blob)) as { present: boolean; value?: unknown };
      if (!decoded || typeof decoded.present !== 'boolean') corrupt('Invalid channel blob');
      if (decoded.present) entries.push([channel, decoded.value]);
    }
    const checkpoint: Checkpoint = {
      ...value.checkpoint,
      channel_values: Object.fromEntries(entries),
    };
    const parent = typeof row.parent === 'string' ? row.parent : undefined;
    // LangGraph's pre-v4 checkpoint format stored Send tasks in the parent's writes.
    if (checkpoint.v < 4 && parent) {
      checkpoint.channel_values[TASKS] = (await this.pending(root, ns, parent))
        .filter((write) => write[1] === TASKS)
        .map((write) => write[2]);
      checkpoint.channel_versions = {
        ...checkpoint.channel_versions,
        [TASKS]: Object.keys(checkpoint.channel_versions).length
          ? maxChannelVersion(...Object.values(checkpoint.channel_versions))
          : this.getNextVersion(undefined),
      };
    }
    return {
      config: configFor(thread, ns, checkpoint.id),
      checkpoint,
      metadata: value.metadata,
      pendingWrites: await this.pending(root, ns, checkpoint.id),
      ...(parent ? { parentConfig: configFor(thread, ns, parent) } : {}),
    };
  }
  override async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    this.checkSignal(config);
    if (config.configurable?.thread_id === undefined) return undefined;
    const { thread, ns, id } = address(config);
    const root = await this.db.root(this.db.pk(thread));
    if (!root || root.state === 'deleted') return undefined;
    this.db.active(root);
    let selectedId = id;
    if (!selectedId) {
      const head = await this.db.get(root.PK, headKey(ns));
      if (!head) {
        if (root.namespaces?.has(hash(ns))) corrupt('Latest checkpoint pointer is missing');
        return undefined;
      }
      if (head.format !== 1 || head.generation !== root.generation || typeof head.id !== 'string')
        corrupt('Invalid latest checkpoint pointer');
      selectedId = head.id;
    }
    const row = await this.db.get(root.PK, cpKey(ns, selectedId));
    if (!row) {
      if (!id || (await this.db.get(root.PK, headKey(ns)))?.id === id)
        corrupt('Latest checkpoint data is missing');
      return undefined;
    }
    const result = await this.tuple(root, thread, row);
    this.checkSignal(config);
    return result;
  }
  override async *list(
    config: RunnableConfig,
    options: CheckpointListOptions = {},
  ): AsyncGenerator<CheckpointTuple> {
    this.checkSignal(config);
    const thread = config.configurable?.thread_id;
    const ns = config.configurable?.checkpoint_ns;
    const id = config.configurable?.checkpoint_id;
    if (thread !== undefined) identifier(thread, 'thread_id');
    if (ns !== undefined) identifier(ns, 'checkpoint_ns', true);
    if (id) identifier(id, 'checkpoint_id');
    if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit < 0))
      invalid('limit must be a non-negative integer');
    let remaining = options.limit ?? Infinity;
    if (!remaining) return;
    const roots: ThreadRecord[] = [];
    if (thread !== undefined) {
      const root = await this.db.root(this.db.pk(thread));
      if (root) roots.push(root);
    } else for await (const root of this.db.scanRoots()) roots.push(root);
    for (const root of roots) {
      if (
        root.state !== 'active' ||
        (root.expiresAt !== undefined && root.expiresAt <= Math.floor(Date.now() / 1000))
      )
        continue;
      for await (const row of this.db.query(root.PK, cpPrefix(ns))) {
        this.checkSignal(config);
        if (
          (id && row.id !== id) ||
          (options.before?.configurable?.checkpoint_id &&
            String(row.id) >= options.before.configurable.checkpoint_id)
        )
          continue;
        // Thread identity is encrypted in each checkpoint's payload rather than its partition key.
        const stored = await this.snapshot(root, row);
        const tuple = await this.tuple(root, thread ?? stored.thread, row, stored);
        if (
          options.filter &&
          !Object.entries(options.filter).every(
            ([key, expected]) =>
              Object.hasOwn(tuple.metadata ?? {}, key) &&
              isDeepStrictEqual(tuple.metadata?.[key as keyof CheckpointMetadata], expected),
          )
        )
          continue;
        yield tuple;
        if (--remaining <= 0) return;
      }
    }
  }
  override async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    newVersions: ChannelVersions = checkpoint.channel_versions,
  ): Promise<RunnableConfig> {
    this.checkSignal(config);
    const { thread, ns, id: parent } = address(config);
    identifier(checkpoint.id, 'checkpoint_id');
    const root = await this.db.ensureRoot(this.db.pk(thread));
    const refs: Record<string, string> = Object.create(null);
    const parentRow = parent ? await this.db.get(root.PK, cpKey(ns, parent)) : undefined;
    const parentRefs = parentRow ? (await this.snapshot(root, parentRow)).refs : {};
    for (const [channel, version] of Object.entries(checkpoint.channel_versions)) {
      if (typeof version !== 'string' && (typeof version !== 'number' || !Number.isFinite(version)))
        invalid('Channel versions must be strings or finite numbers');
      const key = blobKey(ns, channel, version);
      let existing = await this.db.get(root.PK, key);
      if (!existing && Object.hasOwn(newVersions, channel)) {
        const payload = await this.encode(root, key, {
          present: Object.hasOwn(checkpoint.channel_values, channel),
          value: checkpoint.channel_values[channel],
        });
        existing = this.row(root, key, payload);
        try {
          await this.db.commit(root, [
            {
              Put: {
                TableName: this.db.table,
                Item: existing,
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            },
          ]);
        } catch (error) {
          if (!isConditionalFailure(error)) throw error;
          existing = await this.db.get(root.PK, key);
          if (!existing) throw error;
        }
      }
      if (existing) refs[channel] = key;
      else if (parentRefs[channel] === key) corrupt('Unchanged channel data disappeared');
    }
    const { channel_values: _values, ...skeleton } = checkpoint;
    const key = cpKey(ns, checkpoint.id);
    const payload = await this.encode(root, key, { checkpoint: skeleton, metadata, refs, thread });
    const row = {
      ...this.row(root, key, payload),
      id: checkpoint.id,
      ns,
      ...(parent ? { parent } : {}),
    };
    for (let attempt = 0; attempt < 8; attempt++) {
      this.checkSignal(config);
      const head = await this.db.get(root.PK, headKey(ns));
      const advances =
        !head || Buffer.compare(Buffer.from(checkpoint.id), Buffer.from(String(head.id))) >= 0;
      const headCondition = head ? 'id = :old' : 'attribute_not_exists(PK)';
      const values = head ? { ExpressionAttributeValues: { ':old': head.id } } : {};
      try {
        await this.db.commit(
          root,
          [
            { Put: { TableName: this.db.table, Item: row } },
            advances
              ? {
                  Put: {
                    TableName: this.db.table,
                    Item: {
                      PK: root.PK,
                      SK: headKey(ns),
                      format: 1,
                      generation: root.generation,
                      id: checkpoint.id,
                      ...this.db.ttl(root),
                    },
                    ConditionExpression: headCondition,
                    ...values,
                  },
                }
              : {
                  ConditionCheck: {
                    TableName: this.db.table,
                    Key: { PK: root.PK, SK: headKey(ns) },
                    ConditionExpression: headCondition,
                    ...values,
                  },
                },
          ],
          ns,
        );
        return configFor(thread, ns, checkpoint.id);
      } catch (error) {
        // Do not delete any S3 uploads here: the database may have committed.
        if (!isConditionalFailure(error)) throw error;
        const current = await this.db.root(root.PK);
        if (!current) corrupt('Thread control record disappeared');
        this.db.active(current);
      }
    }
    throw new CheckpointStorageError(
      'CONCURRENT_MODIFICATION',
      'Checkpoint head changed repeatedly; serialize runs per thread',
    );
  }
  override async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string,
  ): Promise<void> {
    this.checkSignal(config);
    const { thread, ns, id } = address(config, true);
    identifier(taskId, 'taskId');
    const root = await this.db.ensureRoot(this.db.pk(thread));
    for (let position = 0; position < writes.length; position++) {
      this.checkSignal(config);
      const [channel, value] = writes[position]!;
      identifier(channel, 'channel');
      const index = Object.hasOwn(WRITES_IDX_MAP, channel) ? WRITES_IDX_MAP[channel]! : position;
      const key = writeKey(ns, id!, taskId, index);
      if (index >= 0 && (await this.db.get(root.PK, key))) continue;
      const payload = await this.encode(root, key, { task: taskId, channel, value, index });
      try {
        await this.db.commit(root, [
          {
            Put: {
              TableName: this.db.table,
              Item: this.row(root, key, payload),
              ...(index >= 0 ? { ConditionExpression: 'attribute_not_exists(PK)' } : {}),
            },
          },
        ]);
      } catch (error) {
        if (index >= 0 && isConditionalFailure(error) && (await this.db.get(root.PK, key)))
          continue;
        throw error;
      }
    }
  }
  /** Stop and await in-flight graph runs before deletion. Failure is retryable. */
  override async deleteThread(threadId: string): Promise<void> {
    identifier(threadId, 'thread_id');
    await this.deleteRoot(this.db.pk(threadId));
  }
  private async deleteRoot(pk: string): Promise<void> {
    const root = await this.db.root(pk);
    if (!root) return;
    await this.db.client.send(
      new UpdateCommand({
        TableName: this.db.table,
        Key: { PK: pk, SK: 'ROOT' },
        UpdateExpression: 'SET #state = :deleting',
        ConditionExpression: 'generation = :generation',
        ExpressionAttributeNames: { '#state': 'state' },
        ExpressionAttributeValues: { ':deleting': 'deleting', ':generation': root.generation },
      }),
    );
    try {
      // The durable root survives any failed step. Prefix deletion also finds uncommitted uploads.
      await this.payloads.deletePrefix(this.payloads.prefix(pk));
      let batch: Row[] = [];
      const flush = async () => {
        if (!batch.length) return;
        await this.db.client.send(
          new TransactWriteCommand({
            TransactItems: [
              {
                ConditionCheck: {
                  TableName: this.db.table,
                  Key: { PK: pk, SK: 'ROOT' },
                  ConditionExpression: '#state = :deleting AND generation = :generation',
                  ExpressionAttributeNames: { '#state': 'state' },
                  ExpressionAttributeValues: {
                    ':deleting': 'deleting',
                    ':generation': root.generation,
                  },
                },
              },
              ...batch.map((row) => ({
                Delete: { TableName: this.db.table, Key: { PK: pk, SK: row.SK } },
              })),
            ],
          }),
        );
        batch = [];
      };
      for await (const row of this.db.query(pk)) {
        if (row.SK === 'ROOT') continue;
        batch.push(row);
        if (batch.length >= 99) await flush();
      }
      await flush();
      await this.db.client.send(
        new UpdateCommand({
          TableName: this.db.table,
          Key: { PK: pk, SK: 'ROOT' },
          UpdateExpression: 'SET #state = :deleted REMOVE namespaces',
          ConditionExpression: 'generation = :generation',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: { ':deleted': 'deleted', ':generation': root.generation },
        }),
      );
    } catch (cause) {
      throw new CheckpointStorageError(
        'DELETE_INCOMPLETE',
        'Thread deletion is incomplete; stop writers and retry deleteThread',
        { cause },
      );
    }
  }
  /** Maintenance scan. Schedule separately; never expires a still-live thread. */
  async sweepExpired(options: { limit?: number } = {}): Promise<number> {
    const limit = options.limit ?? 100;
    if (!Number.isSafeInteger(limit) || limit < 1) invalid('limit must be a positive integer');
    let count = 0;
    for await (const root of this.db.scanRoots()) {
      if (root.state === 'deleted') continue;
      if (
        root.state !== 'deleting' &&
        (root.expiresAt === undefined || root.expiresAt > Math.floor(Date.now() / 1000))
      )
        continue;
      await this.deleteRoot(root.PK);
      if (++count >= limit) break;
    }
    return count;
  }
  destroy(): void {
    this.payloads.destroy();
    this.db.destroy();
  }
}
