import { randomBytes, randomUUID } from 'node:crypto';
import { afterEach, describe, expect, it } from 'vitest';
import type { RunnableConfig } from '@langchain/core/runnables';
import {
  DeleteCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand,
  DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { Annotation, Command, END, START, StateGraph, interrupt } from '@langchain/langgraph';
import { DynamoDBSaver } from '../src/index.js';
import { hash, cpKey, headKey } from '../src/keys.js';
import {
  config,
  checkpoint,
  endpoint,
  fixture,
  memoryS3,
  metadata,
  testCipher,
} from './helpers.js';

describe.skipIf(!endpoint)('DynamoDB Local recovery and storage', () => {
  const created: Awaited<ReturnType<typeof fixture>>[] = [];
  async function setup(options: Parameters<typeof fixture>[0] = {}) {
    const f = await fixture(options);
    created.push(f);
    return f;
  }
  afterEach(async () => {
    for (const f of created.splice(0)) await f.close();
  });
  it('resumes a real interrupted graph with a new saver instance', async () => {
    const f = await setup({ cipher: testCipher() });
    const cfg = config();
    const state = Annotation.Root({ value: Annotation<string> });
    const graph = new StateGraph(state)
      .addNode('wait', () => ({ value: interrupt('approval') as string }))
      .addEdge(START, 'wait')
      .addEdge('wait', END);
    await graph.compile({ checkpointer: f.saver }).invoke({ value: 'pending' }, cfg);
    const restarted = new DynamoDBSaver(f.options);
    try {
      expect(
        await graph
          .compile({ checkpointer: restarted })
          .invoke(new Command({ resume: 'approved' }), cfg),
      ).toMatchObject({ value: 'approved' });
    } finally {
      restarted.destroy();
    }
  });
  it('stores an unchanged large channel once across ten checkpoints', async () => {
    const s3 = memoryS3();
    const f = await setup({ s3: s3.options, compression: false });
    const value = new Uint8Array(randomBytes(500000));
    let cfg: RunnableConfig = config();
    for (let i = 0; i < 10; i++) {
      const cp = checkpoint(String(i).padStart(2, '0'), { image: value, step: i });
      cp.channel_versions = { image: 1, step: i + 1 };
      cfg = await f.saver.put(cfg, cp, metadata, i ? { step: i + 1 } : cp.channel_versions);
    }
    expect(s3.state.puts).toBe(1);
    expect((await f.saver.getTuple(cfg))?.checkpoint.channel_values.image).toEqual(value);
  });
  it.each(['channel', 'checkpoint', 'head', 'root'])(
    'fails closed when latest %s data disappears',
    async (kind) => {
      const f = await setup();
      const cfg = config();
      const cp = checkpoint('01', { text: 'saved' });
      await f.saver.put(cfg, cp, metadata, cp.channel_versions);
      const pk = `lg#${hash(cfg.configurable.thread_id)}`;
      const blobs = await f.document.send(
        new QueryCommand({
          TableName: f.tableName,
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :p)',
          ExpressionAttributeValues: { ':pk': pk, ':p': 'B#' },
        }),
      );
      const sk =
        kind === 'channel'
          ? blobs.Items![0]!.SK
          : kind === 'head'
            ? headKey('')
            : kind === 'root'
              ? 'ROOT'
              : cpKey('', '01');
      await f.document.send(new DeleteCommand({ TableName: f.tableName, Key: { PK: pk, SK: sk } }));
      await expect(f.saver.getTuple(cfg)).rejects.toMatchObject({ code: 'CORRUPT_CHECKPOINT' });
    },
  );
  it.each([1, 2])(
    'keeps committed S3 data when acknowledgement %s is lost',
    async (failedTransaction) => {
      const s3 = memoryS3();
      const f = await setup({ s3: s3.options, compression: false });
      const original = f.document.send.bind(f.document);
      let transaction = 0;
      const injected = {
        send: async (command: Parameters<typeof f.document.send>[0]) => {
          const result = await original(command);
          if (
            command.constructor.name === 'TransactWriteCommand' &&
            ++transaction === failedTransaction
          ) {
            throw new Error('Simulated acknowledgement loss');
          }
          return result;
        },
      } as unknown as DynamoDBDocumentClient;
      const saver = new DynamoDBSaver({
        tableName: f.tableName,
        client: injected,
        s3: s3.options,
        compression: false,
      });
      const cfg = config();
      const cp = checkpoint('01', { image: new Uint8Array(randomBytes(500000)) });
      await expect(saver.put(cfg, cp, metadata, cp.channel_versions)).rejects.toThrow(
        'acknowledgement',
      );
      expect(s3.objects.size).toBe(1);
      await saver.put(cfg, cp, metadata, cp.channel_versions);
      expect((await saver.getTuple(cfg))?.checkpoint.channel_values.image).toEqual(
        cp.channel_values.image,
      );
      saver.destroy();
    },
  );
  it('rejects a missing S3 object instead of returning empty state', async () => {
    const s3 = memoryS3();
    const f = await setup({ s3: s3.options, compression: false });
    const cfg = config();
    const cp = checkpoint('01', { image: new Uint8Array(randomBytes(500000)) });
    await f.saver.put(cfg, cp, metadata, cp.channel_versions);
    s3.objects.clear();
    await expect(f.saver.getTuple(cfg)).rejects.toMatchObject({ code: 'CORRUPT_CHECKPOINT' });
  });
  it('preserves a deletion marker and retries S3 failures before removing pointers', async () => {
    const s3 = memoryS3();
    const f = await setup({ s3: s3.options, compression: false });
    const cfg = config();
    const cp = checkpoint('01', { image: randomBytes(500000) });
    await f.saver.put(cfg, cp, metadata, cp.channel_versions);
    s3.state.denyDelete = true;
    await expect(f.saver.deleteThread(cfg.configurable.thread_id)).rejects.toMatchObject({
      code: 'DELETE_INCOMPLETE',
    });
    await expect(f.saver.put(cfg, cp, metadata, cp.channel_versions)).rejects.toMatchObject({
      code: 'THREAD_DELETED',
    });
    expect(s3.objects.size).toBe(1);
    s3.state.denyDelete = false;
    await f.saver.deleteThread(cfg.configurable.thread_id);
    expect(s3.objects.size).toBe(0);
    expect(await f.saver.getTuple(cfg)).toBeUndefined();
    await expect(f.saver.put(cfg, cp, metadata, cp.channel_versions)).rejects.toMatchObject({
      code: 'THREAD_DELETED',
    });
  });
  it('expires the whole thread without silently restarting it, then sweeps S3', async () => {
    const s3 = memoryS3();
    const f = await setup({ s3: s3.options, compression: false, retentionSeconds: 300 });
    const cfg = config();
    const cp = checkpoint('01', { image: randomBytes(500000) });
    await f.saver.put(cfg, cp, metadata, cp.channel_versions);
    await f.document.send(
      new UpdateCommand({
        TableName: f.tableName,
        Key: { PK: `lg#${hash(cfg.configurable.thread_id)}`, SK: 'ROOT' },
        UpdateExpression: 'SET expiresAt = :expired',
        ExpressionAttributeValues: { ':expired': 1 },
      }),
    );
    await expect(f.saver.getTuple(cfg)).rejects.toMatchObject({ code: 'THREAD_EXPIRED' });
    await expect(
      f.saver.putWrites(
        { configurable: { ...cfg.configurable, checkpoint_id: '01' } },
        [['x', 1]],
        'task',
      ),
    ).rejects.toMatchObject({ code: 'THREAD_EXPIRED' });
    expect(await f.saver.sweepExpired()).toBe(1);
    expect(s3.objects.size).toBe(0);
  });
  it('encrypts metadata and writes, isolates scopes, and handles special writes', async () => {
    const f = await setup({ cipher: testCipher() });
    const cfg = config();
    const cp = checkpoint('01', { message: 'secret-text' });
    const saved = await f.saver.put(
      cfg,
      cp,
      { ...metadata, parents: { private: 1 } } as never,
      cp.channel_versions,
    );
    await f.saver.putWrites(
      saved,
      [
        ['result', 'secret-write'],
        ['__interrupt__', 'old'],
      ],
      'task',
    );
    await f.saver.putWrites(
      saved,
      [
        ['result', 'second'],
        ['__interrupt__', 'new'],
      ],
      'task',
    );
    const writes = (await f.saver.getTuple(saved))!.pendingWrites!;
    expect(writes.find((w) => w[1] === 'result')![2]).toBe('secret-write');
    expect(writes.find((w) => w[1] === '__interrupt__')![2]).toBe('new');
    expect(await f.saver.getTuple(config(randomUUID()))).toBeUndefined();
    const all = await f.document.send(new ScanCommand({ TableName: f.tableName }));
    const text = JSON.stringify(all.Items, (_k, v: unknown) =>
      v instanceof Uint8Array ? Buffer.from(v).toString() : v,
    );
    expect(text).not.toContain('secret-text');
    expect(text).not.toContain('secret-write');
  });
  it('honors an already aborted call before any writes', async () => {
    const f = await setup();
    const controller = new AbortController();
    controller.abort();
    await expect(
      f.saver.put({ ...config(), signal: controller.signal }, checkpoint('01'), metadata, {}),
    ).rejects.toThrow();
    const result = await f.document.send(new ScanCommand({ TableName: f.tableName }));
    expect(result.Items).toHaveLength(0);
  });
  it('rejects deleting with missing S3 configuration and changed bucket versioning', async () => {
    const s3 = memoryS3();
    const f = await setup({ s3: s3.options, compression: false });
    const cfg = config();
    const cp = checkpoint('01', { image: new Uint8Array(randomBytes(500000)) });
    await f.saver.put(cfg, cp, metadata, cp.channel_versions);
    const wrong = new DynamoDBSaver({
      tableName: f.tableName,
      clientConfig: f.options.clientConfig,
    });
    try {
      await expect(wrong.deleteThread(cfg.configurable.thread_id)).rejects.toMatchObject({
        code: 'INVALID_ARGUMENT',
      });
    } finally {
      wrong.destroy();
    }
    s3.state.versioned = true;
    await expect(f.saver.deleteThread(cfg.configurable.thread_id)).rejects.toMatchObject({
      code: 'DELETE_INCOMPLETE',
    });
    expect(s3.objects.size).toBe(1);
    s3.state.versioned = false;
    s3.state.deleteError = true;
    await expect(f.saver.deleteThread(cfg.configurable.thread_id)).rejects.toMatchObject({
      code: 'DELETE_INCOMPLETE',
    });
    s3.state.deleteError = false;
    await f.saver.deleteThread(cfg.configurable.thread_id);
    expect(s3.objects.size).toBe(0);
  });
  it('does not move the head backwards when inserting an older checkpoint', async () => {
    const f = await setup();
    const cfg = config();
    await f.saver.put(cfg, checkpoint('03'), { ...metadata, step: 3 }, {});
    await f.saver.put(cfg, checkpoint('01'), { ...metadata, step: 1 }, {});
    expect((await f.saver.getTuple(cfg))!.checkpoint.id).toBe('03');
  });
});
