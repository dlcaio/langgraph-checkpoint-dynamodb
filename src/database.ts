import { randomUUID } from 'node:crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import type { TransactWriteCommandInput } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBSaverOptions, Row, ThreadRecord } from './types.js';
import { CheckpointStorageError, corrupt, invalid, isConditionalFailure } from './errors.js';
import { hash } from './keys.js';

export type TransactionItems = NonNullable<TransactWriteCommandInput['TransactItems']>;

export class Database {
  readonly client: DynamoDBDocumentClient;
  readonly table: string;
  private readonly base: DynamoDBClient | undefined;
  readonly prefix: string;
  readonly store: string;
  constructor(readonly options: DynamoDBSaverOptions) {
    if (!/^[a-zA-Z0-9_.-]{3,255}$/.test(options.tableName)) invalid('Invalid DynamoDB tableName');
    if (options.client && options.clientConfig) invalid('Specify client or clientConfig, not both');
    if (
      options.retentionSeconds !== undefined &&
      (!Number.isSafeInteger(options.retentionSeconds) ||
        options.retentionSeconds < 1 ||
        options.retentionSeconds > 5 * 365 * 86400)
    )
      invalid('retentionSeconds must be 1 second to 5 years');
    this.prefix = options.keyPrefix ?? 'lg';
    if (!/^[a-zA-Z0-9_-]{1,64}$/.test(this.prefix))
      invalid('keyPrefix must be 1–64 alphanumeric, underscore or hyphen characters');
    this.table = options.tableName;
    this.store = options.s3
      ? hash(JSON.stringify([options.s3.bucketName, options.s3.keyPrefix]))
      : 'inline';
    if (options.client) this.client = options.client;
    else {
      this.base = new DynamoDBClient(options.clientConfig ?? {});
      this.client = DynamoDBDocumentClient.from(this.base, {
        marshallOptions: { removeUndefinedValues: true },
      });
    }
  }
  pk(thread: string): string {
    return `${this.prefix}#${hash(thread)}`;
  }
  async get(pk: string, sk: string): Promise<Row | undefined> {
    const result = await this.client.send(
      new GetCommand({ TableName: this.table, Key: { PK: pk, SK: sk }, ConsistentRead: true }),
    );
    return result.Item as Row | undefined;
  }
  async root(pk: string): Promise<ThreadRecord | undefined> {
    const row = await this.get(pk, 'ROOT');
    if (!row) {
      for await (const _orphan of this.query(pk))
        corrupt('Thread control record is missing while data remains');
      return undefined;
    }
    if (row.store !== this.store)
      invalid('S3 configuration differs from the thread storage configuration');
    if (
      row.format !== 1 ||
      typeof row.generation !== 'string' ||
      !['active', 'deleting', 'deleted'].includes(String(row.state)) ||
      (row.expiresAt !== undefined &&
        (!Number.isSafeInteger(row.expiresAt) || Number(row.expiresAt) < 1))
    )
      corrupt('Invalid thread control record');
    return row as unknown as ThreadRecord;
  }
  active(root: ThreadRecord): void {
    if (root.state !== 'active')
      throw new CheckpointStorageError(
        'THREAD_DELETED',
        'Thread is deleted or being deleted; use a new thread_id',
      );
    if (root.expiresAt !== undefined && root.expiresAt <= Math.floor(Date.now() / 1000))
      throw new CheckpointStorageError(
        'THREAD_EXPIRED',
        'Thread retention window has expired; use a new thread_id',
      );
  }
  async ensureRoot(pk: string): Promise<ThreadRecord> {
    let root = await this.root(pk);
    if (!root) {
      const created: ThreadRecord = {
        PK: pk,
        SK: 'ROOT',
        format: 1,
        generation: randomUUID(),
        state: 'active',
        store: this.store,
        ...(this.options.retentionSeconds === undefined
          ? {}
          : { expiresAt: Math.floor(Date.now() / 1000) + this.options.retentionSeconds }),
      };
      try {
        await this.client.send(
          new PutCommand({
            TableName: this.table,
            Item: created,
            ConditionExpression: 'attribute_not_exists(PK)',
          }),
        );
      } catch (error) {
        if (!isConditionalFailure(error)) throw error;
      }
      root = await this.root(pk);
      if (!root) corrupt('Thread control record disappeared after creation');
    }
    this.active(root);
    return root;
  }
  guard(root: ThreadRecord): TransactionItems[number] {
    return {
      ConditionCheck: {
        TableName: this.table,
        Key: { PK: root.PK, SK: 'ROOT' },
        ConditionExpression:
          '#state = :active AND generation = :generation AND (attribute_not_exists(expiresAt) OR expiresAt > :now)',
        ExpressionAttributeNames: { '#state': 'state' },
        ExpressionAttributeValues: {
          ':active': 'active',
          ':generation': root.generation,
          ':now': Math.floor(Date.now() / 1000),
        },
      },
    };
  }
  async commit(root: ThreadRecord, items: TransactionItems, namespace?: string): Promise<void> {
    const guard = this.guard(root);
    const first =
      namespace === undefined
        ? guard
        : {
            Update: {
              ...guard.ConditionCheck!,
              UpdateExpression: 'ADD namespaces :namespace',
              ExpressionAttributeValues: {
                ...guard.ConditionCheck!.ExpressionAttributeValues,
                ':namespace': new Set([hash(namespace)]),
              },
            },
          };
    await this.client.send(
      new TransactWriteCommand({
        TransactItems: [first, ...items],
        ClientRequestToken: randomUUID(),
      }),
    );
  }
  ttl(root: ThreadRecord): { ttl?: number } {
    return root.expiresAt === undefined ? {} : { ttl: root.expiresAt };
  }
  async *query(pk: string, prefix = ''): AsyncGenerator<Row> {
    let cursor: Record<string, unknown> | undefined;
    do {
      const result = await this.client.send(
        new QueryCommand({
          TableName: this.table,
          ConsistentRead: true,
          KeyConditionExpression: prefix ? 'PK = :pk AND begins_with(SK, :prefix)' : 'PK = :pk',
          ExpressionAttributeValues: prefix ? { ':pk': pk, ':prefix': prefix } : { ':pk': pk },
          ExclusiveStartKey: cursor,
          ScanIndexForward: false,
        }),
      );
      for (const row of result.Items ?? []) yield row as Row;
      cursor = result.LastEvaluatedKey;
    } while (cursor);
  }
  async *scanRoots(): AsyncGenerator<ThreadRecord> {
    let cursor: Record<string, unknown> | undefined;
    do {
      const result = await this.client.send(
        new ScanCommand({
          TableName: this.table,
          ConsistentRead: true,
          FilterExpression: 'begins_with(PK, :prefix) AND SK = :root',
          ExpressionAttributeValues: { ':prefix': `${this.prefix}#`, ':root': 'ROOT' },
          ExclusiveStartKey: cursor,
        }),
      );
      for (const row of result.Items ?? []) yield row as ThreadRecord;
      cursor = result.LastEvaluatedKey;
    } while (cursor);
  }
  destroy(): void {
    this.base?.destroy();
  }
}
