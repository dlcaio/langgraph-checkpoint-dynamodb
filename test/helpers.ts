import { randomBytes, randomUUID, createCipheriv, createDecipheriv } from 'node:crypto';
import { DynamoDBClient, CreateTableCommand, DeleteTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { S3Client } from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';
import { DynamoDBSaver } from '../src/index.js';
import type { DynamoDBSaverOptions, PayloadCipher } from '../src/index.js';
import type { Checkpoint, CheckpointMetadata } from '@langchain/langgraph-checkpoint';

export const endpoint = process.env.DYNAMODB_ENDPOINT;
if (endpoint && !['127.0.0.1', 'localhost', '[::1]'].includes(new URL(endpoint).hostname))
  throw new Error('Tests only support localhost DynamoDB');
export const clientConfig = {
  endpoint: endpoint ?? 'http://127.0.0.1:18000',
  region: 'us-east-1',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
  maxAttempts: 1,
};
export const config = (thread = randomUUID(), id?: string, ns = '') => ({
  configurable: {
    thread_id: thread,
    checkpoint_ns: ns,
    ...(id ? { checkpoint_id: id } : {}),
  },
});
export const checkpoint = (id: string, values: Record<string, unknown> = {}): Checkpoint => ({
  v: 4,
  id,
  ts: new Date().toISOString(),
  channel_values: values,
  channel_versions: Object.fromEntries(Object.keys(values).map((key) => [key, 1])),
  versions_seen: {},
});
export const metadata: CheckpointMetadata = { source: 'loop', step: 1, parents: {} };

export async function fixture(extra: Partial<DynamoDBSaverOptions> = {}) {
  const tableName = `checkpointer-test-${randomUUID()}`;
  const admin = new DynamoDBClient(clientConfig);
  const document = DynamoDBDocumentClient.from(admin);
  await admin.send(
    new CreateTableCommand({
      TableName: tableName,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'PK', AttributeType: 'S' },
        { AttributeName: 'SK', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'PK', KeyType: 'HASH' },
        { AttributeName: 'SK', KeyType: 'RANGE' },
      ],
    }),
  );
  const options = { tableName, clientConfig, ...extra };
  const saver = new DynamoDBSaver(options);
  return {
    saver,
    options,
    tableName,
    document,
    async close() {
      saver.destroy();
      await admin.send(new DeleteTableCommand({ TableName: tableName }));
      admin.destroy();
    },
  };
}
export function memoryS3() {
  const objects = new Map<string, Buffer>();
  const state = {
    denyDelete: false,
    failRead: false,
    versioned: false,
    puts: 0,
    deleteError: false,
  };
  const client = {
    async send(command: { constructor: { name: string }; input: Record<string, unknown> }) {
      const input = command.input;
      const key = String(input.Key);
      switch (command.constructor.name) {
        case 'GetBucketVersioningCommand':
          return state.versioned ? { Status: 'Enabled' } : {};
        case 'PutObjectCommand':
          state.puts++;
          objects.set(key, Buffer.from(input.Body as Uint8Array));
          return {};
        case 'GetObjectCommand': {
          if (state.failRead) throw Object.assign(new Error('Denied'), { name: 'AccessDenied' });
          const body = objects.get(key);
          if (!body) throw Object.assign(new Error('Missing'), { name: 'NoSuchKey' });
          return { ContentLength: body.length, Body: Readable.from([body]) };
        }
        case 'ListObjectsV2Command':
          return {
            Contents: [...objects.keys()]
              .filter((key) => key.startsWith(String(input.Prefix)))
              .slice(0, 1000)
              .map((Key) => ({ Key })),
          };
        case 'DeleteObjectsCommand': {
          if (state.denyDelete) throw Object.assign(new Error('Denied'), { name: 'AccessDenied' });
          if (state.deleteError) return { Errors: [{ Code: 'AccessDenied', Key: 'test' }] };
          for (const { Key } of (input.Delete as { Objects: { Key: string }[] }).Objects)
            objects.delete(Key);
          return {};
        }
        default:
          throw new Error(`Unexpected S3 call ${command.constructor.name}`);
      }
    },
    destroy() {},
  } as unknown as S3Client;
  return { objects, state, options: { bucketName: 'test-bucket', keyPrefix: 'tests/', client } };
}
export function testCipher(): PayloadCipher {
  const key = randomBytes(32);
  return {
    id: 'test-aes-gcm-v1',
    async encrypt(value, context) {
      const iv = randomBytes(12);
      const encrypt = createCipheriv('aes-256-gcm', key, iv);
      encrypt.setAAD(Buffer.from(context));
      const ciphertext = Buffer.concat([encrypt.update(value), encrypt.final()]);
      return Buffer.concat([iv, encrypt.getAuthTag(), ciphertext]);
    },
    async decrypt(value, context) {
      const bytes = Buffer.from(value);
      const decrypt = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
      decrypt.setAAD(Buffer.from(context));
      decrypt.setAuthTag(bytes.subarray(12, 28));
      return Buffer.concat([decrypt.update(bytes.subarray(28)), decrypt.final()]);
    },
  };
}
