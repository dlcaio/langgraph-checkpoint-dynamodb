import type { DynamoDBClientConfig } from '@aws-sdk/client-dynamodb';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { S3Client } from '@aws-sdk/client-s3';
import type { SerializerProtocol } from '@langchain/langgraph-checkpoint';

/** Bind authenticated encryption to the storage address supplied by the saver. */
export interface PayloadCipher {
  readonly id: string;
  encrypt(plaintext: Uint8Array, context: string): Promise<Uint8Array>;
  decrypt(ciphertext: Uint8Array, context: string): Promise<Uint8Array>;
}

export interface S3Options {
  bucketName: string;
  /** Dedicated prefix. Bucket must be unversioned. */
  keyPrefix: string;
  client?: S3Client;
}

export interface DynamoDBSaverOptions {
  tableName: string;
  client?: DynamoDBDocumentClient;
  clientConfig?: DynamoDBClientConfig;
  /** Isolate independent applications sharing a table. Not an authorization boundary. */
  keyPrefix?: string;
  serde?: SerializerProtocol;
  s3?: S3Options;
  cipher?: PayloadCipher;
  /** Gzip before encryption. Enabled by default; only stores compression if smaller. */
  compression?: boolean;
  /** Fixed lifetime from first write; never extended implicitly. No TTL by default. */
  retentionSeconds?: number;
  /** Maximum serialized/decompressed size, default 32 MiB. */
  maxPayloadBytes?: number;
  /** Offload above this encoded size, default 300 KiB; maximum 350 KiB. */
  maxInlineBytes?: number;
}

export interface StoredPayload {
  format: 1;
  type: string;
  compressed: boolean;
  cipher: string | null;
  rawSize: number;
  encodedSize: number;
  digest: string;
  bytes?: Uint8Array;
  objectKey?: string;
}

export interface ThreadRecord {
  PK: string;
  SK: 'ROOT';
  format: 1;
  generation: string;
  state: 'active' | 'deleting' | 'deleted';
  store: string;
  expiresAt?: number;
  namespaces?: Set<string>;
}

export type Row = Record<string, unknown> & { PK: string; SK: string };
