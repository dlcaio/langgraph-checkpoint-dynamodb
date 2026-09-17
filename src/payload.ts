import { randomUUID } from 'node:crypto';
import { promisify } from 'node:util';
import { gzip, gunzip } from 'node:zlib';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  GetBucketVersioningCommand,
} from '@aws-sdk/client-s3';
import type { SerializerProtocol } from '@langchain/langgraph-checkpoint';
import type { DynamoDBSaverOptions, StoredPayload } from './types.js';
import { CheckpointStorageError, corrupt, invalid } from './errors.js';
import { hash } from './keys.js';

const zip = promisify(gzip);
const unzip = promisify(gunzip);

/** Internal codec. Never deletes an upload in response to an ambiguous database error. */
export class PayloadStore {
  readonly maxBytes: number;
  readonly inlineBytes: number;
  private readonly s3: S3Client | undefined;
  private checkedBucket: Promise<void> | undefined;
  constructor(
    private readonly options: DynamoDBSaverOptions,
    private readonly serde: SerializerProtocol,
  ) {
    this.maxBytes = options.maxPayloadBytes ?? 32 * 1024 * 1024;
    this.inlineBytes = options.maxInlineBytes ?? 300 * 1024;
    for (const [name, value, max] of [
      ['maxPayloadBytes', this.maxBytes, 1024 ** 3],
      ['maxInlineBytes', this.inlineBytes, 350 * 1024],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 1024 || value > max) invalid(`Invalid ${name}`);
    }
    if (options.cipher && (!options.cipher.id || options.cipher.id.length > 128))
      invalid('cipher.id must identify the payload encryption format');
    if (options.s3) {
      if (
        !options.s3.bucketName ||
        !options.s3.keyPrefix ||
        !options.s3.keyPrefix.endsWith('/') ||
        options.s3.keyPrefix.length > 128
      )
        invalid(
          'S3 requires a bucketName and a non-empty keyPrefix ending in /, at most 128 characters',
        );
      this.s3 =
        options.s3.client ??
        new S3Client(options.clientConfig?.region ? { region: options.clientConfig.region } : {});
    }
  }
  prefix(pk: string, generation?: string): string {
    return `${this.options.s3?.keyPrefix ?? ''}${hash(pk)}/${generation ? `${generation}/` : ''}`;
  }
  private async checkBucket(force = false): Promise<void> {
    if (!this.s3 || !this.options.s3) return;
    if (force) this.checkedBucket = undefined;
    this.checkedBucket ??= this.s3
      .send(new GetBucketVersioningCommand({ Bucket: this.options.s3.bucketName }))
      .then((result) => {
        if (result.Status)
          invalid('S3 bucket must be unversioned; versioned deletion is not supported');
      })
      .catch((error) => {
        this.checkedBucket = undefined;
        throw error;
      });
    return this.checkedBucket;
  }
  async encode(value: unknown, context: string, prefix: string): Promise<StoredPayload> {
    const [type, serialized] = await this.serde.dumpsTyped(value);
    if (serialized.length > this.maxBytes)
      throw new CheckpointStorageError(
        'PAYLOAD_TOO_LARGE',
        'Serialized payload exceeds maxPayloadBytes',
      );
    let data = serialized;
    let compressed = false;
    if (this.options.compression !== false && data.length >= 1024) {
      const smaller = await zip(data);
      if (smaller.length < data.length) {
        data = smaller;
        compressed = true;
      }
    }
    if (this.options.cipher) data = await this.options.cipher.encrypt(data, context);
    if (data.length > this.maxBytes * 2 + 16384)
      throw new CheckpointStorageError('PAYLOAD_TOO_LARGE', 'Encoded payload exceeds safety limit');
    const descriptor: StoredPayload = {
      format: 1,
      type,
      compressed,
      cipher: this.options.cipher?.id ?? null,
      rawSize: serialized.length,
      encodedSize: data.length,
      digest: hash(data),
    };
    if (data.length <= this.inlineBytes) return { ...descriptor, bytes: data };
    if (!this.s3 || !this.options.s3)
      throw new CheckpointStorageError('PAYLOAD_TOO_LARGE', 'Encoded payload requires S3 offload');
    await this.checkBucket();
    const objectKey = `${prefix}${randomUUID()}`;
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.options.s3.bucketName,
        Key: objectKey,
        Body: data,
        ContentType: 'application/octet-stream',
      }),
    );
    return { ...descriptor, objectKey };
  }
  async decode(descriptor: StoredPayload, context: string, prefix: string): Promise<unknown> {
    if (
      !descriptor ||
      descriptor.format !== 1 ||
      typeof descriptor.type !== 'string' ||
      !Number.isSafeInteger(descriptor.rawSize) ||
      descriptor.rawSize < 0 ||
      descriptor.rawSize > this.maxBytes ||
      !Number.isSafeInteger(descriptor.encodedSize) ||
      descriptor.encodedSize < 0 ||
      descriptor.encodedSize > this.maxBytes * 2 + 16384 ||
      typeof descriptor.compressed !== 'boolean' ||
      typeof descriptor.digest !== 'string' ||
      (descriptor.objectKey === undefined) === (descriptor.bytes === undefined)
    )
      corrupt('Invalid payload descriptor');
    let data: Uint8Array;
    if (descriptor.objectKey !== undefined) {
      if (!this.s3 || !this.options.s3)
        corrupt('S3 configuration is required to read this payload');
      if (
        !descriptor.objectKey.startsWith(prefix) ||
        descriptor.objectKey.slice(prefix.length).includes('/')
      )
        corrupt('S3 reference is outside its storage scope');
      try {
        const result = await this.s3.send(
          new GetObjectCommand({ Bucket: this.options.s3.bucketName, Key: descriptor.objectKey }),
        );
        if (
          !result.Body ||
          (result.ContentLength !== undefined && result.ContentLength !== descriptor.encodedSize)
        )
          corrupt('S3 payload size mismatch');
        const chunks: Uint8Array[] = [];
        let size = 0;
        for await (const chunk of result.Body as AsyncIterable<Uint8Array>) {
          size += chunk.length;
          if (size > descriptor.encodedSize) corrupt('S3 payload exceeds declared size');
          chunks.push(chunk);
        }
        data = Buffer.concat(chunks);
      } catch (error) {
        if ((error as { name?: string }).name === 'NoSuchKey')
          corrupt('Referenced S3 payload is missing', error);
        throw error;
      }
    } else {
      if (!(descriptor.bytes instanceof Uint8Array)) corrupt('Invalid inline payload');
      data = descriptor.bytes;
    }
    if (data.length !== descriptor.encodedSize || hash(data) !== descriptor.digest)
      corrupt('Payload integrity check failed');
    if (descriptor.cipher !== null) {
      if (!this.options.cipher || this.options.cipher.id !== descriptor.cipher)
        throw new CheckpointStorageError('CIPHER_REQUIRED', 'Matching payload cipher is required');
      data = await this.options.cipher.decrypt(data, context);
    } else if (this.options.cipher) {
      corrupt('Unencrypted payload encountered with an encryption-only saver');
    }
    try {
      if (descriptor.compressed) data = await unzip(data, { maxOutputLength: this.maxBytes });
      if (data.length !== descriptor.rawSize) corrupt('Decoded payload size mismatch');
      return await this.serde.loadsTyped(descriptor.type, data);
    } catch (error) {
      corrupt('Payload could not be deserialized', error);
    }
  }
  /** Caller must stop existing writers first; a thread tombstone blocks new commits. */
  async deletePrefix(prefix: string): Promise<void> {
    if (!this.s3 || !this.options.s3) return;
    await this.checkBucket(true);
    // Start at the beginning after every deletion. Keeps retries independent of stale cursors.
    while (true) {
      const page = await this.s3.send(
        new ListObjectsV2Command({
          Bucket: this.options.s3.bucketName,
          Prefix: prefix,
          MaxKeys: 1000,
        }),
      );
      const objects = (page.Contents ?? []).map((item) => {
        if (!item.Key?.startsWith(prefix)) corrupt('Invalid S3 listing scope');
        return { Key: item.Key };
      });
      if (objects.length === 0) return;
      const result = await this.s3.send(
        new DeleteObjectsCommand({
          Bucket: this.options.s3.bucketName,
          Delete: { Objects: objects, Quiet: true },
        }),
      );
      if (result.Errors?.length)
        throw new CheckpointStorageError(
          'DELETE_INCOMPLETE',
          'S3 did not delete every object; retry deleteThread',
        );
    }
  }
  destroy(): void {
    if (this.s3 && !this.options.s3?.client) this.s3.destroy();
  }
}
