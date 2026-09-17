import { describe, expect, it } from 'vitest';
import { MemorySaver } from '@langchain/langgraph-checkpoint';
import { PayloadStore } from '../src/payload.js';
import { testCipher, memoryS3 } from './helpers.js';
import { DynamoDBSaver } from '../src/index.js';

describe('payload codec', () => {
  it('compresses before authenticated encryption and reconstructs typed values', async () => {
    const codec = new PayloadStore(
      { tableName: 'test', cipher: testCipher() },
      new MemorySaver().serde,
    );
    const value = {
      text: 'Repeated useful state '.repeat(10000),
      binary: new Uint8Array([1, 2, 3]),
    };
    const saved = await codec.encode(value, 'scope', 'test/');
    expect(saved.compressed).toBe(true);
    expect(saved.encodedSize).toBeLessThan(2000);
    expect(await codec.decode(saved, 'scope', 'test/')).toEqual(value);
    await expect(codec.decode(saved, 'other-scope', 'test/')).rejects.toThrow();
  });
  it('rejects corruption, unknown formats, and oversized state', async () => {
    const codec = new PayloadStore(
      { tableName: 'test', maxPayloadBytes: 1024 },
      new MemorySaver().serde,
    );
    const saved = await codec.encode({ hello: 'world' }, 'scope', 'test/');
    await expect(
      codec.decode({ ...saved, digest: 'wrong' }, 'scope', 'test/'),
    ).rejects.toMatchObject({ code: 'CORRUPT_CHECKPOINT' });
    await expect(
      codec.decode({ ...saved, format: 2 } as never, 'scope', 'test/'),
    ).rejects.toMatchObject({ code: 'CORRUPT_CHECKPOINT' });
    await expect(codec.encode('x'.repeat(2000), 'scope', 'test/')).rejects.toMatchObject({
      code: 'PAYLOAD_TOO_LARGE',
    });
  });
  it('rejects versioned buckets and references outside the caller scope', async () => {
    const s3 = memoryS3();
    const codec = new PayloadStore(
      { tableName: 'test', maxInlineBytes: 1024, compression: false, s3: s3.options },
      new MemorySaver().serde,
    );
    s3.state.versioned = true;
    await expect(codec.encode('x'.repeat(2000), 'scope', 'test/')).rejects.toMatchObject({
      code: 'INVALID_ARGUMENT',
    });
    s3.state.versioned = false;
    const saved = await codec.encode('x'.repeat(2000), 'scope', 'test/');
    await expect(codec.decode(saved, 'scope', 'other/')).rejects.toMatchObject({
      code: 'CORRUPT_CHECKPOINT',
    });
  });
  it('validates configuration before making network calls', () => {
    expect(() => new DynamoDBSaver({ tableName: 'test', retentionSeconds: -1 })).toThrow();
    expect(() => new DynamoDBSaver({ tableName: 'test', maxInlineBytes: 400000 })).toThrow();
    expect(
      () => new DynamoDBSaver({ tableName: 'test', s3: { bucketName: 'test', keyPrefix: '' } }),
    ).toThrow();
  });
});
