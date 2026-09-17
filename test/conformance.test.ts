import { describe } from 'vitest';
import { validate, deltaChannelHistoryTests } from '@langchain/langgraph-checkpoint-validation';
import type { CheckpointSaverTestInitializer } from '@langchain/langgraph-checkpoint-validation';
import type { DynamoDBSaver } from '../src/index.js';
import { endpoint, fixture } from './helpers.js';

describe.skipIf(!endpoint)('official LangGraph checkpointer validation', () => {
  const fixtures = new Map<DynamoDBSaver, Awaited<ReturnType<typeof fixture>>>();
  const initializer: CheckpointSaverTestInitializer<DynamoDBSaver> = {
    checkpointerName: 'DynamoDBSaver',
    beforeAllTimeout: 60000,
    async createCheckpointer() {
      const f = await fixture();
      fixtures.set(f.saver, f);
      return f.saver;
    },
    async destroyCheckpointer(saver) {
      await fixtures.get(saver)?.close();
      fixtures.delete(saver);
    },
  };
  validate(initializer);
  deltaChannelHistoryTests(initializer);
});
