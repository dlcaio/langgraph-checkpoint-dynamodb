import { createHash } from 'node:crypto';
import type { RunnableConfig } from '@langchain/core/runnables';
import { invalid } from './errors.js';

export const hash = (value: string | Uint8Array): string =>
  createHash('sha256').update(value).digest('hex');
export const encode = (value: string): string => Buffer.from(value).toString('hex');
export function identifier(value: unknown, name: string, empty = false): string {
  if (typeof value !== 'string' || (!empty && !value) || Buffer.byteLength(value) > 256)
    invalid(`${name} must be ${empty ? 'a' : 'a non-empty'} string of at most 256 UTF-8 bytes`);
  return value;
}
export function address(config: RunnableConfig, requiredId = false) {
  const thread = identifier(config.configurable?.thread_id, 'thread_id');
  const ns = identifier(config.configurable?.checkpoint_ns ?? '', 'checkpoint_ns', true);
  const rawId = config.configurable?.checkpoint_id || undefined;
  const id = rawId === undefined && !requiredId ? undefined : identifier(rawId, 'checkpoint_id');
  return { thread, ns, id };
}
export const cpKey = (ns: string, id: string): string => `C#${encode(ns)}#${encode(id)}`;
export const cpPrefix = (ns?: string): string => (ns === undefined ? 'C#' : `C#${encode(ns)}#`);
export const headKey = (ns: string): string => `H#${encode(ns)}`;
export const blobKey = (ns: string, channel: string, version: string | number): string =>
  `B#${hash(JSON.stringify([ns, channel, version]))}`;
export const writePrefix = (ns: string, id: string): string => `W#${encode(ns)}#${encode(id)}#`;
export const writeKey = (ns: string, id: string, task: string, index: number): string =>
  `${writePrefix(ns, id)}${hash(task)}#${String(index + 2147483648).padStart(10, '0')}`;
export const configFor = (thread: string, ns: string, id: string): RunnableConfig => ({
  configurable: { thread_id: thread, checkpoint_ns: ns, checkpoint_id: id },
});
