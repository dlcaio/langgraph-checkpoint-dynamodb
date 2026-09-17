/** Stable machine-readable error codes; messages never include payload contents. */
export type CheckpointErrorCode =
  | 'INVALID_ARGUMENT'
  | 'CORRUPT_CHECKPOINT'
  | 'THREAD_EXPIRED'
  | 'THREAD_DELETED'
  | 'CONCURRENT_MODIFICATION'
  | 'PAYLOAD_TOO_LARGE'
  | 'CIPHER_REQUIRED'
  | 'DELETE_INCOMPLETE';

export class CheckpointStorageError extends Error {
  override readonly name = 'CheckpointStorageError';
  constructor(
    readonly code: CheckpointErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export function invalid(message: string): never {
  throw new CheckpointStorageError('INVALID_ARGUMENT', message);
}

export function corrupt(message: string, cause?: unknown): never {
  throw new CheckpointStorageError('CORRUPT_CHECKPOINT', message, { cause });
}

export function isConditionalFailure(error: unknown): boolean {
  const e = error as { name?: string; CancellationReasons?: { Code?: string }[] };
  return (
    e.name === 'ConditionalCheckFailedException' ||
    (e.name === 'TransactionCanceledException' &&
      e.CancellationReasons?.some((reason) => reason.Code === 'ConditionalCheckFailed') === true)
  );
}
