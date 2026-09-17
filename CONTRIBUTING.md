# Contributing

Use Node.js 22 or later and Docker. Install the exact development dependencies:

```sh
npm ci --legacy-peer-deps
```

The legacy peer-resolution flag works around npm's resolution of the upstream validation package's test-runner peers. Consumers do not need this flag.

Start a disposable local database:

```sh
docker run --rm -d --name checkpoint-ddb -p 127.0.0.1:18000:8000 \
  amazon/dynamodb-local:3.3.1 -jar DynamoDBLocal.jar -inMemory -sharedDb
npm run test:integration
docker stop checkpoint-ddb
```

Tests create and delete temporary tables. The test harness rejects non-local endpoints. S3 is simulated in memory to inject missing data, permission failures, and lost acknowledgements without cloud credentials. `npm test` alone runs codec tests and skips database-dependent tests; use `test:integration` before sending a pull request.

```sh
npm run typecheck
npm run format:check
npm run build
npx publint
npm pack --dry-run
```

For changes to persistence behavior, add a test that demonstrates the actual failure or recovery contract. Keep the official LangGraph validation suite passing. Avoid tests that merely repeat implementation logic. Changes to the storage format require explicit compatibility and migration documentation.

This project is intentionally application-neutral. Do not add workflow-specific state types, tenancy assumptions, or agent business logic. Open an issue before undertaking a major storage-layout change.

## Release checklist

1. Run the full local integration suite, type checking, formatting, and package validation.
2. Update the changelog and version. Review `npm pack --dry-run` for accidental files.
3. Install the tarball in an empty ESM project and verify imports and types.
4. Commit the reviewed release, publish with `npm publish --access public`, and tag the matching commit.
5. Verify the registry's package metadata and install the published version in an empty project.

Publishing requires the maintainer's npm authentication and any account-required second factor. Never commit registry tokens. Prefer trusted publishing when a release workflow has been configured and verified; this repository's CI deliberately has no publishing credentials.
