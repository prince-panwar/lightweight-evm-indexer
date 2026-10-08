import { defineConfig } from 'vitest/config';

/**
 * End-to-end: a local anvil chain + Postgres. Needs `anvil` on PATH
 * (Foundry) and DATABASE_URL pointing at a throwaway database, e.g.
 *   docker compose up -d postgres
 *   DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer npm run test:e2e
 */
export default defineConfig({
  test: {
    include: ['test/e2e/**/*.e2e.test.ts'],
    environment: 'node',
    testTimeout: 120_000,
    hookTimeout: 120_000,
    fileParallelism: false,
  },
});
