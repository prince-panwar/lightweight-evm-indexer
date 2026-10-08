import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

export type Pool = pg.Pool;
export type PoolClient = pg.PoolClient;

export function createPool(connectionString: string, max = 10): Pool {
  return new pg.Pool({ connectionString, max });
}

/** Create the engine's tables if they do not exist (idempotent). */
export async function migrate(pool: Pool): Promise<void> {
  const sql = readFileSync(fileURLToPath(new URL('./schema.sql', import.meta.url)), 'utf8');
  await pool.query(sql);
}

/**
 * Run `fn` in one transaction: COMMIT on success, ROLLBACK on any throw.
 * `statement_timeout` bounds a runaway batch instead of holding locks forever.
 */
export async function withTransaction<T>(
  pool: Pool,
  fn: (db: PoolClient) => Promise<T>,
  timeoutMs = 120_000,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL statement_timeout = ${Math.floor(timeoutMs)}`);
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
