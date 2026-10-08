import pg from 'pg';
import type { Logger } from '../types.js';
import { errorMessage } from '../logger.js';

/**
 * Single-writer leadership via a Postgres session advisory lock.
 *
 * The indexer must have exactly one writer per database. The lock lives on a
 * dedicated connection: whoever holds it is the leader, and it is released
 * when that connection ends (process exit, crash, network loss), so a second
 * instance simply idles as a warm standby until then. That makes a
 * zero-downtime redeploy safe: start the new instance, let it wait, stop the
 * old one.
 */
export class LeaderLock {
  private client: pg.Client | null = null;
  private leader = false;
  private inFlight: Promise<boolean> | null = null;

  constructor(
    private readonly connectionString: string,
    private readonly lockKey: bigint,
    private readonly log: Logger,
  ) {}

  isLeader(): boolean {
    return this.leader;
  }

  /** Try to take (or confirm) leadership. Never blocks. */
  async acquire(): Promise<boolean> {
    if (this.leader) return true;
    this.inFlight ??= this.attempt().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async attempt(): Promise<boolean> {
    if (!this.client) {
      const client = new pg.Client({ connectionString: this.connectionString });
      client.on('error', (err) => {
        this.log.error(`leader connection lost: ${err.message}`);
        this.leader = false;
        this.client = null;
      });
      await client.connect();
      this.client = client;
    }
    const { rows } = await this.client.query<{ locked: boolean }>('SELECT pg_try_advisory_lock($1) AS locked', [
      this.lockKey.toString(),
    ]);
    this.leader = rows[0]?.locked === true;
    if (this.leader) this.log.info('acquired indexer leadership');
    return this.leader;
  }

  async release(): Promise<void> {
    const client = this.client;
    if (!client) return;
    this.client = null;
    const wasLeader = this.leader;
    this.leader = false;
    try {
      if (wasLeader) await client.query('SELECT pg_advisory_unlock($1)', [this.lockKey.toString()]);
      await client.end();
    } catch (err) {
      this.log.debug(`leader connection already closed: ${errorMessage(err)}`);
    }
  }
}
