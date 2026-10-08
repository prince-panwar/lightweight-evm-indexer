import { afterEach, describe, expect, it } from 'vitest';
import type { Address } from 'viem';
import pg from 'pg';
import { createIndexer, type RunningIndexer } from '../../src/index.js';
import type { Logger } from '../../src/types.js';
import { erc20TransfersPlugin, SCHEMA } from '../../examples/erc20-transfers/plugin.js';
import { deployToken, resetDatabase, startAnvil, transfer, until, type Anvil } from './harness.js';

const DATABASE_URL = process.env.DATABASE_URL;
const describeE2E = DATABASE_URL ? describe : describe.skip;
const quiet: Logger = { debug() {}, info() {}, warn() {}, error: (m) => console.error(m) };
const ALICE = '0x00000000000000000000000000000000000a11ce' as Address;
const BOB = '0x0000000000000000000000000000000000000b0b' as Address;

describeE2E('indexer end to end (anvil + Postgres)', () => {
  let anvil: Anvil | undefined;
  let indexer: RunningIndexer | undefined;
  let db: pg.Client | undefined;

  afterEach(async () => {
    await indexer?.stop();
    await db?.end();
    await anvil?.stop();
    indexer = undefined;
    db = undefined;
    anvil = undefined;
  });

  async function boot(port: number, opts: { websocket: boolean; sweepMs: number }) {
    await resetDatabase(DATABASE_URL!);
    anvil = await startAnvil(port);
    const { token, deployBlock } = await deployToken(anvil);
    db = new pg.Client({ connectionString: DATABASE_URL });
    await db.connect();
    await db.query(SCHEMA);

    indexer = createIndexer({
      databaseUrl: DATABASE_URL!,
      logger: quiet,
      pollMs: 250,
      sweepMs: opts.sweepMs,
      usageLogMs: 0,
      chains: [
        {
          chainId: 31337,
          name: 'anvil',
          rpcUrls: [anvil.rpcUrl],
          startBlock: deployBlock,
          confirmations: 0,
          blockTimeMs: 1_000,
          websocket: opts.websocket,
        },
      ],
      plugin: erc20TransfersPlugin(new Map([[31337, [token]]])),
    });
    await indexer.start();
    const rows = async () =>
      (await db!.query<{ to_address: string; value: string; block_number: string }>(
        'SELECT to_address, value, block_number FROM erc20_transfer ORDER BY block_number, log_index',
      )).rows;
    await until(async () => (await rows()).length === 1, 15_000, 'the mint to be indexed');
    return { token, rows };
  }

  it('ingests through the websocket trigger alone, about a second per event', async () => {
    // The sweep is pushed out of reach: only the trigger can ingest.
    const { token, rows } = await boot(8611, { websocket: true, sweepMs: 10 * 60_000 });
    await new Promise((r) => setTimeout(r, 1_500)); // let the post-subscribe catch-up pass finish

    const latencies: number[] = [];
    for (let i = 1; i <= 3; i++) {
      const t0 = Date.now(); // from the moment the transaction is sent
      await transfer(anvil!, token, ALICE, BigInt(i));
      await until(async () => (await rows()).length === i + 1, 10_000, `transfer ${i}`);
      latencies.push(Date.now() - t0);
    }
    console.log(`send -> indexed, websocket-triggered (ms): ${latencies.join(', ')}`);
    expect(Math.max(...latencies)).toBeLessThan(5_000);
    expect((await rows()).slice(1).map((r) => r.value)).toEqual(['1', '2', '3']);
  });

  it('rewinds a reorg: orphaned rows are deleted and the new fork is ingested', async () => {
    const { token, rows } = await boot(8612, { websocket: false, sweepMs: 60_000 });

    const snapshot = await anvil!.rpc<string>('evm_snapshot');
    const orphan = await transfer(anvil!, token, ALICE, 111n);
    await until(async () => (await rows()).some((r) => r.value === '111'), 10_000, 'the soon-orphaned transfer');

    // Replace that block with a different one at the same height, then build on it.
    await anvil!.rpc('evm_revert', [snapshot]);
    const replacement = await transfer(anvil!, token, BOB, 222n);
    expect(replacement.block).toBe(orphan.block);
    await anvil!.rpc('anvil_mine', ['0x2']);

    await until(
      async () => {
        const r = await rows();
        return r.some((x) => x.value === '222') && !r.some((x) => x.value === '111');
      },
      15_000,
      'the reorg to be rewound and the new fork ingested',
    );
    const final = await rows();
    expect(final.map((r) => r.value)).toEqual(['1000000000000000000000000', '222']);
    expect(final[1]!.to_address).toBe(BOB);

    const raw = await db!.query("SELECT count(*)::int AS n FROM raw_event WHERE args->>'value' = '111'");
    expect(raw.rows[0].n).toBe(0);
  });
});
