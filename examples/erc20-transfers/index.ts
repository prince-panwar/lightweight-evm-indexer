/**
 * Index ERC-20 transfers for a token list on one EVM chain.
 *
 *   DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer \
 *   RPC_URL=https://<chain>.g.alchemy.com/v2/<key> CHAIN_ID=8453 \
 *   TOKENS=0x...,0x... START_BLOCK=<block> \
 *   npm run example:erc20
 *
 * Optional: CONFIRMATIONS (default 4), BLOCK_TIME_MS (default 2000),
 * WEBSOCKET=false to poll instead, LOG_LEVEL=debug.
 */
import { createPublicClient, http, isAddress, type Address } from 'viem';
import { createIndexer, consoleLogger } from '../../src/index.js';
import { erc20TransfersPlugin, SCHEMA } from './plugin.js';

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required (see the comment at the top of this file)`);
  return value;
}

async function main(): Promise<void> {
  const log = consoleLogger('erc20-example');
  const databaseUrl = required('DATABASE_URL');
  const rpcUrl = required('RPC_URL');
  const chainId = Number(required('CHAIN_ID'));
  const tokens = required('TOKENS')
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean);
  for (const t of tokens) if (!isAddress(t)) throw new Error(`not an address: ${t}`);

  // Default: start ~1,000 blocks back so the demo shows a short backfill, then the tip.
  const startBlock = process.env.START_BLOCK
    ? BigInt(process.env.START_BLOCK)
    : (await createPublicClient({ transport: http(rpcUrl) }).getBlockNumber()) - 1_000n;

  const indexer = createIndexer({
    databaseUrl,
    logger: log,
    chains: [
      {
        chainId,
        name: `chain-${chainId}`,
        rpcUrls: [rpcUrl],
        startBlock,
        confirmations: Number(process.env.CONFIRMATIONS ?? 4),
        blockTimeMs: Number(process.env.BLOCK_TIME_MS ?? 2_000),
        websocket: process.env.WEBSOCKET !== 'false',
      },
    ],
    plugin: erc20TransfersPlugin(new Map([[chainId, tokens as Address[]]])),
    onCommitted: (id, events) => log.info(`chain ${id}: +${events.length} transfer(s)`),
  });

  await indexer.pool.query(SCHEMA);
  await indexer.start();
  log.info(`indexing ${tokens.length} token(s) on chain ${chainId} from block ${startBlock}`);

  const shutdown = async () => {
    log.info('stopping');
    await indexer.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
