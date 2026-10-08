import { parseAbiItem, type Address } from 'viem';
import type { IndexerPlugin } from '../../src/index.js';

const transfer = parseAbiItem('event Transfer(address indexed from, address indexed to, uint256 value)');

/** The example's own table: one row per transfer. Balances are a query away (see README). */
export const SCHEMA = `
CREATE TABLE IF NOT EXISTS erc20_transfer (
  chain_id     INTEGER     NOT NULL,
  block_number BIGINT      NOT NULL,
  log_index    INTEGER     NOT NULL,
  tx_hash      TEXT        NOT NULL,
  token        TEXT        NOT NULL,
  from_address TEXT        NOT NULL,
  to_address   TEXT        NOT NULL,
  value        NUMERIC(78, 0) NOT NULL,
  block_time   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (chain_id, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS erc20_transfer_token ON erc20_transfer (chain_id, token, block_number);
`;

/**
 * Index ERC-20 transfers of a FIXED token list per chain.
 *
 * `Transfer` is emitted by every token on a chain, so it is address-scoped:
 * fetched only from the listed tokens, never by topic alone. The same list
 * filters the websocket subscription, so idle tokens cost nothing between
 * sweeps.
 */
export function erc20TransfersPlugin(tokens: Map<number, Address[]>): IndexerPlugin {
  const list = (chainId: number) => (tokens.get(chainId) ?? []).map((a) => a.toLowerCase() as Address);

  return {
    events: [transfer],
    addressScopedEvents: ['Transfer'],
    subscriptionAddresses: list,

    async loadWatched(chainId) {
      return list(chainId);
    },

    // Only the configured tokens. (A factory-style plugin would also accept
    // contracts its `discover` hook added to ctx.watched.)
    isTrusted(event, ctx) {
      return event.eventName === 'Transfer' && ctx.watched.has(event.address);
    },

    async handle(event, db) {
      await db.query(
        `INSERT INTO erc20_transfer
           (chain_id, block_number, log_index, tx_hash, token, from_address, to_address, value, block_time)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          event.chainId,
          event.blockNumber.toString(),
          event.logIndex,
          event.txHash,
          event.address,
          event.args.from,
          event.args.to,
          event.args.value,
          event.blockTime,
        ],
      );
    },

    async rewind(chainId, forkBlock, db) {
      await db.query('DELETE FROM erc20_transfer WHERE chain_id = $1 AND block_number > $2', [
        chainId,
        forkBlock.toString(),
      ]);
    },
  };
}
