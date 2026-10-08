import { spawn, type ChildProcess } from 'node:child_process';
import {
  createPublicClient,
  createWalletClient,
  http,
  type Address,
  type Hex,
  type PublicClient,
  type WalletClient,
} from 'viem';
import { foundry } from 'viem/chains';
import pg from 'pg';
import { testTokenAbi, testTokenBytecode } from './test-token.js';

export interface Anvil {
  rpcUrl: string;
  stop(): Promise<void>;
  publicClient: PublicClient;
  wallet: WalletClient;
  account: Address;
  rpc<T = unknown>(method: string, params?: unknown[]): Promise<T>;
}

/** A fresh local chain (automine). The first default account is unlocked, so no key is needed. */
export async function startAnvil(port: number): Promise<Anvil> {
  const proc: ChildProcess = spawn('anvil', ['--port', String(port), '--silent', '--chain-id', '31337'], {
    stdio: 'ignore',
  });
  const rpcUrl = `http://127.0.0.1:${port}`;
  const rpc = async <T>(method: string, params: unknown[] = []): Promise<T> => {
    const res = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
    });
    const body = (await res.json()) as { result?: T; error?: { message: string } };
    if (body.error) throw new Error(body.error.message);
    return body.result as T;
  };

  const deadline = Date.now() + 30_000;
  for (;;) {
    try {
      await rpc('eth_chainId');
      break;
    } catch {
      if (Date.now() > deadline) throw new Error('anvil did not start (is Foundry installed?)');
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  const [account] = await rpc<Address[]>('eth_accounts');
  const publicClient = createPublicClient({ chain: foundry, transport: http(rpcUrl), pollingInterval: 50 }) as PublicClient;
  const wallet = createWalletClient({ chain: foundry, transport: http(rpcUrl), account: account! });
  return {
    rpcUrl,
    publicClient,
    wallet,
    account: account!,
    rpc,
    async stop() {
      proc.kill();
    },
  };
}

export async function deployToken(anvil: Anvil): Promise<{ token: Address; deployBlock: bigint }> {
  const hash = await anvil.wallet.deployContract({
    abi: testTokenAbi,
    bytecode: testTokenBytecode,
    account: anvil.account,
    chain: foundry,
  });
  const receipt = await anvil.publicClient.waitForTransactionReceipt({ hash });
  return { token: receipt.contractAddress!, deployBlock: receipt.blockNumber };
}

export async function transfer(anvil: Anvil, token: Address, to: Address, value: bigint): Promise<{ block: bigint; hash: Hex }> {
  const hash = await anvil.wallet.writeContract({
    address: token,
    abi: testTokenAbi,
    functionName: 'transfer',
    args: [to, value],
    account: anvil.account,
    chain: foundry,
  });
  const receipt = await anvil.publicClient.waitForTransactionReceipt({ hash });
  return { block: receipt.blockNumber, hash };
}

/** A clean slate in the database the e2e suite was pointed at. */
export async function resetDatabase(databaseUrl: string): Promise<void> {
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    await client.query(
      'DROP TABLE IF EXISTS erc20_transfer, raw_event, block_checkpoint, indexer_cursor, range_progress',
    );
  } finally {
    await client.end();
  }
}

export async function until<T>(fn: () => Promise<T | null | undefined | false>, timeoutMs: number, what: string): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}
