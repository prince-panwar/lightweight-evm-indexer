import type { Logger } from './types.js';

const LEVELS = ['debug', 'info', 'warn', 'error'] as const;
type Level = (typeof LEVELS)[number];

/**
 * A minimal leveled logger on stdout/stderr. Pass your own `Logger` (pino,
 * winston, Nest's) to `createIndexer` to replace it.
 */
export function consoleLogger(scope: string, level: Level = envLevel()): Logger {
  const min = LEVELS.indexOf(level);
  const line = (l: Level, msg: string) =>
    `${new Date().toISOString()} ${l.toUpperCase().padEnd(5)} [${scope}] ${msg}`;
  return {
    debug: (m) => {
      if (min <= 0) console.log(line('debug', m));
    },
    info: (m) => {
      if (min <= 1) console.log(line('info', m));
    },
    warn: (m) => {
      if (min <= 2) console.warn(line('warn', m));
    },
    error: (m, err) => {
      console.error(line('error', m));
      if (err instanceof Error && err.stack && min <= 0) console.error(err.stack);
    },
  };
}

function envLevel(): Level {
  const v = (process.env.LOG_LEVEL ?? 'info').toLowerCase();
  return (LEVELS as readonly string[]).includes(v) ? (v as Level) : 'info';
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
