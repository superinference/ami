const READ_ONLY_PREFIXES = new Set([
  'ls', 'cat', 'head', 'tail', 'grep', 'egrep', 'fgrep', 'find', 'wc',
  'file', 'stat', 'du', 'df', 'which', 'type', 'whereis', 'whoami',
  'hostname', 'uname', 'date', 'pwd', 'echo', 'printf', 'env', 'printenv',
  'id', 'groups', 'test', '[', 'true', 'false', 'diff', 'cmp', 'sort',
  'uniq', 'cut', 'tr', 'awk', 'sed', 'jq', 'yq', 'xargs', 'less',
  'more', 'tree', 'realpath', 'basename', 'dirname', 'readlink', 'sha256sum',
  'md5sum', 'sha1sum', 'xxd', 'hexdump', 'od', 'strings', 'nm', 'ldd',
]);

const GIT_READ_ONLY = new Set([
  'status', 'log', 'diff', 'show', 'branch', 'tag', 'remote',
  'describe', 'rev-parse', 'rev-list', 'shortlog', 'blame',
  'ls-files', 'ls-tree', 'cat-file', 'config',
]);

const WRITE_INDICATORS = /[>|].*\b(tee|dd|install|cp|mv|rm)\b|>\s*\S|>>\s*\S/;

export function isReadOnlyBashCommand(command: string): boolean {
  const trimmed = command.trim();

  if (WRITE_INDICATORS.test(trimmed) && !trimmed.startsWith('cat') && !trimmed.startsWith('grep')) {
    if (/>\s*\S|>>\s*\S/.test(trimmed)) return false;
    if (/\|\s*(tee|dd)\b/.test(trimmed)) return false;
  }

  const segments = trimmed.split(/\s*\|\s*/);
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    const cmd = tokens[0];
    if (!cmd) continue;

    if (cmd === 'git') {
      const subCmd = tokens.find((t, i) => i > 0 && !t.startsWith('-'));
      if (!subCmd || !GIT_READ_ONLY.has(subCmd)) return false;
      continue;
    }

    if (READ_ONLY_PREFIXES.has(cmd)) continue;

    const base = cmd.includes('/') ? cmd.split('/').pop()! : cmd;
    if (READ_ONLY_PREFIXES.has(base)) continue;

    return false;
  }
  return true;
}

interface CacheEntry {
  result: string;
  timestamp: number;
}

export class BashResultCache {
  private cache = new Map<string, CacheEntry>();
  private maxSize: number;
  private ttlMs: number;

  constructor(maxSize = 200, ttlMs = 30_000) {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;
  }

  private key(command: string, cwd: string): string {
    return `${cwd}\0${command}`;
  }

  get(command: string, cwd: string): string | null {
    const k = this.key(command, cwd);
    const entry = this.cache.get(k);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > this.ttlMs) {
      this.cache.delete(k);
      return null;
    }
    return entry.result;
  }

  set(command: string, cwd: string, result: string): void {
    if (this.cache.size >= this.maxSize) {
      const oldest = this.cache.keys().next().value!;
      this.cache.delete(oldest);
    }
    this.cache.set(this.key(command, cwd), { result, timestamp: Date.now() });
  }

  invalidateForCwd(cwd: string): void {
    const prefix = `${cwd}\0`;
    for (const k of [...this.cache.keys()]) {
      if (k.startsWith(prefix)) this.cache.delete(k);
    }
  }

  clear(): void {
    this.cache.clear();
  }

  get size(): number {
    return this.cache.size;
  }
}
