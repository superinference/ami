import { isGitInvocationReadOnly } from '../permissions';

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

// Top-level shell control/compound operators. A command joined by any of these
// is a sequence of sub-commands: it is read-only ONLY if EVERY sub-command is
// read-only. Splitting on these (in addition to the pipe) can only make the
// classifier stricter — never classify an actual write as read-only.
const COMPOUND_SEPARATORS = /\s*(?:\|\||&&|;|\||&|\n)\s*/;

// `find` actions that mutate the filesystem or spawn arbitrary commands; their
// presence disqualifies an otherwise read-only `find` invocation.
const FIND_WRITE_ACTIONS = new Set([
  '-delete', '-exec', '-execdir', '-ok', '-okdir',
  '-fprint', '-fprint0', '-fprintf', '-fls',
]);

// `xargs` option flags that consume the following token as their value, so the
// value is not mistaken for the target command when locating the command xargs
// will run.
const XARGS_VALUE_FLAGS = new Set([
  '-I', '-i', '-n', '-L', '-P', '-s', '-d', '-E', '-a',
  '--max-args', '--max-lines', '--max-procs', '--delimiter',
  '--replace', '--arg-file', '--max-chars', '--eof',
]);

// True if the command string contains an UNBALANCED quote — i.e. it cannot be
// tokenized reliably. Fail safe: an unparseable command is not trusted as
// read-only (mirrors a shell tokenizer bailing to passthrough).
function hasUnbalancedQuotes(s: string): boolean {
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '\\' && !inSingle) { i++; continue; } // escape next char (not in '')
    if (c === "'" && !inDouble) inSingle = !inSingle;
    else if (c === '"' && !inSingle) inDouble = !inDouble;
  }
  return inSingle || inDouble;
}

// True if the command contains runtime expansion the shell resolves at execution
// time but which is opaque to a literal-text cache key: command substitution
// (`$(...)` / backticks), variable/arithmetic expansion (`$VAR`, `${...}`,
// `$RANDOM`), or process substitution (`<(...)` / `>(...)`). Such commands are
// non-deterministic and/or side-effectful, so caching their output (keyed on the
// literal text) is unsound. Expansion chars inside single quotes are literal and
// ignored. A double-quoted `$` still expands, so it IS flagged.
function containsRuntimeExpansion(s: string): boolean {
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '\\' && !inSingle) { i++; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    if (inSingle) continue;
    if (c === '`' || c === '$') return true;
    if ((c === '<' || c === '>') && s[i + 1] === '(') return true;
  }
  return false;
}

// Locate the command `xargs` will execute (its first positional argument),
// skipping xargs' own flags and their values. Returns null for a bare `xargs`
// (which defaults to echo — read-only).
function xargsTargetCommand(tokens: string[]): string | null {
  for (let i = 1; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.startsWith('--') && t.includes('=')) continue; // --flag=value
    if (t.startsWith('-')) {
      if (XARGS_VALUE_FLAGS.has(t)) i++; // skip this flag's value token
      continue;
    }
    return t;
  }
  return null;
}

// A command whose base name is in READ_ONLY_PREFIXES can still mutate the
// filesystem through specific flags (e.g. `find -delete`, `sed -i`, `sort -o`)
// or by running another program (`xargs rm`, `awk 'system("rm")'`). Returns true
// when such a write-capable invocation is detected, disqualifying the command.
function commandWritesViaFlags(base: string, tokens: string[]): boolean {
  const args = tokens.slice(1);
  switch (base) {
    case 'find':
      return args.some((a) => FIND_WRITE_ACTIONS.has(a));
    case 'sed':
      return args.some((a) => a === '-i' || a.startsWith('-i') || a === '--in-place' || a.startsWith('--in-place'));
    case 'sort':
      return args.some((a) => a === '-o' || a === '--output' || a.startsWith('--output='));
    case 'awk':
      // File-redirect writes inside awk programs (`print > "f"`) are already
      // caught by WRITE_INDICATORS; this closes the `system(...)` escape.
      return args.some((a) => a.includes('system('));
    case 'xargs': {
      const target = xargsTargetCommand(tokens);
      if (target === null) return false; // bare xargs → echo (read-only)
      const tb = target.includes('/') ? target.split('/').pop()! : target;
      return !READ_ONLY_PREFIXES.has(tb);
    }
    default:
      return false;
  }
}

export function isReadOnlyBashCommand(command: string): boolean {
  const trimmed = command.trim();

  // Fail safe: an unparseable (unbalanced-quote) command is never trusted.
  if (hasUnbalancedQuotes(trimmed)) return false;

  // Never cache command-/variable-/process-substitution: the literal text is not
  // a sound cache key for a runtime-expanded, possibly side-effectful command.
  if (containsRuntimeExpansion(trimmed)) return false;

  if (WRITE_INDICATORS.test(trimmed) && !trimmed.startsWith('cat') && !trimmed.startsWith('grep')) {
    if (/>\s*\S|>>\s*\S/.test(trimmed)) return false;
    if (/\|\s*(tee|dd)\b/.test(trimmed)) return false;
  }

  // Split on the pipe AND every top-level compound separator (;, &&, ||, &,
  // newline). `ls && rm -rf foo` becomes [`ls`, `rm -rf foo`]; the writing
  // sub-command disqualifies the whole command.
  const segments = trimmed.split(COMPOUND_SEPARATORS);
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    const cmd = tokens[0];
    if (!cmd) continue;

    if (cmd === 'git') {
      const subCmd = tokens.find((t, i) => i > 0 && !t.startsWith('-'));
      if (!subCmd || !GIT_READ_ONLY.has(subCmd)) return false;
      // A read-only subcommand can still WRITE (ref creation, remote mutation,
      // `git config key value`, `--output` file write) — the shared guard in
      // permissions.ts is the single source of truth, so a mutating git command
      // is never cached as side-effect-free.
      if (!isGitInvocationReadOnly(tokens)) return false;
      continue;
    }

    const base = cmd.includes('/') ? cmd.split('/').pop()! : cmd;
    if (READ_ONLY_PREFIXES.has(cmd) || READ_ONLY_PREFIXES.has(base)) {
      // A listed command can still mutate via write-capable flags/args.
      if (commandWritesViaFlags(base, tokens)) return false;
      continue;
    }

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
