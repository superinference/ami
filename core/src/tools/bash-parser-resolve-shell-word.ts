// Split from bash-parser.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * Recursive descent parser for bash commands.
 * Extracts command structure (pipelines, lists, subshells) without tree-sitter.
 */

export interface CommandNode {
  type: 'simple' | 'pipeline' | 'list' | 'subshell' | 'command_substitution';
  command?: string;
  args?: string[];
  children?: CommandNode[];
  operator?: string;
  /**
   * Output (file) redirections this simple command performs, in source order.
   * Only present on `simple` nodes that actually redirect stdout/stderr to a
   * file (`>`/`>>`). Captured so security consumers can read redirect targets
   * from the AST (write-target collection) and distinguish truncate from append.
   * Mirrors the reference's `OutputRedirection[]` on a parsed command.
   */
  redirections?: OutputRedirection[];
  /**
   * Variable assignments that PREFIX this simple command (`FOO=bar cmd`) or that
   * ARE the command (a bare `FOO=bar`), as a name→resolved-value map. Only present
   * on `simple` nodes that carry at least one assignment. {@link extractCommands}
   * reads these to track a static variable scope so a `$VAR` later used as a
   * command NAME resolves to its assigned value — defeating the
   * `A=nc; $A -e …` blocklist-evasion. Mirrors the reference's `varScope` tracking
   * in utils/bash/ast.ts (collectCommands).
   */
  assignments?: Record<string, string>;
  raw: string;
}

/**
 * A single file-output redirection: the destination path and whether it
 * truncates (`>`) or appends (`>>`). Dup/merge forms (`2>&1`, `>&2`) are NOT
 * output redirections and are excluded. Mirrors the reference's
 * `OutputRedirection` (utils/bash/ParsedCommand.ts).
 */
export interface OutputRedirection {
  target: string;
  operator: '>' | '>>';
}

/** An {@link OutputRedirection} plus its byte span in the scanned source. */
export interface RedirectionSpan extends OutputRedirection {
  start: number;
  end: number;
}

/**
 * Quote-aware scan of a command string for file-output redirections. This is the
 * single canonical redirect detector shared by {@link getOutputRedirections},
 * {@link withoutOutputRedirections}, and per-node redirection capture during
 * parsing — so there is exactly one place that understands the `>`/`>>` grammar.
 *
 * Handles: `>`/`>>`, an optional noclobber override (`>|`) or zsh force-clobber
 * (`>!`), an optional leading file-descriptor digit (`2>log`, `2>|`, `2>!`), and
 * single/double-quoted + backslash-escaped targets (quotes are stripped from the
 * returned target). Operators that appear inside quotes are ignored.
 * Descriptor-dup forms (`>&1`) yield no redirection.
 *
 * The force-clobber `!` must be consumed as part of the operator (not the
 * target): the no-space zsh form `>!file` otherwise captures `!file` verbatim,
 * and a later `path.resolve('!file')` treats it as a relative path, bypassing an
 * absolute-path / system-dir write guard. See bash-security.collectWriteTargets.
 */
export function scanOutputRedirections(command: string): RedirectionSpan[] {
  const spans: RedirectionSpan[] = [];
  let inS = false;
  let inD = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inS) { i++; continue; }
    if (c === "'" && !inD) { inS = !inS; continue; }
    if (c === '"' && !inS) { inD = !inD; continue; }
    if (inS || inD) continue;
    if (c !== '>') continue;

    let operator: '>' | '>>' = '>';
    let opEnd = i;
    if (command[i + 1] === '>') { operator = '>>'; opEnd = i + 1; }
    // noclobber override `>|` and zsh force-clobber `>!` both sit between the
    // redirect operator and its target; consume the extra char so the `!`/`|`
    // never leaks into the captured target path.
    if (command[opEnd + 1] === '|' || command[opEnd + 1] === '!') opEnd += 1;

    // Skip inter-token whitespace (not newlines) before the target.
    let j = opEnd + 1;
    while (j < command.length && command[j] !== '\n' && /\s/.test(command[j])) j++;

    // `>&1` / `>&` is a descriptor dup, not a file redirection.
    if (command[j] === '&') { i = opEnd; continue; }

    let target = '';
    let tS = false;
    let tD = false;
    while (j < command.length) {
      const ch = command[j];
      if (ch === '\\' && !tS) { target += command[j + 1] ?? ''; j += 2; continue; }
      if (ch === "'" && !tD) { tS = !tS; j++; continue; }
      if (ch === '"' && !tS) { tD = !tD; j++; continue; }
      if (!tS && !tD && (/\s/.test(ch) || ';|&()<>'.includes(ch))) break;
      target += ch; j++;
    }

    if (!target) { i = opEnd; continue; }

    // Pull a leading fd descriptor (`2>file`) into the removed span so
    // withoutOutputRedirections drops the whole redirection, not a stray `2`.
    let start = i;
    if (/\d/.test(command[start - 1] ?? '')) {
      let k = start - 1;
      while (k > 0 && /\d/.test(command[k - 1])) k--;
      if (k === 0 || /[\s;|&(]/.test(command[k - 1])) start = k;
    }

    spans.push({ start, end: j, target, operator });
    i = j - 1;
  }
  return spans;
}

/**
 * Structured accessor for a command's output redirections. Accepts either a raw
 * command string (scanned directly) or an already-parsed {@link CommandNode}
 * (walked, collecting redirections from every simple node incl. nested
 * subshells / command substitutions). Returns each redirect's `target` +
 * `operator`. Mirrors the reference's `IParsedCommand.getOutputRedirections()`.
 */
export function getOutputRedirections(input: string | CommandNode): OutputRedirection[] {
  if (typeof input === 'string') {
    return scanOutputRedirections(input).map(({ target, operator }) => ({ target, operator }));
  }
  const out: OutputRedirection[] = [];
  const walk = (n: CommandNode): void => {
    if (n.redirections) out.push(...n.redirections);
    n.children?.forEach(walk);
  };
  walk(input);
  return out;
}

/**
 * Return `command` with ALL file-output redirections removed and whitespace
 * collapsed — e.g. `echo hi > out.txt 2>>err` → `echo hi`. Built on the single
 * {@link scanOutputRedirections} detector so it stays consistent with
 * {@link getOutputRedirections}. Mirrors the reference's
 * `IParsedCommand.withoutOutputRedirections()`.
 */
export function withoutOutputRedirections(command: string): string {
  const spans = scanOutputRedirections(command);
  if (spans.length === 0) return command;
  let result = command;
  for (const s of [...spans].sort((a, b) => b.start - a.start)) {
    result = result.slice(0, s.start) + result.slice(s.end);
  }
  return result.trim().replace(/\s+/g, ' ');
}

/**
 * Split a command into its pipe-separated segments (quote-aware). Only the real
 * pipe operator `|` splits; `||` (logical OR) and quoted `|` do not. Each
 * segment is trimmed; a command with no pipe yields a single-element array.
 * Mirrors the reference's `IParsedCommand.getPipeSegments()`.
 */
export function getPipeSegments(command: string): string[] {
  const segments: string[] = [];
  let cur = '';
  let inS = false;
  let inD = false;
  let esc = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (esc) { cur += c; esc = false; continue; }
    if (c === '\\' && !inS) { cur += c; esc = true; continue; }
    if (c === "'" && !inD) { inS = !inS; cur += c; continue; }
    if (c === '"' && !inS) { inD = !inD; cur += c; continue; }
    if (!inS && !inD && c === '|') {
      if (command[i + 1] === '|') { cur += '||'; i++; continue; } // `||` is not a pipe
      const t = cur.trim();
      if (t) segments.push(t);
      cur = '';
      continue;
    }
    cur += c;
  }
  const last = cur.trim();
  if (last) segments.push(last);
  return segments.length > 0 ? segments : [command.trim()];
}

/**
 * Default parser input cap. Mirrors the auto-approve length gate in
 * bash-security.ts so any AST-consuming path that runs before/independent of
 * validateBashSecurity cannot be fed unbounded-length input (DoS defense).
 */
export const MAX_COMMAND_LENGTH = 10_000;

/**
 * Recursion-depth budget for nested `(...)` subshells and `$(...)` command
 * substitutions. Adversarial input such as `((((…))))` under MAX_COMMAND_LENGTH
 * would otherwise recurse once per nesting level and throw a RangeError
 * (stack overflow) that escapes the security gate. When the cap is reached the
 * parser stops descending but STILL emits the nesting node, so the subshell-depth
 * check in bash-security (which blocks >5 levels) keeps firing — i.e. the abort is
 * fail-CLOSED for the nesting-obfuscation vector, not fail-open. The cap is far
 * above the legitimate >5 depth limit yet well below the JS call-stack limit.
 */
export const MAX_PARSE_DEPTH = 50;

/**
 * Wall-clock parse budget (ms). Bails out on pathological/adversarial input so a
 * single command can't stall the security gate. Override per call via the
 * `timeoutMs` budget option; pass `Infinity` to disable (e.g. correctness tests
 * where CI jitter could otherwise cause a spurious abort). Mirrors the reference's
 * PARSE_TIMEOUT_MS=50.
 */
export const PARSE_TIMEOUT_MS = 50;

/**
 * Node-count budget. Bails out before OOM/stack blow-ups on input that produces
 * an abnormally large AST. Override per call via the `maxNodes` budget option.
 * Mirrors the reference's MAX_NODES=50_000.
 */
export const MAX_NODES = 50_000;

/** Per-call resource budget for {@link parseBashAST}. Both knobs are optional. */
export interface ParseBudget {
  /** Wall-clock cap in ms (default {@link PARSE_TIMEOUT_MS}; `Infinity` disables). */
  timeoutMs?: number;
  /** Max AST nodes before fail-closed abort (default {@link MAX_NODES}). */
  maxNodes?: number;
}

/**
 * Caller-supplied environment used to resolve a `$VAR` used as a COMMAND NAME
 * during extraction. Either a plain name→value map, or a resolver function
 * `(key) => value | undefined`. Mirrors the reference's `tryParseShellCommand(cmd,
 * env?)` second parameter (utils/bash/shellQuote.ts L24-L45), which accepts a
 * `Record<string,string|undefined>` OR a `(key)=>string|undefined` resolver and
 * forwards it to shell-quote so the caller controls `$VAR` expansion during
 * tokenization. AMI applies it where it matters for the security model: an
 * indirect command name (`$EDITOR file` with `EDITOR=nc` in the supplied env)
 * resolves to the real executable so the AST command blocklist sees it.
 */
export type ParseEnv =
  | Record<string, string | undefined>
  | ((key: string) => string | undefined);

/** Look up `key` in a {@link ParseEnv} (map or resolver), or `undefined`. */
export function resolveEnvValue(env: ParseEnv | undefined, key: string): string | undefined {
  if (!env) return undefined;
  return typeof env === 'function' ? env(key) : env[key];
}

export interface ParseState {
  nodeCount: number;
  maxNodes: number;
  deadline: number;
}

/**
 * Resolve a single shell word to the LITERAL string the shell would use, by
 * removing quoting and resolving backslash escapes — exactly the unquoting a
 * shell performs before resolving a command name or argument. Specifically:
 * surrounding/embedded single and double quotes are dropped (`"nc"`→`nc`,
 * `r"m"`→`rm`, `in's'mod`→`insmod`), and a backslash outside single quotes
 * escapes the next character (`\rm`→`rm`, `n\c`→`nc`); backslashes inside single
 * quotes stay literal (`'a\b'`→`a\b`). Variable/command expansions (`$FOO`,
 * `$(...)`) are left untouched — this resolves quoting only, never expands.
 *
 * SECURITY: the AST command name must be this trustworthy literal so the
 * `AST_BLOCKED_COMMANDS` matcher in bash-security sees `nc`/`insmod`/… even when
 * spelled with quotes or an escape (`"nc"`, `'insmod'`, `\rm`) — otherwise the
 * quoted spelling parses to a token carrying the quotes and the blocklist match
 * silently fails open. Mirrors the reference's ast.ts resolved-argv (quotes
 * removed, backslash unescaped) that feeds the permission/security matcher.
 */
export function resolveShellWord(word: string): string {
  let out = '';
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < word.length; i++) {
    const c = word[i];
    if (c === '\\' && !inSingle) {
      if (i + 1 < word.length) { out += word[i + 1]; i++; }
      continue;
    }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    out += c;
  }
  return out;
}

/** Thrown internally when a {@link ParseBudget} is exceeded; caught at the top. */
export class ParseBudgetExceeded extends Error {}

/**
 * Charge one AST node against the budget. Called at every node-producing site so
 * both the node-count and wall-clock caps bound the total parse work. Throws
 * {@link ParseBudgetExceeded} (unwinds to {@link parseBashAST}, which maps it to
 * {@link PARSE_ABORTED}) when either budget is exhausted.
 */
export function countNode(state: ParseState): void {
  state.nodeCount++;
  if (state.nodeCount > state.maxNodes || performance.now() > state.deadline) {
    throw new ParseBudgetExceeded();
  }
}

/** One variable touched by a declaration command, with the keyword that did it. */
export interface DeclaredAssignment {
  /** The declaration keyword (`export`, `declare`, `readonly`, …). */
  command: string;
  /** The variable name declared/assigned/unset (without any `=value` suffix). */
  variable: string;
}
