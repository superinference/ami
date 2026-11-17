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
   * Mirrors Claude Code's `OutputRedirection[]` on a parsed command.
   */
  redirections?: OutputRedirection[];
  /**
   * Variable assignments that PREFIX this simple command (`FOO=bar cmd`) or that
   * ARE the command (a bare `FOO=bar`), as a name→resolved-value map. Only present
   * on `simple` nodes that carry at least one assignment. {@link extractCommands}
   * reads these to track a static variable scope so a `$VAR` later used as a
   * command NAME resolves to its assigned value — defeating the
   * `A=nc; $A -e …` blocklist-evasion. Mirrors Claude Code's `varScope` tracking
   * in utils/bash/ast.ts (collectCommands).
   */
  assignments?: Record<string, string>;
  raw: string;
}

/**
 * A single file-output redirection: the destination path and whether it
 * truncates (`>`) or appends (`>>`). Dup/merge forms (`2>&1`, `>&2`) are NOT
 * output redirections and are excluded. Mirrors Claude Code's
 * `OutputRedirection` (utils/bash/ParsedCommand.ts).
 */
export interface OutputRedirection {
  target: string;
  operator: '>' | '>>';
}

/** An {@link OutputRedirection} plus its byte span in the scanned source. */
interface RedirectionSpan extends OutputRedirection {
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
function scanOutputRedirections(command: string): RedirectionSpan[] {
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
 * `operator`. Mirrors Claude Code's `IParsedCommand.getOutputRedirections()`.
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
 * {@link getOutputRedirections}. Mirrors Claude Code's
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
 * Mirrors Claude Code's `IParsedCommand.getPipeSegments()`.
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
const MAX_COMMAND_LENGTH = 10_000;

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
const MAX_PARSE_DEPTH = 50;

/**
 * SECURITY: Sentinel for "the parser ran but ABORTED" — it exhausted a resource
 * budget (node count or wall-clock timeout) or threw on pathological/adversarial
 * input. It is DISTINCT from `null` (empty / over-length / genuinely unparseable):
 * callers MUST treat `PARSE_ABORTED` as fail-CLOSED ("too complex to analyze →
 * block"), never route it to the permissive "parse-unavailable → PASS" path.
 * Mirrors Claude Code's PARSE_ABORTED symbol (utils/bash/parser.ts).
 */
export const PARSE_ABORTED: unique symbol = Symbol('parse-aborted');

/**
 * Wall-clock parse budget (ms). Bails out on pathological/adversarial input so a
 * single command can't stall the security gate. Override per call via the
 * `timeoutMs` budget option; pass `Infinity` to disable (e.g. correctness tests
 * where CI jitter could otherwise cause a spurious abort). Mirrors Claude's
 * PARSE_TIMEOUT_MS=50.
 */
const PARSE_TIMEOUT_MS = 50;

/**
 * Node-count budget. Bails out before OOM/stack blow-ups on input that produces
 * an abnormally large AST. Override per call via the `maxNodes` budget option.
 * Mirrors Claude's MAX_NODES=50_000.
 */
const MAX_NODES = 50_000;

/** Per-call resource budget for {@link parseBashAST}. Both knobs are optional. */
export interface ParseBudget {
  /** Wall-clock cap in ms (default {@link PARSE_TIMEOUT_MS}; `Infinity` disables). */
  timeoutMs?: number;
  /** Max AST nodes before fail-closed abort (default {@link MAX_NODES}). */
  maxNodes?: number;
}

interface ParseState {
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
 * silently fails open. Mirrors Claude Code's ast.ts resolved-argv (quotes
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
class ParseBudgetExceeded extends Error {}

/**
 * Charge one AST node against the budget. Called at every node-producing site so
 * both the node-count and wall-clock caps bound the total parse work. Throws
 * {@link ParseBudgetExceeded} (unwinds to {@link parseBashAST}, which maps it to
 * {@link PARSE_ABORTED}) when either budget is exhausted.
 */
function countNode(state: ParseState): void {
  state.nodeCount++;
  if (state.nodeCount > state.maxNodes || performance.now() > state.deadline) {
    throw new ParseBudgetExceeded();
  }
}

/**
 * Declaration commands (`export FOO=bar`, `declare -x X`, `readonly Y=1`,
 * `local Z`, `typeset W`, `unset V`, `unsetenv V`). bash/tree-sitter model these
 * as a distinct `declaration_command` node whose operands are variable
 * *declarations*, not ordinary command arguments. AMI's hand-rolled parser emits
 * them as `simple` nodes, so this set lets consumers recover that structure:
 * {@link extractDeclaredAssignments} walks the AST and, for any simple node whose
 * command is a declaration keyword, returns the variable name touched by each
 * operand (`FOO=bar` → `FOO`, bare `FOO` → `FOO`), skipping option flags.
 *
 * The security consumer (`detectCodeLoadingEnvSet`) uses this to block pinned
 * code-loading variables assigned via declaration keywords the textual regex
 * misses — e.g. `readonly LD_PRELOAD=/evil.so`, `typeset LD_LIBRARY_PATH=/x`,
 * `local PATH=/x`, `declare LD_PRELOAD=/evil.so` (no `-x`).
 *
 * Mirrors Claude Code's DECLARATION_COMMANDS allow-set
 * (utils/bash/parser.ts L20-28, extractCommandArguments L189-196).
 */
export const DECLARATION_COMMANDS = new Set([
  'export',
  'declare',
  'typeset',
  'readonly',
  'local',
  'unset',
  'unsetenv',
]);

/** One variable touched by a declaration command, with the keyword that did it. */
export interface DeclaredAssignment {
  /** The declaration keyword (`export`, `declare`, `readonly`, …). */
  command: string;
  /** The variable name declared/assigned/unset (without any `=value` suffix). */
  variable: string;
}

// Shell keywords/control-structure tokens. A keyword that leads a simple-command
// node means the node's real command is hidden behind it (e.g. `then nc …`,
// `do socat …`, `{ insmod …`). extractCommands uses these sets to recover the
// hidden command so AST-based blockers in bash-security see inside control-flow
// bodies and brace command groups instead of only seeing the keyword.
const KEYWORD_CMD_INTRO = new Set(['if', 'elif', 'while', 'until', 'then', 'else', 'do', '{', '!', 'time']);
const KEYWORD_VAR_INTRO = new Set(['for', 'select', 'case']);
const KEYWORD_TERMINATOR = new Set(['fi', 'done', 'esac', '}', 'in']);
const SHELL_KEYWORDS = new Set<string>([...KEYWORD_CMD_INTRO, ...KEYWORD_VAR_INTRO, ...KEYWORD_TERMINATOR]);

class BashLexer {
  private pos = 0;
  private input: string;

  constructor(input: string) {
    this.input = input;
  }

  peek(): string | null {
    this.skipWhitespace();
    if (this.pos >= this.input.length) return null;
    return this.input[this.pos];
  }

  peekTwo(): string | null {
    this.skipWhitespace();
    if (this.pos + 1 >= this.input.length) return null;
    return this.input.slice(this.pos, this.pos + 2);
  }

  advance(): string {
    return this.input[this.pos++];
  }

  skipWhitespace(): void {
    while (this.pos < this.input.length && /\s/.test(this.input[this.pos]) && this.input[this.pos] !== '\n') {
      this.pos++;
    }
  }

  isAtEnd(): boolean {
    this.skipWhitespace();
    return this.pos >= this.input.length;
  }

  readWord(): string {
    this.skipWhitespace();
    let word = '';
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let escaped = false;

    while (this.pos < this.input.length) {
      const c = this.input[this.pos];

      if (escaped) {
        word += c;
        escaped = false;
        this.pos++;
        continue;
      }

      if (c === '\\' && !inSingleQuote) {
        escaped = true;
        word += c;
        this.pos++;
        continue;
      }

      if (c === "'" && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote;
        word += c;
        this.pos++;
        continue;
      }

      if (c === '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
        word += c;
        this.pos++;
        continue;
      }

      if (!inSingleQuote && !inDoubleQuote) {
        if (/\s/.test(c) || '|&;()><\n'.includes(c)) break;
      }

      word += c;
      this.pos++;
    }

    return word;
  }

  readUntilCloseParen(): string {
    let depth = 1;
    let result = '';
    let inSingle = false;
    let inDouble = false;
    while (this.pos < this.input.length && depth > 0) {
      const c = this.input[this.pos];
      if (c === '\\' && inDouble && this.pos + 1 < this.input.length) {
        result += c + this.input[this.pos + 1];
        this.pos += 2;
        continue;
      }
      if (c === "'" && !inDouble) { inSingle = !inSingle; }
      else if (c === '"' && !inSingle) { inDouble = !inDouble; }
      else if (!inSingle && !inDouble) {
        if (c === '(') depth++;
        else if (c === ')') {
          depth--;
          if (depth === 0) { this.pos++; break; }
        }
      }
      result += c;
      this.pos++;
    }
    return result;
  }

  getPosition(): number { return this.pos; }
  getRemaining(): string { return this.input.slice(this.pos); }
  slice(start: number, end: number): string { return this.input.slice(start, end); }
}

function parseSimpleCommand(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
  lexer.skipWhitespace();
  if (lexer.isAtEnd()) return null;
  countNode(state);

  const p = lexer.peek();

  if (p === '(') {
    lexer.advance();
    const inner = lexer.readUntilCloseParen();
    // Fail-closed recursion budget: past the cap, emit the subshell node WITHOUT
    // descending, so excessive-nesting detection still fires but we never risk a
    // stack-overflow RangeError on adversarial `((((…))))` input.
    const children = depth >= MAX_PARSE_DEPTH ? null : parseBashASTInternal(inner, depth + 1, state);
    return {
      type: 'subshell',
      children: children ? [children] : [],
      raw: `(${inner})`,
    };
  }

  if (p === '$' && lexer.peekTwo() === '$(') {
    lexer.advance(); // $
    lexer.advance(); // (
    const inner = lexer.readUntilCloseParen();
    const children = depth >= MAX_PARSE_DEPTH ? null : parseBashASTInternal(inner, depth + 1, state);
    return {
      type: 'command_substitution',
      children: children ? [children] : [],
      raw: `$(${inner})`,
    };
  }

  const words: string[] = [];
  const startPos = lexer.getPosition();

  while (!lexer.isAtEnd()) {
    const next = lexer.peek();
    if (next === null || '|&;\n()'.includes(next)) break;
    if (next === '>' || next === '<') {
      lexer.advance();
      if (!lexer.isAtEnd() && lexer.peek() === '>') lexer.advance();
      if (!lexer.isAtEnd()) lexer.readWord();
      continue;
    }
    const word = lexer.readWord();
    if (word) words.push(word);
  }

  if (words.length === 0) return null;

  const envVars: string[] = [];
  const assignments: Record<string, string> = {};
  let cmdIdx = 0;
  while (cmdIdx < words.length && /^[A-Za-z_]\w*=/.test(words[cmdIdx])) {
    const w = words[cmdIdx];
    envVars.push(w);
    const eq = w.indexOf('=');
    // Resolve quoting/escapes on the VALUE so a quoted alias (`A="nc"`) tracks the
    // literal `nc` the shell would run — the same unquoting applied to command names.
    assignments[w.slice(0, eq)] = resolveShellWord(w.slice(eq + 1));
    cmdIdx++;
  }

  // Resolve quoting/escapes on the command NAME so the AST carries the literal
  // executable name (`"nc"`→`nc`, `\rm`→`rm`) the shell would run — the security
  // AST_BLOCKED_COMMANDS matcher depends on this trustworthy token.
  const command = resolveShellWord(words[cmdIdx] || '');
  const args = words.slice(cmdIdx + 1);

  // Capture file-output redirections from this simple command's source span so
  // AST consumers (getOutputRedirections) see redirect targets + the truncate/
  // append operator. Uses the single canonical scanner — no second grammar.
  const redirections = scanOutputRedirections(lexer.slice(startPos, lexer.getPosition()));

  return {
    type: 'simple',
    command: command || (envVars.length > 0 ? resolveShellWord(envVars[0]) : ''),
    args,
    redirections: redirections.length > 0
      ? redirections.map(({ target, operator }) => ({ target, operator }))
      : undefined,
    assignments: Object.keys(assignments).length > 0 ? assignments : undefined,
    raw: lexer.getRemaining().length < startPos ? command : words.join(' '),
  };
}

function parsePipeline(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
  const first = parseSimpleCommand(lexer, depth, state);
  if (!first) return null;

  const pipelineCommands: CommandNode[] = [first];

  while (!lexer.isAtEnd()) {
    const p = lexer.peek();
    if (p === '|' && lexer.peekTwo() !== '||') {
      lexer.advance(); // consume |
      const next = parseSimpleCommand(lexer, depth, state);
      if (next) pipelineCommands.push(next);
    } else {
      break;
    }
  }

  if (pipelineCommands.length === 1) return pipelineCommands[0];

  countNode(state);
  return {
    type: 'pipeline',
    children: pipelineCommands,
    raw: pipelineCommands.map(c => c.raw).join(' | '),
  };
}

function parseList(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
  const first = parsePipeline(lexer, depth, state);
  if (!first) return null;

  const listItems: CommandNode[] = [first];
  const operators: string[] = [];

  while (!lexer.isAtEnd()) {
    const p = lexer.peek();
    const pp = lexer.peekTwo();

    if (pp === '&&' || pp === '||') {
      operators.push(pp);
      lexer.advance();
      lexer.advance();
      const next = parsePipeline(lexer, depth, state);
      if (next) listItems.push(next);
    } else if (p === ';' || p === '\n') {
      operators.push(';');
      lexer.advance();
      const next = parsePipeline(lexer, depth, state);
      if (next) listItems.push(next);
    } else if (p === '&') {
      operators.push('&');
      lexer.advance();
      const next = parsePipeline(lexer, depth, state);
      if (next) listItems.push(next);
    } else if (p === ')') {
      // A `)` surfacing at list level is a case-pattern terminator (e.g.
      // `case $x in a) cmd;; esac`). Subshell `(...)` parens are fully consumed
      // by readUntilCloseParen, so this is never a subshell close. Treat it as a
      // separator so the case-branch body (`cmd`) is parsed as its own command
      // and stays visible to AST-based security checks instead of being dropped.
      operators.push(';');
      lexer.advance();
      const next = parsePipeline(lexer, depth, state);
      if (next) listItems.push(next);
    } else {
      break;
    }
  }

  if (listItems.length === 1) return listItems[0];

  countNode(state);
  return {
    type: 'list',
    children: listItems,
    operator: operators.join(','),
    raw: listItems.map(c => c.raw).join(' ; '),
  };
}

function parseBashASTInternal(input: string, depth: number, state: ParseState): CommandNode | null {
  const trimmed = input.trim();
  if (!trimmed) return null;
  // Input-length cap (DoS defense): refuse to parse pathologically long input.
  if (trimmed.length > MAX_COMMAND_LENGTH) return null;

  const lexer = new BashLexer(trimmed);
  return parseList(lexer, depth, state);
}

/**
 * Parse a bash command into an AST.
 *
 * Returns:
 *   - {@link CommandNode}: parse succeeded.
 *   - `null`: empty / whitespace-only / over-length input (nothing to analyze).
 *   - {@link PARSE_ABORTED}: the parse ran but exhausted a {@link ParseBudget}
 *     (node count / wall-clock timeout) or threw on pathological input — callers
 *     MUST treat this fail-CLOSED (too complex → block), never as "no AST → pass".
 *
 * `budget` tunes the per-call resource caps (defaults {@link PARSE_TIMEOUT_MS} /
 * {@link MAX_NODES}; pass `{ timeoutMs: Infinity }` to disable the wall-clock cap).
 */
/**
 * Size-1 memo of the most recent SUCCESSFUL parse (a `CommandNode` or a
 * deterministic `null` for empty/over-length input), keyed on the input plus the
 * budget knobs that affect the outcome. The same command is now parsed more than
 * once per security evaluation — `validateASTCommands` and the
 * `validateBackslashEscapedOperators` AST gate both call {@link parseBashAST}, and
 * a permission preflight re-checks the same command the execution gate later
 * validates — so caching the last parse elides the duplicate work. `PARSE_ABORTED`
 * is deliberately NOT cached: wall-clock aborts are time-dependent and must be
 * re-evaluated. The cached AST is treated as IMMUTABLE by all consumers
 * (`extractCommands` / `hasActualOperatorNodes` / `getOutputRedirections` only
 * read it). Mirrors Claude Code's size-1 last-command parse cache.
 */
let lastParse: { key: string; result: CommandNode | null } | null = null;

export function parseBashAST(
  input: string,
  budget?: ParseBudget,
): CommandNode | null | typeof PARSE_ABORTED {
  const key = `${input}\u0000${budget?.timeoutMs ?? ''}\u0000${budget?.maxNodes ?? ''}`;
  if (lastParse && lastParse.key === key) return lastParse.result;

  const state: ParseState = {
    nodeCount: 0,
    maxNodes: budget?.maxNodes ?? MAX_NODES,
    deadline: performance.now() + (budget?.timeoutMs ?? PARSE_TIMEOUT_MS),
  };
  let result: CommandNode | null;
  try {
    result = parseBashASTInternal(input, 0, state);
  } catch {
    // Budget exhausted OR any unexpected parser throw on adversarial input:
    // fail CLOSED with the sentinel so the security gate blocks rather than
    // silently passing an un-analyzable command. Not cached (time-dependent).
    return PARSE_ABORTED;
  }
  lastParse = { key, result };
  return result;
}

/**
 * Recover the real command hidden behind leading shell keywords in a simple-command
 * node. Returns the first genuine command token found after control-structure
 * keywords (`then`/`do`/`else`/`{`/`!`/`time`/`if`/`while`/…), or null when the
 * segment introduces only a loop/case variable (`for`/`select`/`case`) or is a
 * bare terminator (`fi`/`done`/`esac`/`}`).
 */
function recoverHiddenCommand(tokens: string[]): string | null {
  for (const t of tokens) {
    if (KEYWORD_VAR_INTRO.has(t)) return null;
    if (KEYWORD_CMD_INTRO.has(t) || KEYWORD_TERMINATOR.has(t)) continue;
    if (/^[A-Za-z_]\w*=/.test(t)) continue; // env assignment prefix
    return t;
  }
  return null;
}

/**
 * If `token` is a plain variable reference used where a command NAME is expected
 * — `$VAR` or `${VAR}` with a `[A-Za-z_]\w*` name and nothing else — return the
 * variable name; otherwise null. Special parameters (`$?`, `$$`, `$!`, `$#`,
 * `$0`, `$@`, `$*`, `$-`) and expansions with operators (`${X:-y}`, `$VAR/sub`)
 * do not match — only a bare indirect command invocation does. Used by
 * {@link extractCommands} to resolve `$VAR` command names against the tracked
 * assignment scope. Mirrors Claude Code's indirect-command resolution.
 */
function parseVariableReference(token: string): string | null {
  const m = /^\$\{([A-Za-z_]\w*)\}$/.exec(token) ?? /^\$([A-Za-z_]\w*)$/.exec(token);
  return m ? m[1] : null;
}

export function extractCommands(node: CommandNode): string[] {
  const commands: string[] = [];

  // `scope` is a static map of variable name → assigned value, tracked so a
  // `$VAR` later used as a command name resolves to the literal the shell would
  // run. The scope is CARRIED across `&&`/`;` (both clauses run with the prior
  // assignment visible) but RESET across `||`, `|` and `&` and NOT propagated
  // out of a subshell/command-substitution — clauses that may not run, or run in
  // a subshell, must not leak a value. Mirrors Claude's varScope reset rules.
  function walk(n: CommandNode, scope: Map<string, string>): void {
    if (n.type === 'simple' && n.command) {
      if (n.assignments) {
        for (const [k, v] of Object.entries(n.assignments)) scope.set(k, v);
      }
      const varName = parseVariableReference(n.command);
      const resolved = varName !== null && scope.has(varName)
        ? scope.get(varName) as string
        : n.command;
      commands.push(resolved);
      // When a simple node leads with a shell keyword, the real command is hidden
      // behind it (control-flow body or brace command group). Surface it so
      // AST-based blockers see `if …; then nc …; fi`, `{ socat …; }`,
      // `while …; do insmod …; done`, `case … ) mkfs … ;;` etc.
      if (SHELL_KEYWORDS.has(n.command)) {
        const recovered = recoverHiddenCommand([n.command, ...(n.args ?? [])]);
        // The recovered token comes from raw args (unresolved), so resolve its
        // quoting/escapes too — `{ "insmod" x; }` must surface `insmod`.
        if (recovered) commands.push(resolveShellWord(recovered));
      }
      return;
    }
    if (n.type === 'list') {
      const kids = n.children ?? [];
      const ops = (n.operator ?? '').split(',');
      const snapshot = new Map(scope);
      let cur = scope;
      for (let i = 0; i < kids.length; i++) {
        walk(kids[i], cur);
        // Reset the tracked scope entering a clause that bash runs conditionally
        // (`||` RHS), in the background (`&`), so assignments before it do not
        // falsely attribute to it. `&&`/`;` carry the accumulated scope forward.
        const op = ops[i];
        if (op === '||' || op === '&') cur = new Map(snapshot);
      }
      return;
    }
    if (n.type === 'pipeline') {
      // Each pipe stage is its own subshell — assignments never cross a `|`.
      for (const child of n.children ?? []) walk(child, new Map(scope));
      return;
    }
    // subshell / command_substitution: inherits a COPY of the outer scope but its
    // own assignments do not leak back out.
    for (const child of n.children ?? []) walk(child, new Map(scope));
  }

  walk(node, new Map());
  return commands;
}

/**
 * Walk a parsed AST and recover every variable touched by a declaration command
 * (see {@link DECLARATION_COMMANDS}). For each simple node whose command is a
 * declaration keyword, every operand is interpreted as a declaration operand:
 * `FOO=bar` yields `FOO` and a bare `FOO` (e.g. `unset FOO`) yields `FOO`.
 * Option flags (`-x`, `-g`, `--`, …) are skipped. Walks nested subshells /
 * command substitutions so `$(export LD_PRELOAD=x)` is seen too.
 *
 * This is the structural counterpart to the textual code-loading regex: it lets
 * security consumers match pinned variables set via `readonly`/`local`/`typeset`/
 * `declare` (without `-x`) — declaration keywords the regex does not cover.
 * Mirrors Claude Code's declaration-command argument extraction.
 */
export function extractDeclaredAssignments(node: CommandNode): DeclaredAssignment[] {
  const out: DeclaredAssignment[] = [];

  function walk(n: CommandNode): void {
    if (n.type === 'simple' && n.command && DECLARATION_COMMANDS.has(n.command)) {
      for (const arg of n.args ?? []) {
        if (arg.startsWith('-')) continue; // option flag, not a variable operand
        const eq = arg.indexOf('=');
        const variable = eq === -1 ? arg : arg.slice(0, eq);
        if (/^[A-Za-z_]\w*$/.test(variable)) out.push({ command: n.command, variable });
      }
    }
    n.children?.forEach(walk);
  }

  walk(node);
  return out;
}

export function extractCommandsFromString(input: string, budget?: ParseBudget): string[] {
  const ast = parseBashAST(input, budget);
  // No AST (empty/over-length) OR an aborted parse → nothing statically
  // extractable. The abort case is surfaced as a hard block by the security
  // gate (validateCompoundCommand), not here, so extraction stays total.
  if (!ast || ast === PARSE_ABORTED) return [];
  return extractCommands(ast);
}

/**
 * Does the parsed AST contain a REAL control operator — a `list` (`;`/`&&`/`||`/`&`)
 * or a `pipeline` (`|`) node — anywhere in the tree (including inside subshells /
 * command substitutions)?
 *
 * This is the primitive that eliminates the `find -exec … \;` false positive: a
 * backslash-escaped operator such as `\;` is parsed as part of a *word* (an
 * argument to `find`), NOT as a `;` operator, so a command whose only "operator"
 * is an escaped one parses to a single `simple` node and returns `false` here.
 * Security consumers use that to tell a literal escaped operator (benign word
 * argument) from one that is actually chaining commands — mirroring Claude Code's
 * `hasActualOperatorNodes` (utils/bash/treeSitterAnalysis.ts).
 *
 * NOTE: a *dangling* trailing operator (`cmd &&`, `cmd ;`) also parses to a lone
 * `simple` node (there is no second operand to form a list), so this returns
 * `false` for those — dangling-operator detection must therefore use escaped-char
 * analysis, not this helper.
 */
export function hasActualOperatorNodes(node: CommandNode): boolean {
  if (node.type === 'list' || node.type === 'pipeline') return true;
  return (node.children ?? []).some(hasActualOperatorNodes);
}

/**
 * Dangerous textual features of a bash command, as reusable boolean fields.
 * Mirrors Claude Code's `DangerousPatterns`
 * (utils/bash/treeSitterAnalysis.ts L45-L52): one struct consumers read instead
 * of each re-scanning the raw string for `$()`/`${}`/`<()`/heredoc/comment.
 */
export interface DangerousPatterns {
  /** `$(...)` or backtick command substitution (suppressed inside single quotes). */
  hasCommandSubstitution: boolean;
  /** `<(...)` / `>(...)` process substitution. */
  hasProcessSubstitution: boolean;
  /** `${...}` parameter expansion (suppressed inside single quotes). */
  hasParameterExpansion: boolean;
  /** `<<` / `<<-` heredoc operator. */
  hasHeredoc: boolean;
  /** An unquoted `#` comment at a word boundary. */
  hasComment: boolean;
}

/**
 * Compound shape of a bash command, as reusable boolean fields + the control
 * operators and top-level segments. Mirrors Claude Code's `CompoundStructure`
 * (utils/bash/treeSitterAnalysis.ts L54-L64).
 */
export interface CompoundStructure {
  /** `&&`, `||`, `;`, or `&` list operators present. */
  hasCompoundOperators: boolean;
  /** `|` pipeline present. */
  hasPipeline: boolean;
  /** `(...)` subshell present. */
  hasSubshell: boolean;
  /** `{ ...; }` brace command group present (NOT brace expansion `{a,b}`). */
  hasCommandGroup: boolean;
  /** Control operators in source order (`&&`, `||`, `;`, `&`, `|`). */
  operators: string[];
  /** Raw text of each top-level pipeline/list segment. */
  segments: string[];
}

/** Unified structured analysis of a bash command. Mirrors Claude's `TreeSitterAnalysis`. */
export interface CommandStructureAnalysis {
  dangerousPatterns: DangerousPatterns;
  compoundStructure: CompoundStructure;
}

/**
 * Quote-aware scan for a command's dangerous textual features. Single quotes
 * suppress ALL expansion; double quotes still allow `$(`/`${`/backtick. The
 * backslash escape skips the next char (outside single quotes). This is the one
 * place that understands this textual grammar, consumed by
 * {@link analyzeCommandStructure}.
 */
function scanDangerousPatterns(command: string): DangerousPatterns {
  const p: DangerousPatterns = {
    hasCommandSubstitution: false,
    hasProcessSubstitution: false,
    hasParameterExpansion: false,
    hasHeredoc: false,
    hasComment: false,
  };
  let inS = false;
  let inD = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inS) { i++; continue; }
    if (c === "'" && !inD) { inS = !inS; continue; }
    if (c === '"' && !inS) { inD = !inD; continue; }
    if (inS) continue; // single quotes suppress every expansion/operator below
    if (c === '`') { p.hasCommandSubstitution = true; continue; }
    if (c === '$' && command[i + 1] === '(') { p.hasCommandSubstitution = true; i++; continue; }
    if (c === '$' && command[i + 1] === '{') { p.hasParameterExpansion = true; i++; continue; }
    if (inD) continue; // the rest (process sub / heredoc / comment) need an unquoted context
    if ((c === '<' || c === '>') && command[i + 1] === '(') { p.hasProcessSubstitution = true; i++; continue; }
    if (c === '<' && command[i + 1] === '<') { p.hasHeredoc = true; i++; continue; }
    if (c === '#' && (i === 0 || /\s/.test(command[i - 1]))) { p.hasComment = true; }
  }
  return p;
}

/**
 * Detect a brace COMMAND GROUP `{ …; }` (distinct from brace EXPANSION `{a,b}`).
 * bash requires a `{` that is its own token — at a command boundary and followed
 * by whitespace — so that word-boundary + trailing-space test discriminates the
 * two forms without a full grammar.
 */
function hasCommandGroupToken(command: string): boolean {
  let inS = false;
  let inD = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inS) { i++; continue; }
    if (c === "'" && !inD) { inS = !inS; continue; }
    if (c === '"' && !inS) { inD = !inD; continue; }
    if (inS || inD) continue;
    if (c === '{') {
      const prev = command[i - 1];
      const next = command[i + 1];
      const atBoundary = i === 0 || /[\s;&|(]/.test(prev);
      if (atBoundary && (next === ' ' || next === '\t' || next === '\n')) return true;
    }
  }
  return false;
}

/** Collect compound operators / pipeline / subshell presence from the AST. */
function collectCompound(n: CommandNode, out: CompoundStructure): void {
  if (n.type === 'list') {
    out.hasCompoundOperators = true;
    for (const op of (n.operator ?? '').split(',')) if (op) out.operators.push(op);
  } else if (n.type === 'pipeline') {
    out.hasPipeline = true;
    for (let i = 1; i < (n.children?.length ?? 0); i++) out.operators.push('|');
  } else if (n.type === 'subshell') {
    out.hasSubshell = true;
  }
  n.children?.forEach(c => collectCompound(c, out));
}

/**
 * Produce the unified {@link CommandStructureAnalysis} for a bash command: the
 * textual {@link DangerousPatterns} (scanned from `command`) plus the structural
 * {@link CompoundStructure} (walked from the AST — pass a pre-parsed `node` to
 * avoid re-parsing on the security hot path; `null`/{@link PARSE_ABORTED} yield
 * empty structural fields). Mirrors Claude Code's `analyzeCommand`
 * (utils/bash/treeSitterAnalysis.ts), giving consumers one struct of reusable
 * boolean fields instead of each re-scanning the string.
 */
export function analyzeCommandStructure(
  command: string,
  node?: CommandNode | null | typeof PARSE_ABORTED,
): CommandStructureAnalysis {
  const ast = node === undefined ? parseBashAST(command) : node;
  const compoundStructure: CompoundStructure = {
    hasCompoundOperators: false,
    hasPipeline: false,
    hasSubshell: false,
    hasCommandGroup: hasCommandGroupToken(command),
    operators: [],
    segments: [],
  };
  if (ast && ast !== PARSE_ABORTED) {
    collectCompound(ast, compoundStructure);
    compoundStructure.segments = (ast.type === 'list' || ast.type === 'pipeline')
      ? (ast.children ?? []).map(c => c.raw)
      : [ast.raw];
  }
  return { dangerousPatterns: scanDangerousPatterns(command), compoundStructure };
}
