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
  raw: string;
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
  let cmdIdx = 0;
  while (cmdIdx < words.length && /^[A-Za-z_]\w*=/.test(words[cmdIdx])) {
    envVars.push(words[cmdIdx]);
    cmdIdx++;
  }

  const command = words[cmdIdx] || '';
  const args = words.slice(cmdIdx + 1);

  return {
    type: 'simple',
    command: command || (envVars.length > 0 ? envVars[0] : ''),
    args,
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
export function parseBashAST(
  input: string,
  budget?: ParseBudget,
): CommandNode | null | typeof PARSE_ABORTED {
  const state: ParseState = {
    nodeCount: 0,
    maxNodes: budget?.maxNodes ?? MAX_NODES,
    deadline: performance.now() + (budget?.timeoutMs ?? PARSE_TIMEOUT_MS),
  };
  try {
    return parseBashASTInternal(input, 0, state);
  } catch {
    // Budget exhausted OR any unexpected parser throw on adversarial input:
    // fail CLOSED with the sentinel so the security gate blocks rather than
    // silently passing an un-analyzable command.
    return PARSE_ABORTED;
  }
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

export function extractCommands(node: CommandNode): string[] {
  const commands: string[] = [];

  function walk(n: CommandNode): void {
    if (n.type === 'simple' && n.command) {
      commands.push(n.command);
      // When a simple node leads with a shell keyword, the real command is hidden
      // behind it (control-flow body or brace command group). Surface it so
      // AST-based blockers see `if …; then nc …; fi`, `{ socat …; }`,
      // `while …; do insmod …; done`, `case … ) mkfs … ;;` etc.
      if (SHELL_KEYWORDS.has(n.command)) {
        const recovered = recoverHiddenCommand([n.command, ...(n.args ?? [])]);
        if (recovered) commands.push(recovered);
      }
    }
    if (n.children) {
      for (const child of n.children) walk(child);
    }
  }

  walk(node);
  return commands;
}

export function extractCommandsFromString(input: string, budget?: ParseBudget): string[] {
  const ast = parseBashAST(input, budget);
  // No AST (empty/over-length) OR an aborted parse → nothing statically
  // extractable. The abort case is surfaced as a hard block by the security
  // gate (validateCompoundCommand), not here, so extraction stays total.
  if (!ast || ast === PARSE_ABORTED) return [];
  return extractCommands(ast);
}
