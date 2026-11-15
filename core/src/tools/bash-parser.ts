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

function parseSimpleCommand(lexer: BashLexer, depth: number): CommandNode | null {
  lexer.skipWhitespace();
  if (lexer.isAtEnd()) return null;

  const p = lexer.peek();

  if (p === '(') {
    lexer.advance();
    const inner = lexer.readUntilCloseParen();
    // Fail-closed recursion budget: past the cap, emit the subshell node WITHOUT
    // descending, so excessive-nesting detection still fires but we never risk a
    // stack-overflow RangeError on adversarial `((((…))))` input.
    const children = depth >= MAX_PARSE_DEPTH ? null : parseBashASTInternal(inner, depth + 1);
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
    const children = depth >= MAX_PARSE_DEPTH ? null : parseBashASTInternal(inner, depth + 1);
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

function parsePipeline(lexer: BashLexer, depth: number): CommandNode | null {
  const first = parseSimpleCommand(lexer, depth);
  if (!first) return null;

  const pipelineCommands: CommandNode[] = [first];

  while (!lexer.isAtEnd()) {
    const p = lexer.peek();
    if (p === '|' && lexer.peekTwo() !== '||') {
      lexer.advance(); // consume |
      const next = parseSimpleCommand(lexer, depth);
      if (next) pipelineCommands.push(next);
    } else {
      break;
    }
  }

  if (pipelineCommands.length === 1) return pipelineCommands[0];

  return {
    type: 'pipeline',
    children: pipelineCommands,
    raw: pipelineCommands.map(c => c.raw).join(' | '),
  };
}

function parseList(lexer: BashLexer, depth: number): CommandNode | null {
  const first = parsePipeline(lexer, depth);
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
      const next = parsePipeline(lexer, depth);
      if (next) listItems.push(next);
    } else if (p === ';' || p === '\n') {
      operators.push(';');
      lexer.advance();
      const next = parsePipeline(lexer, depth);
      if (next) listItems.push(next);
    } else if (p === '&') {
      operators.push('&');
      lexer.advance();
      const next = parsePipeline(lexer, depth);
      if (next) listItems.push(next);
    } else if (p === ')') {
      // A `)` surfacing at list level is a case-pattern terminator (e.g.
      // `case $x in a) cmd;; esac`). Subshell `(...)` parens are fully consumed
      // by readUntilCloseParen, so this is never a subshell close. Treat it as a
      // separator so the case-branch body (`cmd`) is parsed as its own command
      // and stays visible to AST-based security checks instead of being dropped.
      operators.push(';');
      lexer.advance();
      const next = parsePipeline(lexer, depth);
      if (next) listItems.push(next);
    } else {
      break;
    }
  }

  if (listItems.length === 1) return listItems[0];

  return {
    type: 'list',
    children: listItems,
    operator: operators.join(','),
    raw: listItems.map(c => c.raw).join(' ; '),
  };
}

function parseBashASTInternal(input: string, depth: number): CommandNode | null {
  const trimmed = input.trim();
  if (!trimmed) return null;
  // Input-length cap (DoS defense): refuse to parse pathologically long input.
  if (trimmed.length > MAX_COMMAND_LENGTH) return null;

  const lexer = new BashLexer(trimmed);
  return parseList(lexer, depth);
}

export function parseBashAST(input: string): CommandNode | null {
  return parseBashASTInternal(input, 0);
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

export function extractCommandsFromString(input: string): string[] {
  const ast = parseBashAST(input);
  if (!ast) return [];
  return extractCommands(ast);
}
