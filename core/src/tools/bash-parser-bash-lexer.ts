// Split from bash-parser.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { MAX_COMMAND_LENGTH, MAX_PARSE_DEPTH, countNode, resolveShellWord, scanOutputRedirections } from './bash-parser-resolve-shell-word';
import type { CommandNode, ParseState } from './bash-parser-resolve-shell-word';

export class BashLexer {
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

export function parseSimpleCommand(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
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

export function parsePipeline(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
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

export function parseList(lexer: BashLexer, depth: number, state: ParseState): CommandNode | null {
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

export function parseBashASTInternal(input: string, depth: number, state: ParseState): CommandNode | null {
  const trimmed = input.trim();
  if (!trimmed) return null;
  // Input-length cap (DoS defense): refuse to parse pathologically long input.
  if (trimmed.length > MAX_COMMAND_LENGTH) return null;

  const lexer = new BashLexer(trimmed);
  return parseList(lexer, depth, state);
}
