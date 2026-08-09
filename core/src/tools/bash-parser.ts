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

function parseSimpleCommand(lexer: BashLexer): CommandNode | null {
  lexer.skipWhitespace();
  if (lexer.isAtEnd()) return null;

  const p = lexer.peek();

  if (p === '(') {
    lexer.advance();
    const inner = lexer.readUntilCloseParen();
    const children = parseBashAST(inner);
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
    const children = parseBashAST(inner);
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
  const rawEnd = lexer.getPosition();

  return {
    type: 'simple',
    command: command || (envVars.length > 0 ? envVars[0] : ''),
    args,
    raw: lexer.getRemaining().length < startPos ? command : words.join(' '),
  };
}

function parsePipeline(lexer: BashLexer): CommandNode | null {
  const first = parseSimpleCommand(lexer);
  if (!first) return null;

  const pipelineCommands: CommandNode[] = [first];

  while (!lexer.isAtEnd()) {
    const p = lexer.peek();
    if (p === '|' && lexer.peekTwo() !== '||') {
      lexer.advance(); // consume |
      const next = parseSimpleCommand(lexer);
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

function parseList(lexer: BashLexer): CommandNode | null {
  const first = parsePipeline(lexer);
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
      const next = parsePipeline(lexer);
      if (next) listItems.push(next);
    } else if (p === ';' || p === '\n') {
      operators.push(';');
      lexer.advance();
      const next = parsePipeline(lexer);
      if (next) listItems.push(next);
    } else if (p === '&') {
      operators.push('&');
      lexer.advance();
      const next = parsePipeline(lexer);
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

export function parseBashAST(input: string): CommandNode | null {
  const trimmed = input.trim();
  if (!trimmed) return null;

  const lexer = new BashLexer(trimmed);
  return parseList(lexer);
}

export function extractCommands(node: CommandNode): string[] {
  const commands: string[] = [];

  function walk(n: CommandNode): void {
    if (n.type === 'simple' && n.command) {
      commands.push(n.command);
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
