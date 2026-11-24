import { MAX_NODES, PARSE_TIMEOUT_MS, resolveEnvValue, resolveShellWord } from './bash-parser-resolve-shell-word';
import type { CommandNode, DeclaredAssignment, ParseBudget, ParseEnv, ParseState } from './bash-parser-resolve-shell-word';
import { parseBashASTInternal } from './bash-parser-bash-lexer';
export { getOutputRedirections, getPipeSegments, resolveShellWord, withoutOutputRedirections } from './bash-parser-resolve-shell-word';
export type { CommandNode, DeclaredAssignment, OutputRedirection, ParseBudget, ParseEnv } from './bash-parser-resolve-shell-word';

/**
 * SECURITY: Sentinel for "the parser ran but ABORTED" — it exhausted a resource
 * budget (node count or wall-clock timeout) or threw on pathological/adversarial
 * input. It is DISTINCT from `null` (empty / over-length / genuinely unparseable):
 * callers MUST treat `PARSE_ABORTED` as fail-CLOSED ("too complex to analyze →
 * block"), never route it to the permissive "parse-unavailable → PASS" path.
 * Mirrors the reference's PARSE_ABORTED symbol (utils/bash/parser.ts).
 */
export const PARSE_ABORTED: unique symbol = Symbol('parse-aborted');

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
 * Mirrors the reference's DECLARATION_COMMANDS allow-set
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

// Shell keywords/control-structure tokens. A keyword that leads a simple-command
// node means the node's real command is hidden behind it (e.g. `then nc …`,
// `do socat …`, `{ insmod …`). extractCommands uses these sets to recover the
// hidden command so AST-based blockers in bash-security see inside control-flow
// bodies and brace command groups instead of only seeing the keyword.
const KEYWORD_CMD_INTRO = new Set(['if', 'elif', 'while', 'until', 'then', 'else', 'do', '{', '!', 'time']);
const KEYWORD_VAR_INTRO = new Set(['for', 'select', 'case']);
const KEYWORD_TERMINATOR = new Set(['fi', 'done', 'esac', '}', 'in']);
const SHELL_KEYWORDS = new Set<string>([...KEYWORD_CMD_INTRO, ...KEYWORD_VAR_INTRO, ...KEYWORD_TERMINATOR]);

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
 * read it). Mirrors the reference's size-1 last-command parse cache.
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
 * assignment scope. Mirrors the reference's indirect-command resolution.
 */
function parseVariableReference(token: string): string | null {
  const m = /^\$\{([A-Za-z_]\w*)\}$/.exec(token) ?? /^\$([A-Za-z_]\w*)$/.exec(token);
  return m ? m[1] : null;
}

export function extractCommands(node: CommandNode, env?: ParseEnv): string[] {
  const commands: string[] = [];

  // `scope` is a static map of variable name → assigned value, tracked so a
  // `$VAR` later used as a command name resolves to the literal the shell would
  // run. The scope is CARRIED across `&&`/`;` (both clauses run with the prior
  // assignment visible) but RESET across `||`, `|` and `&` and NOT propagated
  // out of a subshell/command-substitution — clauses that may not run, or run in
  // a subshell, must not leak a value. Mirrors the reference's varScope reset rules.
  function walk(n: CommandNode, scope: Map<string, string>): void {
    if (n.type === 'simple' && n.command) {
      if (n.assignments) {
        for (const [k, v] of Object.entries(n.assignments)) scope.set(k, v);
      }
      const varName = parseVariableReference(n.command);
      // Resolve a `$VAR` command name: the in-command static scope WINS (a value
      // assigned within the command is what the shell runs); otherwise fall back
      // to the caller-supplied env so env-pinned command indirection surfaces.
      let resolved = n.command;
      if (varName !== null) {
        if (scope.has(varName)) {
          resolved = scope.get(varName) as string;
        } else {
          const fromEnv = resolveEnvValue(env, varName);
          if (fromEnv !== undefined) resolved = fromEnv;
        }
      }
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
 * Mirrors the reference's declaration-command argument extraction.
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

export function extractCommandsFromString(
  input: string,
  budget?: ParseBudget,
  env?: ParseEnv,
): string[] {
  const ast = parseBashAST(input, budget);
  // No AST (empty/over-length) OR an aborted parse → nothing statically
  // extractable. The abort case is surfaced as a hard block by the security
  // gate (validateCompoundCommand), not here, so extraction stays total.
  if (!ast || ast === PARSE_ABORTED) return [];
  return extractCommands(ast, env);
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
 * argument) from one that is actually chaining commands — mirroring the reference's
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
 * Mirrors the reference's `DangerousPatterns`
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
 * operators and top-level segments. Mirrors the reference's `CompoundStructure`
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

/** Unified structured analysis of a bash command. Mirrors the reference's `TreeSitterAnalysis`. */
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
 * empty structural fields). Mirrors the reference's `analyzeCommand`
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
