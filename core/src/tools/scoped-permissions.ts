import * as path from 'node:path';

/**
 * A least-privilege CAPABILITY CEILING for a forked / background sub-agent: a hard
 * ceiling the agent can never exceed, applied BEFORE and INDEPENDENT of the ambient
 * permission mode (so it holds even under `auto-allow` / `bypassPermissions`).
 *
 * Parity with Claude Code's `createAutoMemCanUseTool` (the per-tool-call predicate
 * `services/extractMemories/extractMemories.ts` wires into `runForkedAgent` for
 * autoDream / extractMemories): it allows Read/Grep/Glob unrestricted, allows Bash
 * ONLY for read-only commands, allows Edit/Write ONLY for paths inside a confined
 * directory, and otherwise lets the normal permission engine decide. AMI previously
 * forked skill sub-agents with a hard-coded broad `permissionMode` and only filtered
 * the tool LIST (all-or-nothing per tool) — a forked skill that kept `bash` could run
 * any write/destructive command, and there was no way to confine its file writes to a
 * single directory. This ceiling restores the autoDream-style reduced ceiling.
 *
 * The two restrictions are intentionally the exact pair the reference composes:
 * `readOnlyBashOnly` (= `BashTool.isReadOnly`) + a write-root whitelist. Everything
 * else is left to the normal permission rules — the ceiling only ever *narrows*, it
 * never *grants*.
 */
export interface ToolCapabilityCeiling {
  /** When true, a `bash` call whose command is NOT read-only is denied. */
  readOnlyBashOnly?: boolean;
  /** Absolute, normalized directories that file-mutating tools may write WITHIN.
   *  When defined, an edit/write outside every root (or with no resolvable path) is
   *  denied — `[]` means "no writes anywhere". When undefined, writes are not
   *  ceiling-confined (the normal permission rules still apply). */
  writeRoots?: string[];
  /** Base directory a relative input `file_path` is resolved against before the
   *  write-root containment check (the forked sub-engine's cwd). */
  baseDir?: string;
}

/** A ceiling denial: `reason` is a user/model-facing explanation of what the ceiling
 *  forbids. `undefined` from {@link evaluateToolCeiling} means the ceiling does not
 *  deny this call (defer to the normal permission engine). */
export interface CeilingDenial {
  reason: string;
}

/** Canonical file-MUTATING tool names the write-root whitelist confines. */
const CEILING_WRITE_TOOLS: ReadonlySet<string> = new Set([
  'file_edit',
  'file_write',
  'notebook_edit',
  'multi_edit',
]);

/** Pull the bash command text out of a tool input, if present as a string. */
function extractCommand(input: Record<string, unknown>): string | undefined {
  const c = input?.command;
  return typeof c === 'string' ? c : undefined;
}

/** Pull the target file path out of a file-mutating tool input, if present. */
function extractFilePath(input: Record<string, unknown>): string | undefined {
  const fp = input?.file_path ?? input?.path;
  return typeof fp === 'string' && fp.length > 0 ? fp : undefined;
}

/**
 * True when `filePath` resolves INSIDE one of the (already absolute+normalized)
 * `roots`. A relative path is resolved against `baseDir` (falling back to the process
 * cwd) first. Containment is a path-boundary prefix match (`root` itself, or
 * `root + sep + …`) so `/a/b` is contained by `/a` but `/a/bc` is not.
 */
export function isPathWithinRoots(
  filePath: string,
  roots: readonly string[],
  baseDir?: string,
): boolean {
  if (roots.length === 0) return false;
  const abs = path.isAbsolute(filePath)
    ? path.normalize(filePath)
    : path.resolve(baseDir ?? process.cwd(), filePath);
  return roots.some(root => abs === root || abs.startsWith(root + path.sep));
}

/**
 * Evaluate the capability ceiling for a single tool call. Returns a {@link CeilingDenial}
 * when the ceiling forbids the call, or `undefined` to defer to the normal permission
 * engine. Pure + exported for testability.
 *
 * @param isReadOnlyBash predicate reporting whether a bash command is read-only
 *   (injected so this stays decoupled from the permission engine's classifier).
 */
export function evaluateToolCeiling(
  ceiling: ToolCapabilityCeiling | undefined,
  toolName: string,
  input: Record<string, unknown>,
  isReadOnlyBash: (command: string) => boolean,
): CeilingDenial | undefined {
  if (!ceiling) return undefined;

  if (ceiling.readOnlyBashOnly && toolName === 'bash') {
    const command = extractCommand(input);
    if (command !== undefined && !isReadOnlyBash(command)) {
      return {
        reason:
          'Only read-only shell commands are permitted in this sandboxed skill context ' +
          '(ls, find, grep, cat, stat, wc, head, tail, and similar). Anything that writes, ' +
          'redirects to a file, or modifies state is denied.',
      };
    }
  }

  if (ceiling.writeRoots && CEILING_WRITE_TOOLS.has(toolName)) {
    const filePath = extractFilePath(input);
    if (filePath === undefined || !isPathWithinRoots(filePath, ceiling.writeRoots, ceiling.baseDir)) {
      const scope = ceiling.writeRoots.length > 0 ? ceiling.writeRoots.join(', ') : '(no directory)';
      return {
        reason: `Writes in this sandboxed skill context are confined to: ${scope}.`,
      };
    }
  }

  return undefined;
}

/**
 * Build a {@link ToolCapabilityCeiling} from a skill's declared sandbox settings, or
 * `undefined` when the skill declares none (the ceiling is inert — the normal
 * permission engine governs). Relative write roots are resolved against `cwd` so the
 * containment check compares absolute paths. Pure + exported for testability.
 */
export function buildSkillSandboxCeiling(
  readOnlyBash: boolean | undefined,
  writeRoots: readonly string[] | undefined,
  cwd: string,
): ToolCapabilityCeiling | undefined {
  const ceiling: ToolCapabilityCeiling = {};
  if (readOnlyBash) ceiling.readOnlyBashOnly = true;
  if (writeRoots) {
    ceiling.writeRoots = writeRoots.map(r =>
      path.isAbsolute(r) ? path.normalize(r) : path.resolve(cwd, r),
    );
    ceiling.baseDir = cwd;
  }
  return ceiling.readOnlyBashOnly || ceiling.writeRoots ? ceiling : undefined;
}
