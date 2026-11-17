import { globToRegexSource } from '../permissions';

/**
 * Read-ignore (file-READ deny) exclusions for the search tools.
 *
 * Files the permission layer denies for READING (e.g. `.env`, credential stores)
 * must never surface in search output — in EVERY output mode, including `content`
 * mode which would otherwise print their matching LINES, not just their names.
 * Claude Code injects the permission context's read-deny patterns into ripgrep as
 * negated globs (`--glob !pattern`, see GrepTool.ts getFileReadIgnorePatterns); AMI
 * mirrors that and additionally honors the same patterns on the grep and in-process
 * search fallbacks, so no AMI search path can leak the contents or path of a file
 * the user forbade reading. Previously AMI's grep only excluded VCS metadata dirs
 * and consulted no read-deny patterns at all.
 */

/**
 * Convert a file-read deny glob into ripgrep's negated-glob form. ripgrep only
 * applies ignore globs relative to the working directory, so a pattern that is not
 * already anchored with a leading `/` is prefixed with `**` + `/` to match at any
 * depth; the result is negated with `!`. Mirrors Claude Code's `rgIgnorePattern`
 * (prefix an unanchored pattern with the any-depth glob, then negate).
 */
export function toRgIgnoreGlob(pattern: string): string {
  return `!${toAnyDepthGlob(pattern)}`;
}

/**
 * The POSITIVE any-depth glob for a read-deny pattern: a pattern already anchored with
 * a leading slash, or already carrying the any-depth prefix, is kept as-is; otherwise
 * the any-depth prefix is prepended so it matches at every directory level. Used as a
 * plain exclude glob by the glob tool (which negates it for ripgrep and feeds it to
 * tinyglobby's `ignore`); `toRgIgnoreGlob` is just this negated for grep's rg args.
 */
export function toAnyDepthGlob(pattern: string): string {
  if (pattern.startsWith('/') || pattern.startsWith('**/')) return pattern;
  return `**/${pattern}`;
}

/**
 * Best-effort basename glob for GNU grep's `--exclude`, which matches only the final
 * path component. Strips a leading slash anchor and an any-depth prefix and keeps the
 * last segment, so a deny pattern anchored at any depth (e.g. matching `.env`) excludes
 * files named `.env` on the grep fallback path (grep has no path-relative ignore).
 */
export function toGrepExcludeGlob(pattern: string): string {
  const noAnchor = pattern.replace(/^\/+/, '').replace(/^\*\*\/+/, '');
  const slash = noAnchor.lastIndexOf('/');
  return slash >= 0 ? noAnchor.slice(slash + 1) : noAnchor;
}

/**
 * True when a file's relative path matches any read-deny glob — used by the
 * in-process fallback walk (no rg/grep) to drop denied files before reading them.
 * Reuses the permission layer's own glob→regex primitive so the exclusion matches
 * exactly what a read of the file would be denied for. A `**​/`-anchored or
 * unanchored pattern is tested against both the full relative path and the bare
 * basename, so an any-depth `.env` pattern, a bare `.env`, and `src/*.key` all hide
 * the right files.
 */
export function matchesReadIgnore(relPath: string, patterns: readonly string[]): boolean {
  const normalizedRel = relPath.replace(/\\/g, '/');
  const base = normalizedRel.slice(normalizedRel.lastIndexOf('/') + 1);
  for (const pattern of patterns) {
    const norm = pattern.replace(/^\/+/, '');
    const core = norm.startsWith('**/') ? norm.slice(3) : norm;
    let re: RegExp;
    try {
      re = new RegExp(`^${globToRegexSource(core)}$`, 's');
    } catch {
      continue;
    }
    if (re.test(normalizedRel) || re.test(base)) return true;
  }
  return false;
}
