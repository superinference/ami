// Split from file-edit.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { isENOENT } from '../utils/errno';
import { getLineNumberStyle, type LineNumberStyle } from './file-read-limits';

/**
 * Boundary-transparency disclosure for a BLOCKED symlink write target (parity:
 * the reference's ShowInIDEPrompt symlink-target surfacing). AMI takes a STRONGER
 * posture than the reference — {@link validateFileWritePath} blocks EVERY symlink write
 * target rather than disclosing-and-allowing an in-workspace one — but the bare
 * "is a symlink — write blocked" error never told the user WHERE the link
 * redirects, so the boundary crossing it protected against was invisible at the
 * approval/result surface. This returns a suffix naming the symlink's resolved
 * target and whether it points inside or outside the workspace, appended to the
 * block error so the user sees the redirection they were protected from. A live
 * link uses its realpath; a DANGLING link falls back to its declared destination
 * (readlink). Best-effort: a non-symlink path, or any lstat/readlink/realpath
 * failure, yields '' (the base block error still stands) — so it contributes
 * nothing to a non-symlink validation failure (null byte, glob metachar, …).
 */
export function disclaimSymlinkTarget(resolved: string, cwd: string): string {
  try {
    const st = fs.lstatSync(resolved);
    if (!st.isSymbolicLink()) return '';
    const cwdAbs = path.resolve(cwd);
    let target: string;
    try {
      target = fs.realpathSync(resolved);
    } catch {
      // Dangling link (target does not exist yet) — disclose the DECLARED
      // destination it would create/redirect to, resolved relative to the link.
      target = path.resolve(path.dirname(resolved), fs.readlinkSync(resolved));
    }
    const inside = target === cwdAbs || target.startsWith(cwdAbs + path.sep);
    return ` It points to "${target}" (${inside ? 'inside' : 'outside'} the workspace); edit the target path directly instead.`;
  } catch {
    return '';
  }
}

export function verifyWriteTarget(resolved: string, cwd: string): string | undefined {
  const cwdAbs = path.resolve(cwd);

  try {
    const stat = fs.lstatSync(resolved);
    if (stat.isSymbolicLink()) {
      let target: string;
      try {
        target = fs.realpathSync(resolved);
      } catch (realErr) {
        // realpath threw ENOENT but lstat says this entry IS a symlink → it is a
        // DANGLING symlink (its target does not exist yet). An attacker can pre-place
        // such a link (e.g. x -> ~/.ssh/authorized_keys) so a later write creates and
        // populates the target OUTSIDE the workspace. Resolve the link's DECLARED
        // destination and block it when it escapes, instead of silently falling
        // through to the "nonexistent, nothing to escape" path.
        if (isENOENT(realErr)) {
          const linkDest = path.resolve(path.dirname(resolved), fs.readlinkSync(resolved));
          if (!linkDest.startsWith(cwdAbs + path.sep) && linkDest !== cwdAbs) {
            return `Error: "${resolved}" is a dangling symlink pointing to "${linkDest}" outside the workspace — write blocked (symlink-escape).`;
          }
          // Dangling but points back inside the workspace — harmless; fall through to
          // the parent-directory checks below.
          target = linkDest;
        } else {
          throw realErr;
        }
      }
      if (!target.startsWith(cwdAbs + path.sep) && target !== cwdAbs) {
        return `Error: "${resolved}" is a symlink resolving to "${target}" outside the workspace — write blocked.`;
      }
    }
  } catch (err) {
    // A thrown error from the dangling-symlink branch above must propagate as a block,
    // not be swallowed as "nonexistent". Only a genuinely-absent path (lstat ENOENT)
    // means there is no symlink to escape through.
    if (!isENOENT(err)) {
      return `Error: cannot verify write target "${resolved}": ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  // Check parent directories for symlinks escaping the workspace
  let dir = path.dirname(path.resolve(resolved));
  while (dir.startsWith(cwdAbs + path.sep) || dir === cwdAbs) {
    if (dir === cwdAbs) break;
    try {
      const dirStat = fs.lstatSync(dir);
      if (dirStat.isSymbolicLink()) {
        const realDir = fs.realpathSync(dir);
        if (!realDir.startsWith(cwdAbs + path.sep) && realDir !== cwdAbs) {
          return `Error: parent directory "${dir}" is a symlink resolving to "${realDir}" outside the workspace — write blocked.`;
        }
      }
    } catch {
      // Directory doesn't exist yet — will be created by mkdirSync. (A non-ENOENT
      // problem in an ancestor makes the write-target lstat above throw first,
      // where it already fails closed, so this path only sees genuine absence.)
    }
    dir = path.dirname(dir);
  }

  return undefined;
}

/**
 * Build the file_edit tool description, reflecting the ACTIVE Read line-number
 * prefix format so the model knows exactly which prefix to strip from
 * old_string/new_string. In the default `compact` style the prefix is a number +
 * tab (`N\t<content>`); in the `arrow` style it is a padded number + arrow
 * (`     N→<content>`). Mirrors the reference's getEditToolDescription(), which
 * switches the "line number prefix format" clause on isCompactLinePrefixEnabled().
 */
export function buildFileEditDescription(style: LineNumberStyle = getLineNumberStyle()): string {
  const prefixClause = style === 'arrow'
    ? 'file_read output prefixes EVERY line with its right-padded line number and an arrow ("     N→<content>"), and old_string must contain ONLY the content that appears AFTER that arrow — never the "N→" prefix'
    : 'file_read output prefixes EVERY line with its line number and a tab ("N\\t<content>"), and old_string must contain ONLY the content that appears AFTER that tab — never the "N\\t" prefix';
  return 'Create or edit a file. You MUST file_read an existing file before changing it. ALWAYS prefer editing an existing file (one of the replace modes, which send only the diff) over creating a new one; use the Write/create mode only to create a genuinely new file or for a complete rewrite. NEVER proactively create documentation files (*.md) or README files unless the User explicitly requests them. Only write emojis into a file if the User explicitly asks for them. Modes: (1) Replace — file_path + old_string + new_string; old_string must match exactly (use the smallest unique 2–4 line span). Set replace_all to replace every occurrence. (2) Multiple replaces — file_path + edits: [{old_string, new_string, replace_all?}]; applied in order. (3) Write/create — file_path + new_string (or content) with old_string omitted or empty: creates the file or replaces its entire contents. Do not include line numbers: '
    + prefixClause
    + ' — with the original indentation (tabs/spaces) preserved exactly as written in the file. Never use bash sed/awk to edit files. Edits are addressed by EXACT STRING MATCH on content (old_string), never by line number: this is the canonical, intentional addressing scheme — AMI does not use line-reference ("hashline") edits, because a string match stays correct even when surrounding lines shift, whereas a stale line number silently edits the wrong span.';
}
