// Extracted leaf: git read-only invocation classifier. Dependency-free so
// tools/bash-cache.ts imports it without a bash-cache -> permissions cycle.

/**
 * Git read-only write-guard — shared by the bash classifier AND the bash-result
 * cache (`isReadOnlyBashCommand`) so there is ONE place that knows which git
 * forms actually mutate state. A git subcommand can appear in a "read-only"
 * allowlist yet still WRITE: creating a ref (`git branch <name>`, `git tag
 * <name>`), rewriting remotes (`git remote add/remove/set-url/rename`), writing
 * a file via `--output`/`-o`, or setting config (`git config key value`).
 * the reference's readOnlyCommandValidation.ts rejects each of these before
 * auto-approval; this mirrors that.
 *
 * `tokens` is a single git invocation beginning at the `git` token. Returns
 * true when the invocation is a genuine read-only form, false when it writes.
 */
export function isGitInvocationReadOnly(tokens: string[]): boolean {
  if (tokens[0] !== 'git') return true; // not a git invocation — not our concern

  // Skip git global options (and the value they consume) to reach the subcommand.
  const valueGlobals = new Set(['-C', '-c', '--git-dir', '--work-tree', '--namespace']);
  // Globals that change git's EXECUTION CONTEXT — a different repo/worktree
  // (`-C`, `--git-dir`, `--work-tree`, `--namespace`) whose on-disk config can run
  // arbitrary code during a "read" (e.g. a malicious `core.fsmonitor`), or an
  // inline config injection (`-c core.pager=<cmd>` / `--config-env`) — are NEVER
  // auto-allowed as read-only, mirroring AMI's existing cd+git RCE guard
  // (isGitReadOnlyCommandSafe) so a global flag before the subcommand cannot
  // smuggle an exec vector past the classifier. A prompt is still shown.
  const execContextGlobals = new Set([
    '-C', '-c', '--git-dir', '--work-tree', '--namespace', '--config-env',
  ]);
  let i = 1;
  while (i < tokens.length) {
    const t = tokens[i]!;
    if (!t.startsWith('-')) break;
    const bare = t.includes('=') ? t.slice(0, t.indexOf('=')) : t;
    if (execContextGlobals.has(bare)) return false; // exec-context change / config injection
    if (valueGlobals.has(t)) { i += 2; continue; } // `--opt value`
    i += 1;                                         // flag or `--opt=value`
  }
  const sub = tokens[i];
  if (!sub) return true; // bare `git` prints help — harmless
  const rest = tokens.slice(i + 1);

  // A file-writing output flag turns ANY read subcommand into a write.
  for (const a of rest) {
    if (a === '-o' || a === '--output' || a.startsWith('--output=')) return false;
  }

  // `git ls-remote` is read-only EXCEPT `--server-option`/`-o`, which transmit an
  // arbitrary attacker-controlled string to the remote in the protocol-v2
  // capability advertisement — a network WRITE / exfil primitive on a nominally
  // read-only command. `-o` is already caught by the output-flag scan above; block
  // the long form here too. Mirrors the reference's ls-remote --server-option exclusion.
  if (sub === 'ls-remote') {
    for (const a of rest) {
      if (a === '--server-option' || a.startsWith('--server-option=')) return false;
    }
  }

  if (sub === 'branch' || sub === 'tag') {
    const writeFlags = sub === 'branch'
      ? new Set(['-d', '-D', '--delete', '-m', '-M', '--move', '-c', '-C', '--copy',
          '-f', '--force', '--set-upstream-to', '-u', '--unset-upstream', '--edit-description'])
      : new Set(['-d', '--delete', '-a', '--annotate', '-s', '--sign', '-m', '--message',
          '-F', '--file', '-f', '--force']);
    // Read flags whose FOLLOWING token is a value (not a ref name / positional).
    const valueFlags = new Set(['--contains', '--no-contains', '--merged', '--no-merged',
      '--points-at', '--sort', '--format', '--color', '--column']);
    let hasList = false;
    let hasPositional = false;
    let endOfOptions = false; // after a bare `--`, everything is a positional
    for (let k = 0; k < rest.length; k++) {
      const a = rest[k]!;
      if (endOfOptions) { hasPositional = true; continue; } // post-`--` tokens are ref names
      if (a === '--') { endOfOptions = true; continue; } // `git branch -- -l` → `-l` is a ref name
      if (a === '-l' || a === '--list') { hasList = true; continue; }
      if (writeFlags.has(a)) return false;
      if (a.startsWith('-')) {
        if (!a.includes('=') && valueFlags.has(a)) k++; // skip the consumed value
        continue;
      }
      hasPositional = true; // a bare ref name
    }
    // A bare positional without --list is ref CREATION (a write).
    return !(hasPositional && !hasList);
  }

  if (sub === 'remote') {
    const first = rest.find(a => !a.startsWith('-'));
    if (!first) return true; // bare `git remote` / `git remote -v`
    return first === 'show' || first === 'get-url';
  }

  if (sub === 'config') {
    const writeSubops = ['--add', '--unset', '--unset-all', '--replace-all',
      '--remove-section', '--rename-section', '--edit', '-e'];
    if (rest.some(a => writeSubops.includes(a))) return false;
    const readSubops = ['--get', '--get-all', '--get-regexp', '--get-urlmatch', '--list', '-l'];
    if (rest.some(a => readSubops.includes(a))) return true;
    // `git config key value` writes; `git config key` reads a single value.
    const positionals = rest.filter(a => !a.startsWith('-'));
    return positionals.length < 2;
  }

  return true; // other read subcommands (status/log/diff/show/...) stay read-only
}
