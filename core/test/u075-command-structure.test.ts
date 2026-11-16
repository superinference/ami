import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  analyzeCommandStructure,
  parseBashAST,
  PARSE_ABORTED,
  type CommandNode,
} from '../src/tools/bash-parser';
import { validateASTCommands, CHECK_IDS } from '../src/tools/bash-security';

// Parity: Claude Code's treeSitterAnalysis.ts exposes a single structured result
// (DangerousPatterns { hasCommandSubstitution, hasProcessSubstitution,
// hasParameterExpansion, hasHeredoc, hasComment } + CompoundStructure
// { hasCompoundOperators, hasPipeline, hasSubshell, hasCommandGroup, operators[],
// segments[] }) so consumers read reusable boolean fields instead of each
// re-scanning the string. See
// references/claude/claude-code-main/src/utils/bash/treeSitterAnalysis.ts:L45-L64.

describe('analyzeCommandStructure — DangerousPatterns', () => {
  it('detects $(...) command substitution (unquoted)', () => {
    const p = analyzeCommandStructure('echo $(whoami)').dangerousPatterns;
    assert.equal(p.hasCommandSubstitution, true);
  });

  it('detects $(...) command substitution inside double quotes', () => {
    const p = analyzeCommandStructure('echo "$(whoami)"').dangerousPatterns;
    assert.equal(p.hasCommandSubstitution, true);
  });

  it('detects backtick command substitution', () => {
    const p = analyzeCommandStructure('echo `id`').dangerousPatterns;
    assert.equal(p.hasCommandSubstitution, true);
  });

  it('does NOT flag command substitution inside single quotes', () => {
    const p = analyzeCommandStructure("echo '$(whoami)'").dangerousPatterns;
    assert.equal(p.hasCommandSubstitution, false);
    assert.equal(p.hasParameterExpansion, false);
  });

  it('detects <(...) and >(...) process substitution', () => {
    assert.equal(analyzeCommandStructure('diff <(a) <(b)').dangerousPatterns.hasProcessSubstitution, true);
    assert.equal(analyzeCommandStructure('tee >(cat)').dangerousPatterns.hasProcessSubstitution, true);
  });

  it('detects ${...} parameter expansion (and inside double quotes)', () => {
    assert.equal(analyzeCommandStructure('echo ${HOME}').dangerousPatterns.hasParameterExpansion, true);
    assert.equal(analyzeCommandStructure('echo "${HOME}"').dangerousPatterns.hasParameterExpansion, true);
  });

  it('detects heredoc <<', () => {
    assert.equal(analyzeCommandStructure("cat <<'EOF'\nhi\nEOF").dangerousPatterns.hasHeredoc, true);
  });

  it('detects an unquoted # comment but not a quoted/mid-word #', () => {
    assert.equal(analyzeCommandStructure('ls # listing').dangerousPatterns.hasComment, true);
    assert.equal(analyzeCommandStructure('echo "a # b"').dangerousPatterns.hasComment, false);
    assert.equal(analyzeCommandStructure('echo a#b').dangerousPatterns.hasComment, false);
  });

  it('returns all-false for a plain simple command', () => {
    const p = analyzeCommandStructure('ls -la').dangerousPatterns;
    assert.deepEqual(p, {
      hasCommandSubstitution: false,
      hasProcessSubstitution: false,
      hasParameterExpansion: false,
      hasHeredoc: false,
      hasComment: false,
    });
  });
});

describe('analyzeCommandStructure — CompoundStructure', () => {
  it('flags compound operators and records them in source order', () => {
    const s = analyzeCommandStructure('cd src && git status || echo no').compoundStructure;
    assert.equal(s.hasCompoundOperators, true);
    assert.deepEqual(s.operators, ['&&', '||']);
  });

  it('flags pipelines and records | operators', () => {
    const s = analyzeCommandStructure('cat f | grep x | wc -l').compoundStructure;
    assert.equal(s.hasPipeline, true);
    assert.deepEqual(s.operators, ['|', '|']);
    assert.equal(s.segments.length, 3);
  });

  it('flags subshells', () => {
    assert.equal(analyzeCommandStructure('(cd /tmp && ls)').compoundStructure.hasSubshell, true);
  });

  it('flags a brace command group but not brace expansion', () => {
    assert.equal(analyzeCommandStructure('{ echo hi; }').compoundStructure.hasCommandGroup, true);
    assert.equal(analyzeCommandStructure('echo {a,b}').compoundStructure.hasCommandGroup, false);
  });

  it('exposes top-level segments for a compound list', () => {
    const s = analyzeCommandStructure('a && b && c').compoundStructure;
    assert.equal(s.segments.length, 3);
  });

  it('is all-false with a single segment for a plain command', () => {
    const s = analyzeCommandStructure('ls -la').compoundStructure;
    assert.equal(s.hasCompoundOperators, false);
    assert.equal(s.hasPipeline, false);
    assert.equal(s.hasSubshell, false);
    assert.equal(s.hasCommandGroup, false);
    assert.deepEqual(s.operators, []);
    assert.deepEqual(s.segments, ['ls -la']);
  });

  it('accepts a pre-parsed AST node (no re-parse)', () => {
    const ast = parseBashAST('a | b') as CommandNode;
    const s = analyzeCommandStructure('a | b', ast).compoundStructure;
    assert.equal(s.hasPipeline, true);
  });

  it('yields empty structural fields when the AST is null/aborted', () => {
    const s = analyzeCommandStructure('', null).compoundStructure;
    assert.equal(s.hasPipeline, false);
    assert.deepEqual(s.segments, []);
    const s2 = analyzeCommandStructure('x', PARSE_ABORTED).compoundStructure;
    assert.deepEqual(s2.segments, []);
  });
});

// Wiring: validateASTCommands consumes the structured analysis to skip the
// subshell-depth walk when no subshell / command substitution is present. This
// must stay behavior-preserving — deeply nested subshells / $() still blocked.
describe('validateASTCommands still blocks deep nesting after structure short-circuit', () => {
  it('blocks >5 nested subshells', () => {
    const r = validateASTCommands('((((((echo hi))))))');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.AST_HIDDEN_COMMAND);
  });

  it('blocks >5 nested leading command substitutions', () => {
    const r = validateASTCommands('$($($($($($($(whoami)))))))');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.AST_HIDDEN_COMMAND);
  });

  it('passes a plain command (short-circuit path, no subshell)', () => {
    assert.equal(validateASTCommands('ls -la').safe, true);
  });
});
