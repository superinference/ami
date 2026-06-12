import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'path';

import { ToolCallGuardrailController, type GuardrailDecision } from '../src/tool-guardrails';
import {
  validateRequiredString,
  resolveSearchPath,
  resolveFilePath,
  validatePatternAndPath,
  extractQuery,
  buildToolDescriptionContext,
  renderToolDescription,
  detectLineEnding,
  normalizeToLf,
  convertToLineEnding,
} from '../src/tools/tool-utils';
import { setSearchableTools, toolSearchTool } from '../src/tools/tool-search';

// ============================================================================
// ToolCallGuardrailController
// ============================================================================

describe('ToolCallGuardrailController', () => {
  let ctrl: ToolCallGuardrailController;

  beforeEach(() => {
    ctrl = new ToolCallGuardrailController();
  });

  // --------------------------------------------------------------------------
  // beforeCall
  // --------------------------------------------------------------------------

  describe('beforeCall', () => {
    it('allows the first call with no prior failures', () => {
      const d = ctrl.beforeCall('bash', { cmd: 'ls' });
      assert.equal(d.action, 'allow');
      assert.equal(d.reason, undefined);
    });

    it('allows when failures exist but below warn threshold', () => {
      // Record 1 failure
      ctrl.afterCall('bash', { cmd: 'ls' }, 'error output', true);
      const d = ctrl.beforeCall('bash', { cmd: 'ls' });
      assert.equal(d.action, 'allow');
    });

    it('warns after EXACT_FAILURE_WARN (2) failures with identical args', () => {
      const args = { cmd: 'npm test' };
      ctrl.afterCall('bash', args, 'fail1', true);
      ctrl.afterCall('bash', args, 'fail2', true);
      const d = ctrl.beforeCall('bash', args);
      assert.equal(d.action, 'warn');
      assert.ok(d.reason!.includes('failed 2 times'));
      assert.ok(d.reason!.includes('bash'));
    });

    it('blocks after EXACT_FAILURE_BLOCK (4) failures with identical args', () => {
      const args = { cmd: 'npm test' };
      for (let i = 0; i < 4; i++) {
        ctrl.afterCall('bash', args, `fail${i}`, true);
      }
      const d = ctrl.beforeCall('bash', args);
      assert.equal(d.action, 'block');
      assert.ok(d.reason!.includes('failed 4 times'));
      assert.ok(d.reason!.includes('Do NOT retry'));
    });

    it('includes recovery hint for bash tool', () => {
      const args = { cmd: 'test' };
      ctrl.afterCall('bash', args, 'err', true);
      ctrl.afterCall('bash', args, 'err', true);
      const d = ctrl.beforeCall('bash', args);
      assert.ok(d.reason!.includes('file_read'));
    });

    it('includes recovery hint for file_edit tool', () => {
      const args = { file: 'x.ts', old_string: 'a', new_string: 'b' };
      ctrl.afterCall('file_edit', args, 'err', true);
      ctrl.afterCall('file_edit', args, 'err', true);
      const d = ctrl.beforeCall('file_edit', args);
      assert.ok(d.reason!.includes('file_read'));
      assert.ok(d.reason!.includes('exact text'));
    });

    it('includes generic recovery hint for unknown tools', () => {
      const args = { q: 'test' };
      ctrl.afterCall('web_search', args, 'err', true);
      ctrl.afterCall('web_search', args, 'err', true);
      const d = ctrl.beforeCall('web_search', args);
      assert.ok(d.reason!.includes('different approach'));
    });

    it('does not confuse different args for the same tool', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'err', true);
      ctrl.afterCall('bash', { cmd: 'a' }, 'err', true);
      // Different args should be allowed
      const d = ctrl.beforeCall('bash', { cmd: 'b' });
      assert.equal(d.action, 'allow');
    });

    it('does not confuse different tools with similar args', () => {
      const args = { path: '/tmp/file.ts' };
      ctrl.afterCall('file_read', args, 'err', true);
      ctrl.afterCall('file_read', args, 'err', true);
      // Different tool should be allowed
      const d = ctrl.beforeCall('file_edit', args);
      assert.equal(d.action, 'allow');
    });
  });

  // --------------------------------------------------------------------------
  // afterCall
  // --------------------------------------------------------------------------

  describe('afterCall', () => {
    it('allows on first successful call', () => {
      const d = ctrl.afterCall('bash', { cmd: 'ls' }, 'success', false);
      assert.equal(d.action, 'allow');
    });

    it('allows on first failed call', () => {
      const d = ctrl.afterCall('bash', { cmd: 'ls' }, 'error', true);
      assert.equal(d.action, 'allow');
    });

    it('clears failure count on successful call with same args', () => {
      const args = { cmd: 'npm test' };
      ctrl.afterCall('bash', args, 'fail', true);
      ctrl.afterCall('bash', args, 'fail', true);
      // Now succeed — should clear the counter
      ctrl.afterCall('bash', args, 'pass', false);
      // beforeCall should now allow again
      const d = ctrl.beforeCall('bash', args);
      assert.equal(d.action, 'allow');
    });

    it('tracks no-progress for idempotent tools (warn at 3)', () => {
      const args = { path: '/tmp/file.txt' };
      const sameOutput = 'hello world content';
      // file_read is idempotent
      ctrl.afterCall('file_read', args, sameOutput, false);
      ctrl.afterCall('file_read', args, sameOutput, false);
      const d3 = ctrl.afterCall('file_read', args, sameOutput, false);
      assert.equal(d3.action, 'warn');
      assert.ok(d3.reason!.includes('same results repeatedly'));
      assert.ok(d3.reason!.includes('3 times'));
    });

    it('tracks no-progress for idempotent tools (block at 5)', () => {
      const args = { path: '/tmp/file.txt' };
      const sameOutput = 'hello world content';
      // Interleave different tool calls between each identical call to avoid
      // triggering the loop detector (which fires before no-progress check).
      for (let i = 0; i < 4; i++) {
        ctrl.afterCall('file_read', args, sameOutput, false);
        // Unique call to break any repeating pattern in callHistory
        ctrl.afterCall('bash', { cmd: `echo ${i}` }, `unique-${i}`, false);
      }
      const d5 = ctrl.afterCall('file_read', args, sameOutput, false);
      assert.equal(d5.action, 'block');
      assert.ok(d5.reason!.includes('identical results'));
      assert.ok(d5.reason!.includes('5 times'));
    });

    it('resets no-progress counter when output changes', () => {
      const args = { query: 'test' };
      ctrl.afterCall('grep', args, 'result1', false);
      ctrl.afterCall('grep', args, 'result1', false);
      // Output changes — counter resets
      ctrl.afterCall('grep', args, 'result2', false);
      ctrl.afterCall('grep', args, 'result2', false);
      // Only 2 repetitions of result2, should still be allow
      const d = ctrl.afterCall('grep', args, 'result2', false);
      assert.equal(d.action, 'warn'); // 3rd repetition of result2
    });

    it('does not track no-progress for non-idempotent tools', () => {
      const args = { cmd: 'echo hi' };
      const sameOutput = 'hi';
      for (let i = 0; i < 10; i++) {
        const d = ctrl.afterCall('bash', args, sameOutput, false);
        // bash is not idempotent so should never warn/block for no-progress
        if (d.action !== 'allow') {
          // Only loop detection could trigger non-allow, check it's not no-progress
          assert.ok(!d.reason!.includes('identical results'));
        }
      }
    });

    it('does not track no-progress for failed idempotent calls', () => {
      const args = { path: '/tmp/file.txt' };
      const sameOutput = 'file not found';
      for (let i = 0; i < 6; i++) {
        ctrl.afterCall('file_read', args, sameOutput, true);
      }
      // The beforeCall might block on exact failures but afterCall shouldn't
      // trigger no-progress since failed=true
      // We check that beforeCall triggers block on exact failures instead
      const d = ctrl.beforeCall('file_read', args);
      assert.equal(d.action, 'block');
    });

    it('detects repeating loop patterns', () => {
      // Build a repeating 2-element pattern
      ctrl.afterCall('bash', { cmd: 'a' }, 'out-a', false);
      ctrl.afterCall('file_read', { path: 'b' }, 'out-b', false);
      // Repeat the same pattern
      const d = ctrl.afterCall('bash', { cmd: 'a' }, 'out-a', false);
      const d2 = ctrl.afterCall('file_read', { path: 'b' }, 'out-b', false);
      // One of these should detect the loop
      const found = d.action === 'warn' || d2.action === 'warn';
      assert.ok(found, 'loop should be detected after repeating 2-element sequence');
    });
  });

  // --------------------------------------------------------------------------
  // detectLoop
  // --------------------------------------------------------------------------

  describe('detectLoop', () => {
    it('returns null with fewer than 4 history entries', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'out', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'out', false);
      ctrl.afterCall('bash', { cmd: 'c' }, 'out', false);
      assert.equal(ctrl.detectLoop(), null);
    });

    it('returns null with no repeating pattern', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'out-a', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'out-b', false);
      ctrl.afterCall('bash', { cmd: 'c' }, 'out-c', false);
      ctrl.afterCall('bash', { cmd: 'd' }, 'out-d', false);
      assert.equal(ctrl.detectLoop(), null);
    });

    it('detects a 2-element repeating sequence', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'out-a', false);
      ctrl.afterCall('file_read', { path: 'b' }, 'out-b', false);
      ctrl.afterCall('bash', { cmd: 'a' }, 'out-a', false);
      ctrl.afterCall('file_read', { path: 'b' }, 'out-b', false);
      const loop = ctrl.detectLoop();
      assert.ok(loop !== null);
      assert.equal(loop!.length, 2);
    });

    it('detects a 3-element repeating sequence', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('file_edit', { f: 'b' }, 'y', false);
      ctrl.afterCall('bash', { cmd: 'c' }, 'z', false);
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('file_edit', { f: 'b' }, 'y', false);
      ctrl.afterCall('bash', { cmd: 'c' }, 'z', false);
      const loop = ctrl.detectLoop();
      assert.ok(loop !== null);
      assert.ok(loop!.length >= 2);
    });

    it('prefers shortest repeating sequence', () => {
      // 2-element pattern repeated
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'y', false);
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'y', false);
      const loop = ctrl.detectLoop();
      assert.ok(loop !== null);
      assert.equal(loop!.length, 2);
    });
  });

  // --------------------------------------------------------------------------
  // reset
  // --------------------------------------------------------------------------

  describe('reset', () => {
    it('clears all failure tracking', () => {
      const args = { cmd: 'test' };
      for (let i = 0; i < 4; i++) {
        ctrl.afterCall('bash', args, 'fail', true);
      }
      assert.equal(ctrl.beforeCall('bash', args).action, 'block');

      ctrl.reset();
      assert.equal(ctrl.beforeCall('bash', args).action, 'allow');
    });

    it('clears no-progress tracking', () => {
      const args = { path: 'file.txt' };
      for (let i = 0; i < 4; i++) {
        ctrl.afterCall('file_read', args, 'same', false);
      }
      ctrl.reset();
      // After reset, should start fresh
      const d = ctrl.afterCall('file_read', args, 'same', false);
      assert.equal(d.action, 'allow');
    });

    it('clears loop detection history', () => {
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'y', false);
      ctrl.afterCall('bash', { cmd: 'a' }, 'x', false);
      ctrl.afterCall('bash', { cmd: 'b' }, 'y', false);
      assert.ok(ctrl.detectLoop() !== null);

      ctrl.reset();
      assert.equal(ctrl.detectLoop(), null);
    });
  });

  // --------------------------------------------------------------------------
  // evictOldest (tested indirectly via > MAX_TRACKED_SIGNATURES)
  // --------------------------------------------------------------------------

  describe('eviction', () => {
    it('evicts oldest signatures when exceeding 500', () => {
      // Add 501 unique failure signatures to trigger eviction
      for (let i = 0; i <= 500; i++) {
        ctrl.afterCall('bash', { idx: i }, 'fail', true);
      }
      // The first signature should have been evicted
      // Can't directly check but we verify no crash and the code handles it
      const d = ctrl.beforeCall('bash', { idx: 0 });
      // After eviction, the count for idx:0 might be 0 (allow) since it was evicted
      assert.equal(d.action, 'allow');
    });
  });

  // --------------------------------------------------------------------------
  // callHistory overflow (LOOP_HISTORY_SIZE = 20)
  // --------------------------------------------------------------------------

  describe('call history overflow', () => {
    it('keeps only last 20 entries in history', () => {
      // Add 25 unique calls
      for (let i = 0; i < 25; i++) {
        ctrl.afterCall('bash', { idx: i }, `out-${i}`, false);
      }
      // detectLoop should not crash and should only look at last 20
      assert.equal(ctrl.detectLoop(), null);
    });
  });
});

// ============================================================================
// tool-utils
// ============================================================================

describe('tool-utils', () => {
  // --------------------------------------------------------------------------
  // resolveFilePath
  // --------------------------------------------------------------------------

  describe('resolveFilePath', () => {
    it('resolves an absolute path inside workspace', () => {
      const cwd = '/home/user/project';
      const r = resolveFilePath('/home/user/project/src/index.ts', cwd);
      assert.equal(r.resolved, '/home/user/project/src/index.ts');
      assert.equal(r.error, undefined);
    });

    it('resolves a relative path inside workspace', () => {
      const cwd = '/home/user/project';
      const r = resolveFilePath('src/index.ts', cwd);
      assert.equal(r.resolved, path.resolve(cwd, 'src/index.ts'));
      assert.equal(r.error, undefined);
    });

    it('returns error for path outside workspace', () => {
      const cwd = '/home/user/project';
      const r = resolveFilePath('/etc/passwd', cwd);
      assert.ok(r.error);
      assert.ok(r.error!.isError);
      assert.ok(r.error!.output.includes('outside the workspace'));
    });

    it('allows the workspace root itself', () => {
      const cwd = '/home/user/project';
      const r = resolveFilePath('/home/user/project', cwd);
      assert.equal(r.error, undefined);
    });

    it('rejects parent traversal outside workspace', () => {
      const cwd = '/home/user/project';
      const r = resolveFilePath('../../../etc/passwd', cwd);
      assert.ok(r.error);
      assert.ok(r.error!.isError);
    });
  });

  // --------------------------------------------------------------------------
  // validatePatternAndPath
  // --------------------------------------------------------------------------

  describe('validatePatternAndPath', () => {
    it('returns pattern and resolved path for valid inputs', () => {
      const cwd = '/home/user/project';
      const result = validatePatternAndPath('*.ts', 'src', cwd);
      assert.ok(!result.error);
      assert.equal(result.pattern, '*.ts');
      assert.equal(result.resolved, path.resolve(cwd, 'src'));
    });

    it('returns error for empty pattern', () => {
      const result = validatePatternAndPath('', undefined, '/tmp');
      assert.ok(result.error);
      assert.ok(result.error!.isError);
      assert.ok(result.error!.output.includes('pattern'));
    });

    it('returns error for null pattern', () => {
      const result = validatePatternAndPath(null, undefined, '/tmp');
      assert.ok(result.error);
      assert.ok(result.error!.isError);
    });

    it('returns error for path outside workspace', () => {
      const result = validatePatternAndPath('*.ts', '/etc', '/home/user/project');
      assert.ok(result.error);
      assert.ok(result.error!.isError);
      assert.ok(result.error!.output.includes('outside the workspace'));
    });

    it('uses cwd when no search path provided', () => {
      const cwd = '/home/user/project';
      const result = validatePatternAndPath('*.ts', undefined, cwd);
      assert.ok(!result.error);
      assert.equal(result.resolved, cwd);
    });
  });

  // --------------------------------------------------------------------------
  // extractQuery
  // --------------------------------------------------------------------------

  describe('extractQuery', () => {
    it('extracts valid query string', () => {
      const result = extractQuery({ query: 'hello world' });
      assert.ok(!result.error);
      assert.equal(result.query, 'hello world');
    });

    it('returns error for empty query', () => {
      const result = extractQuery({ query: '' });
      assert.ok(result.error);
      assert.ok(result.error!.isError);
      assert.ok(result.error!.output.includes('query'));
    });

    it('returns error for missing query', () => {
      const result = extractQuery({});
      assert.ok(result.error);
      assert.ok(result.error!.isError);
    });

    it('returns error for whitespace-only query', () => {
      const result = extractQuery({ query: '   ' });
      assert.ok(result.error);
      assert.ok(result.error!.isError);
    });

    it('returns error for null query', () => {
      const result = extractQuery({ query: null });
      assert.ok(result.error);
    });

    it('returns error for undefined query', () => {
      const result = extractQuery({ query: undefined });
      assert.ok(result.error);
    });
  });

  // --------------------------------------------------------------------------
  // validateRequiredString (additional coverage)
  // --------------------------------------------------------------------------

  describe('validateRequiredString', () => {
    it('accepts a non-empty string', () => {
      assert.equal(validateRequiredString('valid', 'f'), null);
    });

    it('rejects false-y values like 0', () => {
      // 0 is falsy — should be rejected
      const r = validateRequiredString(0 as unknown, 'num');
      assert.ok(r?.isError);
    });
  });

  // --------------------------------------------------------------------------
  // resolveSearchPath (additional edge cases)
  // --------------------------------------------------------------------------

  describe('resolveSearchPath', () => {
    it('accepts an absolute path that equals cwd', () => {
      const r = resolveSearchPath('/home/user', '/home/user');
      assert.equal(r.error, undefined);
    });

    it('accepts absolute subpath of cwd', () => {
      const r = resolveSearchPath('/home/user/sub', '/home/user');
      assert.equal(r.error, undefined);
      assert.equal(r.resolved, '/home/user/sub');
    });
  });

  // --------------------------------------------------------------------------
  // CRLF helpers (additional coverage)
  // --------------------------------------------------------------------------

  describe('detectLineEnding (extra)', () => {
    it('returns LF for LF-only content', () => {
      assert.equal(detectLineEnding('line1\nline2\n'), '\n');
    });

    it('returns CRLF for CRLF-only content', () => {
      assert.equal(detectLineEnding('line1\r\nline2\r\n'), '\r\n');
    });

    it('returns LF when counts are equal', () => {
      // 1 CRLF, 1 standalone LF — equal counts, crlf NOT > lf, so returns LF
      assert.equal(detectLineEnding('a\r\nb\nc'), '\n');
    });
  });

  describe('normalizeToLf (extra)', () => {
    it('handles text with no line endings', () => {
      assert.equal(normalizeToLf('no newlines'), 'no newlines');
    });

    it('handles mixed endings', () => {
      assert.equal(normalizeToLf('a\r\nb\nc\r\n'), 'a\nb\nc\n');
    });
  });

  describe('convertToLineEnding (extra)', () => {
    it('handles empty string', () => {
      assert.equal(convertToLineEnding('', '\r\n'), '');
      assert.equal(convertToLineEnding('', '\n'), '');
    });

    it('handles text with no line endings', () => {
      assert.equal(convertToLineEnding('no newlines', '\r\n'), 'no newlines');
    });
  });

  // --------------------------------------------------------------------------
  // buildToolDescriptionContext + renderToolDescription (additional)
  // --------------------------------------------------------------------------

  describe('renderToolDescription (extra)', () => {
    it('handles template with no placeholders', () => {
      assert.equal(renderToolDescription('plain text', { cwd: '/tmp' }), 'plain text');
    });

    it('handles nested conditional blocks', () => {
      const tmpl = '{{#hasGit}}git{{/hasGit}} {{#hasPackageJson}}npm{{/hasPackageJson}}';
      const result = renderToolDescription(tmpl, { cwd: '/tmp', hasGit: true, hasPackageJson: false });
      assert.equal(result, 'git ');
    });
  });
});

// ============================================================================
// tool-search
// ============================================================================

describe('tool-search', () => {
  const ctx = { cwd: process.cwd(), abortSignal: new AbortController().signal };

  const sampleTools = [
    {
      name: 'file_read',
      description: 'Read a file from the filesystem',
      inputSchema: { type: 'object' as const, properties: { path: { type: 'string' } }, required: ['path'] },
      isReadOnly: true,
      isConcurrencySafe: true,
      execute: async () => ({ output: '', isError: false }),
    },
    {
      name: 'file_write',
      description: 'Write content to a file on the filesystem',
      inputSchema: { type: 'object' as const, properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] },
      isReadOnly: false,
      isConcurrencySafe: false,
      execute: async () => ({ output: '', isError: false }),
    },
    {
      name: 'bash',
      description: 'Execute a bash command',
      inputSchema: { type: 'object' as const, properties: { command: { type: 'string' } }, required: ['command'] },
      isReadOnly: false,
      isConcurrencySafe: false,
      execute: async () => ({ output: '', isError: false }),
    },
  ];

  beforeEach(() => {
    setSearchableTools(sampleTools);
  });

  describe('setSearchableTools', () => {
    it('registers tools for search', async () => {
      setSearchableTools(sampleTools);
      const result = await toolSearchTool.execute({ query: 'file' }, ctx);
      assert.equal(result.isError, false);
      assert.ok(result.output.includes('file_read'));
    });

    it('replaces previous tools', async () => {
      setSearchableTools([sampleTools[2]!]); // Only bash
      const result = await toolSearchTool.execute({ query: 'file' }, ctx);
      // file_read should not be found
      assert.ok(!result.output.includes('file_read') || result.output.includes('No tools matched'));
    });
  });

  describe('execute', () => {
    it('returns error for empty query', async () => {
      const result = await toolSearchTool.execute({ query: '' }, ctx);
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('query must not be empty'));
    });

    it('returns error for whitespace-only query', async () => {
      const result = await toolSearchTool.execute({ query: '   ' }, ctx);
      // trimmed to empty
      assert.equal(result.isError, true);
    });

    it('handles missing query field', async () => {
      const result = await toolSearchTool.execute({}, ctx);
      assert.equal(result.isError, true);
    });

    // select: prefix ---

    it('returns full schema for select:existing_tool', async () => {
      const result = await toolSearchTool.execute({ query: 'select:file_read' }, ctx);
      assert.equal(result.isError, false);
      const parsed = JSON.parse(result.output);
      assert.equal(parsed.name, 'file_read');
      assert.ok(parsed.description);
      assert.ok(parsed.inputSchema);
      assert.equal(parsed.isReadOnly, true);
    });

    it('returns error for select:nonexistent_tool', async () => {
      const result = await toolSearchTool.execute({ query: 'select:does_not_exist' }, ctx);
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('not found'));
      assert.ok(result.output.includes('Available'));
    });

    it('trims tool name after select: prefix', async () => {
      const result = await toolSearchTool.execute({ query: 'select:  bash  ' }, ctx);
      assert.equal(result.isError, false);
      const parsed = JSON.parse(result.output);
      assert.equal(parsed.name, 'bash');
    });

    // keyword search ---

    it('finds tools by name keyword', async () => {
      const result = await toolSearchTool.execute({ query: 'bash' }, ctx);
      assert.equal(result.isError, false);
      assert.ok(result.output.includes('bash'));
      assert.ok(result.output.includes('Tools matching'));
    });

    it('finds tools by description keyword', async () => {
      const result = await toolSearchTool.execute({ query: 'filesystem' }, ctx);
      assert.equal(result.isError, false);
      assert.ok(result.output.includes('file_read') || result.output.includes('file_write'));
    });

    it('name matches score higher than description matches', async () => {
      const result = await toolSearchTool.execute({ query: 'file' }, ctx);
      assert.equal(result.isError, false);
      // file_read and file_write should appear (name match gives +2)
      assert.ok(result.output.includes('file_read'));
      assert.ok(result.output.includes('file_write'));
    });

    it('handles multi-keyword search', async () => {
      const result = await toolSearchTool.execute({ query: 'file read' }, ctx);
      assert.equal(result.isError, false);
      assert.ok(result.output.includes('file_read'));
    });

    it('returns no-match message when no tools match', async () => {
      const result = await toolSearchTool.execute({ query: 'xyznonexistent' }, ctx);
      assert.equal(result.isError, false);
      assert.ok(result.output.includes('No tools matched'));
      assert.ok(result.output.includes('Available'));
    });

    it('limits results to top 5', async () => {
      // Create 10 tools that all match "tool"
      const manyTools = Array.from({ length: 10 }, (_, i) => ({
        name: `tool_${i}`,
        description: `A tool number ${i}`,
        inputSchema: { type: 'object' as const, properties: {}, required: [] },
        isReadOnly: true,
        execute: async () => ({ output: '', isError: false }),
      }));
      setSearchableTools(manyTools);
      const result = await toolSearchTool.execute({ query: 'tool' }, ctx);
      // Count bullet points
      const bullets = result.output.match(/^- \*\*/gm);
      assert.ok(bullets);
      assert.ok(bullets!.length <= 5);
    });

    it('truncates long descriptions to 100 chars with ellipsis', async () => {
      const longDesc = 'A'.repeat(150);
      setSearchableTools([{
        name: 'long_desc_tool',
        description: longDesc,
        inputSchema: { type: 'object' as const, properties: {}, required: [] },
        isReadOnly: true,
        execute: async () => ({ output: '', isError: false }),
      }]);
      const result = await toolSearchTool.execute({ query: 'long_desc_tool' }, ctx);
      assert.ok(result.output.includes('...'));
      // Should not contain the full 150-char description
      assert.ok(!result.output.includes(longDesc));
    });

    it('does not add ellipsis for short descriptions', async () => {
      setSearchableTools([{
        name: 'short_tool',
        description: 'Short desc',
        inputSchema: { type: 'object' as const, properties: {}, required: [] },
        isReadOnly: true,
        execute: async () => ({ output: '', isError: false }),
      }]);
      const result = await toolSearchTool.execute({ query: 'short_tool' }, ctx);
      assert.ok(!result.output.includes('...'));
    });

    it('includes usage hint in results', async () => {
      const result = await toolSearchTool.execute({ query: 'file' }, ctx);
      assert.ok(result.output.includes('select:'));
    });
  });

  describe('tool metadata', () => {
    it('has correct name', () => {
      assert.equal(toolSearchTool.name, 'tool_search');
    });

    it('is read-only', () => {
      assert.equal(toolSearchTool.isReadOnly, true);
    });

    it('is concurrency safe', () => {
      assert.equal(toolSearchTool.isConcurrencySafe, true);
    });

    it('requires query in schema', () => {
      assert.ok(toolSearchTool.inputSchema.required?.includes('query'));
    });
  });
});
