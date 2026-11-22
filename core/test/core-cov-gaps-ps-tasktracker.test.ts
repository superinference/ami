/**
 * Wave-3 coverage: powershell-parser statement/element type maps + task-tracker
 * sanitize/resolve/debug catch arms that the existing suites leave cold.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';

import {
  mapStatementType,
  mapElementType,
  toUtf16LeBase64,
  buildParseScript,
} from '../src/tools/powershell-parser';
import {
  sanitizeTaskListId,
  resolveTaskListId,
  parseTaskId,
  normalizeOwner,
  wouldCreateCycle,
  resetTaskState,
  setTaskPersistPath,
  getTaskState,
  DEFAULT_TASKS_MODE_TASK_LIST_ID,
  resolveTaskWatchDebounceMs,
  DEFAULT_TASK_WATCH_DEBOUNCE_MS,
} from '../src/tools/task-tracker';

afterEach(() => {
  mock.restoreAll();
  resetTaskState();
});

describe('powershell-parser mapStatementType / mapElementType exhaustiveness', () => {
  it('maps every known statement AST type and falls back to Unknown', () => {
    const cases: Array<[string, string]> = [
      ['PipelineAst', 'PipelineAst'],
      ['PipelineChainAst', 'PipelineChainAst'],
      ['AssignmentStatementAst', 'AssignmentStatementAst'],
      ['IfStatementAst', 'IfStatementAst'],
      ['ForStatementAst', 'ForStatementAst'],
      ['ForEachStatementAst', 'ForEachStatementAst'],
      ['WhileStatementAst', 'WhileStatementAst'],
      ['DoWhileStatementAst', 'DoWhileStatementAst'],
      ['DoUntilStatementAst', 'DoUntilStatementAst'],
      ['SwitchStatementAst', 'SwitchStatementAst'],
      ['TryStatementAst', 'TryStatementAst'],
      ['TrapStatementAst', 'TrapStatementAst'],
      ['FunctionDefinitionAst', 'FunctionDefinitionAst'],
      ['DataStatementAst', 'DataStatementAst'],
      ['TotallyUnknownAst', 'UnknownStatementAst'],
    ];
    for (const [raw, want] of cases) {
      assert.equal(mapStatementType(raw), want, raw);
    }
  });

  it('maps element types including ArrayExpressionAst → SubExpression', () => {
    assert.equal(mapElementType('ScriptBlockExpressionAst'), 'ScriptBlock');
    assert.equal(mapElementType('SubExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('ArrayExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('ExpandableStringExpressionAst'), 'ExpandableString');
    assert.equal(mapElementType('InvokeMemberExpressionAst'), 'MemberInvocation');
    assert.equal(mapElementType('MemberExpressionAst'), 'MemberInvocation');
    assert.equal(mapElementType('VariableExpressionAst'), 'Variable');
    assert.equal(mapElementType('StringConstantExpressionAst'), 'StringConstant');
    assert.equal(mapElementType('ConstantExpressionAst'), 'StringConstant');
    assert.equal(mapElementType('CommandParameterAst'), 'Parameter');
    assert.equal(mapElementType('ParenExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('CommandExpressionAst'), 'Other');
    assert.equal(mapElementType('CommandExpressionAst', 'SubExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('TotallyUnknown'), 'Other');
  });

  it('toUtf16LeBase64 + buildParseScript are deterministic helpers', () => {
    const b64 = toUtf16LeBase64('hi');
    assert.equal(Buffer.from(b64, 'base64').toString('utf16le'), 'hi');
    const script = buildParseScript('Get-ChildItem');
    assert.ok(script.includes('$EncodedCommand'));
    assert.ok(script.includes(Buffer.from('Get-ChildItem', 'utf8').toString('base64')));
  });
});

describe('task-tracker helpers gap closure', () => {
  it('sanitizeTaskListId strips traversal and treats non-strings as empty', () => {
    assert.equal(sanitizeTaskListId('../evil'), 'evil');
    assert.equal(sanitizeTaskListId('ok_list-1'), 'ok_list-1');
    assert.equal(sanitizeTaskListId('@@@'), '');
    assert.equal(sanitizeTaskListId(null), '');
    assert.equal(sanitizeTaskListId(12), '');
  });

  it('resolveTaskListId falls back to the default id', () => {
    assert.equal(resolveTaskListId(undefined), DEFAULT_TASKS_MODE_TASK_LIST_ID);
    assert.equal(resolveTaskListId('@@@'), DEFAULT_TASKS_MODE_TASK_LIST_ID);
    assert.equal(resolveTaskListId('my-team'), 'my-team');
  });

  it('parseTaskId / normalizeOwner / wouldCreateCycle edge arms', () => {
    assert.equal(parseTaskId('3'), 3);
    assert.equal(parseTaskId(3), 3);
    assert.equal(parseTaskId(''), null);
    assert.equal(parseTaskId('x'), null);
    assert.equal(parseTaskId(-1), null);
    assert.equal(normalizeOwner('  alice  '), 'alice');
    assert.equal(normalizeOwner(''), undefined);
    assert.equal(normalizeOwner(null), undefined);
    assert.equal(wouldCreateCycle(1, 1), true);
    assert.equal(wouldCreateCycle(1, 2), false);
  });

  it('resolveTaskWatchDebounceMs clamps invalid values to the default', () => {
    assert.equal(resolveTaskWatchDebounceMs(undefined), DEFAULT_TASK_WATCH_DEBOUNCE_MS);
    assert.equal(resolveTaskWatchDebounceMs(NaN), DEFAULT_TASK_WATCH_DEBOUNCE_MS);
    assert.equal(resolveTaskWatchDebounceMs(-5), DEFAULT_TASK_WATCH_DEBOUNCE_MS);
    assert.equal(resolveTaskWatchDebounceMs(2500), 2500);
  });

  it('setTaskPersistPath + getTaskState round-trip an empty map on a fresh dir', () => {
    const d = fs.mkdtempSync(path.join('/home/ccamacho/tmp-ami-cov', 'tt-'));
    try {
      setTaskPersistPath(d, 'list-a');
      assert.equal(getTaskState().size, 0);
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });
});
