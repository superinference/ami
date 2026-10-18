import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  TRANSCRIPT_FILE_EXTENSION,
  validateTranscriptLine,
  repairTranscriptFile,
  safeAppendTranscript,
} from '../src/session';

describe('TRANSCRIPT_FILE_EXTENSION', () => {
  it('equals .jsonl', () => {
    assert.equal(TRANSCRIPT_FILE_EXTENSION, '.jsonl');
  });
});

describe('validateTranscriptLine', () => {
  it('accepts valid JSON line with type and timestamp (number)', () => {
    assert.equal(
      validateTranscriptLine('{"type":"text_delta","timestamp":1700000000,"text":"hi"}'),
      true,
    );
  });

  it('accepts valid JSON line with type and timestamp (string)', () => {
    assert.equal(
      validateTranscriptLine('{"type":"tool_use","timestamp":"2024-01-01T00:00:00Z","data":{}}'),
      true,
    );
  });

  it('rejects empty string', () => {
    assert.equal(validateTranscriptLine(''), false);
  });

  it('rejects whitespace-only string', () => {
    assert.equal(validateTranscriptLine('   '), false);
  });

  it('rejects invalid JSON', () => {
    assert.equal(validateTranscriptLine('{"type":"text_delta","timestamp":17000'), false);
  });

  it('rejects JSON without type field', () => {
    assert.equal(validateTranscriptLine('{"timestamp":1700000000,"data":"hi"}'), false);
  });

  it('rejects JSON without timestamp field', () => {
    assert.equal(validateTranscriptLine('{"type":"text_delta","data":"hi"}'), false);
  });

  it('rejects JSON with non-string type', () => {
    assert.equal(validateTranscriptLine('{"type":42,"timestamp":1700000000}'), false);
  });

  it('rejects JSON with boolean timestamp', () => {
    assert.equal(validateTranscriptLine('{"type":"x","timestamp":true}'), false);
  });

  it('rejects JSON array', () => {
    assert.equal(validateTranscriptLine('[{"type":"x","timestamp":1}]'), false);
  });

  it('rejects primitive JSON value', () => {
    assert.equal(validateTranscriptLine('"hello"'), false);
    assert.equal(validateTranscriptLine('42'), false);
    assert.equal(validateTranscriptLine('null'), false);
  });
});

describe('repairTranscriptFile', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'transcript-repair-'));
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns zeros for nonexistent file', () => {
    const result = repairTranscriptFile(path.join(tmpDir, 'nope.jsonl'));
    assert.deepEqual(result, { repairedLines: 0, droppedLines: 0 });
  });

  it('returns zeros for empty file', () => {
    const fp = path.join(tmpDir, 'empty.jsonl');
    fs.writeFileSync(fp, '');
    const result = repairTranscriptFile(fp);
    assert.deepEqual(result, { repairedLines: 0, droppedLines: 0 });
  });

  it('keeps all valid lines unchanged', () => {
    const fp = path.join(tmpDir, 'allvalid.jsonl');
    const lines = [
      '{"type":"start","timestamp":1}',
      '{"type":"text_delta","timestamp":2,"text":"hi"}',
      '{"type":"done","timestamp":3}',
    ];
    fs.writeFileSync(fp, lines.join('\n') + '\n');
    const result = repairTranscriptFile(fp);
    assert.deepEqual(result, { repairedLines: 3, droppedLines: 0 });
    const content = fs.readFileSync(fp, 'utf-8');
    assert.equal(content.trim().split('\n').length, 3);
  });

  it('drops truncated last line', () => {
    const fp = path.join(tmpDir, 'truncated.jsonl');
    const lines = [
      '{"type":"start","timestamp":1}',
      '{"type":"text_delta","timestamp":2,"text":"hi"}',
      '{"type":"done","timestamp":3,"dat',  // truncated
    ];
    fs.writeFileSync(fp, lines.join('\n') + '\n');
    const result = repairTranscriptFile(fp);
    assert.equal(result.repairedLines, 2);
    assert.equal(result.droppedLines, 1);
    const content = fs.readFileSync(fp, 'utf-8').trim();
    assert.equal(content.split('\n').length, 2);
  });

  it('drops mid-file corrupt line too', () => {
    const fp = path.join(tmpDir, 'midcorrupt.jsonl');
    const lines = [
      '{"type":"start","timestamp":1}',
      'NOT JSON AT ALL',
      '{"type":"done","timestamp":3}',
    ];
    fs.writeFileSync(fp, lines.join('\n') + '\n');
    const result = repairTranscriptFile(fp);
    assert.equal(result.repairedLines, 2);
    assert.equal(result.droppedLines, 1);
  });

  it('handles file with only invalid lines', () => {
    const fp = path.join(tmpDir, 'allinvalid.jsonl');
    fs.writeFileSync(fp, 'garbage\nmore garbage\n');
    const result = repairTranscriptFile(fp);
    assert.equal(result.repairedLines, 0);
    assert.equal(result.droppedLines, 2);
  });

  it('handles whitespace-only file', () => {
    const fp = path.join(tmpDir, 'whitespace.jsonl');
    fs.writeFileSync(fp, '   \n  \n');
    const result = repairTranscriptFile(fp);
    assert.deepEqual(result, { repairedLines: 0, droppedLines: 0 });
  });
});

describe('safeAppendTranscript', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'transcript-append-'));
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('creates file and writes valid JSON line', () => {
    const fp = path.join(tmpDir, 'new.jsonl');
    safeAppendTranscript(fp, { type: 'start', timestamp: 1 });
    const content = fs.readFileSync(fp, 'utf-8');
    assert.ok(content.endsWith('\n'));
    const parsed = JSON.parse(content.trim());
    assert.equal(parsed.type, 'start');
    assert.equal(parsed.timestamp, 1);
  });

  it('appends multiple entries', () => {
    const fp = path.join(tmpDir, 'multi.jsonl');
    safeAppendTranscript(fp, { type: 'a', timestamp: 1 });
    safeAppendTranscript(fp, { type: 'b', timestamp: 2 });
    safeAppendTranscript(fp, { type: 'c', timestamp: 3 });
    const lines = fs.readFileSync(fp, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 3);
    assert.equal(JSON.parse(lines[0]).type, 'a');
    assert.equal(JSON.parse(lines[2]).type, 'c');
  });

  it('creates parent directories if needed', () => {
    const fp = path.join(tmpDir, 'nested', 'deep', 'file.jsonl');
    safeAppendTranscript(fp, { type: 'test', timestamp: 42 });
    assert.ok(fs.existsSync(fp));
    const parsed = JSON.parse(fs.readFileSync(fp, 'utf-8').trim());
    assert.equal(parsed.type, 'test');
  });

  it('does not throw on write to unwritable path', () => {
    // A path that can't be created (invalid chars or permission)
    assert.doesNotThrow(() => {
      safeAppendTranscript('/dev/null/impossible/path.jsonl', { type: 'x', timestamp: 0 });
    });
  });

  it('each appended line is independently parseable', () => {
    const fp = path.join(tmpDir, 'parseable.jsonl');
    for (let i = 0; i < 5; i++) {
      safeAppendTranscript(fp, { type: `event_${i}`, timestamp: i, data: { idx: i } });
    }
    const lines = fs.readFileSync(fp, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 5);
    for (let i = 0; i < 5; i++) {
      const obj = JSON.parse(lines[i]);
      assert.equal(obj.type, `event_${i}`);
      assert.equal(obj.data.idx, i);
    }
  });

  it('written lines pass validateTranscriptLine', () => {
    const fp = path.join(tmpDir, 'validate.jsonl');
    safeAppendTranscript(fp, { type: 'text_delta', timestamp: Date.now(), text: 'hello' });
    const line = fs.readFileSync(fp, 'utf-8').trim();
    assert.equal(validateTranscriptLine(line), true);
  });
});
