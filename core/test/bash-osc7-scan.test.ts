import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { scanOscCwdTitle, osc7UriToPath } from '../src/tools/bash/osc7-scan';

describe('osc7UriToPath', () => {
  it('decodes a localhost file URI to a path', () => {
    assert.equal(osc7UriToPath('file://localhost/home/me/proj'), '/home/me/proj');
  });

  it('decodes an empty-authority file URI', () => {
    assert.equal(osc7UriToPath('file:///var/log'), '/var/log');
  });

  it('percent-decodes the path', () => {
    assert.equal(osc7UriToPath('file:///home/a%20b/c'), '/home/a b/c');
  });

  it('returns a remote-host path too', () => {
    assert.equal(osc7UriToPath('file://box/srv/data'), '/srv/data');
  });

  it('returns null for a non-file scheme', () => {
    assert.equal(osc7UriToPath('http://example/x'), null);
  });

  it('returns null when there is no path component', () => {
    assert.equal(osc7UriToPath('file://host'), null);
  });

  it('falls back to the raw path on malformed percent-encoding', () => {
    assert.equal(osc7UriToPath('file:///bad/%zz'), '/bad/%zz');
  });
});

describe('scanOscCwdTitle', () => {
  it('returns nulls when no OSC introducer is present', () => {
    assert.deepEqual(scanOscCwdTitle('plain output\nno escapes'), { cwd: null, title: null });
  });

  it('extracts an OSC 7 cwd terminated by BEL', () => {
    const r = scanOscCwdTitle('before\x1b]7;file:///work/dir\x07after');
    assert.equal(r.cwd, '/work/dir');
    assert.equal(r.title, null);
  });

  it('extracts an OSC 7 cwd terminated by ST', () => {
    const r = scanOscCwdTitle('\x1b]7;file:///a/b\x1b\\');
    assert.equal(r.cwd, '/a/b');
  });

  it('extracts an OSC 2 title', () => {
    const r = scanOscCwdTitle('\x1b]2;build 42/128\x07');
    assert.equal(r.title, 'build 42/128');
  });

  it('extracts OSC 0 and OSC 1 as titles', () => {
    assert.equal(scanOscCwdTitle('\x1b]0;t0\x07').title, 't0');
    assert.equal(scanOscCwdTitle('\x1b]1;t1\x07').title, 't1');
  });

  it('returns the LAST cwd and LAST title when several are present', () => {
    const data =
      '\x1b]7;file:///first\x07' +
      '\x1b]2;title one\x07' +
      '\x1b]7;file:///second\x07' +
      '\x1b]2;title two\x07';
    const r = scanOscCwdTitle(data);
    assert.equal(r.cwd, '/second');
    assert.equal(r.title, 'title two');
  });

  it('stops cleanly at a truncated (unterminated) OSC', () => {
    const r = scanOscCwdTitle('\x1b]2;done\x07\x1b]7;file:///partia');
    assert.equal(r.title, 'done');
    assert.equal(r.cwd, null); // the second OSC never terminated
  });

  it('ignores an OSC 7 whose payload is not a decodable file path', () => {
    const r = scanOscCwdTitle('\x1b]7;notaurl\x07');
    assert.equal(r.cwd, null);
  });

  it('ignores an OSC with no ps;payload separator', () => {
    const r = scanOscCwdTitle('\x1b]noseparator\x07x');
    assert.deepEqual(r, { cwd: null, title: null });
  });

  it('aborts an OSC on a bare ESC that is not ST and resumes scanning', () => {
    // The first OSC is interrupted by a bare ESC (treated as abort); the second
    // well-formed OSC still yields its title.
    const r = scanOscCwdTitle('\x1b]2;broken\x1bX\x1b]2;good\x07');
    assert.equal(r.title, 'good');
  });
});
