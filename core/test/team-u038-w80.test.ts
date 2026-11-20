import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { teamCreateTool, resetTeam } from '../src/tools/team';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U038 (w80) — team-name NORMALIZATION (sanitize-to-slug) instead of
// reject-on-invalid.
//
// Parity: the reference's sanitizeName() (utils/swarm/teamHelpers.ts) NORMALIZES
// any team name into a filesystem-safe slug (every unsafe char -> '-') so
// human-friendly names like "My Team!" are ACCEPTED and used, rather than
// failing validation. AMI previously REJECTED any name containing a char
// outside [a-zA-Z0-9_-]. This brings AMI to parity (accept + normalize) while
// keeping AMI's superset (preserve '_', '-', and case in already-valid names)
// and preserving the path-traversal security invariant (the slug is always a
// single, contained path component — it can never escape the teams dir).
// ---------------------------------------------------------------------------

let tmp: string;

function ctx(sessionId = 'sess-slug'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}

function teamsRoot(): string {
  return path.join(tmp, '.ami', 'teams');
}

describe('U038 w80 — team-name normalization (sanitize to slug, parity)', () => {
  beforeEach(() => { resetTeam(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-teamslug-')); });
  afterEach(() => { resetTeam(); try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('accepts a friendly name with spaces/punctuation by normalizing to a slug', async () => {
    const res = await teamCreateTool.execute({ team_name: 'My Team!' }, ctx());
    assert.equal(res.isError, undefined);
    // "My Team!" -> "My-Team" (space -> '-', trailing '!' trimmed).
    assert.ok(res.output.includes('My-Team'), `expected normalized name in output, got: ${res.output}`);
    assert.ok(fs.existsSync(path.join(teamsRoot(), 'My-Team')));
  });

  it('normalizes a slashed name to a single safe component (no traversal)', async () => {
    const res = await teamCreateTool.execute({ team_name: 'foo/bar' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(fs.existsSync(path.join(teamsRoot(), 'foo-bar')));
  });

  it('NEUTRALIZES a path-traversal attempt into a contained slug (security preserved)', async () => {
    const res = await teamCreateTool.execute({ team_name: '../../../etc/evil' }, ctx());
    // Parity: normalized rather than hard-rejected — but the result is a single
    // contained component, so NO directory is ever created outside the teams dir.
    assert.equal(res.isError, undefined);
    const escaped = path.join(tmp, '..', '..', '..', 'etc', 'evil');
    assert.ok(!fs.existsSync(escaped), 'traversal target must never be created');
    // The created dir lives strictly under the teams root.
    const entries = fs.readdirSync(teamsRoot());
    assert.equal(entries.length, 1);
    const created = path.join(teamsRoot(), entries[0]);
    assert.ok(path.resolve(created).startsWith(path.resolve(teamsRoot()) + path.sep));
    // No slashes/dots survive in the slug component.
    assert.ok(!entries[0].includes('/') && !entries[0].includes('..'));
  });

  it('preserves an already-valid name verbatim (AMI superset: keeps _, -, case)', async () => {
    const res = await teamCreateTool.execute({ team_name: 'Alpha-Team_1' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Alpha-Team_1'));
    assert.ok(fs.existsSync(path.join(teamsRoot(), 'Alpha-Team_1')));
  });

  it('rejects a name that normalizes to empty (only special chars)', async () => {
    const res = await teamCreateTool.execute({ team_name: '...' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(!fs.existsSync(teamsRoot()) || fs.readdirSync(teamsRoot()).length === 0);
  });

  it('rejects an empty name', async () => {
    const res = await teamCreateTool.execute({ team_name: '' }, ctx());
    assert.equal(res.isError, true);
  });

  it('rejects a name that is too long after normalization', async () => {
    const res = await teamCreateTool.execute({ team_name: 'a'.repeat(65) }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('64'));
  });
});
