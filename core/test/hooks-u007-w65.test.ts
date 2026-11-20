import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  HookManager,
  HOOK_EVENTS,
  getHookEventMetadata,
  eventSupportsMatcher,
  serializeHookEvent,
  type TeammateContext,
} from '../src/hooks';

// ---------------------------------------------------------------------------
// U007 (w65) — TeammateIdle hook event + BOM-tolerant config/hooks parsing
// ---------------------------------------------------------------------------

describe('U007/w65 — TeammateIdle hook event', () => {
  it('teammateIdle is in the introspectable HOOK_EVENTS catalog', () => {
    assert.ok(
      (HOOK_EVENTS as readonly string[]).includes('teammateIdle'),
      'HOOK_EVENTS must advertise teammateIdle (the reference parity)',
    );
  });

  it('getHookEventMetadata describes teammateIdle, scoped on teammateName', () => {
    const meta = getHookEventMetadata('teammateIdle');
    assert.ok(meta, 'metadata present for teammateIdle');
    assert.equal(meta!.matcherField, 'teammateName');
    assert.match(meta!.summary, /teammate/i);
    assert.match(meta!.description, /teammate_name/);
    assert.equal(eventSupportsMatcher('teammateIdle'), true);
  });

  it('executeTeammateIdle fires registered hooks with the teammate/team identity', async () => {
    const hm = new HookManager();
    const seen: TeammateContext[] = [];
    hm.onTeammateIdle(async (ctx) => { seen.push(ctx); });
    await hm.executeTeammateIdle({ teammateName: 'scout', teamName: 'alpha' });
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], { teammateName: 'scout', teamName: 'alpha' });
    assert.equal(hm.getSummary().teammateIdle, 1);
  });

  it('serializeHookEvent emits Claude snake_case keys for teammateIdle', () => {
    const line = serializeHookEvent({ event: 'teammateIdle', teammateName: 'scout', teamName: 'alpha' });
    const obj = JSON.parse(line);
    assert.equal(obj.hook_event_name, 'TeammateIdle');
    assert.equal(obj.teammate_name, 'scout');
    assert.equal(obj.team_name, 'alpha');
  });

  it('a file-loaded teammateIdle command hook registers and a matcher scopes it', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-ti-'));
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-home-'));
    const origHome = process.env.HOME;
    const origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    try {
      fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
      fs.writeFileSync(
        path.join(tmp, '.ami', 'hooks.json'),
        JSON.stringify({ hooks: [{ event: 'teammateIdle', matcher: 'scout', hook: { type: 'command', command: 'echo idle' } }] }),
      );
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      assert.equal(hm.getSummary().teammateIdle, 1, 'teammateIdle hook loaded from hooks.json');
      const d = hm.describeHooks().find(x => x.event === 'teammateIdle');
      assert.ok(d, 'teammateIdle descriptor retained for introspection');
      assert.equal(d!.matcher, 'scout');
    } finally {
      if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
      if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH; else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
      for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
    }
  });
});

describe('U007/w65 — BOM-tolerant hooks/config parsing', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-bom-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-bomhome-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH; else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  it('loads hooks from a BOM-prefixed .ami/hooks.json (Windows/PowerShell editors)', () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    const body = JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo hi' } }] });
    // Prepend a UTF-8 BOM (U+FEFF) — plain JSON.parse would throw on this.
    fs.writeFileSync(path.join(tmp, '.ami', 'hooks.json'), '﻿' + body, 'utf-8');
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().notification, 1, 'BOM-prefixed hooks.json still parses and registers');
  });

  it('loads hooks from a BOM-prefixed .ami/config.json', () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    const body = JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo cfg' } }] });
    fs.writeFileSync(path.join(tmp, '.ami', 'config.json'), '﻿' + body, 'utf-8');
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().notification, 1, 'BOM-prefixed config.json still parses and registers');
  });
});
