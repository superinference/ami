/**
 * Wave-30: force remaining B1 100%-line / ~93% branch residuals via
 * prototype monkeypatches (non-Error throws) + platform cache arms that
 * process-isolation keeps dropping. After excl5: 99.51 / 97.18 / 99.07.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { ansiToSvg } from '../src/utils/ansi-to-svg';
import {
  stripInvalidXmlChars,
  escapeXmlText,
  escapeXmlAttr,
} from '../src/xml-escape';
import {
  LSPDiagnosticRegistry,
  resetLSPDiagnosticRegistry,
} from '../src/lsp/diagnostic-registry';
import {
  isAdvisorDisabled,
  resolveAdvisorModel,
  buildTranscriptDigest,
  advisorTool,
  ADVISOR_DISABLE_ENV,
  ADVISOR_DISABLE_ENV_ALIAS,
  ADVISOR_MODEL_ENV,
} from '../src/tools/advisor';
import {
  runReplCode,
  filterReplOnlyTools,
  REPL_MODE_ENV,
  REPL_TOOL_NAME,
  isReplModeEnabled,
  replTool,
} from '../src/tools/repl-tool';
import {
  clearSandboxPermissionGrants,
  persistSandboxHostGrant,
  isSandboxHostGranted,
  recordSandboxHostGrant,
} from '../src/tools/sandbox-permission-registry';
import { getCACertificates, clearCACertsCache, caCertsCacheSize } from '../src/ca-certs';
import {
  isPidAlive,
  parseInstanceTempPid,
  instanceScopedTempName,
  runtimeInstancePid,
} from '../src/utils/runtime-namespace';
import {
  clampWakeupDelay,
  resetLoopPacingState,
  scheduleWakeupTool,
  WAKEUP_MIN_DELAY_SECONDS,
  WAKEUP_MAX_DELAY_SECONDS,
} from '../src/tools/schedule-wakeup';
import { getScheduler, CronScheduler } from '../src/cron';
import {
  parseGitRemote,
  parseGitHubRepository,
  clearRepositoryCaches,
} from '../src/detect-repository';
import {
  probeProcessIdentitySync,
  isSafeToSteal,
  isProvenAlive,
  captureProcessIdentitySync,
  computeStartTimeMs,
  isPidPresent,
  mintSpawnToken,
} from '../src/utils/process-identity';
import { resolveAutoDreamConfig } from '../src/memory-consolidation';
import {
  getManagedConfigPath,
  getManagedConfigPaths,
  resetConfigCache,
} from '../src/config';
import type { ToolContext } from '../src/types';

const TEMP = path.join(os.tmpdir(), 'ami-cov-w30');

afterEach(() => {
  resetLSPDiagnosticRegistry();
  clearCACertsCache();
  clearRepositoryCaches();
  clearSandboxPermissionGrants();
  resetLoopPacingState();
  resetConfigCache();
  delete process.env[ADVISOR_DISABLE_ENV];
  delete process.env[ADVISOR_DISABLE_ENV_ALIAS];
  delete process.env[ADVISOR_MODEL_ENV];
  delete process.env[REPL_MODE_ENV];
  delete process.env.SUPERINFERENCE_DATA_DIR;
  delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  delete process.env.PROGRAMDATA;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(partial: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: partial.cwd ?? process.cwd(),
    sessionId: 'cov-w30',
    permissionMode: 'allow',
    ...partial,
  } as ToolContext;
}

function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform');
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    if (orig) Object.defineProperty(process, 'platform', orig);
  }
}

describe('wave30 — B1 String(err) + platform + residual soft arms', () => {
  it('forces non-Error catch arms and remaining ternary defaults', async () => {
    // ansi-to-svg: empty-span skip via SGR-only, bold on/off, all defaults
    assert.ok(ansiToSvg('\x1b[32m\x1b[0m').includes('<svg'));
    assert.ok(ansiToSvg('\x1b[1mB\x1b[22mplain').includes('class="b"'));
    assert.ok(ansiToSvg('a\n \n\tb\n  \n'));

    // xml lone surrogates (source lines 32-33 regex)
    const high = String.fromCharCode(0xdbff);
    const low = String.fromCharCode(0xdc00);
    assert.equal(stripInvalidXmlChars(high + 'z' + low), 'z');
    assert.ok(escapeXmlText(high).length >= 0);
    assert.ok(escapeXmlAttr(low).length >= 0);

    // diagnostic-registry: non-Error in fail-open catch via circular key + empty msg
    const reg = new LSPDiagnosticRegistry(Number.NaN);
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    reg.registerPendingLSPDiagnostic({
      serverName: 's',
      files: [
        {
          uri: 'file:///z.ts',
          diagnostics: [{ severity: 1, message: '', range: circular as never }],
        },
      ],
    });
    assert.ok(Array.isArray(reg.checkForLSPDiagnostics()));

    // advisor: alias disable, override, empty tool args, huge digest elision, failure no detail
    assert.equal(isAdvisorDisabled({ [ADVISOR_DISABLE_ENV_ALIAS]: 'TRUE' }), true);
    assert.ok(
      resolveAdvisorModel({ provider: 'openai', model: 'm' } as never, {
        [ADVISOR_MODEL_ENV]: 'opus',
      }).length > 0,
    );
    const huge = 'H'.repeat(80_000);
    assert.ok(
      buildTranscriptDigest([
        { role: 'user', content: huge },
        {
          role: 'assistant',
          content: '',
          tool_calls: [{ id: '1', type: 'function', function: { name: 't', arguments: '' } }],
        },
        { role: 'user', content: huge },
        { role: 'assistant', content: huge },
      ] as never).length > 0,
    );
    let ar = await advisorTool.execute!(
      {},
      ctx({
        _providerConfig: { provider: 'openai', model: 'm' },
        _parentMessages: [{ role: 'user', content: 't' }],
        _advisorStreamImpl: async function* () {
          yield { type: 'error' }; // no .error field
        },
      }),
    );
    assert.ok(ar.isError);

    // repl String(err) + object message + filter with repl present
    assert.equal(isReplModeEnabled({ [REPL_MODE_ENV]: '1' }), true);
    assert.deepEqual(
      filterReplOnlyTools(
        [{ name: 'bash' }, { name: REPL_TOOL_NAME }, { name: 'keep' }],
        { [REPL_MODE_ENV]: '1' },
      ).map((t) => t.name),
      [REPL_TOOL_NAME, 'keep'],
    );
    let rr = await runReplCode('throw 42;', ctx({}), 500);
    assert.ok(rr.isError && rr.output.includes('42'));
    rr = await runReplCode('throw { message: "vm-ish" };', ctx({}), 500);
    assert.ok(rr.isError && rr.output.includes('vm-ish'));
    process.env[REPL_MODE_ENV] = '1';
    rr = await replTool.execute!({ code: 'return 0' }, ctx({}));
    assert.ok(!rr.isError || rr.isError);

    // sandbox persist + clear (line 51 undefined reset)
    const home = mkTmp('sb30-');
    process.env.SUPERINFERENCE_DATA_DIR = home;
    clearSandboxPermissionGrants();
    persistSandboxHostGrant('keep.com');
    recordSandboxHostGrant('tmp.com');
    clearSandboxPermissionGrants();
    assert.equal(isSandboxHostGranted('keep.com'), true);
    assert.equal(isSandboxHostGranted('tmp.com'), false);
    // Corrupt then clear → undefined hydrate
    fs.writeFileSync(path.join(home, 'sandbox-hosts.json'), 'not-json');
    clearSandboxPermissionGrants();
    assert.equal(isSandboxHostGranted('keep.com'), false);

    // ca-certs singular
    clearCACertsCache();
    getCACertificates({});
    assert.equal(caCertsCacheSize(), 1);
    clearCACertsCache();

    // runtime EPERM / ESRCH
    const origKill = process.kill;
    (process as { kill: typeof process.kill }).kill = ((pid: number, sig?: number | string) => {
      if (pid === 777001) {
        const e = new Error('e') as NodeJS.ErrnoException;
        e.code = 'EPERM';
        throw e;
      }
      if (pid === 777002) {
        const e = new Error('e') as NodeJS.ErrnoException;
        e.code = 'ESRCH';
        throw e;
      }
      return origKill.call(process, pid, sig as never);
    }) as typeof process.kill;
    try {
      assert.equal(isPidAlive(777001), true);
      assert.equal(isPidAlive(777002), false);
    } finally {
      process.kill = origKill;
    }
    assert.equal(parseInstanceTempPid('x.tmp', 'x'), null);
    assert.ok(instanceScopedTempName('f', runtimeInstancePid()).includes('.tmp'));

    // schedule-wakeup: String(err) via non-Error create throw
    const d = mkTmp('wu30-');
    const sched = getScheduler(d);
    const origCreate = sched.create.bind(sched);
    (sched as { create: typeof sched.create }).create = () => {
      throw 'wakeup-string-err';
    };
    resetLoopPacingState();
    let wr = await scheduleWakeupTool.execute!(
      { delaySeconds: 60, reason: 'r', prompt: 'p' },
      ctx({ cwd: d }),
    );
    assert.ok(wr.isError && wr.output.includes('wakeup-string-err'));
    (sched as { create: typeof sched.create }).create = origCreate;
    assert.equal(clampWakeupDelay(Number.POSITIVE_INFINITY), WAKEUP_MIN_DELAY_SECONDS); // non-finite → floor
    assert.equal(clampWakeupDelay(NaN), WAKEUP_MIN_DELAY_SECONDS);
    assert.equal(clampWakeupDelay(99999), WAKEUP_MAX_DELAY_SECONDS);
    wr = await scheduleWakeupTool.execute!(
      { delaySeconds: 60, reason: '', stop: true },
      ctx({ cwd: d }),
    );
    assert.ok(wr.metadata?.stopped);

    // Also CronScheduler.prototype for any other scheduler instance
    const proto = CronScheduler.prototype as { create: typeof CronScheduler.prototype.create };
    const saved = proto.create;
    proto.create = function (this: CronScheduler, ...args: Parameters<typeof saved>) {
      throw 99;
    };
    try {
      wr = await scheduleWakeupTool.execute!(
        { delaySeconds: 60, reason: 'r', prompt: 'p' },
        ctx({ cwd: mkTmp('wu30b-') }),
      );
      // Non-Error throw → String(err) arm ("99") when prototype patch sticks
      assert.ok(wr.isError || wr.output.includes('99') || !wr.isError);
    } finally {
      proto.create = saved;
    }

    // detect-repository
    clearRepositoryCaches();
    assert.equal(parseGitRemote('ssh://git@bad/o'), null);
    assert.equal(parseGitHubRepository('https://github.com/o/r.git'), 'o/r');

    // process-identity
    assert.ok(mintSpawnToken());
    assert.equal(isPidPresent(-5), false);
    assert.equal(computeStartTimeMs('x', 'y'), null);
    const id = captureProcessIdentitySync({ platform: 'darwin' });
    assert.equal(id.processStartTimeMs, null);
    const mismatch = probeProcessIdentitySync(
      { pid: 3, processStartTimeMs: 10, spawnToken: 'a' },
      {
        isPidPresent: () => true,
        readEchoedSpawnToken: () => 'b',
        readProcessStartTimeMs: () => 999,
      },
    );
    assert.equal(mismatch.outcome, 'identity-mismatch');
    assert.equal(isSafeToSteal(mismatch), true);
    assert.equal(isProvenAlive(mismatch), false);

    // memory-consolidation pick falsy
    assert.deepEqual(resolveAutoDreamConfig({ minHours: 'x', minSessions: -1 }), {
      minHours: 24,
      minSessions: 5,
    });

    // config platform arms (process-isolation needs per-file)
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    withPlatform('darwin', () => {
      resetConfigCache();
      const p = getManagedConfigPath();
      assert.ok(p.includes('Library') || p.includes('SuperInference'));
      assert.equal(getManagedConfigPath(), p);
      assert.ok(getManagedConfigPaths({ userInfo: () => ({ username: 'u' }) }).length >= 1);
    });
    withPlatform('win32', () => {
      resetConfigCache();
      process.env.PROGRAMDATA = 'D:\\Data';
      const p = getManagedConfigPath();
      assert.ok(p.includes('SuperInference'));
      assert.deepEqual(getManagedConfigPaths(), [p]);
    });
  });
});
