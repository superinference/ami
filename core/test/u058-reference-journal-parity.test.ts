/**
 * U058 — reference-journal subsystem parity additions (confirmed gaps whose impl target
 * is the reference-journal / telemetry / version-gate building blocks). DEDUP-first, each
 * block EXTENDS an existing capability rather than adding a parallel one:
 *  - ProcessMetricsSampler now also surfaces `arrayBuffers` + `constrainedMemory`
 *    (cgroup/container ceiling)                                                  [analytics.ts]
 *  - isCI() + detectDeploymentEnvironment() env-knob classifiers, stamped onto
 *    the OTLP resource attributes (deployment.environment / ci)                  [analytics.ts]
 *  - satisfies(version, range) semver range engine + range support in
 *    isAmiVersionCompatible (bare = floor preserved)                            [skill-catalog.ts]
 *
 * RED/GREEN: each block fails if its feature is reverted (the fields/functions did not
 * exist before this unit).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  ProcessMetricsSampler,
  AnalyticsTracker,
  isCI,
  detectDeploymentEnvironment,
  buildTelemetryResourceAttrs,
} from '../src/analytics';
import { satisfies, isAmiVersionCompatible, compareSemver } from '../src/skill-catalog';

// ---------------------------------------------------------------------------
// ProcessMetricsSampler — arrayBuffers + constrainedMemory
// ---------------------------------------------------------------------------
describe('ProcessMetricsSampler arrayBuffers + constrainedMemory (U058)', () => {
  const mem = (arrayBuffers: number): NodeJS.MemoryUsage =>
    ({ rss: 10, heapTotal: 8, heapUsed: 4, external: 3, arrayBuffers }) as NodeJS.MemoryUsage;

  it('surfaces arrayBuffers from memoryUsage()', () => {
    const s = new ProcessMetricsSampler({
      memoryUsage: () => mem(7), cpuUsage: () => ({ user: 0, system: 0 }), uptime: () => 1, now: () => 1,
      constrainedMemory: () => 0,
    });
    assert.equal(s.sample()!.arrayBuffers, 7);
  });

  it('includes constrainedMemory only when the reader returns a positive finite ceiling', () => {
    const withCeiling = new ProcessMetricsSampler({
      memoryUsage: () => mem(1), cpuUsage: () => ({ user: 0, system: 0 }), uptime: () => 1, now: () => 1,
      constrainedMemory: () => 512 * 1024 * 1024,
    });
    assert.equal(withCeiling.sample()!.constrainedMemory, 512 * 1024 * 1024);
  });

  it('omits constrainedMemory when unconstrained (0), undefined, non-finite, or the reader throws', () => {
    const base = { memoryUsage: () => mem(1), cpuUsage: () => ({ user: 0, system: 0 }), uptime: () => 1, now: () => 1 };
    for (const reader of [
      () => 0,
      () => undefined,
      () => Infinity,
      () => { throw new Error('unsupported'); },
    ]) {
      const s = new ProcessMetricsSampler({ ...base, constrainedMemory: reader });
      const out = s.sample()!;
      assert.equal('constrainedMemory' in out, false, `reader ${reader} must not add the field`);
      assert.equal(out.arrayBuffers, 1); // sampler still returns the rest
    }
  });

  it('attaches the extended fields to analytics events when process metrics are enabled', () => {
    const t = new AnalyticsTracker(undefined, {
      env: {}, includeProcessMetrics: true,
      processMetricsDeps: {
        memoryUsage: () => mem(5), cpuUsage: () => ({ user: 1, system: 1 }), uptime: () => 1, now: () => 1,
        constrainedMemory: () => 123,
      },
    });
    t.log('e', {});
    const proc = t.getEvents()[0].data.process as { arrayBuffers: number; constrainedMemory: number };
    assert.equal(proc.arrayBuffers, 5);
    assert.equal(proc.constrainedMemory, 123);
  });
});

// ---------------------------------------------------------------------------
// isCI
// ---------------------------------------------------------------------------
describe('isCI (U058)', () => {
  it('reads the CI env knob through isEnvTruthy', () => {
    assert.equal(isCI({ CI: '1' }), true);
    assert.equal(isCI({ CI: 'true' }), true);
    assert.equal(isCI({ CI: '0' }), false);
    assert.equal(isCI({ CI: 'false' }), false);
    assert.equal(isCI({}), false);
  });
});

// ---------------------------------------------------------------------------
// detectDeploymentEnvironment
// ---------------------------------------------------------------------------
describe('detectDeploymentEnvironment (U058)', () => {
  const noFs = { readFileSync: () => { throw new Error('enoent'); }, existsSync: () => false };

  it('classifies cloud dev environments first', () => {
    assert.equal(detectDeploymentEnvironment({ CODESPACES: 'true' }, noFs), 'codespaces');
    assert.equal(detectDeploymentEnvironment({ GITPOD_WORKSPACE_ID: 'x' }, noFs), 'gitpod');
    assert.equal(detectDeploymentEnvironment({ REPL_ID: 'x' }, noFs), 'replit');
  });

  it('classifies cloud platforms', () => {
    assert.equal(detectDeploymentEnvironment({ VERCEL: '1' }, noFs), 'vercel');
    assert.equal(detectDeploymentEnvironment({ AWS_LAMBDA_FUNCTION_NAME: 'fn' }, noFs), 'aws-lambda');
    assert.equal(detectDeploymentEnvironment({ K_SERVICE: 's' }, noFs), 'gcp-cloud-run');
  });

  it('detects AWS EC2 via the hypervisor uuid file', () => {
    const ec2 = { readFileSync: (p: string) => (p === '/sys/hypervisor/uuid' ? 'ec2abcd\n' : ''), existsSync: () => false };
    assert.equal(detectDeploymentEnvironment({}, ec2), 'aws-ec2');
  });

  it('prefers a specific CI provider over the generic ci flag', () => {
    assert.equal(detectDeploymentEnvironment({ GITHUB_ACTIONS: 'true', CI: 'true' }, noFs), 'github-actions');
    assert.equal(detectDeploymentEnvironment({ CI: 'true' }, noFs), 'ci');
  });

  it('classifies container orchestration (kubernetes before docker)', () => {
    assert.equal(detectDeploymentEnvironment({ KUBERNETES_SERVICE_HOST: '10.0.0.1' }, noFs), 'kubernetes');
    assert.equal(detectDeploymentEnvironment({}, { ...noFs, existsSync: (p) => p === '/.dockerenv' }), 'docker');
  });

  it('falls back to unknown-<platform> when nothing matches', () => {
    assert.equal(detectDeploymentEnvironment({}, { ...noFs, platform: 'linux' }), 'unknown-linux');
    assert.equal(detectDeploymentEnvironment({}, { ...noFs, platform: 'darwin' }), 'unknown-darwin');
    assert.equal(detectDeploymentEnvironment({}, { ...noFs, platform: 'win32' }), 'unknown-win32');
    assert.equal(detectDeploymentEnvironment({}, { ...noFs, platform: 'freebsd' as NodeJS.Platform }), 'unknown');
  });
});

// ---------------------------------------------------------------------------
// buildTelemetryResourceAttrs wiring
// ---------------------------------------------------------------------------
describe('buildTelemetryResourceAttrs deployment/ci wiring (U058)', () => {
  it('stamps deployment.environment and ci when detectable', () => {
    const attrs = buildTelemetryResourceAttrs('sess', { GITHUB_ACTIONS: 'true', CI: 'true' });
    assert.equal(attrs['deployment.environment'], 'github-actions');
    assert.equal(attrs['ci'], 'true');
  });

  // The host's real filesystem is a deployment signal (/.dockerenv exists on every
  // container runner), so these cases inject the probes instead of reading it.
  const bareHost = {
    readFileSync: () => { throw new Error('enoent'); },
    existsSync: () => false,
    platform: 'linux' as const,
  };

  it('elides the generic unknown-* deployment fallback (no signal)', () => {
    const attrs = buildTelemetryResourceAttrs('sess', {}, {}, bareHost);
    assert.equal('deployment.environment' in attrs, false);
    assert.equal('ci' in attrs, false);
  });

  it('stamps docker when only the container marker is present', () => {
    const attrs = buildTelemetryResourceAttrs('sess', {}, {}, { ...bareHost, existsSync: (p: string) => p === '/.dockerenv' });
    assert.equal(attrs['deployment.environment'], 'docker');
  });

  it('honors the cardinality opt-out for deployment.environment', () => {
    const attrs = buildTelemetryResourceAttrs('sess', { CODESPACES: 'true', AMI_OTEL_INCLUDE_DEPLOYMENT: '0' });
    assert.equal('deployment.environment' in attrs, false);
  });
});

// ---------------------------------------------------------------------------
// satisfies — semver range engine
// ---------------------------------------------------------------------------
describe('satisfies semver range engine (U058)', () => {
  it('matches exact + partial + wildcard', () => {
    assert.equal(satisfies('1.2.3', '1.2.3'), true);
    assert.equal(satisfies('1.2.4', '1.2.3'), false);
    assert.equal(satisfies('1.2.9', '1.2'), true);   // 1.2 → >=1.2.0 <1.3.0
    assert.equal(satisfies('1.3.0', '1.2'), false);
    assert.equal(satisfies('1.9.9', '1.x'), true);
    assert.equal(satisfies('2.0.0', '1.x'), false);
    assert.equal(satisfies('9.9.9', '*'), true);
    assert.equal(satisfies('9.9.9', ''), true);
  });

  it('handles caret ranges incl. 0.x special cases', () => {
    assert.equal(satisfies('1.5.0', '^1.2.3'), true);
    assert.equal(satisfies('2.0.0', '^1.2.3'), false);
    assert.equal(satisfies('1.2.2', '^1.2.3'), false);
    assert.equal(satisfies('0.2.9', '^0.2.3'), true);
    assert.equal(satisfies('0.3.0', '^0.2.3'), false);
    assert.equal(satisfies('0.0.3', '^0.0.3'), true);
    assert.equal(satisfies('0.0.4', '^0.0.3'), false);
  });

  it('handles tilde ranges', () => {
    assert.equal(satisfies('1.2.9', '~1.2.3'), true);
    assert.equal(satisfies('1.3.0', '~1.2.3'), false);
    assert.equal(satisfies('1.9.9', '~1'), true);
    assert.equal(satisfies('2.0.0', '~1'), false);
  });

  it('handles comparators, compound AND, hyphen and OR', () => {
    assert.equal(satisfies('1.5.0', '>=1.0.0 <2.0.0'), true);
    assert.equal(satisfies('2.0.0', '>=1.0.0 <2.0.0'), false);
    assert.equal(satisfies('1.5.0', '1.0.0 - 2.0.0'), true);
    assert.equal(satisfies('2.0.1', '1.0.0 - 2.0.0'), false);
    assert.equal(satisfies('3.1.0', '2.x || 3.x'), true);
    assert.equal(satisfies('4.0.0', '2.x || 3.x'), false);
  });

  it('fails closed on a bad version or unparseable range', () => {
    assert.equal(satisfies('garbage', '^1.0.0'), false);
    assert.equal(satisfies('1.0.0', '>=not-a-version'), false);
    assert.equal(satisfies(1 as unknown as string, '1.0.0'), false);
  });
});

// ---------------------------------------------------------------------------
// isAmiVersionCompatible — range support, floor preserved
// ---------------------------------------------------------------------------
describe('isAmiVersionCompatible range support (U058)', () => {
  it('keeps the minimum-floor semantics for a bare version (AMI perk)', () => {
    assert.equal(isAmiVersionCompatible('0.7.0', '0.7.4'), true);  // current >= floor
    assert.equal(isAmiVersionCompatible('1.0.0', '0.7.4'), false); // current < floor
    assert.equal(isAmiVersionCompatible('2', '2.9.0'), true);      // partial bare = floor too
  });

  it('evaluates a range expression when operators are present', () => {
    assert.equal(isAmiVersionCompatible('^2.0.0', '2.5.0'), true);
    assert.equal(isAmiVersionCompatible('^2.0.0', '3.0.0'), false);
    assert.equal(isAmiVersionCompatible('>=2 <4', '3.9.0'), true);
    assert.equal(isAmiVersionCompatible('2.x || 3.x', '3.1.0'), true);
    assert.equal(isAmiVersionCompatible('2.x || 3.x', '4.0.0'), false);
  });

  it('imposes no floor when required is absent/empty', () => {
    assert.equal(isAmiVersionCompatible(undefined, '0.1.0'), true);
    assert.equal(isAmiVersionCompatible('   ', '0.1.0'), true);
  });

  it('shares the prerelease-agnostic contract with compareSemver', () => {
    assert.equal(compareSemver('1.2.3-beta', '1.2.3'), 0);
    assert.equal(satisfies('1.2.3-beta', '>=1.2.3'), true);
  });
});
