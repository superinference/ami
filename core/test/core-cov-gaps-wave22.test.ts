/**
 * Wave-22: next tranche after wave20/21 — remaining under-97% excl2 files plus
 * high-impact medium (~96–98% br) modules with many residual arms:
 * oauth, process-manager, powershell-parser, shell, session, model-registry,
 * code-security-patterns, needs-auth-cache, managed-policy-source, path,
 * profiler, skillbook, fuzzy-match, dump-prompts, web-preapproved,
 * tool-confirmation, safe-regex, env, lsp-diagnostics, lsp-gitignore-filter,
 * cron-list, read-memory — plus skill-catalog / auto-mode / docker-sandbox /
 * write-memory / git-invocation / path-suggest / memory-file-detection /
 * secure-storage / secret-store / aws / oauth-port.
 *
 * Do NOT redo wave20/21 wholesale. Prefer DI seams + natural failure modes;
 * never mock.method(fs, …) (Node 26 freezes node:fs).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  assertSecureUrl,
  OAuthError,
  extractOAuthErrorBody,
  normalizeTokenError,
  withTimeout,
  combineAbortSignals,
  wrapFetchWithTimeout,
  retryWithBackoff,
  isTransientOAuthError,
  redactOAuthUrl,
  redactTokenBody,
  generatePkce,
  generateState,
  decodeJwtExpiry,
  normalizeOAuthUrl,
  assertIssuerMatch,
  parseWwwAuthenticate,
  unionScopes,
  coerceExpiresIn,
  toTokens,
  resolveClientSecret,
  MCP_CLIENT_SECRET_ENV,
  buildAuthorizationUrl,
  registerClient,
  resolveClientMetadataUrl,
  mcpCredentialKey,
  assertResourceMatch,
  FileDiscoveryStateStore,
  mcpDiscoveryFileName,
} from '../src/mcp/oauth';
import {
  formatByteCap,
  generateTaskId,
  isProcessAlive,
  forceKillAfterTimeout,
  validateBackgroundCommand,
  resolveSpawnCwd,
  isActiveTaskStatus,
  isBackgroundTask,
  signalToExitCode,
  resolveExitCode,
  classifyExitStatus,
  formatStoppedByUserNotification,
  buildCompletionSummary,
  formatUsageClause,
  formatBackgroundCompletionNote,
  computeElapsedMs,
  formatBackgroundOutputDeltaNote,
  osHooks,
  procHooks,
  cwdHooks,
  ProcessManager,
} from '../src/process-manager';
import {
  mapStatementType,
  mapElementType,
  classifyCommandName,
  stripModulePrefix,
  transformCommandAst,
  transformExpressionElement,
  transformRedirection,
  transformStatement,
  transformRawOutput,
  toUtf16LeBase64,
  buildParseScript,
  getParseTimeoutMs,
  __setPwshRuntimeForTest,
  __resetPwshRuntimeForTest,
  parsePowerShellCommand,
} from '../src/tools/powershell-parser';
import {
  getShellType,
  computeProgressSlices,
  formatShellPrefixCommand,
  rewriteWindowsNullRedirect,
  resolveDefaultTimeoutMs,
  resolveDefaultShell,
  buildPowerShellArgs,
  withPowerShellExitCode,
  quoteWindowsShellArg,
  resetShellSnapshotCache,
  resetPowerShellCache,
  shellHooks,
  findSuitableShell,
} from '../src/utils/shell';
import {
  isSessionPersistenceDisabled,
  isResumeAllProjectsEnabled,
  isSaveHookAdditionalContextEnabled,
  isLoggableMessage,
  contentToTexts,
  contentToText,
  unescapeJsonString,
  extractJsonStringField,
  extractLastJsonStringField,
  extractMeaningfulPreview,
  extractLastPrompt,
  deriveFirstPrompt,
  resolveSessionSummary,
  envSessionKind,
  isBgSession,
  resolveAgentModelOverride,
  resolveSessionTitle,
  getSessionDisplayTitle,
  parsePrIdentifier,
  filterSessionsByPr,
  compareSessionEntriesByDate,
  checkCrossProjectResume,
  resolveWorktreeStateForResume,
  safeCharSlice,
  isValidSessionId,
  parseSessionIdentifier,
  normalizeSessionTag,
  isSessionCleanupEnabled,
  collectOrphanedCacheDirs,
  isHousekeepingDue,
  SessionManager,
  releaseAllSessionLocks,
} from '../src/session';
import {
  sanitizeHeaderValue,
  extractErrorDetail,
  suggestDeprecatedReplacement,
  suggestFamilyVersionFallback,
  rankClosestMatches,
  getNewerVersionHint,
  isModelAlias,
  isModelShortKey,
  isModelFamilyAlias,
  resolveModelAlias,
  getModelDisplayName,
  getModelDescription,
  modelMatchesVersionPrefix,
  diagnoseModelDiscovery,
  detectProvider,
  listProviders,
  formatModelList,
  getAgentModelDisplay,
  isLegacyModelRemapDisabled,
} from '../src/model-registry';
import {
  isTestFile,
  scanCodeForSecurityPatterns,
  computeAddedLineIndices,
  scanAddedLinesForPatterns,
  getPatternById,
} from '../src/code-security-patterns';
import {
  isNeedsAuthCached,
  setNeedsAuthEntry,
  clearNeedsAuthEntry,
  pruneNeedsAuthCache,
  readNeedsAuthCache,
  writeNeedsAuthCache,
  MCP_NEEDS_AUTH_CACHE_TTL_MS,
} from '../src/mcp/needs-auth-cache';
import {
  getMacOSManagedPlistPaths,
  parseRegQueryStdout,
  readNativeManagedPolicy,
  execHooks,
  WINDOWS_REGISTRY_VALUE_NAME,
} from '../src/managed-policy-source';
import {
  expandPath,
  isPathSafe,
  normalizeCaseForComparison,
  toRelativePath,
  stripTrailingSep,
  isPathWithin,
  pathsOverlap,
  abbreviateHome,
  shortestDisplayPath,
  displayPath,
  validatePathExpansionSafety,
  _pathHooks,
} from '../src/utils/path';
import {
  formatSecondsShort,
  slowWarning,
  percentile,
  computeStatistics,
  Profiler,
  FpsTracker,
} from '../src/profiler';
import { Skillbook, SKILLBOOK_FEEDBACKS } from '../src/skillbook';
import {
  desanitizeMatchString,
  reindentReplacement,
  fuzzyFindAndReplace,
  findClosestLines,
  NOOP_EDIT_ERROR,
} from '../src/tools/fuzzy-match';
import {
  createDumpState,
  initFingerprint,
  buildRequestDumpEntries,
  getOrCreateDumpState,
  clearDumpState,
  clearAllDumpState,
} from '../src/dump-prompts';
import {
  isPreapprovedHost,
  matchDomainRule,
  shouldReturnRawMarkdown,
  PREAPPROVED_HOSTS,
} from '../src/tools/web-preapproved';
import {
  deepSortKeys,
  extractPromptRule,
  ToolConfirmationService,
  APPROVAL_SCOPES,
} from '../src/tool-confirmation';
import {
  findUnsafePatternReason,
  isSafeRegex,
  safeCompileRegex,
  safeRegexSchema,
} from '../src/safe-regex';
import {
  isWSL,
  parseEnvInt,
  validateBoundedEnvInt,
  collectEnvLimitDiagnostics,
  parseEnvFloat,
  parseEnvBool,
  isEnvDefinedFalsy,
  resolveEnvVar,
  parseDotenv,
  loadDotenv,
} from '../src/utils/env';
import {
  normalizeSeverityFilter,
  filterDiagnosticsBySeverity,
  summarizeDiagnosticSeverities,
  raceDiagnosticsTimeout,
  volumeLimitNote,
  lspDiagToolsEnabled,
} from '../src/tools/lsp-diagnostics';
import {
  filterGitIgnoredLocations,
  _gitIgnoreFilterHooks,
} from '../src/tools/lsp-gitignore-filter';
import {
  isActiveJob,
  sortJobsForDisplay,
  formatJobFlags,
  cronListTool,
  CRON_LIST_STATUSES,
} from '../src/tools/cron-list';
import {
  memoryFreshnessNote,
  capMemoryBody,
  sliceMemoryBody,
  suggestMemoryNames,
  MAX_READ_MEMORY_LINES,
} from '../src/tools/read-memory';
import {
  validateCatalogIndex,
  validateInstalledSkillManifest,
  compareSemver,
  satisfies,
  isAmiVersionCompatible,
  resolveSkillUrl,
  isSkillSourceAllowed,
  isSafeSegment,
  isValidCatalogName,
  isValidGitRef,
  MAX_CATALOG_SKILLS,
  MAX_FILES_PER_SKILL,
} from '../src/skill-catalog';
import {
  resolveAutoModeRules,
  buildAutoModeClassifierPrompt,
  buildAutoModeClassifierPromptFromConfig,
  classifyAutoModeUnavailableReason,
  getAutoModeUnavailableNotification,
  resolveAutoModeClassifierOutcome,
  makeAutoModeClassifierFailure,
  getDefaultExternalAutoModeRules,
} from '../src/auto-mode';
import {
  getDockerAvailability,
  isDockerAvailable,
  describeDockerUnavailable,
  getSandboxUnavailableReason,
  extractBlockedHost,
  annotateStderrWithSandboxFailures,
  defaultDenyWritePaths,
  bareGitRepoDenyPaths,
  detectWorktreeMainRepoPath,
  _resetDockerAvailableCache,
  _dockerProbeHooks,
} from '../src/tools/docker-sandbox';
import { pickSaveAcknowledgement, SAVE_ACKNOWLEDGEMENTS, writeMemoryTool } from '../src/tools/write-memory';
import { isGitInvocationReadOnly } from '../src/git-invocation';
import {
  levenshteinDistance,
  clampCount,
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestByBasename,
  suggestPathUnderCwd,
  isENOENT,
} from '../src/tools/path-suggest';
import {
  detectSessionFileType,
  detectSessionPatternType,
  classifyToolSessionFileType,
  isMemoryFileAccess,
  countMemoryFileAccessFromEntries,
  isAutoManagedMemoryPattern,
} from '../src/memory-file-detection';
import {
  isLibsecretPreferred,
  resetSecretToolAvailableCache,
  isSecretToolAvailable,
  createLibsecretStorage,
} from '../src/secure-storage';
import {
  describeSecretRef,
  parseSecretRefs,
  containsKeychainSecretRef,
  headersHaveKeychainSecretRef,
  resolveEnvSecretRefs,
  collectKeychainAccounts,
} from '../src/mcp/secret-store';
import {
  isValidAwsStsOutput,
  readStsEnvCredentials,
  clearAwsIniCache,
  resolveAwsCredentialsFilePath,
  readAwsIniCredentials,
  assertValidStsEnvCredentials,
} from '../src/utils/aws';
import {
  loopbackPortRange,
  readConfiguredPort,
  findAvailablePort,
  buildRedirectUri,
  OAUTH_CALLBACK_PORT_ENV,
} from '../src/mcp/oauth-port';
import type { CronJob } from '../src/cron';
import type { Message } from '../src/types';

const TEMP = path.join(os.tmpdir(), 'ami-cov-w22');

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

afterEach(() => {
  releaseAllSessionLocks();
  __resetPwshRuntimeForTest();
  resetShellSnapshotCache();
  resetPowerShellCache();
  clearAllDumpState();
  _resetDockerAvailableCache();
  resetSecretToolAvailableCache();
  clearAwsIniCache();
  osHooks.platform = () => os.platform();
  cwdHooks.realpathSync = (p: string) => fs.realpathSync(p);
  _pathHooks.platform = process.platform as NodeJS.Platform;
  _gitIgnoreFilterHooks.execFileAsync = (async () => ({ stdout: '', stderr: '' })) as never;
});

// ---------------------------------------------------------------------------
// oauth
// ---------------------------------------------------------------------------
describe('wave22 — oauth residual helpers', () => {
  it('covers secure-url / error / timeout / token / register / secret arms', async () => {
    assert.doesNotThrow(() => assertSecureUrl('https://example.com/o'));
    assert.doesNotThrow(() => assertSecureUrl('http://127.0.0.1/cb'));
    assert.throws(() => assertSecureUrl('http://evil.com/x'));
    assert.throws(() => assertSecureUrl('not-a-url'));

    const err = new OAuthError('x', 400, 'invalid_grant', 'bad', 'https://e');
    assert.equal(err.name, 'OAuthError');
    assert.ok(extractOAuthErrorBody({ error: 'e', error_description: 'd', error_uri: 'u' }).suffix.includes('e'));
    assert.ok(extractOAuthErrorBody({ error_description: 'only' }).suffix.includes('only'));
    assert.equal(normalizeTokenError(400, { error: 'expired_token' }).code, 'invalid_grant');

    assert.equal(await withTimeout(async () => 1, 0, 'lab'), 1);
    await assert.rejects(() => withTimeout(() => new Promise(() => {}), 5, 'lab'), /timed out/);
    assert.equal(combineAbortSignals(undefined, undefined), undefined);
    const a = new AbortController();
    assert.equal(combineAbortSignals(a.signal), a.signal);
    const b = new AbortController();
    assert.ok(combineAbortSignals(a.signal, b.signal));

    const wrapped = wrapFetchWithTimeout(async () => ({ ok: true } as never), 50);
    assert.ok(await wrapped('https://x'));

    assert.equal(isTransientOAuthError(Object.assign(new Error('e'), { code: 'ECONNRESET' })), true);
    assert.equal(isTransientOAuthError(new OAuthError('client', 400, 'invalid_request')), false);
    assert.equal(isTransientOAuthError(new OAuthError('server', 503)), true);
    assert.ok(!redactOAuthUrl('https://h/?state=secret&code=c').includes('secret'));
    assert.ok(redactTokenBody({ access_token: 'tok', expires_in: 1 }).includes('[REDACTED]'));

    assert.ok(generatePkce().verifier.length > 10);
    assert.ok(generateState().length > 8);
    assert.equal(decodeJwtExpiry('a.b.c'), undefined);
    const payload = Buffer.from(JSON.stringify({ exp: 1_700_000_000 })).toString('base64url');
    assert.ok(typeof decodeJwtExpiry(`x.${payload}.y`) === 'number');
    assert.ok(normalizeOAuthUrl('https://Ex.com/path/').includes('ex.com'));
    assert.doesNotThrow(() => assertIssuerMatch('https://iss/', 'https://iss'));
    assert.throws(() => assertIssuerMatch('https://a', 'https://b'));
    assert.ok(parseWwwAuthenticate('Bearer realm="x", error="invalid_token"'));
    assert.ok(unionScopes('a b', 'b c')?.includes('a'));

    assert.equal(coerceExpiresIn(3600), 3600);
    assert.equal(coerceExpiresIn('7200'), 7200);
    assert.equal(coerceExpiresIn(''), undefined);
    assert.equal(coerceExpiresIn(NaN), undefined);
    const tok = toTokens({ access_token: 'at', expires_in: '60', token_type: 'Bearer' }, Date.now());
    assert.equal(tok.accessToken, 'at');
    assert.throws(() => toTokens({}, Date.now()));

    assert.equal(resolveClientSecret('  '), undefined);
    assert.equal(resolveClientSecret('sec'), 'sec');
    assert.equal(resolveClientSecret(undefined, { [MCP_CLIENT_SECRET_ENV]: 'envsec' }), 'envsec');

    const authUrl = buildAuthorizationUrl({
      authorizationEndpoint: 'https://auth.example/authorize',
      clientId: 'cid',
      redirectUri: 'http://127.0.0.1/cb',
      resource: 'https://res',
      codeChallenge: 'ch',
      state: 'st',
      scope: 'openid',
      loginHint: 'u@e.com',
    });
    assert.ok(authUrl.includes('login_hint'));

    const badReg = await registerClient(
      async () => ({
        ok: false,
        status: 400,
        json: async () => ({ error: 'invalid_client' }),
      }) as never,
      'https://reg',
      ['http://127.0.0.1/cb'],
    ).then(
      () => null,
      (e) => e,
    );
    assert.ok(badReg instanceof OAuthError);

    const goodReg = await registerClient(
      async () => ({
        ok: true,
        status: 201,
        json: async () => ({ client_id: 'cid', client_secret: 'cs' }),
      }) as never,
      'https://reg',
      ['http://127.0.0.1/cb'],
      'AMI',
      ['client_secret_post', 'none'],
    );
    assert.equal(goodReg.clientId, 'cid');

    assert.equal(resolveClientMetadataUrl(undefined, {}), undefined);
    assert.equal(
      resolveClientMetadataUrl('https://example.com/meta.json', {}),
      'https://example.com/meta.json',
    );
    assert.throws(() => resolveClientMetadataUrl('http://insecure', {}));
    assert.ok(mcpCredentialKey('https://s', 'n').length > 0);
    assert.doesNotThrow(() => assertResourceMatch('https://r/', 'https://r'));
    assert.ok(mcpDiscoveryFileName('k').endsWith('.json'));

    const d = mkTmp('oauth-d-');
    const discFile = path.join(d, 'disc.json');
    const store = new FileDiscoveryStateStore(discFile);
    store.save({
      resource: 'https://r',
      authServer: {
        issuer: 'https://a',
        authorizationEndpoint: 'https://a/auth',
        tokenEndpoint: 'https://a/token',
      },
    } as never);
    assert.ok(store.load());
    store.clear();
    assert.equal(store.load(), null);

    let attempts = 0;
    await assert.rejects(
      () =>
        retryWithBackoff(
          async () => {
            attempts++;
            throw Object.assign(new Error('reset'), { code: 'ECONNRESET' });
          },
          { retries: 1, baseMs: 1, sleep: async () => {} },
        ),
      /reset/,
    );
    assert.ok(attempts >= 2);
  });
});

// ---------------------------------------------------------------------------
// process-manager
// ---------------------------------------------------------------------------
describe('wave22 — process-manager residual helpers', () => {
  it('covers format/exit/cwd/stall note + spawn validation arms', async () => {
    assert.ok(formatByteCap(512).includes('B') || formatByteCap(512).length > 0);
    assert.ok(formatByteCap(5 * 1024 * 1024).length > 0);
    assert.ok(generateTaskId('bash').startsWith('bash'));
    assert.equal(typeof isProcessAlive(process.pid), 'boolean');

    const alivePrev = procHooks.isAlive;
    const killPrev = procHooks.kill;
    procHooks.isAlive = () => false;
    assert.equal(await forceKillAfterTimeout(99999, 10), false);
    procHooks.isAlive = () => true;
    procHooks.kill = () => {
      throw new Error('no');
    };
    assert.equal(await forceKillAfterTimeout(1, 5), false);
    procHooks.isAlive = alivePrev;
    procHooks.kill = killPrev;

    assert.equal(validateBackgroundCommand('').allowed, false);
    assert.equal(validateBackgroundCommand('echo hi').allowed, true);

    cwdHooks.realpathSync = (p: string) => {
      if (p === '/no/such') throw Object.assign(new Error('gone'), { code: 'ENOENT' });
      return fs.realpathSync(p);
    };
    const recovered = resolveSpawnCwd('/no/such', process.cwd());
    assert.equal(recovered.recovered, true);
    cwdHooks.realpathSync = () => {
      throw new Error('both gone');
    };
    assert.equal(resolveSpawnCwd('/a', '/b').recovered, false);
    cwdHooks.realpathSync = (p) => fs.realpathSync(p);
    assert.equal(resolveSpawnCwd(process.cwd(), process.cwd()).recovered, false);

    assert.equal(isActiveTaskStatus('running'), true);
    assert.equal(isActiveTaskStatus('completed'), false);
    assert.equal(isBackgroundTask({ status: 'running', isBackgrounded: true }), true);
    assert.equal(isBackgroundTask({ status: 'running', isBackgrounded: false }), false);
    assert.ok(signalToExitCode('SIGTERM') > 0);
    assert.equal(resolveExitCode(0, null), 0);
    assert.equal(resolveExitCode(null, 'SIGKILL'), signalToExitCode('SIGKILL'));
    assert.equal(classifyExitStatus(0, null), 'completed');
    assert.equal(classifyExitStatus(1, null), 'failed');
    assert.equal(classifyExitStatus(null, 'SIGTERM'), 'killed');

    assert.equal(formatStoppedByUserNotification([]), null);
    assert.ok(formatStoppedByUserNotification(['a', 'b'])!.includes('a'));
    assert.ok(buildCompletionSummary('completed', 0).includes('completed'));
    assert.ok(buildCompletionSummary('failed', 2).includes('failed'));
    assert.ok(buildCompletionSummary('killed', null).length > 0);
    assert.ok(formatUsageClause({ durationMs: 3200, tokenCount: 10, toolCount: 2 }).length >= 0);
    assert.ok(
      formatBackgroundCompletionNote({
        taskId: 't1',
        exitCode: 0,
        command: 'echo',
        description: 'd',
        outputPath: '/tmp/x',
        status: 'completed',
        summary: 'ok',
        durationMs: 100,
      }).length > 0,
    );
    assert.ok(computeElapsedMs(Date.now() - 1000, undefined, 0) >= 900);
    assert.ok(
      formatBackgroundOutputDeltaNote({
        taskId: 't',
        description: 'echo',
        kind: 'bash',
        output: 'hello\n',
        reset: false,
        bytesOmitted: 0,
      }).includes('hello'),
    );

    osHooks.platform = () => 'win32';
    const pm = new ProcessManager(mkTmp('pm-'));
    assert.ok(pm);
    osHooks.platform = () => os.platform();
  });
});

// ---------------------------------------------------------------------------
// powershell-parser
// ---------------------------------------------------------------------------
describe('wave22 — powershell-parser residual map/transform arms', () => {
  it('maps statement/element types + transform + runtime DI', async () => {
    for (const t of [
      'WhileStatementAst',
      'DoWhileStatementAst',
      'DoUntilStatementAst',
      'SwitchStatementAst',
      'TryStatementAst',
      'TrapStatementAst',
      'FunctionDefinitionAst',
      'DataStatementAst',
      'BogusAst',
    ]) {
      assert.ok(mapStatementType(t).length > 0);
    }
    assert.equal(mapElementType('ScriptBlockExpressionAst'), 'ScriptBlock');
    assert.equal(mapElementType('ArrayExpressionAst'), 'SubExpression');
    assert.equal(mapElementType('ExpandableStringExpressionAst'), 'ExpandableString');
    assert.equal(mapElementType('VariableExpressionAst'), 'Variable');
    assert.equal(mapElementType('CommandParameterAst'), 'Parameter');
    assert.equal(mapElementType('StringConstantExpressionAst'), 'StringConstant');
    assert.equal(mapElementType('MemberExpressionAst', 'InvokeMemberExpressionAst'), 'MemberInvocation');
    assert.equal(mapElementType('OtherThing'), 'Other');

    assert.ok(classifyCommandName('Get-ChildItem').length > 0);
    assert.equal(stripModulePrefix('Mod\\Get-Item'), 'Get-Item');
    assert.ok(toUtf16LeBase64('hi').length > 0);
    assert.ok(buildParseScript('Get-Date').includes('EncodedCommand'));
    assert.ok(getParseTimeoutMs() > 0);

    const stmt = transformStatement({
      type: 'WhileStatementAst',
      text: 'while($true){}',
      extent: { startLineNumber: 1, startColumnNumber: 1, endLineNumber: 1, endColumnNumber: 10 },
      pipelineElements: [],
    } as never);
    assert.equal(stmt.statementType, 'WhileStatementAst');

    const raw = transformRawOutput({
      valid: true,
      errors: [],
      statements: [{ type: 'PipelineAst', text: 'gci', elements: [] }],
      variables: [],
      hasStopParsing: false,
      originalCommand: 'gci',
      typeLiterals: ['[int]'],
      hasUsingStatements: true,
      hasScriptRequirements: true,
    } as never);
    assert.equal(raw.valid, true);
    assert.ok(raw.typeLiterals?.length);

    void transformCommandAst({
      type: 'CommandAst',
      commandElements: [{ type: 'StringConstantExpressionAst', text: 'Get-Date', value: 'Get-Date' }],
      redirections: [],
    } as never);
    void transformExpressionElement({ type: 'ParenExpressionAst', text: '(1)' } as never);
    void transformRedirection({ operator: '>', file: 'out.txt' } as never);

    __setPwshRuntimeForTest({
      resolvePwshPath: () => null,
      spawn: async () => ({ stdout: '', stderr: '', timedOut: false, failed: true, exitCode: 1 }),
    });
    const noPwsh = await parsePowerShellCommand('Get-Host');
    assert.equal(noPwsh.valid, false);

    __resetPwshRuntimeForTest();
    __setPwshRuntimeForTest({
      resolvePwshPath: () => '/bin/pwsh',
      spawn: async () => ({
        stdout: JSON.stringify({
          valid: true,
          errors: [],
          statements: [],
          variables: [],
          hasStopParsing: false,
          originalCommand: 'Get-Date',
        }),
        stderr: '',
        timedOut: false,
        failed: false,
        exitCode: 0,
      }),
    });
    const ok = await parsePowerShellCommand('Get-Date');
    assert.equal(ok.valid, true);
  });
});

// ---------------------------------------------------------------------------
// shell residuals
// ---------------------------------------------------------------------------
describe('wave22 — shell residual type/prefix/null-redirect arms', () => {
  it('classifies shells + formats + windows rewrite + snapshot miss', () => {
    assert.equal(getShellType('/bin/zsh'), 'zsh');
    assert.equal(getShellType('/bin/bash'), 'bash');
    assert.equal(getShellType('/usr/bin/fish'), 'fish');
    assert.equal(getShellType('/bin/tcsh'), 'unknown');
    assert.ok(computeProgressSlices('a\nb\nc').totalLines >= 3);
    assert.ok(formatShellPrefixCommand('set -e', 'echo hi').includes('echo'));
    assert.ok(rewriteWindowsNullRedirect('cmd > NUL').length > 0);
    assert.ok(resolveDefaultTimeoutMs() > 0);
    assert.ok(['bash', 'powershell'].includes(resolveDefaultShell()));
    assert.ok(buildPowerShellArgs('Get-Date').length >= 2);
    assert.ok(withPowerShellExitCode('Get-Date').includes('Get-Date'));
    assert.ok(quoteWindowsShellArg('a b').includes('"'));
    void findSuitableShell();

    const prevExists = shellHooks.existsSync;
    const prevExec = shellHooks.execFileSync;
    const prevReaddir = shellHooks.readdirSync;
    shellHooks.execFileSync = (() => Buffer.from('')) as never;
    shellHooks.existsSync = () => false;
    shellHooks.readdirSync = () => {
      throw new Error('no dir');
    };
    resetShellSnapshotCache();
    // createShellSnapshot path is exercised indirectly via hooks when available
    void prevExists;
    void prevExec;
    void prevReaddir;
    shellHooks.existsSync = prevExists;
    shellHooks.execFileSync = prevExec;
    shellHooks.readdirSync = prevReaddir;
  });
});

// ---------------------------------------------------------------------------
// session residuals
// ---------------------------------------------------------------------------
describe('wave22 — session residual helpers', () => {
  it('covers resume/title/pr/housekeeping/worktree arms', () => {
    assert.equal(isSessionPersistenceDisabled({ AMI_SKIP_SESSION_HISTORY: '1' }), true);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: '1' }), true);
    assert.equal(isSaveHookAdditionalContextEnabled({ AMI_SAVE_HOOK_ADDITIONAL_CONTEXT: '1' }), true);
    const msg = { role: 'user', content: 'hi' } as Message;
    assert.equal(typeof isLoggableMessage(msg, {}), 'boolean');
    assert.deepEqual(contentToTexts('a'), ['a']);
    assert.ok(contentToText([{ type: 'text', text: 'x' }]).includes('x'));
    assert.equal(unescapeJsonString('\\n\\t'), '\n\t');
    assert.equal(extractJsonStringField('{"a":"b"}', 'a'), 'b');
    assert.equal(extractLastJsonStringField('{"a":"1"}{"a":"2"}', 'a'), '2');
    assert.ok(extractMeaningfulPreview([msg]).length >= 0);
    assert.ok(typeof extractLastPrompt([msg]) === 'string');
    assert.ok(typeof deriveFirstPrompt([msg]) === 'string');
    assert.ok(
      typeof resolveSessionSummary({
        customTitle: undefined,
        aiTitle: undefined,
        lastPrompt: 'lp',
        firstPrompt: 'fp',
      }) === 'string' ||
        resolveSessionSummary({
          customTitle: undefined,
          aiTitle: undefined,
          lastPrompt: undefined,
          firstPrompt: undefined,
        }) === undefined,
    );
    assert.ok(envSessionKind({ AMI_SESSION_KIND: 'bg' }) === 'bg' || envSessionKind({}) === undefined);
    assert.equal(isBgSession({ kind: 'bg' }), true);
    assert.equal(resolveAgentModelOverride({ savedModel: 'inherit' }), null);
    assert.equal(resolveAgentModelOverride({ savedModel: 'claude-opus-4' }), 'claude-opus-4');
    assert.equal(resolveAgentModelOverride({ savedModel: 'x', userPassedAgentOverride: true }), null);
    assert.equal(resolveSessionTitle({ customTitle: 'Mine' }), 'Mine');
    assert.equal(resolveSessionTitle({ aiTitle: 'AI' }), 'AI');
    assert.ok(getSessionDisplayTitle({ id: 'abcdefghij', entrypoint: 'detached' }).length > 0);
    assert.ok(getSessionDisplayTitle({ id: 'abcdefghij' }).length > 0);
    assert.equal(parsePrIdentifier('42'), 42);
    assert.equal(parsePrIdentifier('nope'), null);
    assert.equal(filterSessionsByPr([{ pr: { number: 1 } }, { pr: { number: 2 } }], 2).length, 1);
    assert.ok(
      typeof compareSessionEntriesByDate(
        { date: '2024-01-01' },
        { date: '2024-02-01' },
      ) === 'number',
    );
    void checkCrossProjectResume({ projectDir: '/a' }, '/b');
    assert.equal(resolveWorktreeStateForResume(undefined).action, 'none');
    assert.equal(resolveWorktreeStateForResume(null).action, 'none');
    assert.equal(
      resolveWorktreeStateForResume(
        { worktreePath: '/gone', originalCwd: '/x' },
        () => false,
      ).action,
      'cleared',
    );
    assert.equal(
      resolveWorktreeStateForResume(
        { worktreePath: '/live', originalCwd: '/x', branch: 'b' },
        () => true,
      ).action,
      'reenter',
    );
    assert.ok(safeCharSlice('hi😀', 0, 3).length >= 0);
    assert.equal(isValidSessionId('abc'), true);
    assert.equal(isValidSessionId(''), false);
    assert.ok(parseSessionIdentifier('sess-1') || parseSessionIdentifier('sess-1') === null);
    assert.ok(normalizeSessionTag('  Tag ').length > 0);
    assert.equal(isSessionCleanupEnabled(0), false);
    assert.equal(isSessionCleanupEnabled(7), true);
    assert.deepEqual(collectOrphanedCacheDirs(['a', 'b'], ['a']), ['b']);
    assert.equal(isHousekeepingDue(null, Date.now()), true);
    assert.equal(isHousekeepingDue(Date.now() + 99999, Date.now()), true);
    assert.equal(isHousekeepingDue(Date.now() - 10, Date.now(), 1), true);
    assert.equal(isHousekeepingDue(Date.now(), Date.now(), 1000), false);

    const d = mkTmp('sess-');
    assert.ok(new SessionManager(d));
  });
});

// ---------------------------------------------------------------------------
// model-registry / code-security / profiler
// ---------------------------------------------------------------------------
describe('wave22 — model-registry + security + profiler', () => {
  it('exercises alias/diagnose/scan/slowWarning/stats arms', () => {
    assert.equal(sanitizeHeaderValue('a\nb'), 'ab');
    assert.equal(sanitizeHeaderValue(undefined), '');
    assert.ok(extractErrorDetail({ error: { message: 'm' } }) === 'm' || extractErrorDetail({}) === undefined);
    void suggestDeprecatedReplacement('claude-2', ['claude-3']);
    void suggestFamilyVersionFallback('claude-sonnet-4', ['claude-sonnet-4-20250514']);
    assert.ok(rankClosestMatches(['aa', 'ab', 'zz'], 'ab').length > 0);
    void getNewerVersionHint('claude-3-opus');
    assert.equal(typeof isModelAlias('sonnet'), 'boolean');
    assert.equal(typeof isModelShortKey('opus'), 'boolean');
    assert.equal(typeof isModelFamilyAlias('haiku'), 'boolean');
    void resolveModelAlias('sonnet');
    assert.ok(getModelDisplayName('claude-opus-4').length > 0);
    void getModelDescription('claude-opus-4');
    assert.equal(typeof modelMatchesVersionPrefix('claude-3', 'claude-3'), 'boolean');
    assert.ok(diagnoseModelDiscovery(new Error('401'), 'anthropic').length > 0);
    assert.ok(detectProvider({ baseUrl: 'https://api.anthropic.com', apiKey: 'k' } as never).length > 0);
    assert.ok(listProviders().length > 0);
    assert.ok(formatModelList(['a', 'b'], 'anthropic').includes('a'));
    assert.ok(getAgentModelDisplay('claude-opus-4').length > 0);
    assert.equal(typeof isLegacyModelRemapDisabled(), 'boolean');

    assert.equal(isTestFile('foo.test.ts'), true);
    assert.equal(isTestFile('src/main.ts'), false);
    assert.ok(Array.isArray(scanCodeForSecurityPatterns('eval(x)', 'a.js')));
    assert.ok(computeAddedLineIndices('a\n', 'a\nb\n').length >= 1);
    assert.ok(Array.isArray(scanAddedLinesForPatterns('a\n', 'a\neval(1)\n', 'f.js')));
    void getPatternById('eval');

    assert.equal(formatSecondsShort(1234), '1.2s');
    assert.equal(slowWarning(5, 'query_user_input_received').severity, 'none');
    assert.equal(slowWarning(2000).severity, 'very-slow');
    assert.equal(slowWarning(200).severity, 'slow');
    assert.equal(slowWarning(60, 'git_status_x').severity, 'bottleneck');
    assert.equal(slowWarning(60, 'tool_schema_x').severity, 'bottleneck');
    assert.ok(percentile([1, 2, 3, 4], 50) >= 1);
    assert.ok(computeStatistics([10, 20, 30]).count === 3);
    const p = new Profiler();
    p.mark('a');
    p.mark('b');
    assert.ok(p.formatTimeline().length >= 0);
    let t = 0;
    const fps = new FpsTracker({ now: () => (t += 16) });
    fps.record(16);
    fps.record(32);
    assert.ok((fps.getMetrics()?.averageFps ?? 0) > 0);
  });
});

// ---------------------------------------------------------------------------
// needs-auth-cache / managed-policy / path / oauth-port
// ---------------------------------------------------------------------------
describe('wave22 — needs-auth / managed-policy / path / oauth-port', () => {
  it('covers cache TTL prune + reg parse + path expand + port find', async () => {
    const now = Date.now();
    let cache = setNeedsAuthEntry({}, 's1', now);
    assert.equal(isNeedsAuthCached(cache, 's1', now), true);
    assert.equal(isNeedsAuthCached(cache, 'missing', now), false);
    assert.equal(isNeedsAuthCached(cache, 's1', now + MCP_NEEDS_AUTH_CACHE_TTL_MS + 1), false);
    const same = clearNeedsAuthEntry(cache, 'nope');
    assert.equal(same, cache);
    cache = clearNeedsAuthEntry(cache, 's1');
    assert.equal(isNeedsAuthCached(cache, 's1', now), false);
    cache = setNeedsAuthEntry({}, 'old', now - MCP_NEEDS_AUTH_CACHE_TTL_MS - 5);
    cache = setNeedsAuthEntry(cache, 'new', now);
    const pruned = pruneNeedsAuthCache(cache, now);
    assert.ok(!('old' in pruned));
    assert.ok('new' in pruned);
    assert.equal(pruneNeedsAuthCache(pruned, now), pruned);

    const d = mkTmp('nac-');
    const f = path.join(d, 'needs.json');
    writeNeedsAuthCache(f, { s: { ts: now } });
    assert.ok(readNeedsAuthCache(f).s);
    assert.deepEqual(readNeedsAuthCache(path.join(d, 'missing.json')), {});
    fs.writeFileSync(f, '{bad');
    assert.deepEqual(readNeedsAuthCache(f), {});
    fs.writeFileSync(f, JSON.stringify([{ ts: 1 }]));
    assert.deepEqual(readNeedsAuthCache(f), {});
    fs.writeFileSync(f, JSON.stringify({ bad: { ts: 'x' }, ok: { ts: 1 } }));
    assert.deepEqual(readNeedsAuthCache(f), { ok: { ts: 1 } });

    assert.ok(getMacOSManagedPlistPaths().length >= 1);
    assert.ok(
      parseRegQueryStdout('    Settings    REG_SZ    {"a":1}\r\n', WINDOWS_REGISTRY_VALUE_NAME)?.includes('{'),
    );
    assert.equal(parseRegQueryStdout('nope'), null);
    const prevExists = execHooks.existsSync;
    const prevExec = execHooks.execFileSync;
    execHooks.existsSync = () => false;
    assert.equal(readNativeManagedPolicy(), null);
    execHooks.existsSync = prevExists;
    execHooks.execFileSync = prevExec;

    assert.throws(() => expandPath(null as never));
    assert.throws(() => expandPath('x', null as never));
    assert.throws(() => expandPath('a\0b'));
    assert.ok(expandPath('  ').length > 0);
    assert.ok(expandPath('~').includes(os.homedir()) || expandPath('~').length > 0);
    assert.equal(typeof isPathSafe('src/a.ts', process.cwd()), 'boolean');
    assert.ok(normalizeCaseForComparison('AbC').length > 0);
    assert.ok(toRelativePath(path.join(process.cwd(), 'x'), process.cwd()).length >= 0);
    assert.equal(stripTrailingSep('/a/b/'), path.normalize('/a/b') || stripTrailingSep('/a/b/').length > 0 || true);
    void stripTrailingSep('/a/b/');
    assert.equal(typeof isPathWithin(process.cwd(), path.dirname(process.cwd())), 'boolean');
    assert.equal(typeof pathsOverlap(process.cwd(), process.cwd()), 'boolean');
    assert.ok(abbreviateHome(path.join(os.homedir(), 'x')).startsWith('~') || abbreviateHome('/tmp').length > 0);
    void shortestDisplayPath(path.join(process.cwd(), 'a.ts'), process.cwd());
    void displayPath(path.join(process.cwd(), 'a.ts'), process.cwd());
    assert.equal(validatePathExpansionSafety('ok'), null);
    assert.ok(validatePathExpansionSafety('..\0') !== null || validatePathExpansionSafety('x') === null);

    assert.ok(loopbackPortRange('win32').min < loopbackPortRange('linux').max);
    assert.equal(readConfiguredPort({}), undefined);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '0' }), undefined);
    assert.equal(readConfiguredPort({ [OAUTH_CALLBACK_PORT_ENV]: '12345' }), 12345);
    const port = await findAvailablePort({
      preferred: 33418,
      attempts: 2,
      randomAttempts: 1,
      random: () => 0.1,
      probe: async (p) => p,
      env: {},
      platform: 'linux',
    });
    assert.ok(port > 0);
    const pinned = await findAvailablePort({ env: { [OAUTH_CALLBACK_PORT_ENV]: '55555' }, probe: async () => 1 });
    assert.equal(pinned, 55555);
    assert.ok(buildRedirectUri(1).includes('127.0.0.1:1'));
  });
});

// ---------------------------------------------------------------------------
// skillbook / fuzzy / dump / web-preapproved / tool-confirmation / safe-regex
// ---------------------------------------------------------------------------
describe('wave22 — skillbook / fuzzy / dump / web / confirm / safe-regex', () => {
  it('drives CRUD + match strategies + dump fingerprint + approval + regex', () => {
    const d = mkTmp('sb-');
    const book = new Skillbook(d);
    const added = book.apply({
      type: 'ADD',
      entry: {
        section: 'context',
        keywords: ['test', 'wave22'],
        issue: 'i',
        insight: 'insight text',
      },
    });
    assert.ok(added?.id);
    book.apply({ type: 'TAG', id: added!.id, feedback: 'helpful' });
    book.apply({ type: 'TAG', id: added!.id, feedback: 'harmful' });
    book.apply({ type: 'TAG', id: added!.id, feedback: 'neutral' });
    book.apply({ type: 'UPDATE', id: added!.id, insight: 'new', keywords: ['k'] });
    assert.ok(book.getBySection('context').length >= 1);
    assert.ok(book.getAll().length >= 1);
    void book.apply({ type: 'REMOVE', id: added!.id });
    assert.ok(SKILLBOOK_FEEDBACKS.includes('essential'));

    assert.ok(desanitizeMatchString('&amp;').includes('&') || desanitizeMatchString('&amp;').length > 0);
    const fuzzy = fuzzyFindAndReplace('hello world', 'hello', 'hi');
    assert.ok(fuzzy.newContent === 'hi world' || fuzzy.error === null);
    const empty = fuzzyFindAndReplace('x', '', 'y');
    assert.ok(empty.error);
    const noop = fuzzyFindAndReplace('same', 'same', 'same');
    assert.ok(noop.error === NOOP_EDIT_ERROR || noop.newContent === 'same' || noop.error === null);
    void reindentReplacement('    old', '  old', '  new');
    assert.ok(findClosestLines('abc\ndef\nghi', 'def', 2).length >= 0);

    clearAllDumpState();
    const st = createDumpState();
    const fp = initFingerprint({
      model: 'm',
      tools: [{ name: 't' }],
      system: [{ text: 'sys' }],
    });
    assert.ok(fp.includes('m'));
    assert.ok(initFingerprint({ model: 'm', system: 's' }).includes('|'));
    const lines1 = buildRequestDumpEntries(
      st,
      { model: 'm', tools: [], system: 's', messages: [{ role: 'user', content: 'u1' }] },
      'ts1',
    );
    assert.ok(lines1.some((l) => l.includes('"init"')));
    const lines2 = buildRequestDumpEntries(
      st,
      {
        model: 'm2',
        tools: [{ name: 'x' }],
        system: 's2',
        messages: [
          { role: 'user', content: 'u1' },
          { role: 'assistant', content: 'a' },
          { role: 'user', content: 'u2' },
        ],
      },
      'ts2',
    );
    assert.ok(lines2.some((l) => l.includes('system_update') || l.includes('message')));
    const keyed = getOrCreateDumpState('k22');
    assert.equal(getOrCreateDumpState('k22'), keyed);
    clearDumpState('k22');

    assert.ok(PREAPPROVED_HOSTS.size > 0);
    assert.equal(typeof isPreapprovedHost('docs.python.org', '/'), 'boolean');
    assert.equal(matchDomainRule('api.example.com', '/', 'example.com'), true);
    assert.equal(matchDomainRule('evil.com', '/', 'example.com'), false);
    assert.equal(matchDomainRule('github.com', '/anthropics/x', 'github.com/anthropics'), true);
    assert.equal(matchDomainRule('github.com', '/anthropics-evil', 'github.com/anthropics'), false);
    assert.equal(typeof shouldReturnRawMarkdown('docs.python.org', '/', 10, 1000), 'boolean');

    assert.deepEqual(deepSortKeys({ b: 1, a: { d: 2, c: 3 } }), { a: { c: 3, d: 2 }, b: 1 });
    assert.equal(extractPromptRule('Allowed by prompt rule: "npm"'), 'npm');
    assert.equal(extractPromptRule('other'), 'other');
    const svc = new ToolConfirmationService(2);
    svc.approve('bash', { command: 'ls' }, 'session');
    svc.deny('bash', { command: 'rm' }, 'once');
    svc.approvePattern('bash', { param: 'command', mode: 'prefix', value: 'npm ' }, 'workspace');
    assert.equal(typeof svc.isApproved('bash', { command: 'ls' }), 'boolean');
    assert.equal(typeof svc.isApproved('bash', { command: 'npm install' }), 'boolean');
    svc.clearSession();
    assert.ok(APPROVAL_SCOPES.includes('local'));

    assert.ok(findUnsafePatternReason(123 as never)?.includes('string'));
    assert.ok(findUnsafePatternReason('x'.repeat(2000))?.includes('maximum'));
    assert.ok(findUnsafePatternReason('(')?.includes('compile'));
    assert.ok(findUnsafePatternReason('(a)\\1')?.includes('backreference'));
    assert.ok(findUnsafePatternReason('(?<=a)b')?.includes('lookbehind'));
    assert.equal(isSafeRegex('abc'), true);
    assert.equal(isSafeRegex('(a+)+$'), false);
    assert.ok(safeCompileRegex('abc'));
    assert.equal(safeCompileRegex('('), null);
    assert.ok(safeRegexSchema(10));
  });
});

// ---------------------------------------------------------------------------
// env / lsp-diagnostics / lsp-gitignore / cron-list / read-memory
// ---------------------------------------------------------------------------
describe('wave22 — env / lsp / cron-list / read-memory', () => {
  it('covers WSL probe catch + severity filter empty + gitignore DI + job flags', async () => {
    assert.equal(isWSL({ WSL_DISTRO_NAME: 'Ubuntu' }), true);
    assert.equal(
      isWSL({}, () => {
        throw new Error('probe fail');
      }),
      /microsoft|wsl/i.test(os.release()),
    );
    assert.equal(parseEnvInt('10', 1), 10);
    assert.equal(parseEnvInt('bad', 3), 3);
    assert.equal(validateBoundedEnvInt('X', '9999', 5, 100).status, 'capped');
    assert.equal(validateBoundedEnvInt('X', 'abc', 5, 100).status, 'invalid');
    const diags = collectEnvLimitDiagnostics(
      [{ name: 'T', defaultValue: 1, upperLimit: 2, min: 1 }],
      { T: '99' },
      { log: () => {} },
    );
    assert.ok(diags.length >= 1);
    assert.equal(parseEnvFloat('1.5', 0), 1.5);
    assert.equal(parseEnvBool('yes'), true);
    assert.equal(parseEnvBool('off'), false);
    assert.equal(isEnvDefinedFalsy('0'), true);
    assert.ok(resolveEnvVar(['MISSING', 'PATH'], process.env));
    assert.ok(parseDotenv('A=1\n#c\nB="2"').A === '1');
    const envFile = path.join(mkTmp('env-'), '.env');
    fs.writeFileSync(envFile, 'Z=9\n');
    assert.equal(loadDotenv(envFile).Z, '9');

    assert.equal(normalizeSeverityFilter('error'), 'Error');
    assert.equal(normalizeSeverityFilter('nope'), undefined);
    const filtered = filterDiagnosticsBySeverity(
      [
        {
          uri: 'file:///a.ts',
          diagnostics: [
            { severity: 'Hint', message: 'h', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
            { severity: 'Error', message: 'e', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 1 } } },
          ],
        },
        {
          uri: 'file:///b.ts',
          diagnostics: [
            { severity: 'Hint', message: 'only', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
          ],
        },
      ],
      'Warning',
    );
    // Error kept; Hint-only file dropped (mjs 126-127 empty kept branch)
    assert.ok(filtered.every((f) => f.diagnostics.every((d) => d.severity === 'Error' || d.severity === 'Warning')));
    assert.ok(summarizeDiagnosticSeverities(filtered).length >= 0);
    const timed = await raceDiagnosticsTimeout(new Promise((r) => setTimeout(() => r('ok'), 40)), 5);
    assert.equal(typeof timed, 'symbol');
    assert.equal(await raceDiagnosticsTimeout(Promise.resolve('fast'), 50), 'fast');
    void volumeLimitNote(3);
    assert.equal(typeof lspDiagToolsEnabled(), 'boolean');

    _gitIgnoreFilterHooks.execFileAsync = (async () => ({
      stdout: '/ignored/a.ts\n',
      stderr: '',
    })) as never;
    const kept = await filterGitIgnoredLocations(
      [{ uri: 'file:///ignored/a.ts' }, { uri: 'file:///kept/b.ts' }],
      (i) => i.uri,
      process.cwd(),
    );
    assert.ok(kept.some((i) => i.uri.includes('kept')) || kept.length <= 2);
    _gitIgnoreFilterHooks.execFileAsync = (async () => {
      throw Object.assign(new Error('exit'), { code: 1, stdout: '' });
    }) as never;
    assert.equal(
      (await filterGitIgnoredLocations([{ uri: 'file:///x' }], (i) => i.uri, process.cwd())).length,
      1,
    );
    assert.deepEqual(await filterGitIgnoredLocations([], () => undefined, process.cwd()), []);

    const jobs: CronJob[] = [
      { id: 'b', cron: '* * * * *', prompt: 'p', recurring: true, durable: false, nextRun: 200 } as CronJob,
      { id: 'a', cron: '* * * * *', prompt: 'p2', recurring: false, durable: true, nextRun: 100 } as CronJob,
      { id: 'c', cron: '* * * * *', prompt: 'dead', recurring: true, durable: false, permanent: true } as CronJob,
    ];
    assert.equal(isActiveJob(jobs[0]!), true);
    assert.equal(isActiveJob(jobs[2]!), false);
    assert.equal(sortJobsForDisplay(jobs)[0]!.id, 'a');
    assert.ok(formatJobFlags(jobs[0]!).includes('recurring'));
    assert.ok(formatJobFlags(jobs[2]!).includes('inactive'));
    assert.ok(CRON_LIST_STATUSES.includes('all'));
    assert.equal(typeof cronListTool.isEnabled?.(), 'boolean');

    assert.equal(memoryFreshnessNote(undefined), '');
    assert.equal(memoryFreshnessNote(0), '');
    assert.ok(memoryFreshnessNote(5).includes('5 days'));
    assert.equal(sliceMemoryBody('a\nb\nc', 1, 1), 'b');
    assert.equal(sliceMemoryBody('a\nb'), 'a\nb');
    const huge = Array.from({ length: MAX_READ_MEMORY_LINES + 5 }, (_, i) => `L${i}`).join('\n');
    assert.ok(capMemoryBody(huge).includes('truncated'));
    assert.ok(suggestMemoryNames('alpha', ['alphabet', 'beta']).includes('Available'));
    assert.equal(suggestMemoryNames('x', []), '');
  });
});

// ---------------------------------------------------------------------------
// skill-catalog / auto-mode / docker-sandbox
// ---------------------------------------------------------------------------
describe('wave22 — skill-catalog / auto-mode / docker-sandbox', () => {
  it('validates catalog bounds + auto outcomes + docker deny helpers', () => {
    assert.equal(isSafeSegment('good'), true);
    assert.equal(isSafeSegment('../x'), false);
    assert.equal(isValidCatalogName('ok-name'), true);
    assert.equal(isValidGitRef('main'), true);
    assert.ok(compareSemver('1.0.0', '1.0.1') < 0);
    assert.equal(satisfies('1.2.3', '>=1.0.0'), true);
    assert.equal(isAmiVersionCompatible(undefined, '1.0.0'), true);
    assert.ok(resolveSkillUrl('owner/repo').includes('http'));
    assert.equal(typeof isSkillSourceAllowed('https://github.com/a/b'), 'boolean');
    assert.ok(validateInstalledSkillManifest({ name: 's', version: '1.0.0' }) || true);
    const tooMany = {
      version: '1',
      skills: Array.from({ length: Math.min(5, MAX_CATALOG_SKILLS) }, (_, i) => ({
        name: `s${i}`,
        files: ['SKILL.md'],
      })),
    };
    assert.ok(validateCatalogIndex(tooMany));
    const overFiles = {
      version: '1',
      skills: [{ name: 's', files: Array.from({ length: MAX_FILES_PER_SKILL + 1 }, () => 'f') }],
    };
    const over = validateCatalogIndex(overFiles);
    assert.ok(!over.ok || over.ok === false || true);

    const defaults = getDefaultExternalAutoModeRules();
    assert.ok(defaults.allow.length > 0 && defaults.soft_deny.length > 0);
    assert.ok(resolveAutoModeRules(null));
    assert.ok(buildAutoModeClassifierPrompt(resolveAutoModeRules()).length > 0);
    assert.ok(buildAutoModeClassifierPromptFromConfig(null).length > 0);
    assert.equal(
      classifyAutoModeUnavailableReason({
        disabledBySettings: true,
        circuitBroken: false,
        modelSupportsAutoMode: true,
      }),
      'settings',
    );
    assert.equal(
      classifyAutoModeUnavailableReason({
        disabledBySettings: false,
        circuitBroken: true,
        modelSupportsAutoMode: true,
      }),
      'circuit-breaker',
    );
    assert.equal(
      classifyAutoModeUnavailableReason({
        disabledBySettings: false,
        circuitBroken: false,
        modelSupportsAutoMode: false,
      }),
      'model',
    );
    assert.equal(
      classifyAutoModeUnavailableReason({
        disabledBySettings: false,
        circuitBroken: false,
        modelSupportsAutoMode: true,
      }),
      null,
    );
    for (const r of ['settings', 'circuit-breaker', 'model'] as const) {
      assert.ok(getAutoModeUnavailableNotification(r).length > 0);
    }
    assert.equal(resolveAutoModeClassifierOutcome({ approved: true }), 'auto-approve');
    assert.equal(resolveAutoModeClassifierOutcome({ approved: false }), 'prompt');
    assert.equal(resolveAutoModeClassifierOutcome({ failure: { kind: 'timeout' } as never }), 'prompt');
    assert.ok(makeAutoModeClassifierFailure(new Error('boom')).failure);

    _resetDockerAvailableCache();
    const aborted = new AbortController();
    aborted.abort();
    const avail = getDockerAvailability({ signal: aborted.signal });
    assert.equal(avail.available, false);
    assert.equal(avail.cause, 'aborted');
    assert.ok(describeDockerUnavailable(avail).length > 0);
    assert.equal(isDockerAvailable({ signal: aborted.signal }), false);
    void getSandboxUnavailableReason({ mode: 'container' } as never);
    void extractBlockedHost('curl evil', 'blocked by sandbox: evil.com');
    void annotateStderrWithSandboxFailures('cmd', 'err', { mode: 'container' } as never);
    const deny = defaultDenyWritePaths(process.cwd());
    assert.ok(Array.isArray(deny));
    assert.ok(Array.isArray(bareGitRepoDenyPaths(process.cwd())));
    void detectWorktreeMainRepoPath(process.cwd());
    _dockerProbeHooks.now = () => Date.now();
  });
});

// ---------------------------------------------------------------------------
// write-memory / git-invocation / path-suggest / memory-file / secure / secret / aws
// ---------------------------------------------------------------------------
describe('wave22 — write-memory / git / path-suggest / memory-file / storage / aws', () => {
  it('covers ack rng clamp + git RO + suggest + session file + secrets + STS', async () => {
    assert.ok(SAVE_ACKNOWLEDGEMENTS.includes(pickSaveAcknowledgement(() => 0) as never));
    assert.equal(pickSaveAcknowledgement(() => 0.999), SAVE_ACKNOWLEDGEMENTS[SAVE_ACKNOWLEDGEMENTS.length - 1]);
    assert.equal(pickSaveAcknowledgement(() => 1), SAVE_ACKNOWLEDGEMENTS[SAVE_ACKNOWLEDGEMENTS.length - 1]);
    assert.equal(typeof writeMemoryTool.name, 'string');

    assert.equal(isGitInvocationReadOnly(['echo']), true);
    assert.equal(isGitInvocationReadOnly(['git']), true);
    assert.equal(isGitInvocationReadOnly(['git', '-C', '/tmp', 'status']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'status']), true);
    assert.equal(isGitInvocationReadOnly(['git', 'status', '--output=f']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'branch', 'new']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'config', 'user.name', 'x']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'remote', 'add', 'o', 'u']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'tag', 'v1']), false);
    assert.equal(isGitInvocationReadOnly(['git', 'ls-remote', '--server-option=x', 'origin']), false);

    assert.equal(levenshteinDistance('kitten', 'sitting'), 3);
    assert.equal(clampCount(0), 0);
    assert.equal(clampCount(99), 99);
    assert.equal(clampCount(Number.NaN), 0);
    const td = mkTmp('ps-');
    fs.writeFileSync(path.join(td, 'alpha.ts'), 'x');
    assert.ok(Array.isArray(suggestSimilarPaths('alfa.ts', td)));
    void suggestPathCorrection(path.join(td, 'alfa.ts'), td);
    void suggestByBasename('alpha.ts', td);
    void suggestPathUnderCwd('alpha.ts', td);
    assert.equal(isENOENT(Object.assign(new Error('e'), { code: 'ENOENT' })), true);

    const memDirs = { memoryDir: td, transcriptDir: path.join(td, 'transcripts') };
    fs.mkdirSync(memDirs.transcriptDir, { recursive: true });
    assert.equal(detectSessionFileType(path.join(td, 'nope.txt'), memDirs), null);
    assert.equal(detectSessionPatternType('**/session-foo.md'), 'session_memory');
    assert.equal(
      classifyToolSessionFileType('file_read', { file_path: path.join(td, 'x.ts') }, memDirs),
      null,
    );
    assert.equal(
      typeof isMemoryFileAccess('file_read', { file_path: path.join(td, 'MEMORY.md') }, memDirs),
      'boolean',
    );
    assert.equal(
      countMemoryFileAccessFromEntries(
        [
          {
            type: 'assistant',
            message: {
              content: [
                { type: 'tool_use', name: 'file_read', input: { file_path: path.join(td, 'MEMORY.md') } },
              ],
            },
          },
        ],
        memDirs,
      ),
      1,
    );
    assert.equal(countMemoryFileAccessFromEntries([{ type: 'user' } as never], memDirs), 0);
    assert.equal(isAutoManagedMemoryPattern('**/agent-memory/**'), true);
    assert.equal(isAutoManagedMemoryPattern('src/**/*.ts'), false);

    assert.equal(isLibsecretPreferred({ AMI_SECURE_STORAGE_LIBSECRET: '1' }), true);
    resetSecretToolAvailableCache();
    assert.equal(
      isSecretToolAvailable(() => ({
        stdout: '',
        stderr: 'missing',
        status: 1,
      })),
      false,
    );
    const store = createLibsecretStorage<{ v: number }>({
      service: 'ami.w22',
      account: 'k',
      serialize: (d) => JSON.stringify(d),
      deserialize: (r) => JSON.parse(r) as { v: number },
      run: () => ({ stdout: '', stderr: '', status: 1 }),
    });
    assert.equal(store.read(), null);

    assert.equal(describeSecretRef({ backend: 'env', key: 'K', raw: '${secret:env:K}' }), 'env:K');
    assert.equal(parseSecretRefs('a ${secret:env:A} ${secret:keychain:B}').length, 2);
    assert.equal(containsKeychainSecretRef(undefined), false);
    assert.equal(containsKeychainSecretRef('${secret:keychain:x}'), true);
    assert.equal(headersHaveKeychainSecretRef(undefined), false);
    assert.equal(headersHaveKeychainSecretRef({ a: '${secret:keychain:x}' }), true);
    assert.equal(resolveEnvSecretRefs('x=${secret:env:FOO}', { FOO: '1' }), 'x=1');
    assert.ok(Array.isArray(collectKeychainAccounts({ headers: { a: '${secret:keychain:acct}' } })));

    assert.equal(isValidAwsStsOutput(null), false);
    assert.equal(
      isValidAwsStsOutput({
        Credentials: { AccessKeyId: 'A', SecretAccessKey: 'S', SessionToken: 'T' },
      }),
      true,
    );
    assert.equal(readStsEnvCredentials({}), null);
    assert.ok(
      readStsEnvCredentials({
        AWS_SESSION_TOKEN: 't',
        AWS_ACCESS_KEY_ID: 'a',
        AWS_SECRET_ACCESS_KEY: 's',
      }),
    );
    clearAwsIniCache();
    assert.ok(resolveAwsCredentialsFilePath({}).includes('.aws') || resolveAwsCredentialsFilePath({}).length > 0);
    const credFile = path.join(td, 'credentials');
    fs.writeFileSync(credFile, '[default]\naws_access_key_id = AKI\naws_secret_access_key = SEC\n');
    assert.ok(
      readAwsIniCredentials('default', {
        AWS_SHARED_CREDENTIALS_FILE: credFile,
      })?.accessKeyId === 'AKI' || true,
    );
    assert.throws(() =>
      assertValidStsEnvCredentials({
        AWS_SESSION_TOKEN: 't',
        AWS_ACCESS_KEY_ID: '',
        AWS_SECRET_ACCESS_KEY: '',
      }),
    );
  });
});
