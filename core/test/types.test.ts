import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildSubsystems, EXIT_REASONS, STREAM_CHUNK_TYPES, OUTPUT_STYLES, ENGINE_EVENT_TYPES, OUTPUT_FORMATS, INPUT_FORMATS, CHAT_MODES, THEMES, NOTIFICATION_CHANNELS, asSessionId, asAgentId, toAgentId } from '../src/types';
import type {
  EngineConfig, SessionId, AgentId, NotificationChannel, ToolDefinition, ToolValidationResult,
  ToolAccessClassification, AutoModeConfig, UserMessage, AssistantMessage, SystemMessage,
  MessageOrigin, ToolResult, DiffHunk, EngineSubmitOptions, UserQuestionOption,
} from '../src/types';

function cfg(overrides: Partial<EngineConfig> = {}): EngineConfig {
  return {
    provider: { apiKey: 'k', baseUrl: 'http://localhost', model: 'm' },
    cwd: '/tmp',
    ...overrides,
  };
}

describe('buildSubsystems', () => {
  it('maps maxTurns 0 to the finite default instead of Infinity', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: 0 }));
    assert.equal(session.maxTurns, 100);
    assert.ok(Number.isFinite(session.maxTurns));
  });

  it('maps non-finite maxTurns to the finite default', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: Number.POSITIVE_INFINITY }));
    assert.equal(session.maxTurns, 100);
  });

  it('preserves a positive maxTurns', () => {
    const { session } = buildSubsystems(cfg({ maxTurns: 42 }));
    assert.equal(session.maxTurns, 42);
  });

  it('passes architectModel through to provider subsystem', () => {
    const { provider } = buildSubsystems(cfg({ architectModel: 'claude-opus-4' }));
    assert.equal(provider.architectModel, 'claude-opus-4');
  });

  it('leaves architectModel undefined when not provided', () => {
    const { provider } = buildSubsystems(cfg({}));
    assert.equal(provider.architectModel, undefined);
  });

  it('passes compactionModel through to provider subsystem', () => {
    const { provider } = buildSubsystems(cfg({ compactionModel: 'gpt-4o-mini' }));
    assert.equal(provider.compactionModel, 'gpt-4o-mini');
  });
});

describe('EXIT_REASONS', () => {
  it('contains all 15 exit reasons', () => {
    assert.equal(EXIT_REASONS.length, 15);
    for (const reason of ['completed', 'max_turns', 'error', 'abort', 'session_timeout',
      'max_steps', 'budget_exceeded', 'hook_denied', 'superinference_stop',
      'consecutive_errors', 'escalation_terminate', 'no_edit_stall',
      'keepalive_exhausted', 'context_too_small', 'usage_limit_watchdog'] as const) {
      assert.ok(EXIT_REASONS.includes(reason), `missing exit reason: ${reason}`);
    }
  });
});

describe('STREAM_CHUNK_TYPES', () => {
  it('contains all 5 stream chunk types', () => {
    assert.equal(STREAM_CHUNK_TYPES.length, 5);
    for (const type of ['content_delta', 'tool_call_delta', 'thinking_delta', 'done', 'error'] as const) {
      assert.ok(STREAM_CHUNK_TYPES.includes(type), `missing chunk type: ${type}`);
    }
  });
});

describe('OUTPUT_STYLES', () => {
  it('contains the 4 built-in output styles (incl. learning — audit #5)', () => {
    assert.equal(OUTPUT_STYLES.length, 4);
    assert.ok(OUTPUT_STYLES.includes('default'));
    assert.ok(OUTPUT_STYLES.includes('concise'));
    assert.ok(OUTPUT_STYLES.includes('explanatory'));
    assert.ok(OUTPUT_STYLES.includes('learning'));
  });
});

describe('runtime const arrays are non-empty', () => {
  it('ENGINE_EVENT_TYPES has 22+ events', () => {
    assert.ok(ENGINE_EVENT_TYPES.length >= 22);
  });
  it('OUTPUT_FORMATS has 3 values', () => {
    assert.equal(OUTPUT_FORMATS.length, 3);
  });
  it('INPUT_FORMATS has 2 values', () => {
    assert.equal(INPUT_FORMATS.length, 2);
  });
  it('CHAT_MODES has 3 values', () => {
    assert.equal(CHAT_MODES.length, 3);
  });
  it('THEMES has 3 values', () => {
    assert.equal(THEMES.length, 3);
  });
});

describe('type-branded SessionId', () => {
  it('asSessionId is runtime-identity (brand is compile-time only)', () => {
    assert.equal(asSessionId('session-x'), 'session-x');
    const sid: SessionId = asSessionId('s1');
    assert.equal(typeof sid, 'string');
    assert.equal(sid.toUpperCase(), 'S1'); // behaves as a real string
  });

  it('the brand is nominal (compile-time regression guard)', () => {
    const sid: SessionId = asSessionId('s');
    // @ts-expect-error a raw string must NOT be assignable to a SessionId (branding on).
    const bad: SessionId = 'raw';
    // A SessionId IS assignable to a plain string (subtype) — no error expected here.
    const ok: string = sid;
    assert.ok([bad, ok].every(v => typeof v === 'string'));
  });
});

describe('type-branded AgentId', () => {
  it('asAgentId is runtime-identity (unvalidated mint boundary)', () => {
    assert.equal(asAgentId('agent-1'), 'agent-1');
    const id: AgentId = asAgentId('a1');
    assert.equal(typeof id, 'string');
  });

  it('toAgentId validates the format: brands well-formed ids, null for malformed', () => {
    assert.equal(toAgentId('agent_7.worker-2'), 'agent_7.worker-2');
    assert.equal(toAgentId('A9'), 'A9');
    // Free-text teammate names with spaces / leading punctuation are NOT agent ids.
    assert.equal(toAgentId('my teammate'), null);
    assert.equal(toAgentId(''), null);
    assert.equal(toAgentId('-leading-dash'), null);
    assert.equal(toAgentId('has/slash'), null);
  });

  it('the AgentId brand is nominal (compile-time regression guard)', () => {
    const id: AgentId = asAgentId('a');
    // @ts-expect-error a raw string must NOT be assignable to an AgentId.
    const bad: AgentId = 'raw';
    const ok: string = id; // AgentId IS assignable to string (subtype).
    assert.ok([bad, ok].every(v => typeof v === 'string'));
  });
});

describe('NOTIFICATION_CHANNELS', () => {
  it('enumerates the 7 terminal notification channels (parity with Claude)', () => {
    assert.equal(NOTIFICATION_CHANNELS.length, 7);
    for (const ch of ['auto', 'iterm2', 'iterm2_with_bell', 'terminal_bell', 'kitty', 'ghostty', 'notifications_disabled'] as const) {
      assert.ok(NOTIFICATION_CHANNELS.includes(ch), `missing channel: ${ch}`);
    }
  });

  it('NotificationChannel is derived from the runtime array', () => {
    const ch: NotificationChannel = 'ghostty';
    assert.ok(NOTIFICATION_CHANNELS.includes(ch));
    // @ts-expect-error a non-member string is not a NotificationChannel.
    const bad: NotificationChannel = 'pushover';
    assert.equal(typeof bad, 'string');
  });
});

describe('ToolDefinition parity knobs (compile-time shape)', () => {
  it('accepts searchHint/shouldDefer/isDestructive/isEnabled/maxResultSizeChars/aliases/strict/outputSchema/interruptBehavior/validateInput/classification/prompt', async () => {
    const vr: ToolValidationResult = { result: false, message: 'bad path', errorCode: 42 };
    const classification: ToolAccessClassification = { isSearch: true, isRead: false, isList: false };
    const tool: ToolDefinition = {
      name: 'demo',
      description: 'd',
      inputSchema: { type: 'object', properties: {} },
      isReadOnly: false,
      searchHint: 'jupyter notebook cells',
      shouldDefer: true,
      isDestructive: (input) => input.action === 'remove',
      isEnabled: () => true,
      maxResultSizeChars: 100_000,
      aliases: ['KillShell'],
      strict: true,
      outputSchema: { type: 'object', properties: { ok: { type: 'boolean' } } },
      interruptBehavior: 'cancel',
      validateInput: async () => vr,
      getActivityDescription: (input) => `Reading ${String(input.path)}`,
      isSearchOrReadCommand: () => classification,
      toAutoClassifierInput: (input) => `ls ${String(input.path)}`,
      prompt: 'Use this tool proactively and often.',
      renderUseMessage: () => 'cleanup team: current',
      execute: async () => ({ output: '' }),
    };
    assert.equal(tool.isDestructive?.({ action: 'remove' }), true);
    assert.equal(tool.isDestructive?.({ action: 'keep' }), false);
    assert.equal(tool.isEnabled?.(), true);
    assert.equal(tool.getActivityDescription?.({ path: 'a.ts' }), 'Reading a.ts');
    assert.deepEqual(tool.isSearchOrReadCommand?.({}), classification);
    assert.deepEqual(await tool.validateInput?.({}, {} as never), vr);
  });
});

describe('EngineConfig parity knobs (compile-time shape)', () => {
  it('accepts the appended system-prompt + sandbox-adjacent + memory/mcp/ui config knobs', () => {
    const autoMode: AutoModeConfig = {
      allow: ['read files'], soft_deny: ['delete anything'], environment: ['CI runner'],
      model: 'cheap-model', twoStageClassifier: 'fast',
    };
    const c: EngineConfig = {
      provider: { apiKey: 'k', baseUrl: 'http://localhost', model: 'm' },
      cwd: '/tmp',
      appendSystemPrompt: 'extra policy',
      sessionPersistenceDisabled: true,
      editorMode: 'vim',
      verbose: true,
      advisorModel: 'reviewer',
      autoMode,
      autoUpdatesChannel: 'stable',
      customApiKeyResponses: { approved: ['sk-…abc'], rejected: [] },
      idleReturnPromptMinutes: 30,
      autoMemoryEnabled: false,
      sessionMemoryInterval: 3,
      idleMicrocompactEnabled: true,
      idleMicrocompactThresholdMinutes: 60,
      fileCheckpointingEnabled: false,
      strictMcpConfig: true,
      disabledMcpServers: ['flaky'],
      enabledMcpServers: ['builtin'],
      maxOutputTokens: 64_000,
      spinnerTipsEnabled: false,
      syntaxHighlightingDisabled: true,
      prefersReducedMotion: true,
      promptSuggestionEnabled: true,
      cleanupPeriodDays: 30,
      permissionExplainerEnabled: false,
      plansDirectory: 'docs/plans',
      heapDumpOnMemoryWarning: true,
      immediateInferenceConfig: true,
      teammateMode: 'in-process',
      subagentDefaultModel: 'fast-model',
      modelOverrides: { 'claude-opus-4-8': 'arn:aws:bedrock:…' },
      notificationChannel: 'kitty',
      lspRecommendationNeverPlugins: ['py-lsp'],
      lspRecommendationDisabled: true,
    };
    const opts: EngineSubmitOptions = { appendSystemPrompt: 'turn-local policy' };
    assert.equal(c.editorMode, 'vim');
    assert.equal(c.autoMode?.twoStageClassifier, 'fast');
    assert.equal(c.teammateMode, 'in-process');
    assert.equal(opts.appendSystemPrompt, 'turn-local policy');
  });
});

describe('Message provenance + transcript metadata (compile-time shape)', () => {
  it('UserMessage carries origin + permissionMode + transcript-tree fields', () => {
    const origin: MessageOrigin = { kind: 'channel', source: 'slack', user: 'u1', chatId: 'c9' };
    const m: UserMessage = {
      role: 'user', content: 'hi', origin, permissionMode: 'plan',
      uuid: 'm1', parentUuid: 'm0', isSidechain: true, agentId: 'a1',
      teamName: 'team-x', agentName: 'worker-2', promptId: 'p1', logicalParentUuid: 'mL',
    };
    assert.equal(m.origin?.kind, 'channel');
    assert.equal(m.permissionMode, 'plan');
    assert.equal(m.isSidechain, true);
  });

  it('AssistantMessage carries a per-turn model attribution', () => {
    const m: AssistantMessage = { role: 'assistant', content: 'x', model: 'claude-opus-4-8' };
    assert.equal(m.model, 'claude-opus-4-8');
  });

  it('SystemMessage carries a severity level', () => {
    const m: SystemMessage = { role: 'system', content: 'y', level: 'warning' };
    assert.equal(m.level, 'warning');
  });

  it('MessageOrigin discriminates each framing kind', () => {
    const kinds: MessageOrigin[] = [
      { kind: 'user' }, { kind: 'task-notification' }, { kind: 'coordinator' },
      { kind: 'teammate', name: 't' }, { kind: 'channel', source: 's' },
    ];
    assert.equal(kinds.length, 5);
  });
});

describe('ToolResult structured patch + partial-read tracking', () => {
  it('ToolResult carries an optional structured per-hunk diff', () => {
    const hunk: DiffHunk = { oldStart: 1, oldLines: 2, newStart: 1, newLines: 3, lines: ['-a', '+a', '+b'] };
    const r: ToolResult = { output: 'edited', structuredPatch: [hunk] };
    assert.equal(r.structuredPatch?.[0].newLines, 3);
  });
});

describe('UserQuestionOption parity fields', () => {
  it('carries per-option preview + disabled, and remains a {label,description} supertype-compatible shape', () => {
    const opts: UserQuestionOption[] = [
      { label: 'Keep', description: 'leave on disk', preview: '```\nkeep\n```' },
      { label: 'Remove', description: 'delete it', disabled: true },
      { label: 'Plain', description: 'no extras' },
    ];
    assert.equal(opts[0].preview, '```\nkeep\n```');
    assert.equal(opts[1].disabled, true);
    assert.equal(opts[2].preview, undefined);
    // A bare {label,description} literal is still a valid UserQuestionOption.
    const bare: UserQuestionOption = { label: 'x', description: 'y' };
    assert.equal(bare.disabled, undefined);
  });
});
