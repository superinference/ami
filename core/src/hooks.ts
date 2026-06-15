import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
import type { Message, AssistantMessage } from './types';
import { log as coreLog } from './logger';
import { EventEmitter } from 'events';

export type HookFn = (context: HookContext) => Promise<void>;

export interface HookContext {
  messages: Message[];
  lastAssistantMessage?: AssistantMessage;
  toolResults?: Array<{ toolName: string; output: string; isError: boolean }>;
  turnCount: number;
}

// ---------------------------------------------------------------------------
// Extended hook context types
// ---------------------------------------------------------------------------

export interface PreToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
}

export interface PostToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  toolOutput: string;
  isError: boolean;
}

export interface PreSamplingContext extends HookContext {
  apiMessages: Message[];
}

export type HookDecision =
  | { action: 'allow'; additionalContext?: string }
  | { action: 'deny'; reason: string; additionalContext?: string }
  | { action: 'modify'; updatedInput?: Record<string, unknown>; updatedMessages?: Message[]; additionalContext?: string };

// ---------------------------------------------------------------------------
// Hook function types for new hooks
// ---------------------------------------------------------------------------

export type PreToolUseHookFn = (context: PreToolUseContext) => Promise<HookDecision>;
export type PostToolUseHookFn = (context: PostToolUseContext) => Promise<{ additionalContext?: string } | void>;
export type PreSamplingHookFn = (context: PreSamplingContext) => Promise<HookDecision>;

export interface SessionContext {
  sessionId: string;
  cwd: string;
}

export interface SubagentContext {
  parentSessionId: string;
  subagentSessionId: string;
  prompt: string;
  mode?: string;
}

export interface CompactContext {
  messageCount: number;
  tokenEstimate: number;
}

export interface PermissionContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  command?: string;
  reason?: string;
  retry?: boolean;
}

export interface TaskEventContext {
  taskId: string;
  subject: string;
  status?: string;
  description?: string;
}

export interface UserPromptContext {
  prompt: string;
  turnCount: number;
}

export interface NotificationContext {
  message: string;
  notificationType?: string;
  title?: string;
}

export interface ConfigChangeContext {
  source: string;
  filePath?: string;
}

export interface InstructionsContext {
  filePath: string;
  loadReason: string;
}

export interface WorktreeContext {
  name?: string;
  worktreePath?: string;
}

export interface FileChangeContext {
  filePath: string;
  event: 'change' | 'add' | 'unlink';
}

export interface CwdChangedContext {
  oldCwd: string;
  newCwd: string;
}

export interface StopFailureContext {
  error: string;
  hookIndex: number;
}

export interface ElicitationContext {
  mcpServerName: string;
  message?: string;
  mode?: string;
}

export type SessionHookFn = (context: SessionContext) => Promise<{ initialUserMessage?: string } | void>;
export type SubagentHookFn = (context: SubagentContext) => Promise<void>;
export type CompactHookFn = (context: CompactContext) => Promise<void>;
export type PermissionHookFn = (context: PermissionContext) => Promise<{ action?: 'allow' | 'deny'; reason?: string } | void>;
export type TaskEventHookFn = (context: TaskEventContext) => Promise<void>;
export type UserPromptHookFn = (context: UserPromptContext) => Promise<{ additionalContext?: string } | void>;
export type NotificationHookFn = (context: NotificationContext) => Promise<void>;
export type ConfigChangeHookFn = (context: ConfigChangeContext) => Promise<void>;
export type InstructionsHookFn = (context: InstructionsContext) => Promise<void>;
export type WorktreeHookFn = (context: WorktreeContext) => Promise<void>;
export type FileChangeHookFn = (context: FileChangeContext) => Promise<void>;
export type CwdChangedHookFn = (context: CwdChangedContext) => Promise<void>;
export type ElicitationHookFn = (context: ElicitationContext) => Promise<void>;
export type StopFailureHookFn = (context: StopFailureContext) => Promise<void>;

// ---------------------------------------------------------------------------
// HookManager
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Hook config file types
// ---------------------------------------------------------------------------

export interface CommandHookConfig {
  type: 'command';
  command: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  asyncRewake?: boolean;
}

export interface PromptHookConfig {
  type: 'prompt';
  prompt: string;
  model?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  asyncRewake?: boolean;
}

export interface HttpHookConfig {
  type: 'http';
  url: string;
  headers?: Record<string, string>;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  asyncRewake?: boolean;
}

export interface AgentHookConfig {
  type: 'agent';
  prompt: string;
  model?: string;
  timeout?: number;
  once?: boolean;
  if?: string;
  async?: boolean;
  asyncRewake?: boolean;
}

export interface HookDecisionOutput {
  decision?: 'approve' | 'deny' | 'ask' | 'block';
  permissionDecision?: 'allow' | 'deny' | 'ask';
  updatedInput?: Record<string, unknown>;
  additionalContext?: string;
  continue?: boolean;
  stopReason?: string;
  systemMessage?: string;
  suppressOutput?: boolean;
  reason?: string;
}

export type HookCallback = (event: string, data: unknown) => Promise<HookDecisionOutput | string | void>;

export interface HookConfigEntry {
  event: string;
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig;
  matcher?: string;
  _source?: 'project' | 'user' | 'config';
}

export interface HookConfigFile {
  hooks: HookConfigEntry[];
}

export type EngineFactory = (config: any) => { submit(prompt: string): AsyncIterable<{ type: string; text?: string; error?: string }>; shutdown?(): void };

async function runCommandHookAsync(
  command: string,
  cwd: string,
  env: Record<string, string>,
  timeout: number,
  stdinData?: string,
): Promise<{ exitCode: number; output: string }> {
  return new Promise((resolve, reject) => {
    const proc = spawn('bash', ['-c', command], {
      cwd,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let output = '';
    proc.stdout?.on('data', (d: Buffer) => { output += d.toString(); });
    proc.stderr?.on('data', (d: Buffer) => { output += d.toString(); });

    proc.stdin?.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EPIPE') coreLog('hooks', `Hook stdin error: ${err.message}`);
    });

    if (stdinData) {
      try {
        proc.stdin?.write(stdinData, 'utf-8');
        proc.stdin?.end();
      } catch { proc.stdin?.end(); }
    } else {
      proc.stdin?.end();
    }

    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      reject(new Error(`Hook timed out after ${timeout}ms`));
    }, timeout);

    proc.on('close', (code) => {
      clearTimeout(timer);
      resolve({ exitCode: code ?? 1, output });
    });

    proc.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

export class HookManager extends EventEmitter {
  private _engineFactory: EngineFactory | null = null;
  private _providerConfig: any = null;
  private _cwd: string = process.cwd();
  private _sessionId: string = '';
  private _workspaceTrusted = false;
  private _callbackHooks: Map<string, HookCallback[]> = new Map();

  constructor() {
    super();
  }

  setEngineFactory(factory: EngineFactory): void { this._engineFactory = factory; }
  setProviderConfig(config: any): void { this._providerConfig = config; }
  setCwd(cwd: string): void { this._cwd = cwd; }
  setSessionId(id: string): void { this._sessionId = id; }

  setWorkspaceTrusted(trusted: boolean): void { this._workspaceTrusted = trusted; }
  isWorkspaceTrusted(): boolean { return this._workspaceTrusted; }

  registerCallback(event: string, callback: HookCallback): () => void {
    const hooks = this._callbackHooks.get(event) || [];
    hooks.push(callback);
    this._callbackHooks.set(event, hooks);
    return () => {
      const current = this._callbackHooks.get(event) || [];
      const index = current.indexOf(callback);
      if (index >= 0) current.splice(index, 1);
    };
  }

  /**
   * Execute all registered callback hooks for the given event.
   * Returns the first non-void HookDecisionOutput if any callback returns one.
   */
  async executeCallbacks(event: string, data: unknown): Promise<HookDecisionOutput | void> {
    const hooks = this._callbackHooks.get(event);
    if (!hooks || hooks.length === 0) return;
    for (const cb of hooks) {
      try {
        const result = await cb(event, data);
        if (result && typeof result === 'object') return result as HookDecisionOutput;
      } catch (err) {
        coreLog('hooks', `Callback hook error for event "${event}": ${err}`);
      }
    }
  }

  private buildHookEnv(event: string, toolName?: string): Record<string, string> {
    return {
      ...process.env as Record<string, string>,
      SUPERINFERENCE_PROJECT_DIR: this._cwd,
      SUPERINFERENCE_SESSION_ID: this._sessionId,
      SUPERINFERENCE_TOOL_NAME: toolName || '',
      SUPERINFERENCE_HOOK_EVENT: event,
      SUPERINFERENCE_CWD: this._cwd,
      SUPERINFERENCE_MODEL: this._providerConfig?.model ?? '',
      SUPERINFERENCE_PROVIDER: this._providerConfig?.provider ?? '',
    };
  }

  private postSamplingHooks: HookFn[] = [];
  private stopHooks: HookFn[] = [];
  private errorHooks: HookFn[] = [];
  private preToolUseHooks: PreToolUseHookFn[] = [];
  private postToolUseHooks: PostToolUseHookFn[] = [];
  private preSamplingHooks: PreSamplingHookFn[] = [];
  private sessionStartHooks: SessionHookFn[] = [];
  private sessionEndHooks: SessionHookFn[] = [];
  private subagentStartHooks: SubagentHookFn[] = [];
  private subagentStopHooks: SubagentHookFn[] = [];
  private preCompactHooks: CompactHookFn[] = [];
  private postCompactHooks: CompactHookFn[] = [];
  private permissionRequestHooks: PermissionHookFn[] = [];
  private permissionDeniedHooks: PermissionHookFn[] = [];
  private userPromptSubmitHooks: UserPromptHookFn[] = [];
  private taskCreatedHooks: TaskEventHookFn[] = [];
  private taskCompletedHooks: TaskEventHookFn[] = [];
  private postToolUseFailureHooks: PostToolUseHookFn[] = [];
  private notificationHooks: NotificationHookFn[] = [];
  private setupHooks: SessionHookFn[] = [];
  private configChangeHooks: ConfigChangeHookFn[] = [];
  private instructionsLoadedHooks: InstructionsHookFn[] = [];
  private worktreeCreateHooks: WorktreeHookFn[] = [];
  private worktreeRemoveHooks: WorktreeHookFn[] = [];
  private cwdChangedHooks: CwdChangedHookFn[] = [];
  private fileChangedHooks: FileChangeHookFn[] = [];
  private elicitationHooks: ElicitationHookFn[] = [];
  private elicitationResultHooks: ElicitationHookFn[] = [];
  private stopFailureHooks: StopFailureHookFn[] = [];

  /**
   * Load hooks from .superinference/hooks.json.
   * Command hooks execute a shell command; exit code 0 = allow, non-zero = deny.
   */
  loadFromFile(cwd: string): void {
    const filePath = path.join(cwd, '.superinference', 'hooks.json');
    const userHooksPath = path.join(os.homedir(), '.superinference', 'hooks.json');

    const sources: Array<{ path: string; source: 'project' | 'user' }> = [
      { path: filePath, source: 'project' },
      { path: userHooksPath, source: 'user' },
    ];

    for (const { path: p, source } of sources) {
      let raw: string;
      try {
        raw = fs.readFileSync(p, 'utf-8');
      } catch {
        continue;
      }
      try {
        const config: HookConfigFile = JSON.parse(raw);
        if (!config.hooks || !Array.isArray(config.hooks)) continue;
        for (const entry of config.hooks) {
          entry._source = source;
          if (source === 'project' && !this._workspaceTrusted) {
            coreLog('hooks', `Skipping project hook (workspace not trusted): ${entry.event}`);
            continue;
          }
          this.processHookEntry(entry, cwd);
        }
      } catch (err) {
        coreLog('hooks', `Invalid hooks.json at ${p}: ${err}`);
      }
    }

    try {
      const configPath = path.join(cwd, '.superinference', 'config.json');
      if (fs.existsSync(configPath)) {
        const raw = fs.readFileSync(configPath, 'utf-8');
        const config = JSON.parse(raw);
        if (config.hooks && Array.isArray(config.hooks)) {
          for (const entry of config.hooks) {
            entry._source = 'config';
            this.processHookEntry(entry, cwd);
          }
        }
      }
    } catch {}
  }

  private processHookEntry(entry: HookConfigEntry, cwd: string): void {
        const hookConfig = entry.hook;
        if (!hookConfig) return;
        const matcher = entry.matcher;

        switch (entry.event) {
        case 'preToolUse': {
          const fn: PreToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.toolName)) return { action: 'allow' };
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return { action: 'allow' };
            const eventData = JSON.stringify({ event: 'preToolUse', toolName: ctx.toolName, toolInput: ctx.toolInput });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return { action: 'allow' }; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.preToolUseHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            if (result.startsWith('MODIFY:')) {
              try {
                const parsed = JSON.parse(result.slice(7));
                if (parsed.additionalContext && !parsed.updatedInput && Object.keys(parsed).length === 1) {
                  return { action: 'allow', additionalContext: parsed.additionalContext };
                }
                return { action: 'modify', updatedInput: parsed.updatedInput ?? parsed, additionalContext: parsed.additionalContext };
              } catch { /* ignore parse errors */ }
            }
            if (result.startsWith('SYSTEM:')) return { action: 'allow', additionalContext: result.slice(7).trim() };
            return { action: 'allow' };
          };
          this.onPreToolUse(fn);
          break;
        }
        case 'postToolUse': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.toolName)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = JSON.stringify({ event: 'postToolUse', toolName: ctx.toolName, isError: ctx.isError });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.postToolUseHooks, fn);
            try {
              const parsed = JSON.parse(result);
              if (parsed.additionalContext) return { additionalContext: parsed.additionalContext };
            } catch {}
            return undefined;
          };
          this.onPostToolUse(fn);
          break;
        }
        case 'preSampling': {
          const fn: PreSamplingHookFn = async (_ctx) => {
            const eventData = JSON.stringify({ event: 'preSampling' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return { action: 'allow' }; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.preSamplingHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny', reason: result.slice(5).trim() };
            return { action: 'allow' };
          };
          this.onPreSampling(fn);
          break;
        }
        case 'postSampling': {
          const fn: HookFn = async () => {
            const eventData = JSON.stringify({ event: 'postSampling' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postSamplingHooks, fn);
          };
          this.onPostSampling(fn);
          break;
        }
        case 'stop': {
          const fn: HookFn = async () => {
            const eventData = JSON.stringify({ event: 'stop' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.stopHooks, fn);
          };
          this.onStop(fn);
          break;
        }
        case 'error': {
          const fn: HookFn = async () => {
            const eventData = JSON.stringify({ event: 'error' });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.errorHooks, fn);
          };
          this.onError(fn);
          break;
        }
        case 'sessionStart': {
          const fn: SessionHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'sessionStart', sessionId: ctx.sessionId });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.sessionStartHooks, fn);
            try {
              const parsed = JSON.parse(result);
              if (parsed.initialUserMessage) return { initialUserMessage: parsed.initialUserMessage };
              if (parsed.systemMessage) return { initialUserMessage: parsed.systemMessage };
            } catch {}
            if (result.startsWith('SYSTEM:')) return { initialUserMessage: result.slice(7).trim() };
            return undefined;
          };
          this.onSessionStart(fn);
          break;
        }
        case 'sessionEnd': {
          const fn: SessionHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'sessionEnd', sessionId: ctx.sessionId });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.sessionEndHooks, fn);
          };
          this.onSessionEnd(fn);
          break;
        }
        case 'subagentStart': {
          const fn: SubagentHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'subagentStart', prompt: ctx.prompt });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.subagentStartHooks, fn);
          };
          this.onSubagentStart(fn);
          break;
        }
        case 'subagentStop': {
          const fn: SubagentHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'subagentStop', subagentSessionId: ctx.subagentSessionId });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.subagentStopHooks, fn);
          };
          this.onSubagentStop(fn);
          break;
        }
        case 'preCompact': {
          const fn: CompactHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'preCompact', messageCount: ctx.messageCount });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.preCompactHooks, fn);
          };
          this.onPreCompact(fn);
          break;
        }
        case 'postCompact': {
          const fn: CompactHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'postCompact', messageCount: ctx.messageCount });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postCompactHooks, fn);
          };
          this.onPostCompact(fn);
          break;
        }
        case 'permissionRequest': {
          const fn: PermissionHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'permissionRequest', toolName: ctx.toolName, toolInput: ctx.toolInput });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.permissionRequestHooks, fn);
            if (result.startsWith('DENY:')) return { action: 'deny' as const, reason: result.slice(5).trim() };
            if (result === 'ALLOW' || result.startsWith('ALLOW:')) return { action: 'allow' as const };
            try {
              const parsed: HookDecisionOutput = JSON.parse(result);
              if (parsed.permissionDecision === 'allow' || parsed.decision === 'approve') return { action: 'allow' as const };
              if (parsed.permissionDecision === 'deny' || parsed.decision === 'deny' || parsed.decision === 'block') return { action: 'deny' as const, reason: parsed.reason ?? 'Hook denied' };
            } catch {}
            return undefined;
          };
          this.onPermissionRequest(fn);
          break;
        }
        case 'permissionDenied': {
          const fn: PermissionHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'permissionDenied', toolName: ctx.toolName });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.permissionDeniedHooks, fn);
          };
          this.onPermissionDenied(fn);
          break;
        }
        case 'userPromptSubmit': {
          const fn: UserPromptHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'userPromptSubmit', turnCount: ctx.turnCount });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            const result = await runner();
            if (hookConfig.once) this.removeHookOnce(this.userPromptSubmitHooks, fn);
            try {
              const parsed = JSON.parse(result);
              if (parsed.additionalContext) return { additionalContext: parsed.additionalContext };
            } catch {}
            return undefined;
          };
          this.onUserPromptSubmit(fn);
          break;
        }
        case 'taskCreated': {
          const fn: TaskEventHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'taskCreated', taskId: ctx.taskId, subject: ctx.subject });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.taskCreatedHooks, fn);
          };
          this.onTaskCreated(fn);
          break;
        }
        case 'taskCompleted': {
          const fn: TaskEventHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'taskCompleted', taskId: ctx.taskId, subject: ctx.subject });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.taskCompletedHooks, fn);
          };
          this.onTaskCompleted(fn);
          break;
        }
        case 'postToolUseFailure': {
          const fn: PostToolUseHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.toolName)) return;
            if (matcher && !matchesPattern(matcher, ctx.toolName)) return;
            const eventData = JSON.stringify({ event: 'postToolUseFailure', toolName: ctx.toolName, isError: ctx.isError });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.postToolUseFailureHooks, fn);
          };
          this.onPostToolUseFailure(fn);
          break;
        }
        case 'notification': {
          const fn: NotificationHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'notification', message: ctx.message, title: ctx.title });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.notificationHooks, fn);
          };
          this.onNotification(fn);
          break;
        }
        case 'setup': {
          const fn: SessionHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'setup', sessionId: ctx.sessionId, cwd: ctx.cwd });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.setupHooks, fn);
          };
          this.onSetup(fn);
          break;
        }
        case 'configChange': {
          const fn: ConfigChangeHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'configChange', source: ctx.source, filePath: ctx.filePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.configChangeHooks, fn);
          };
          this.onConfigChange(fn);
          break;
        }
        case 'instructionsLoaded': {
          const fn: InstructionsHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'instructionsLoaded', filePath: ctx.filePath, loadReason: ctx.loadReason });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.instructionsLoadedHooks, fn);
          };
          this.onInstructionsLoaded(fn);
          break;
        }
        case 'worktreeCreate': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'worktreeCreate', name: ctx.name, worktreePath: ctx.worktreePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.worktreeCreateHooks, fn);
          };
          this.onWorktreeCreate(fn);
          break;
        }
        case 'worktreeRemove': {
          const fn: WorktreeHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'worktreeRemove', name: ctx.name, worktreePath: ctx.worktreePath });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.worktreeRemoveHooks, fn);
          };
          this.onWorktreeRemove(fn);
          break;
        }
        case 'cwdChanged': {
          const fn: CwdChangedHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'cwdChanged', oldCwd: ctx.oldCwd, newCwd: ctx.newCwd });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.cwdChangedHooks, fn);
          };
          this.onCwdChanged(fn);
          break;
        }
        case 'fileChanged': {
          const fn: FileChangeHookFn = async (ctx) => {
            if (hookConfig.if && !matchesPattern(hookConfig.if, ctx.filePath)) return;
            if (matcher && !matchesPattern(matcher, ctx.filePath)) return;
            const eventData = JSON.stringify({ event: 'fileChanged', filePath: ctx.filePath, fileEvent: ctx.event });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.fileChangedHooks, fn);
          };
          this.onFileChanged(fn);
          break;
        }
        case 'elicitation': {
          const fn: ElicitationHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'elicitation', mcpServerName: ctx.mcpServerName, message: ctx.message });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.elicitationHooks, fn);
          };
          this.onElicitation(fn);
          break;
        }
        case 'elicitationResult': {
          const fn: ElicitationHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'elicitationResult', mcpServerName: ctx.mcpServerName, message: ctx.message });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.elicitationResultHooks, fn);
          };
          this.onElicitationResult(fn);
          break;
        }
        case 'stopFailure': {
          const fn: StopFailureHookFn = async (ctx) => {
            const eventData = JSON.stringify({ event: 'stopFailure', error: ctx.error, hookIndex: ctx.hookIndex });
            const runner = () => this.runHookByType(hookConfig, cwd, eventData);
            if (hookConfig.async) { runner().catch(() => {}); return; }
            await runner();
            if (hookConfig.once) this.removeHookOnce(this.stopFailureHooks, fn);
          };
          this.onStopFailure(fn);
          break;
        }
        default:
          coreLog('hooks', `Unknown hook event: ${entry.event}`);
      }
  }

  // ---- Existing registration methods (backward-compatible) ----

  onPostSampling(fn: HookFn): void {
    this.postSamplingHooks.push(fn);
  }

  onStop(fn: HookFn): void {
    this.stopHooks.push(fn);
  }

  onError(fn: HookFn): void {
    this.errorHooks.push(fn);
  }

  // ---- New registration methods ----

  onPreToolUse(fn: PreToolUseHookFn): void {
    this.preToolUseHooks.push(fn);
  }

  onPostToolUse(fn: PostToolUseHookFn): void {
    this.postToolUseHooks.push(fn);
  }

  onPreSampling(fn: PreSamplingHookFn): void {
    this.preSamplingHooks.push(fn);
  }

  onSessionStart(fn: SessionHookFn): void { this.sessionStartHooks.push(fn); }
  onSessionEnd(fn: SessionHookFn): void { this.sessionEndHooks.push(fn); }
  onSubagentStart(fn: SubagentHookFn): void { this.subagentStartHooks.push(fn); }
  onSubagentStop(fn: SubagentHookFn): void { this.subagentStopHooks.push(fn); }
  onPreCompact(fn: CompactHookFn): void { this.preCompactHooks.push(fn); }
  onPostCompact(fn: CompactHookFn): void { this.postCompactHooks.push(fn); }
  onPermissionRequest(fn: PermissionHookFn): void { this.permissionRequestHooks.push(fn); }
  onPermissionDenied(fn: PermissionHookFn): void { this.permissionDeniedHooks.push(fn); }
  onUserPromptSubmit(fn: UserPromptHookFn): void { this.userPromptSubmitHooks.push(fn); }
  onTaskCreated(fn: TaskEventHookFn): void { this.taskCreatedHooks.push(fn); }
  onTaskCompleted(fn: TaskEventHookFn): void { this.taskCompletedHooks.push(fn); }
  onPostToolUseFailure(fn: PostToolUseHookFn): void { this.postToolUseFailureHooks.push(fn); }
  onNotification(fn: NotificationHookFn): void { this.notificationHooks.push(fn); }
  onSetup(fn: SessionHookFn): void { this.setupHooks.push(fn); }
  onConfigChange(fn: ConfigChangeHookFn): void { this.configChangeHooks.push(fn); }
  onInstructionsLoaded(fn: InstructionsHookFn): void { this.instructionsLoadedHooks.push(fn); }
  onWorktreeCreate(fn: WorktreeHookFn): void { this.worktreeCreateHooks.push(fn); }
  onWorktreeRemove(fn: WorktreeHookFn): void { this.worktreeRemoveHooks.push(fn); }
  onCwdChanged(fn: CwdChangedHookFn): void { this.cwdChangedHooks.push(fn); }
  onFileChanged(fn: FileChangeHookFn): void { this.fileChangedHooks.push(fn); }
  onElicitation(fn: ElicitationHookFn): void { this.elicitationHooks.push(fn); }
  onElicitationResult(fn: ElicitationHookFn): void { this.elicitationResultHooks.push(fn); }
  onStopFailure(fn: StopFailureHookFn): void { this.stopFailureHooks.push(fn); }

  // ---- Existing execution methods (backward-compatible, now with error logging) ----

  async executePostSampling(ctx: HookContext): Promise<void> {
    for (const hook of this.postSamplingHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  async executeStop(ctx: HookContext): Promise<void> {
    for (let i = 0; i < this.stopHooks.length; i++) {
      try {
        await this.stopHooks[i](ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
        await this.executeStopFailure({ error: String(err), hookIndex: i });
      }
    }
  }

  async executeError(ctx: HookContext): Promise<void> {
    for (const hook of this.errorHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  // ---- New execution methods ----

  /**
   * Execute pre-tool-use hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreToolUse(ctx: PreToolUseContext): Promise<HookDecision> {
    return this.executeDecisionHooks(this.preToolUseHooks, ctx);
  }

  /**
   * Execute post-tool-use hooks. Returns the last additionalContext if any hook provides one.
   */
  async executePostToolUse(ctx: PostToolUseContext): Promise<{ additionalContext?: string } | void> {
    let additionalContext: string | undefined;
    for (const hook of this.postToolUseHooks) {
      try {
        const result = await hook(ctx);
        if (result && result.additionalContext) additionalContext = result.additionalContext;
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
    if (additionalContext) return { additionalContext };
  }

  /**
   * Execute pre-sampling hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreSampling(ctx: PreSamplingContext): Promise<HookDecision> {
    return this.executeDecisionHooks(this.preSamplingHooks, ctx);
  }

  async executeSessionStart(ctx: SessionContext): Promise<{ initialUserMessage?: string } | void> {
    let initialUserMessage: string | undefined;
    for (const hook of this.sessionStartHooks) {
      try {
        const result = await hook(ctx);
        if (result && result.initialUserMessage) initialUserMessage = result.initialUserMessage;
      } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
    if (initialUserMessage) return { initialUserMessage };
  }

  async executeSessionEnd(ctx: SessionContext): Promise<void> {
    for (const hook of this.sessionEndHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeSubagentStart(ctx: SubagentContext): Promise<void> {
    for (const hook of this.subagentStartHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeSubagentStop(ctx: SubagentContext): Promise<void> {
    for (const hook of this.subagentStopHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executePreCompact(ctx: CompactContext): Promise<void> {
    coreLog('hooks', `[compact] Pre-compact triggered: ${ctx.messageCount} messages, ~${ctx.tokenEstimate} tokens`);
    for (const hook of this.preCompactHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executePostCompact(ctx: CompactContext): Promise<void> {
    for (const hook of this.postCompactHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executePermissionRequest(ctx: PermissionContext): Promise<{ action?: 'allow' | 'deny'; reason?: string } | void> {
    for (const hook of this.permissionRequestHooks) {
      try {
        const result = await hook(ctx);
        if (result && result.action) return result;
      } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executePermissionDenied(ctx: PermissionContext): Promise<{ retry?: boolean } | void> {
    for (const hook of this.permissionDeniedHooks) {
      try {
        const result = await hook(ctx);
        if (result && (result as any).action === 'allow') {
          return { retry: true };
        }
      } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
    if (ctx.retry) return { retry: true };
  }

  async executeUserPromptSubmit(ctx: UserPromptContext): Promise<{ additionalContext?: string } | void> {
    let additionalContext: string | undefined;
    for (const hook of this.userPromptSubmitHooks) {
      try {
        const result = await hook(ctx);
        if (result && result.additionalContext) additionalContext = result.additionalContext;
      } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
    if (additionalContext) return { additionalContext };
  }

  async executeTaskCreated(ctx: TaskEventContext): Promise<void> {
    for (const hook of this.taskCreatedHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeTaskCompleted(ctx: TaskEventContext): Promise<void> {
    for (const hook of this.taskCompletedHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executePostToolUseFailure(ctx: PostToolUseContext): Promise<void> {
    for (const hook of this.postToolUseFailureHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeNotification(ctx: NotificationContext): Promise<void> {
    for (const hook of this.notificationHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeSetup(ctx: SessionContext): Promise<void> {
    for (const hook of this.setupHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeConfigChange(ctx: ConfigChangeContext): Promise<void> {
    for (const hook of this.configChangeHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeInstructionsLoaded(ctx: InstructionsContext): Promise<void> {
    for (const hook of this.instructionsLoadedHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeWorktreeCreate(ctx: WorktreeContext): Promise<void> {
    for (const hook of this.worktreeCreateHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeWorktreeRemove(ctx: WorktreeContext): Promise<void> {
    for (const hook of this.worktreeRemoveHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeCwdChanged(ctx: CwdChangedContext): Promise<void> {
    for (const hook of this.cwdChangedHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeFileChanged(ctx: FileChangeContext): Promise<void> {
    for (const hook of this.fileChangedHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeElicitation(ctx: ElicitationContext): Promise<void> {
    for (const hook of this.elicitationHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeElicitationResult(ctx: ElicitationContext): Promise<void> {
    for (const hook of this.elicitationResultHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  async executeStopFailure(ctx: StopFailureContext): Promise<void> {
    for (const hook of this.stopFailureHooks) {
      try { await hook(ctx); } catch (err) { coreLog('hooks', 'Hook error', { error: String(err) }); }
    }
  }

  getSummary(): Record<string, number> {
    return {
      preToolUse: this.preToolUseHooks.length,
      postToolUse: this.postToolUseHooks.length,
      preSampling: this.preSamplingHooks.length,
      postSampling: this.postSamplingHooks.length,
      stop: this.stopHooks.length,
      error: this.errorHooks.length,
      sessionStart: this.sessionStartHooks.length,
      sessionEnd: this.sessionEndHooks.length,
      subagentStart: this.subagentStartHooks.length,
      subagentStop: this.subagentStopHooks.length,
      preCompact: this.preCompactHooks.length,
      postCompact: this.postCompactHooks.length,
      permissionRequest: this.permissionRequestHooks.length,
      permissionDenied: this.permissionDeniedHooks.length,
      userPromptSubmit: this.userPromptSubmitHooks.length,
      taskCreated: this.taskCreatedHooks.length,
      taskCompleted: this.taskCompletedHooks.length,
      postToolUseFailure: this.postToolUseFailureHooks.length,
      notification: this.notificationHooks.length,
      setup: this.setupHooks.length,
      configChange: this.configChangeHooks.length,
      instructionsLoaded: this.instructionsLoadedHooks.length,
      worktreeCreate: this.worktreeCreateHooks.length,
      worktreeRemove: this.worktreeRemoveHooks.length,
      cwdChanged: this.cwdChangedHooks.length,
      fileChanged: this.fileChangedHooks.length,
      elicitation: this.elicitationHooks.length,
      elicitationResult: this.elicitationResultHooks.length,
      stopFailure: this.stopFailureHooks.length,
      callbackHooks: Array.from(this._callbackHooks.values()).reduce((a, b) => a + b.length, 0),
    };
  }

  /**
   * Shared decision-hook executor. First deny wins; modify decisions are
   * accumulated (later hooks override earlier ones). Errors are logged.
   */
  private async executeDecisionHooks<T extends HookContext>(
    hooks: Array<(ctx: T) => Promise<HookDecision>>,
    ctx: T,
  ): Promise<HookDecision> {
    let accumulated: HookDecision = { action: 'allow' };

    for (const hook of hooks) {
      try {
        const decision = await hook(ctx);
        if (decision.action === 'deny') {
          return decision;
        }
        if (decision.action === 'modify') {
          accumulated = {
            action: 'modify',
            updatedInput: decision.updatedInput ?? (accumulated.action === 'modify' ? accumulated.updatedInput : undefined),
            updatedMessages: decision.updatedMessages ?? (accumulated.action === 'modify' ? accumulated.updatedMessages : undefined),
            additionalContext: decision.additionalContext ?? accumulated.additionalContext,
          };
        } else if (decision.additionalContext) {
          accumulated = { ...accumulated, additionalContext: decision.additionalContext };
        }
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }

    return accumulated;
  }

  private async runHookByType(
    config: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
    cwd: string,
    eventData: string,
  ): Promise<string> {
    switch (config.type) {
      case 'command': {
        const timeout = config.timeout ?? 10000;
        const hookEnv = this.buildHookEnv(
          'command',
          eventData ? (() => { try { return JSON.parse(eventData).toolName; } catch { return undefined; } })() : undefined,
        );
        try {
          const { exitCode, output } = await runCommandHookAsync(config.command, cwd, hookEnv, timeout, eventData);
          if (exitCode !== 0) {
            if (config.asyncRewake && exitCode === 2) {
              this.emit('hookRewake', { event: 'command', message: output || 'Background hook completed with exit code 2' });
            }
            return `DENY:${output || 'Hook denied'}`;
          }
          const trimmed = output.trim();
          try {
            const parsed: HookDecisionOutput = JSON.parse(trimmed);
            if (parsed.decision === 'block' || parsed.decision === 'deny') return `DENY:${parsed.reason ?? 'Hook blocked'}`;
            if (parsed.decision === 'ask' || parsed.permissionDecision === 'ask') return `ASK:${parsed.reason ?? ''}`;
            if (parsed.continue === false) return `DENY:${parsed.stopReason ?? 'Hook requested stop'}`;
            if (parsed.updatedInput) return `MODIFY:${JSON.stringify(parsed.updatedInput)}`;
            if (parsed.additionalContext) return `MODIFY:${JSON.stringify({ additionalContext: parsed.additionalContext })}`;
            if (parsed.systemMessage) return `SYSTEM:${parsed.systemMessage}`;
          } catch {
            // Not JSON — fall back to prefix detection
          }
          return trimmed;
        } catch (err: unknown) {
          coreLog('hooks', `Command hook error: ${err}`);
          return '';
        }
      }
      case 'prompt':
        return this.runPromptHook(config, eventData);
      case 'http':
        return this.runHttpHook(config, eventData);
      case 'agent':
        return this.runAgentHook(config, eventData);
      default:
        return '';
    }
  }

  private async runPromptHook(config: PromptHookConfig, eventData: string): Promise<string> {
    if (!this._engineFactory) {
      coreLog('hooks', 'Prompt hook skipped: no engine factory available');
      return '';
    }
    const providerConfig = { ...(this._providerConfig || {}) };
    if (config.model) providerConfig.model = config.model;

    const engine = this._engineFactory({
      provider: providerConfig,
      cwd: this._cwd,
      permissionMode: 'auto-allow',
    });

    const fullPrompt = `${config.prompt}\n\nEvent data:\n${eventData}`;
    let result = '';
    const timeout = config.timeout ?? 30000;
    const deadline = Date.now() + timeout;

    try {
      for await (const event of engine.submit(fullPrompt)) {
        if (Date.now() > deadline) break;
        if (event.type === 'text_delta' && event.text) result += event.text;
        if (event.type === 'error') { result += event.error ?? ''; break; }
      }
    } catch (err) {
      coreLog('hooks', `Prompt hook error: ${err}`);
      if (config.asyncRewake) {
        this.emit('hookRewake', { event: 'prompt', message: `Prompt hook error: ${err}` });
      }
    }
    engine.shutdown?.();

    if (config.asyncRewake && result.startsWith('DENY:')) {
      this.emit('hookRewake', { event: 'prompt', message: result });
    }
    return result;
  }

  private async runHttpHook(config: HttpHookConfig, eventData: string): Promise<string> {
    const http = require('http');
    const https = require('https');
    const url = new URL(config.url);
    const client = url.protocol === 'https:' ? https : http;
    return new Promise((resolve) => {
      const req = client.request(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...config.headers },
        timeout: config.timeout ?? 10000,
      }, (res: any) => {
        let body = '';
        res.on('data', (d: Buffer) => body += d);
        res.on('end', () => resolve(body));
      });
      req.on('error', () => resolve(''));
      req.write(eventData);
      req.end();
    });
  }

  private async runAgentHook(config: AgentHookConfig, eventData: string): Promise<string> {
    if (!this._engineFactory) {
      coreLog('hooks', 'Agent hook skipped: no engine factory available');
      return '';
    }
    const providerConfig = { ...(this._providerConfig || {}) };
    if (config.model) providerConfig.model = config.model;

    const engine = this._engineFactory({
      provider: providerConfig,
      cwd: this._cwd,
      permissionMode: 'auto-allow',
    });

    const fullPrompt = `${config.prompt}\n\nContext:\n${eventData}`;
    let result = '';
    const timeout = config.timeout ?? 60000;
    const deadline = Date.now() + timeout;

    try {
      for await (const event of engine.submit(fullPrompt)) {
        if (Date.now() > deadline) break;
        if (event.type === 'text_delta' && event.text) result += event.text;
        if (event.type === 'error') { result += event.error ?? ''; break; }
      }
    } catch (err) {
      coreLog('hooks', `Agent hook error: ${err}`);
    }
    engine.shutdown?.();
    return result;
  }

  private removeHookOnce<T>(hookArray: T[], hookFn: T): void {
    const idx = hookArray.indexOf(hookFn);
    if (idx >= 0) hookArray.splice(idx, 1);
  }
}

// ---------------------------------------------------------------------------
// Pattern matching utility
// ---------------------------------------------------------------------------

function matchesPattern(pattern: string, value: string): boolean {
  if (!pattern || pattern === '*') return true;
  if (pattern.includes('|')) return pattern.split('|').some(p => matchesPattern(p, value));
  try { return new RegExp(pattern).test(value); } catch { return pattern === value; }
}

