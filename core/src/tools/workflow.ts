import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { projectStateDir } from '../reference-journal';
import { executeWorkflow, parseWorkflowMeta, WorkflowResolver, DiskWorkflowCache } from './workflow-runtime';
import { SUBAGENT_RETURN_DIRECTIVE } from './task';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';

/**
 * Upper bound on a workflow script's size. A workflow body is compiled with the
 * Function constructor and run in-process, so an unbounded script is a memory /
 * compile-time DoS vector. 512 KiB is far larger than any legitimate
 * orchestration script yet rejects pathological input before it is compiled.
 * (Parity hardening — Claude Code's Workflow tool guards script size.)
 */
export const MAX_WORKFLOW_SCRIPT_BYTES = 512 * 1024;

/** Reduce an opaque caller-supplied id to a single safe path segment so a
 *  runId / resumeKey can never escape the `.ami/workflows/` tree. Mirrors the
 *  sanitization already applied to the resume cache namespace. */
export function sanitizeWorkflowRunId(id: string): string {
  return id.replace(/[^A-Za-z0-9_-]/g, '_');
}

/** Persisted record for an opt-in, resumable workflow run (keyed by runId). */
export interface WorkflowRunRecord {
  /** The fully-resolved script source — persisted so a resume reproduces the
   *  exact body even if the original scriptPath is later moved or deleted. */
  script: string;
  /** The scriptPath the run was launched from, if any (informational). */
  scriptPath?: string;
  /** The saved-workflow name the run was launched from, if any (informational). */
  name?: string;
  meta: { name: string; description: string };
  createdAt: string;
}

function workflowRunsDir(cwd: string): string {
  return path.join(cwd, '.ami', 'workflows', '.runs');
}

/** Best-effort persistence of a run record so `resumeFromRunId` can reload it.
 *  Failures (read-only cwd, `.ami` is a file, …) are swallowed — persistence is
 *  an optimization, never a hard requirement for the run to complete. */
export function persistWorkflowRun(cwd: string, runId: string, record: WorkflowRunRecord): void {
  try {
    const dir = workflowRunsDir(cwd);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `${runId}.json`), JSON.stringify(record));
  } catch { /* best-effort: a run must not fail because its record could not be saved */ }
}

/** Load a persisted run record by id, or null if absent / unreadable / malformed. */
export function loadWorkflowRun(cwd: string, runId: string): WorkflowRunRecord | null {
  try {
    const raw = fs.readFileSync(path.join(workflowRunsDir(cwd), `${runId}.json`), 'utf-8');
    const rec = JSON.parse(raw) as WorkflowRunRecord;
    return typeof rec?.script === 'string' ? rec : null;
  } catch {
    return null;
  }
}

export const workflowTool: ToolDefinition = {
  name: 'workflow',
  description:
    'Execute a workflow script that orchestrates multiple subagents. ' +
    'Workflows run agent(), parallel(), pipeline(), phase(), and log() to coordinate multi-step work. ' +
    'Scripts must begin with `export const meta = { name, description }` followed by the workflow body.',
  inputSchema: {
    type: 'object',
    properties: {
      script: {
        type: 'string',
        description: 'Self-contained workflow script (JavaScript). Must start with export const meta = {...}.',
      },
      scriptPath: {
        type: 'string',
        description: 'Path to a workflow script file on disk. Takes precedence over script.',
      },
      name: {
        type: 'string',
        description: 'Name of a saved workflow to load from .ami/workflows/.',
      },
      args: {
        type: 'string',
        description: 'Optional input value exposed to the script as the global `args`. A JSON value (string, number, object, array, …) is passed through to the script unchanged.',
      },
      maxConcurrent: {
        type: 'number',
        description: 'Max agents running concurrently for this run (1-256). Lower for strict rate limits, higher for fast self-hosted providers. Defaults to a CPU-derived cap.',
      },
      resumeKey: {
        type: 'string',
        description: 'Opt-in resume: re-invoking with the same resumeKey reuses the cached results of agents that already completed (same prompt + opts), so a resumed/retried run skips finished work. Cached under .ami/workflows/.cache/.',
      },
      runId: {
        type: 'string',
        description: 'Opt-in: tag this run with a stable id so its script is persisted (under .ami/workflows/.runs/) and its agent results cached — enabling a later `resumeFromRunId` to replay it without re-supplying the script or repeating finished agents.',
      },
      resumeFromRunId: {
        type: 'string',
        description: 'Resume a previous run saved under this runId: reloads the persisted script (no script/scriptPath/name needed) and reuses that run\'s agent cache so completed work is skipped. Takes precedence over script/scriptPath/name.',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // Reject wrong-typed string inputs up front so a non-string can never reach
    // fs.*/path.join and fail obscurely (parity: argument-shape validation).
    for (const key of ['script', 'scriptPath', 'name', 'resumeKey', 'runId', 'resumeFromRunId'] as const) {
      const val = input[key];
      if (val !== undefined && typeof val !== 'string') {
        return { output: `Error: '${key}' must be a string.`, isError: true };
      }
    }

    let script: string;
    let scriptPathUsed: string | undefined;
    let nameUsed: string | undefined;

    const resumeFromRunId = typeof input.resumeFromRunId === 'string' && input.resumeFromRunId.trim()
      ? input.resumeFromRunId.trim()
      : undefined;

    if (resumeFromRunId) {
      // Resume takes precedence over script/scriptPath/name: reload the exact
      // persisted body so a retry replays the same workflow even if the original
      // source is gone.
      const record = loadWorkflowRun(context.cwd, sanitizeWorkflowRunId(resumeFromRunId));
      if (!record) {
        return { output: `Error: No saved workflow run found for runId '${resumeFromRunId}'.`, isError: true };
      }
      script = record.script;
      scriptPathUsed = record.scriptPath;
      nameUsed = record.name;
    } else if (input.scriptPath) {
      const scriptPath = input.scriptPath as string;
      if (!fs.existsSync(scriptPath)) {
        return { output: `Error: Script file not found: ${scriptPath}`, isError: true };
      }
      script = fs.readFileSync(scriptPath, 'utf-8');
      scriptPathUsed = scriptPath;
    } else if (input.name) {
      const name = input.name as string;
      const searchPaths = [
        path.join(context.cwd, '.ami', 'workflows', `${name}.js`),
        path.join(context.cwd, '.ami', 'workflows', `${name}.mjs`),
      ];
      const found = searchPaths.find(p => fs.existsSync(p));
      if (!found) {
        return { output: `Error: Workflow '${name}' not found in .ami/workflows/`, isError: true };
      }
      script = fs.readFileSync(found, 'utf-8');
      nameUsed = name;
    } else if (input.script) {
      script = input.script as string;
    } else {
      return { output: 'Error: One of script, scriptPath, or name is required (or resumeFromRunId to resume a saved run).', isError: true };
    }

    if (Buffer.byteLength(script, 'utf-8') > MAX_WORKFLOW_SCRIPT_BYTES) {
      return { output: `Error: Workflow script exceeds the maximum size of ${MAX_WORKFLOW_SCRIPT_BYTES} bytes.`, isError: true };
    }

    const meta = parseWorkflowMeta(script);
    if (!meta) {
      return { output: 'Error: Script must begin with `export const meta = { name, description }`', isError: true };
    }
    // Claude Code parity: meta must declare BOTH a name and a description.
    // parseWorkflowMeta coerces missing fields to '' rather than failing, so the
    // tool enforces the required-field contract here.
    if (!meta.name.trim() || !meta.description.trim()) {
      return { output: 'Error: Workflow meta must define a non-empty `name` and `description`.', isError: true };
    }

    // Opt-in run persistence (parity: resumable runs). A fresh run tagged with an
    // explicit runId saves its resolved script so `resumeFromRunId` can replay it.
    const runId = !resumeFromRunId && typeof input.runId === 'string' && input.runId.trim()
      ? sanitizeWorkflowRunId(input.runId.trim())
      : undefined;
    if (runId) {
      persistWorkflowRun(context.cwd, runId, {
        script,
        scriptPath: scriptPathUsed,
        name: nameUsed,
        meta: { name: meta.name, description: meta.description },
        createdAt: new Date().toISOString(),
      });
    }
    // Cache namespace drives resume: results are keyed under the run/resume id so
    // a resumed run reuses the finished agents of the original.
    const effectiveRunId = resumeFromRunId ? sanitizeWorkflowRunId(resumeFromRunId) : runId;

    // One flow per workflow invocation so the inspector shows "1 flow, many
    // agents" — each agent()/parallel()/pipeline() call joins it. Guarded on
    // processManager so the workflow runs unchanged when there is none.
    const pm = context.processManager;
    const fm = pm?.flowManager;
    const workflowFlowId = fm ? fm.createFlow(meta.name) : undefined;

    const agentHandler = async (prompt: string, opts?: any): Promise<string> => {
      if (!context._engineFactory) return `[Agent not available: no engine factory]`;
      let taskId: string | undefined;
      let outputPath: string | undefined;
      if (pm) {
        taskId = `agent-${crypto.randomBytes(4).toString('hex')}`;
        const label = String(opts?.label || prompt).slice(0, 80);
        outputPath = path.join(projectStateDir(context.cwd, 'tasks'), `${taskId}.out`);
        try { fs.mkdirSync(path.dirname(outputPath), { recursive: true }); } catch { /* ok */ }
        pm.registerAgent(taskId, {
          command: `[agent] ${label}`,
          description: `Workflow agent: ${label}`,
          outputPath,
          abortFn: () => {},
        });
        if (fm && workflowFlowId) fm.addAgent(workflowFlowId, taskId);
      }
      const subEngine = context._engineFactory({
        provider: context._providerConfig!,
        cwd: context.cwd,
        permissionMode: 'auto-allow',
        tools: opts?.tools,
      });
      // Same return-and-stop framing as task sub-agents: a workflow agent should
      // complete its step and return, not run the full never-stop coding loop.
      const framedPrompt = `${SUBAGENT_RETURN_DIRECTIVE}\n\n---\n\n${prompt}`;
      const finish = (status: 'completed' | 'failed', text: string): void => {
        if (!pm || !taskId) return;
        try { if (outputPath) fs.writeFileSync(outputPath, text || '(no output)'); } catch { /* dir gone */ }
        pm.updateEntry(taskId, { status, exitCode: status === 'failed' ? 1 : 0 });
      };
      let result = '';
      try {
        for await (const event of subEngine.submit(framedPrompt)) {
          if (event.type === 'text_delta') result += event.text;
        }
      } catch (err) {
        finish('failed', result);
        (subEngine as any).shutdown?.();
        throw err;
      }
      (subEngine as any).shutdown?.();
      finish('completed', result);
      return result || '[Agent produced no output]';
    };

    const workflowResolver: WorkflowResolver = (nameOrRef) => {
      let scriptContent: string | null = null;
      if (typeof nameOrRef === 'string') {
        const searchPaths = [
          path.join(context.cwd, '.ami', 'workflows', `${nameOrRef}.js`),
          path.join(context.cwd, '.ami', 'workflows', `${nameOrRef}.mjs`),
        ];
        const found = searchPaths.find(p => fs.existsSync(p));
        if (found) scriptContent = fs.readFileSync(found, 'utf-8');
      } else if (nameOrRef.scriptPath) {
        if (fs.existsSync(nameOrRef.scriptPath)) {
          scriptContent = fs.readFileSync(nameOrRef.scriptPath, 'utf-8');
        }
      }
      return scriptContent;
    };

    try {
      // Opt-in resume cache (B2): keyed per run/resume id, else per resumeKey,
      // under a contained .cache subdir (segment sanitized to block traversal).
      const cacheNamespace = effectiveRunId
        ?? (typeof input.resumeKey === 'string' && input.resumeKey.trim()
          ? input.resumeKey.replace(/[^A-Za-z0-9_-]/g, '_')
          : undefined);
      const cache = cacheNamespace
        ? new DiskWorkflowCache(path.join(context.cwd, '.ami', 'workflows', '.cache', cacheNamespace))
        : undefined;

      const result = await executeWorkflow(script, agentHandler, {
        args: input.args,
        abortSignal: context.abortSignal,
        workflowResolver,
        maxConcurrent: typeof input.maxConcurrent === 'number' ? input.maxConcurrent : undefined,
        cache,
      });

      const output = [
        `Workflow '${meta.name}' completed.`,
        `Agents spawned: ${result.agentCount}`,
        effectiveRunId ? `Run id: ${effectiveRunId}` : '',
        result.logs.length > 0 ? `\nLogs:\n${result.logs.map(l => `  - ${l}`).join('\n')}` : '',
        result.result !== null ? `\nResult: ${JSON.stringify(result.result, null, 2)}` : '',
      ].filter(Boolean).join('\n');

      return { output, isError: false };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return { output: `Error executing workflow: ${message}`, isError: true };
    }
  },
};
