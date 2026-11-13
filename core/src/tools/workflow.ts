import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { projectStateDir } from '../reference-journal';
import { executeWorkflow, parseWorkflowMeta, WorkflowResolver, DiskWorkflowCache } from './workflow-runtime';
import { SUBAGENT_RETURN_DIRECTIVE } from './task';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';

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
        description: 'Optional input value exposed to the script as the global `args`.',
      },
      maxConcurrent: {
        type: 'number',
        description: 'Max agents running concurrently for this run (1-256). Lower for strict rate limits, higher for fast self-hosted providers. Defaults to a CPU-derived cap.',
      },
      resumeKey: {
        type: 'string',
        description: 'Opt-in resume: re-invoking with the same resumeKey reuses the cached results of agents that already completed (same prompt + opts), so a resumed/retried run skips finished work. Cached under .ami/workflows/.cache/.',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    let script: string;

    if (input.scriptPath) {
      const scriptPath = input.scriptPath as string;
      if (!fs.existsSync(scriptPath)) {
        return { output: `Error: Script file not found: ${scriptPath}`, isError: true };
      }
      script = fs.readFileSync(scriptPath, 'utf-8');
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
    } else if (input.script) {
      script = input.script as string;
    } else {
      return { output: 'Error: One of script, scriptPath, or name is required.', isError: true };
    }

    const meta = parseWorkflowMeta(script);
    if (!meta) {
      return { output: 'Error: Script must begin with `export const meta = { name, description }`', isError: true };
    }

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
      // Opt-in resume cache (B2): keyed per resumeKey, under a contained
      // .cache subdir (segment sanitized to block path traversal).
      const cache = typeof input.resumeKey === 'string' && input.resumeKey.trim()
        ? new DiskWorkflowCache(path.join(context.cwd, '.ami', 'workflows', '.cache', input.resumeKey.replace(/[^A-Za-z0-9_-]/g, '_')))
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
