import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { executeWorkflow, parseWorkflowMeta, WorkflowResolver } from './workflow-runtime';
import * as fs from 'fs';
import * as path from 'path';

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
        description: 'Name of a saved workflow to load from .superinference/workflows/.',
      },
      args: {
        type: 'string',
        description: 'Optional input value exposed to the script as the global `args`.',
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
        path.join(context.cwd, '.superinference', 'workflows', `${name}.js`),
        path.join(context.cwd, '.superinference', 'workflows', `${name}.mjs`),
      ];
      const found = searchPaths.find(p => fs.existsSync(p));
      if (!found) {
        return { output: `Error: Workflow '${name}' not found in .superinference/workflows/`, isError: true };
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

    const agentHandler = async (prompt: string, opts?: any): Promise<string> => {
      if (!context._engineFactory) return `[Agent not available: no engine factory]`;
      const subEngine = context._engineFactory({
        provider: context._providerConfig!,
        cwd: context.cwd,
        permissionMode: 'auto-allow',
        tools: opts?.tools,
      });
      let result = '';
      for await (const event of subEngine.submit(prompt)) {
        if (event.type === 'text_delta') result += event.text;
      }
      (subEngine as any).shutdown?.();
      return result || '[Agent produced no output]';
    };

    const workflowResolver: WorkflowResolver = (nameOrRef) => {
      let scriptContent: string | null = null;
      if (typeof nameOrRef === 'string') {
        const searchPaths = [
          path.join(context.cwd, '.superinference', 'workflows', `${nameOrRef}.js`),
          path.join(context.cwd, '.superinference', 'workflows', `${nameOrRef}.mjs`),
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
      const result = await executeWorkflow(script, agentHandler, {
        args: input.args,
        abortSignal: context.abortSignal,
        workflowResolver,
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
