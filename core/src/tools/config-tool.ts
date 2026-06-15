import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const configTool: ToolDefinition = {
  name: 'config',
  description: 'Read or update project configuration. Use to check or modify settings like model, provider, thinking level, etc.',
  inputSchema: {
    type: 'object',
    properties: {
      action: { type: 'string', enum: ['get', 'set'], description: 'Get current config or set a value.' },
      key: { type: 'string', description: 'Config key (e.g., "model", "provider", "thinkingLevel").' },
      value: { type: 'string', description: 'Value to set (only for action="set").' },
    },
    required: ['action'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const configPath = path.join(context.cwd, '.superinference', 'config.json');
    const action = input.action as string;
    if (action === 'get') {
      try {
        const raw = fs.readFileSync(configPath, 'utf-8');
        const config = JSON.parse(raw);
        if (input.key) return { output: `${input.key} = ${JSON.stringify(config[input.key as string])}` };
        return { output: JSON.stringify(config, null, 2) };
      } catch { return { output: 'No project config found. Create .superinference/config.json.' }; }
    }
    if (action === 'set' && input.key && input.value !== undefined) {
      try {
        fs.mkdirSync(path.dirname(configPath), { recursive: true });
        let config: Record<string, unknown> = {};
        try { config = JSON.parse(fs.readFileSync(configPath, 'utf-8')); } catch {}
        config[input.key as string] = input.value;
        fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
        return { output: `Set ${input.key} = ${JSON.stringify(input.value)}` };
      } catch (err) { return { output: `Error: ${err}`, isError: true }; }
    }
    return { output: 'Usage: action="get" [key] or action="set" key=... value=...', isError: true };
  },
};
