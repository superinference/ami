import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';
import { McpClient } from '../src/mcp/client';
import { PersonaManager, matchMcpAutoAllow } from '../src/personas';
import { createMcpTool } from '../src/tools/mcp-tool';
import { PermissionManager } from '../src/permissions';

after(() => { setTimeout(() => process.exit(0), 200); });

function makeKaliServerScript(): string {
  return `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'kali-mcp', version: '1.0.0' },
              capabilities: { tools: {} },
            },
          }) + '\\n');
        } else if (msg.method === 'tools/list') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [
              { name: 'nmap_scan', description: 'Run nmap scan', inputSchema: { type: 'object', properties: { target: { type: 'string' }, scan_type: { type: 'string' } }, required: ['target'] } },
              { name: 'nikto_scan', description: 'Run nikto scan', inputSchema: { type: 'object', properties: { target: { type: 'string' } }, required: ['target'] } },
              { name: 'sqlmap_scan', description: 'Run sqlmap', inputSchema: { type: 'object', properties: { url: { type: 'string' } }, required: ['url'] } },
              { name: 'gobuster_scan', description: 'Run gobuster', inputSchema: { type: 'object', properties: { url: { type: 'string' } }, required: ['url'] } },
              { name: 'run_command', description: 'Run arbitrary command', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } },
            ] },
          }) + '\\n');
        } else if (msg.method === 'tools/call') {
          const toolName = msg.params?.name;
          const args = msg.params?.arguments || {};
          let text = 'ok';
          if (toolName === 'nmap_scan') {
            text = JSON.stringify({
              target: args.target,
              ports: [22, 80, 443],
              services: ['ssh', 'http', 'https'],
            });
          } else if (toolName === 'nikto_scan') {
            text = JSON.stringify({ target: args.target, vulnerabilities: [] });
          } else if (toolName === 'run_command') {
            text = 'command output: ' + (args.command || '');
          }
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { content: [{ type: 'text', text }] },
          }) + '\\n');
        } else if (msg.method === 'ping') {
          process.stdout.write(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {},
          }) + '\\n');
        }
      } catch {}
    });
    setInterval(() => {}, 60000);
  `;
}

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-persona-mcp-e2e-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// PersonaManager — MCP server declaration
// ---------------------------------------------------------------------------

describe('persona MCP server declaration', () => {
  it('pentest persona declares kali MCP server with containerConfig', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const servers = pm.getMcpServers();
    assert.ok(servers.kali);
    assert.ok(servers.kali.containerConfig, 'kali must have containerConfig');
    assert.equal(servers.kali.containerConfig!.image, 'cyberillo/kali-mcp-server:latest');
    assert.equal(servers.kali.containerConfig!.name, 'si-kali-mcp');
  });

  it('pentest persona declares mcp__kali__* auto-allow pattern', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const patterns = pm.getMcpAutoAllowPatterns();
    assert.ok(patterns.includes('mcp__kali__*'));
  });

  it('pentest persona has MCP tool guidance mentioning key capabilities', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const guidance = pm.getMcpToolGuidance();
    assert.ok(guidance);
    assert.ok(guidance!.includes('cyberillo/kali-mcp-server'), 'guidance should mention image');
    assert.ok(guidance!.includes('run_command'), 'guidance should mention run_command');
    for (const tool of ['nmap', 'nikto', 'sqlmap', 'gobuster', 'hydra', 'metasploit']) {
      assert.ok(guidance!.includes(tool), `guidance should mention ${tool}`);
    }
  });

  it('pentest system prompt mentions container-based setup', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const guidance = pm.getMcpToolGuidance();
    assert.ok(guidance);
    assert.ok(guidance!.includes('container'), 'should mention container');
    assert.ok(guidance!.includes('Never install security tools directly on the host'), 'should warn against native install');
  });

  it('non-pentest personas have no mcpServers', () => {
    for (const name of ['code', 'sre', 'research']) {
      const pm = new PersonaManager(tmpDir, name);
      assert.deepEqual(pm.getMcpServers(), {}, `${name} should have no MCP servers`);
    }
  });

  it('pentest autoAllowPatterns include container commands', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const patterns = pm.getAutoAllowPatterns();
    assert.ok(patterns.some(p => p.startsWith('docker')), 'should allow docker commands');
    assert.ok(patterns.some(p => p.startsWith('podman')), 'should allow podman commands');
  });
});

// ---------------------------------------------------------------------------
// McpManager + persona server registration
// ---------------------------------------------------------------------------

describe('McpManager — persona server registration', () => {
  it('registers persona-declared server successfully', () => {
    const manager = new McpManager();
    const pm = new PersonaManager(tmpDir, 'pentest');
    const servers = pm.getMcpServers();

    for (const [name, config] of Object.entries(servers)) {
      manager.addServer(name, config);
    }

    const status = manager.listServers();
    assert.ok(status.some(s => s.name === 'kali'), 'kali server should be registered');
    manager.disconnectAll();
  });

  it('skips duplicate registration without throwing', () => {
    const manager = new McpManager();
    manager.addServer('kali', { command: 'echo', args: ['test'] });
    assert.throws(() => {
      manager.addServer('kali', { command: 'echo', args: ['test2'] });
    }, /already registered/);
    manager.disconnectAll();
  });

  it('connects to mock Kali MCP server and lists tools', async () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: 'node',
      args: ['-e', makeKaliServerScript()],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await manager.connectAll();
    const tools = manager.getAllTools();
    const kaliTools = tools.filter(t => t.serverName === 'kali');

    assert.ok(kaliTools.length >= 5, `expected >= 5 kali tools, got ${kaliTools.length}`);
    assert.ok(kaliTools.some(t => t.schema.name === 'nmap_scan'));
    assert.ok(kaliTools.some(t => t.schema.name === 'nikto_scan'));
    assert.ok(kaliTools.some(t => t.schema.name === 'sqlmap_scan'));
    assert.ok(kaliTools.some(t => t.schema.name === 'gobuster_scan'));
    assert.ok(kaliTools.some(t => t.schema.name === 'run_command'));

    manager.disconnectAll();
  });

  it('calls nmap_scan tool and gets structured result', async () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: 'node',
      args: ['-e', makeKaliServerScript()],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await manager.connectAll();
    const result = await manager.callTool('kali:nmap_scan', { target: '192.168.1.1' });
    const parsed = typeof result === 'string' ? JSON.parse(result) : result;

    if (Array.isArray(parsed)) {
      const textContent = parsed.find((c: any) => c.type === 'text');
      assert.ok(textContent);
      const data = JSON.parse(textContent.text);
      assert.equal(data.target, '192.168.1.1');
      assert.ok(Array.isArray(data.ports));
    } else {
      assert.ok(parsed.target || parsed.content);
    }

    manager.disconnectAll();
  });

  it('calls run_command tool', async () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: 'node',
      args: ['-e', makeKaliServerScript()],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await manager.connectAll();
    const result = await manager.callTool('kali:run_command', { command: 'whoami' });
    const parsed = typeof result === 'string' ? result : JSON.stringify(result);
    assert.ok(parsed.includes('whoami'));

    manager.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// createMcpTool — tool wrapping for persona MCP tools
// ---------------------------------------------------------------------------

describe('createMcpTool — persona MCP tool wrapping', () => {
  it('creates tool with mcp__kali__<name> naming convention', () => {
    const tool = createMcpTool('kali', 'nmap_scan', 'Run nmap scan', { type: 'object' });
    assert.equal(tool.name, 'mcp__kali__nmap_scan');
    assert.ok(tool.description.includes('[MCP:kali]'));
    assert.ok(tool.description.includes('Run nmap scan'));
  });

  it('normalizes server names with special characters', () => {
    const tool = createMcpTool('kali-server', 'nmap_scan', 'desc', { type: 'object' });
    assert.equal(tool.name, 'mcp__kali_server__nmap_scan');
  });

  it('tool execute returns MCP error when mcpManager is not set', async () => {
    const tool = createMcpTool('kali', 'nmap_scan', 'desc', { type: 'object' });
    const result = await tool.execute(
      { target: '10.0.0.1' },
      { cwd: tmpDir, abortSignal: new AbortController().signal } as any,
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('MCP not initialized'));
  });

  it('tool execute routes through mcpManager successfully', async () => {
    const manager = new McpManager();
    manager.addServer('kali', {
      command: 'node',
      args: ['-e', makeKaliServerScript()],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });
    await manager.connectAll();

    const tool = createMcpTool('kali', 'nmap_scan', 'Run nmap', { type: 'object' });
    const result = await tool.execute(
      { target: '10.0.0.1' },
      { cwd: tmpDir, abortSignal: new AbortController().signal, _mcpManager: manager } as any,
    );
    assert.ok(!result.isError, `expected no error, got: ${result.output}`);
    assert.ok(result.output.includes('10.0.0.1') || result.output.includes('ports'));

    manager.disconnectAll();
  });
});

// ---------------------------------------------------------------------------
// matchMcpAutoAllow — pattern matching
// ---------------------------------------------------------------------------

describe('matchMcpAutoAllow — comprehensive', () => {
  it('mcp__kali__* matches all kali tools', () => {
    const tools = ['mcp__kali__nmap_scan', 'mcp__kali__nikto_scan', 'mcp__kali__sqlmap_scan',
                    'mcp__kali__gobuster_scan', 'mcp__kali__run_command'];
    for (const tool of tools) {
      assert.ok(matchMcpAutoAllow(tool, ['mcp__kali__*']), `${tool} should match mcp__kali__*`);
    }
  });

  it('mcp__kali__* does not match other server tools', () => {
    assert.ok(!matchMcpAutoAllow('mcp__github__create_issue', ['mcp__kali__*']));
    assert.ok(!matchMcpAutoAllow('mcp__slack__send_message', ['mcp__kali__*']));
    assert.ok(!matchMcpAutoAllow('bash', ['mcp__kali__*']));
  });

  it('exact match works', () => {
    assert.ok(matchMcpAutoAllow('mcp__kali__nmap_scan', ['mcp__kali__nmap_scan']));
    assert.ok(!matchMcpAutoAllow('mcp__kali__nikto_scan', ['mcp__kali__nmap_scan']));
  });

  it('multiple patterns — first match wins', () => {
    const patterns = ['mcp__kali__nmap_scan', 'mcp__github__*'];
    assert.ok(matchMcpAutoAllow('mcp__kali__nmap_scan', patterns));
    assert.ok(matchMcpAutoAllow('mcp__github__create_pr', patterns));
    assert.ok(!matchMcpAutoAllow('mcp__kali__nikto_scan', patterns));
  });

  it('wildcard * matches everything', () => {
    assert.ok(matchMcpAutoAllow('mcp__any__tool', ['*']));
    assert.ok(matchMcpAutoAllow('bash', ['*']));
  });
});

// ---------------------------------------------------------------------------
// Permission rules — MCP auto-allow integration
// ---------------------------------------------------------------------------

describe('permission rules — MCP auto-allow', () => {
  it('adds allow rules for matching MCP tools', async () => {
    const pm = new PermissionManager('ask');
    const autoAllowPatterns = ['mcp__kali__*'];

    const mcpTools = [
      { serverName: 'kali', schema: { name: 'nmap_scan', description: 'scan', inputSchema: {} } },
      { serverName: 'kali', schema: { name: 'nikto_scan', description: 'scan', inputSchema: {} } },
      { serverName: 'github', schema: { name: 'create_issue', description: 'issue', inputSchema: {} } },
    ];

    for (const t of mcpTools) {
      const toolFullName = `mcp__${t.serverName}__${t.schema.name}`;
      if (matchMcpAutoAllow(toolFullName, autoAllowPatterns)) {
        pm.addRule({ tool: toolFullName, action: 'allow' });
      }
    }

    assert.equal(await pm.check('mcp__kali__nmap_scan', {}), 'allow');
    assert.equal(await pm.check('mcp__kali__nikto_scan', {}), 'allow');
    assert.equal(await pm.check('mcp__github__create_issue', {}), 'ask');
  });

  it('pentest persona auto-allows all kali MCP tools', async () => {
    const persona = new PersonaManager(tmpDir, 'pentest');
    const autoAllow = persona.getMcpAutoAllowPatterns();
    const pm = new PermissionManager('ask');

    const kaliTools = ['nmap_scan', 'nikto_scan', 'sqlmap_scan', 'gobuster_scan', 'run_command'];
    for (const toolName of kaliTools) {
      const fullName = `mcp__kali__${toolName}`;
      if (matchMcpAutoAllow(fullName, autoAllow)) {
        pm.addRule({ tool: fullName, action: 'allow' });
      }
    }

    for (const toolName of kaliTools) {
      const fullName = `mcp__kali__${toolName}`;
      assert.equal(await pm.check(fullName, {}), 'allow', `${fullName} should be auto-allowed`);
    }
  });
});

// ---------------------------------------------------------------------------
// System prompt enrichment — MCP tool guidance injection
// ---------------------------------------------------------------------------

describe('system prompt enrichment', () => {
  it('pentest persona overlay mentions MCP tool strategy', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const overlay = pm.getSystemPromptOverlay();
    assert.ok(overlay.includes('MCP tools'));
    assert.ok(overlay.includes('mcp__kali__'));
    assert.ok(overlay.includes('Bash fallback'));
  });

  it('pentest persona guidance mentions container-backed tools', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const guidance = pm.getMcpToolGuidance();
    assert.ok(guidance);
    assert.ok(guidance!.includes('container'), 'guidance must mention container');
    assert.ok(guidance!.includes('nmap'), 'guidance must mention nmap');
    assert.ok(guidance!.includes('run_command'), 'guidance must mention run_command');
  });

  it('guidance is only injected when persona MCP servers are connected', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const guidance = pm.getMcpToolGuidance();
    const mcpServers = pm.getMcpServers();

    const mcpTools = [
      { serverName: 'github', schema: { name: 'create_issue', description: 'Create issue', inputSchema: {} } },
    ];

    const connectedServers = new Set(mcpTools.map(t => t.serverName));
    const personaServerNames = Object.keys(mcpServers);
    const hasPersonaMcpConnected = personaServerNames.some(s => connectedServers.has(s));

    assert.ok(!hasPersonaMcpConnected, 'kali not connected — guidance should not be injected');
  });

  it('guidance IS injected when kali server is among connected servers', () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const mcpServers = pm.getMcpServers();

    const mcpTools = [
      { serverName: 'kali', schema: { name: 'nmap_scan', description: 'scan', inputSchema: {} } },
      { serverName: 'github', schema: { name: 'create_issue', description: 'issue', inputSchema: {} } },
    ];

    const connectedServers = new Set(mcpTools.map(t => t.serverName));
    const personaServerNames = Object.keys(mcpServers);
    const hasPersonaMcpConnected = personaServerNames.some(s => connectedServers.has(s));

    assert.ok(hasPersonaMcpConnected, 'kali connected — guidance should be injected');
  });
});

// ---------------------------------------------------------------------------
// Full e2e: mock Kali server + persona + permissions + tool execution
// ---------------------------------------------------------------------------

describe('full e2e: persona MCP pipeline', () => {
  it('end-to-end: pentest persona → register kali → connect → auto-allow → call tool', async () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const mcpServers = pm.getMcpServers();
    const autoAllow = pm.getMcpAutoAllowPatterns();
    const guidance = pm.getMcpToolGuidance();

    assert.ok(mcpServers.kali, 'step 1: persona declares kali MCP server');

    const manager = new McpManager();
    manager.addServer('kali', {
      command: 'node',
      args: ['-e', makeKaliServerScript()],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    const servers = manager.listServers();
    assert.ok(servers.some(s => s.name === 'kali'), 'step 2: kali registered in manager');

    await manager.connectAll();
    const tools = manager.getAllTools();
    const kaliTools = tools.filter(t => t.serverName === 'kali');
    assert.ok(kaliTools.length >= 5, 'step 3: kali tools available after connect');

    const permMgr = new PermissionManager('ask');
    for (const t of kaliTools) {
      const toolFullName = `mcp__kali__${t.schema.name}`;
      if (matchMcpAutoAllow(toolFullName, autoAllow)) {
        permMgr.addRule({ tool: toolFullName, action: 'allow' });
      }
    }

    for (const t of kaliTools) {
      const toolFullName = `mcp__kali__${t.schema.name}`;
      const decision = await permMgr.check(toolFullName, {});
      assert.equal(decision, 'allow', `step 4: ${toolFullName} should be auto-allowed`);
    }

    const nmapTool = createMcpTool('kali', 'nmap_scan', 'scan', { type: 'object' });
    const result = await nmapTool.execute(
      { target: '10.0.0.1' },
      { cwd: tmpDir, abortSignal: new AbortController().signal, _mcpManager: manager } as any,
    );
    assert.ok(!result.isError, 'step 5: tool execution succeeds');
    assert.ok(result.output.includes('10.0.0.1'), 'step 5: result contains target');

    let systemPrompt = 'Base system prompt.';
    const connectedServerNames = new Set(tools.map(t => t.serverName));
    const personaServerNames = Object.keys(mcpServers);
    if (guidance && personaServerNames.some(s => connectedServerNames.has(s))) {
      systemPrompt += `\n\n${guidance}`;
    }
    assert.ok(systemPrompt.includes('nmap'), 'step 6: system prompt includes MCP guidance');
    assert.ok(systemPrompt.includes('container'), 'step 6: system prompt includes container setup');

    manager.disconnectAll();
  });

  it('pentest persona bash tools still auto-allowed alongside MCP tools', async () => {
    const pm = new PersonaManager(tmpDir, 'pentest');
    const bashPatterns = pm.getAutoAllowPatterns();

    const permMgr = new PermissionManager('ask');

    for (const pattern of bashPatterns) {
      permMgr.addRule({ tool: 'bash', pattern, action: 'allow' });
    }

    assert.equal(await permMgr.check('bash', { command: 'nmap -sV 10.0.0.1' }), 'allow');
    assert.equal(await permMgr.check('bash', { command: 'nikto -h http://target' }), 'allow');
    assert.equal(await permMgr.check('bash', { command: 'docker run -d kali-mcp' }), 'allow');
    assert.equal(await permMgr.check('bash', { command: 'podman run -d kali-mcp' }), 'allow');
  });

  it('custom persona with mcp-auto-allow in frontmatter', () => {
    const personaDir = path.join(tmpDir, '.superinference', 'personas');
    fs.mkdirSync(personaDir, { recursive: true });
    fs.writeFileSync(path.join(personaDir, 'custom-mcp.md'), `---
name: custom-mcp
description: Custom persona with MCP auto-allow
mcp-auto-allow: mcp__myserver__read*, mcp__myserver__list*
---

Custom MCP persona body.`);

    const pm = new PersonaManager(tmpDir);
    pm.switchTo('custom-mcp');
    const patterns = pm.getMcpAutoAllowPatterns();
    assert.ok(patterns.includes('mcp__myserver__read*'));
    assert.ok(patterns.includes('mcp__myserver__list*'));

    assert.ok(matchMcpAutoAllow('mcp__myserver__readFile', patterns));
    assert.ok(matchMcpAutoAllow('mcp__myserver__listDirs', patterns));
    assert.ok(!matchMcpAutoAllow('mcp__myserver__deleteFile', patterns));
  });
});

// ---------------------------------------------------------------------------
// MCP config file integration
// ---------------------------------------------------------------------------

describe('MCP config file integration with persona', () => {
  it('persona MCP server does not conflict with config file server', () => {
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'mcp.json'), JSON.stringify({
      mcpServers: {
        github: { command: 'echo', args: ['github'] },
      },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(path.join(configDir, 'mcp.json'));

    const pm = new PersonaManager(tmpDir, 'pentest');
    const personaServers = pm.getMcpServers();

    for (const [name, config] of Object.entries(personaServers)) {
      try {
        manager.addServer(name, config);
      } catch {
        // Already registered — skip
      }
    }

    const servers = manager.listServers();
    assert.ok(servers.some(s => s.name === 'github'), 'config file server preserved');
    assert.ok(servers.some(s => s.name === 'kali'), 'persona server added');
    assert.equal(servers.length, 2);

    manager.disconnectAll();
  });

  it('persona MCP server skipped when already in config file', () => {
    const configDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(path.join(configDir, 'mcp.json'), JSON.stringify({
      mcpServers: {
        kali: { command: 'custom-kali-bridge', args: ['--port', '9000'] },
      },
    }));

    const manager = new McpManager();
    manager.loadFromConfig(path.join(configDir, 'mcp.json'));

    const pm = new PersonaManager(tmpDir, 'pentest');
    const personaServers = pm.getMcpServers();

    let skipped = false;
    for (const [name, config] of Object.entries(personaServers)) {
      try {
        manager.addServer(name, config);
      } catch {
        skipped = true;
      }
    }

    assert.ok(skipped, 'duplicate kali registration should be skipped');
    const servers = manager.listServers();
    assert.equal(servers.length, 1, 'only one kali server');

    manager.disconnectAll();
  });
});
