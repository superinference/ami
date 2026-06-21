import * as child_process from 'child_process';
import * as path from 'path';

const LANGUAGE_SERVERS: Record<string, { command: string; args: string[] }> = {
  typescript: { command: 'npx', args: ['typescript-language-server', '--stdio'] },
  python: { command: 'pyright-langserver', args: ['--stdio'] },
  go: { command: 'gopls', args: ['serve'] },
  rust: { command: 'rust-analyzer', args: [] },
};

function detectLanguage(filePath: string): string | null {
  const ext = path.extname(filePath).toLowerCase();
  const map: Record<string, string> = {
    '.ts': 'typescript', '.tsx': 'typescript', '.js': 'typescript', '.jsx': 'typescript',
    '.py': 'python', '.pyw': 'python',
    '.go': 'go',
    '.rs': 'rust',
  };
  return map[ext] ?? null;
}

export class LSPClient {
  private processes: Map<string, child_process.ChildProcess> = new Map();
  private initialized: Set<string> = new Set();
  private messageId = 0;

  async ensureServer(language: string, cwd: string): Promise<boolean> {
    if (this.processes.has(language)) return true;
    const config = LANGUAGE_SERVERS[language];
    if (!config) return false;
    try {
      const which = child_process.spawnSync('which', [config.command], { timeout: 2000, stdio: 'pipe' });
      if (which.status !== 0) return false;
      const proc = child_process.spawn(config.command, config.args, {
        cwd, stdio: ['pipe', 'pipe', 'pipe'],
      });
      proc.on('error', () => { this.processes.delete(language); this.initialized.delete(language); });
      proc.on('exit', () => { this.processes.delete(language); this.initialized.delete(language); });
      proc.stderr?.on('data', () => {});
      proc.stdout?.on('data', () => {});
      if (!proc.pid) return false;
      this.processes.set(language, proc);
      const initId = ++this.messageId;
      this.sendRequest(proc, initId, 'initialize', {
        processId: process.pid,
        rootUri: `file://${cwd}`,
        capabilities: {},
      });
      this.initialized.add(language);
      return true;
    } catch { return false; }
  }

  async notifyDidChange(filePath: string, content: string, cwd: string): Promise<void> {
    const lang = detectLanguage(filePath);
    if (!lang) return;
    if (!this.processes.has(lang)) {
      const ok = await this.ensureServer(lang, cwd);
      if (!ok) return;
    }
    const proc = this.processes.get(lang)!;
    this.sendNotification(proc, 'textDocument/didChange', {
      textDocument: { uri: `file://${filePath}`, version: Date.now() },
      contentChanges: [{ text: content }],
    });
  }

  async notifyDidSave(filePath: string, cwd: string): Promise<void> {
    const lang = detectLanguage(filePath);
    if (!lang) return;
    if (!this.processes.has(lang)) {
      const ok = await this.ensureServer(lang, cwd);
      if (!ok) return;
    }
    const proc = this.processes.get(lang)!;
    this.sendNotification(proc, 'textDocument/didSave', {
      textDocument: { uri: `file://${filePath}` },
    });
  }

  shutdown(): void {
    for (const [, proc] of this.processes) {
      try { proc.kill(); } catch {}
    }
    this.processes.clear();
    this.initialized.clear();
  }

  private sendRequest(proc: child_process.ChildProcess, id: number, method: string, params: unknown): void {
    const body = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    const header = `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n`;
    try { proc.stdin?.write(header + body); } catch {}
  }

  private sendNotification(proc: child_process.ChildProcess, method: string, params: unknown): void {
    const body = JSON.stringify({ jsonrpc: '2.0', method, params });
    const header = `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n`;
    try { proc.stdin?.write(header + body); } catch {}
  }
}

let _lspClient: LSPClient | null = null;
export function getLSPClient(): LSPClient {
  if (!_lspClient) _lspClient = new LSPClient();
  return _lspClient;
}
