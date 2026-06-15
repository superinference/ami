import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';

const MAX_OUTPUT_BYTES = 5 * 1024 * 1024; // 5 MB per task

export interface BackgroundProcess {
  taskId: string;
  pid: number;
  command: string;
  description: string;
  status: 'running' | 'completed' | 'failed' | 'killed';
  exitCode: number | null;
  outputPath: string;
  startTime: number;
}

interface InternalProcess extends BackgroundProcess {
  proc: child_process.ChildProcess;
  outputFd: number | null;
  bytesWritten: number;
}

export class ProcessManager extends EventEmitter {
  private processes = new Map<string, InternalProcess>();
  private tasksDir: string;

  constructor(cwd: string) {
    super();
    this.tasksDir = path.join(cwd, '.superinference', 'tasks');
    fs.mkdirSync(this.tasksDir, { recursive: true });
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv }): string {
    const taskId = `bg-${crypto.randomBytes(4).toString('hex')}`;
    const outputPath = path.join(this.tasksDir, `${taskId}.output`);

    const isWindows = os.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    const proc = child_process.spawn(shell, shellArgs, {
      cwd: opts.cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: !isWindows,
      ...(opts.env ? { env: opts.env } : {}),
    });

    const fd = fs.openSync(outputPath, 'w');

    const entry: InternalProcess = {
      taskId,
      pid: proc.pid!,
      command,
      description: opts.description || command.slice(0, 80),
      status: 'running',
      exitCode: null,
      outputPath,
      startTime: Date.now(),
      proc,
      outputFd: fd,
      bytesWritten: 0,
    };

    this.processes.set(taskId, entry);

    const writeChunk = (data: Buffer) => {
      if (entry.bytesWritten >= MAX_OUTPUT_BYTES) return;
      const remaining = MAX_OUTPUT_BYTES - entry.bytesWritten;
      const chunk = data.length <= remaining ? data : data.subarray(0, remaining);
      try {
        fs.writeSync(fd, chunk);
        entry.bytesWritten += chunk.length;
        if (entry.bytesWritten >= MAX_OUTPUT_BYTES) {
          fs.writeSync(fd, Buffer.from('\n[output truncated: exceeded 5MB disk cap]\n'));
        }
      } catch { /* fd may be closed */ }
    };

    proc.stdout?.on('data', writeChunk);
    proc.stderr?.on('data', writeChunk);

    // Stall detection for background processes
    let lastOutputSize = 0;
    const stallCheck = setInterval(() => {
      const currentSize = this.getOutputSize(taskId);
      if (currentSize === lastOutputSize) {
        const result = this.getOutput(taskId, 10);
        if (result && this.looksLikePrompt(result.output)) {
          this.kill(taskId);
        }
      }
      lastOutputSize = currentSize;
    }, 45_000);

    proc.on('close', (code) => {
      clearInterval(stallCheck);
      entry.status = code === 0 ? 'completed' : 'failed';
      entry.exitCode = code;
      try { fs.closeSync(fd); } catch { /* already closed */ }
      entry.outputFd = null;
      this.emit('complete', { taskId, exitCode: code, command: entry.command, description: entry.description });
    });

    proc.on('error', (err) => {
      clearInterval(stallCheck);
      entry.status = 'failed';
      try {
        fs.writeSync(fd, Buffer.from(`\n[process error: ${err.message}]\n`));
        fs.closeSync(fd);
      } catch { /* ignore */ }
      entry.outputFd = null;
      this.emit('complete', { taskId, exitCode: null, command: entry.command, description: entry.description });
    });

    return taskId;
  }

  kill(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    try {
      if (os.platform() === 'win32') {
        child_process.execSync(`taskkill /pid ${entry.proc.pid} /T /F`, { stdio: 'ignore' });
      } else {
        process.kill(-entry.proc.pid!, 'SIGKILL');
      }
    } catch {
      try { entry.proc.kill('SIGKILL'); } catch { /* already dead */ }
    }

    if (entry.outputFd !== null) {
      try { fs.closeSync(entry.outputFd); } catch { /* ignore */ }
      entry.outputFd = null;
    }

    return true;
  }

  getOutput(taskId: string, tailLines = 50): { status: string; exitCode: number | null; output: string; elapsedMs: number } | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;

    let output = '';
    try {
      const content = fs.readFileSync(entry.outputPath, 'utf-8');
      if (tailLines > 0) {
        const lines = content.split('\n');
        output = lines.slice(-tailLines).join('\n');
      } else {
        output = content;
      }
    } catch { /* file may not exist yet */ }

    return {
      status: entry.status,
      exitCode: entry.exitCode,
      output,
      elapsedMs: Date.now() - entry.startTime,
    };
  }

  list(): BackgroundProcess[] {
    return Array.from(this.processes.values()).map(({ proc, outputFd, bytesWritten, ...rest }) => rest);
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, ...rest } = entry;
    return rest;
  }

  getOutputSize(taskId: string): number {
    const entry = this.processes.get(taskId);
    if (!entry) return 0;
    return entry.bytesWritten;
  }

  private looksLikePrompt(text: string): boolean {
    const lastLine = text.trim().split('\n').pop() ?? '';
    return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|Press Enter|Continue\?|Overwrite\?|password:/i.test(lastLine);
  }

  runningCount(): number {
    let count = 0;
    for (const entry of this.processes.values()) {
      if (entry.status === 'running') count++;
    }
    return count;
  }

  monitorMcp(serverId: string, checkFn: () => Promise<boolean>, intervalMs: number = 30000): string {
    const taskId = `monitor-${serverId}-${Date.now()}`;
    const interval = setInterval(async () => {
      try {
        const healthy = await checkFn();
        if (!healthy) {
          clearInterval(interval);
          this.emit('complete', { taskId, exitCode: 1, command: `monitor:${serverId}`, description: `MCP monitor: ${serverId}` });
        }
      } catch {
        clearInterval(interval);
        this.emit('complete', { taskId, exitCode: 1, command: `monitor:${serverId}`, description: `MCP monitor: ${serverId}` });
      }
    }, intervalMs);
    this.processes.set(taskId, {
      taskId,
      pid: 0,
      command: `monitor:${serverId}`,
      description: `MCP monitor: ${serverId}`,
      status: 'running',
      exitCode: null,
      outputPath: '',
      startTime: Date.now(),
      proc: null as any,
      outputFd: null,
      bytesWritten: 0,
    });
    return taskId;
  }

  cleanup(): void {
    for (const entry of this.processes.values()) {
      if (entry.status === 'running') {
        this.kill(entry.taskId);
      }
    }
    this.removeAllListeners();
  }
}
