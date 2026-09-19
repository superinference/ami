import { execSync, spawn } from 'child_process';

export interface ContainerConfig {
  image: string;
  name?: string;
  runtime?: 'docker' | 'podman' | 'auto';
  extraArgs?: string[];
}

export function detectRuntime(): string | null {
  for (const cmd of ['podman', 'docker']) {
    try {
      execSync(`command -v ${cmd}`, { stdio: 'pipe' });
      return cmd;
    } catch {}
  }
  return null;
}

export function resolveRuntime(preference?: 'docker' | 'podman' | 'auto'): string | null {
  if (preference && preference !== 'auto') {
    try {
      execSync(`command -v ${preference}`, { stdio: 'pipe' });
      return preference;
    } catch {
      return null;
    }
  }
  return detectRuntime();
}

export function buildContainerArgs(config: ContainerConfig): string[] {
  const args = ['run', '-i', '--rm'];
  if (config.name) args.push('--name', config.name);
  if (config.extraArgs) args.push(...config.extraArgs);
  args.push(config.image);
  return args;
}

export interface ImageInfo {
  available: boolean;
  id?: string;
  size?: string;
  created?: string;
}

export function isImageAvailable(runtime: string, image: string): boolean {
  try {
    execSync(`${runtime} image inspect ${image}`, { stdio: 'pipe' });
    return true;
  } catch {
    return false;
  }
}

export function pullImage(
  runtime: string,
  image: string,
  onProgress?: (line: string) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn(runtime, ['pull', image], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = '';
    proc.stdout?.on('data', (chunk: Buffer) => {
      const lines = chunk.toString().split('\n').filter(Boolean);
      for (const line of lines) onProgress?.(line);
    });
    proc.stderr?.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
      const lines = chunk.toString().split('\n').filter(Boolean);
      for (const line of lines) onProgress?.(line);
    });
    proc.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${runtime} pull failed (exit ${code}): ${stderr.trim()}`));
    });
    proc.on('error', (err) => reject(err));
  });
}

export function getImageInfo(runtime: string, image: string): ImageInfo {
  try {
    const out = execSync(
      `${runtime} image inspect --format '{{.Id}}|||{{.Size}}|||{{.Created}}' ${image}`,
      { stdio: 'pipe', encoding: 'utf-8' },
    );
    const [id, size, created] = out.trim().split('|||');
    return { available: true, id, size, created };
  } catch {
    return { available: false };
  }
}
