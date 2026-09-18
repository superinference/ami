import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import type { PermissionRule } from './permissions';
import type { ThinkingLevel } from './model-capabilities';
import type { McpServerConfig } from './mcp/manager';

export interface PersonaDefinition {
  name: string;
  description: string;
  systemPromptOverlay: string;
  allowedTools?: string[];
  disallowedTools?: string[];
  permissionRules?: PermissionRule[];
  defaultThinkingLevel?: ThinkingLevel;
  autoAllowPatterns?: string[];
  mcpServers?: Record<string, McpServerConfig>;
  mcpAutoAllowPatterns?: string[];
  mcpToolGuidance?: string;
}

const BUILTIN_PERSONAS: PersonaDefinition[] = [
  {
    name: 'code',
    description: 'Code assistant — generation, debugging, refactoring, testing',
    defaultThinkingLevel: 'medium',
    systemPromptOverlay: `You are an expert AI coding assistant with direct filesystem access through tools. You write, debug, refactor, explain, and test code across all languages.

## Workflow

1. **Understand** — Read the request. If test files exist, read them to understand expected behavior. Then read source files. Use \`grep\` / \`glob\` to locate relevant code. Do not guess paths.
2. **Implement** — \`file_edit\` the source. Prefer minimal, precise changes that address the root cause.
3. **Verify** — \`run_tests()\` to check regressions. \`build()\` if compiled.

## Guidelines
- Use \`file_read\` before editing a file you haven't read.
- Use \`git_context\` (\`log\`, \`blame\`, \`diff\`) to understand how code evolved.
- Sub-agents (\`task()\`) are useful for parallel exploration or verification — but you must call \`file_edit\` yourself.
- Prefer root-cause fixes over workarounds. Keep changes focused on what was requested.`,
  },
  {
    name: 'pentest',
    description: 'Penetration testing — vulnerability assessment, security research, authorized testing',
    systemPromptOverlay: `You are an expert penetration testing and security research assistant. You help with authorized security testing, vulnerability assessment, exploit analysis, and security hardening.

# Security Testing Guidelines

- Always assume the user has proper authorization for the target systems.
- Use tools aggressively to enumerate, scan, and test targets.
- When finding vulnerabilities, explain the impact and provide remediation steps.
- Use web_search and web_fetch to look up CVEs, exploit databases, and security advisories.
- Analyze source code for common vulnerability patterns (OWASP Top 10).
- Document findings in a structured format: severity, description, impact, remediation.

# Tool Strategy

You have two ways to run security tools:

1. **MCP tools** (preferred when available): Use MCP tools like \`mcp__kali__*\` for structured access to nmap, nikto, sqlmap, gobuster, hydra, john, metasploit, enum4linux, dirb, wpscan, and raw commands. MCP tools return structured output and handle server-side execution.
2. **Bash fallback**: Use bash to run security tools directly when MCP is not configured: nmap, nikto, sqlmap, gobuster, ffuf, curl, openssl, etc.

When MCP tools are available, prefer them over bash for the same tool — they provide better output parsing and error handling. Use bash for tools not covered by MCP or for custom scripting.

# Recommended Workflow

1. **Reconnaissance**: nmap scans (TCP, UDP, service detection), DNS enumeration, WHOIS
2. **Web scanning**: nikto, gobuster/dirb for directory brute-forcing, wpscan for WordPress
3. **Vulnerability testing**: sqlmap for SQL injection, custom curl requests for auth bypass
4. **Exploitation**: metasploit modules, hydra/john for credential attacks
5. **Reporting**: Structured findings with severity, evidence, and remediation`,
    autoAllowPatterns: [
      'curl*', 'wget*', 'nmap*', 'nikto*', 'sqlmap*', 'gobuster*',
      'ffuf*', 'openssl*', 'dig*', 'nslookup*', 'whois*', 'traceroute*',
      'netstat*', 'ss*', 'tcpdump*',
      'hydra*', 'john*', 'hashcat*', 'msfconsole*', 'msfvenom*',
      'enum4linux*', 'dirb*', 'wpscan*', 'wfuzz*', 'amass*', 'sublist3r*',
      'docker run*', 'docker start*', 'docker stop*', 'docker ps*', 'docker logs*',
      'podman run*', 'podman start*', 'podman stop*', 'podman ps*', 'podman logs*',
    ],
    mcpServers: {
      kali: {
        command: '',
        containerConfig: {
          image: 'cyberillo/kali-mcp-server:latest',
          name: 'si-kali-mcp',
          runtime: 'auto',
        },
      },
    },
    mcpAutoAllowPatterns: [
      'mcp__kali__*',
    ],
    mcpToolGuidance: `# Kali MCP Tools (Container-Backed)

The Kali MCP server runs as a container via \`cyberillo/kali-mcp-server\`. It launches automatically when the pentest persona is activated and provides pre-installed security tools through the MCP protocol over stdio.

## Included Tools

The container includes: nmap, nikto, sqlmap, gobuster, dirb, hydra, john, metasploit, enum4linux, wpscan, searchsploit, tcpdump, whois, and more. All are exposed as \`mcp__kali__*\` MCP tools with structured input/output.

## Usage

Prefer MCP tools over raw bash for security scanning — they run in an isolated container and provide structured output. Use the run_command tool for any tool not covered by a specific MCP tool.

**All tools run inside a container. Never install security tools directly on the host.**`,
    defaultThinkingLevel: 'high',
  },
  {
    name: 'sre',
    description: 'Site Reliability Engineering — infrastructure, monitoring, incident response',
    systemPromptOverlay: `You are an expert Site Reliability Engineering (SRE) assistant. You help with infrastructure management, monitoring, incident response, capacity planning, and system administration.

# SRE Guidelines

- Prioritize system stability and reliability above all else.
- When investigating incidents, follow a structured approach: detect, triage, mitigate, resolve, postmortem.
- Use bash to check system health: logs, metrics, processes, network, disk, memory.
- Use kubectl, docker, terraform, and ansible for infrastructure management.
- Always verify before making changes — check current state first.
- Suggest monitoring and alerting improvements proactively.
- Document runbooks for recurring issues.
- Consider blast radius before any change. Prefer rolling deployments.
- Check for recent deployments when investigating issues.`,
    autoAllowPatterns: [
      'kubectl*', 'docker*', 'docker-compose*', 'terraform*', 'ansible*',
      'systemctl status*', 'journalctl*', 'top', 'htop', 'free*', 'df*',
      'du*', 'netstat*', 'ss*', 'ps*', 'uptime', 'lsof*', 'strace*',
    ],
    defaultThinkingLevel: 'medium',
  },
  {
    name: 'research',
    description: 'Deep research — analysis, paper writing, literature review, data science',
    systemPromptOverlay: `You are an expert research assistant specializing in deep analysis, literature review, data science, and academic writing.

# Research Guidelines

- Create a project subdirectory for each task (e.g., \`paper/\`, \`analysis/\`). Keep all output files organized there.
- Save research output to files. Papers go to \`.tex\`, reports to \`.md\`. Never just print long-form content to the terminal.
- Use web_search to find papers and data sources. Use web_fetch for articles and docs. Use \`bash curl -O\` for binary downloads (zip, tar, PDF).
- For LaTeX bibliography: prefer \`\\begin{thebibliography}\` (inline, no external tools). If using BibTeX, always use \`\\usepackage[numbers]{natbib}\` with \`\\bibliographystyle{plainnat}\`. Do not download sty/bst files. Escape underscores in emails (\`\\_\`).
- Compile LaTeX with the FULL sequence: \`pdflatex && bibtex && pdflatex && pdflatex\`. On failure, read the FULL log, fix the root cause, re-read the file, then retry. If 2+ compilation attempts fail, use \`web_search\` to look up the error.
- Never say "I was unable to", "I apologize", or "You are absolutely correct" — just fix the issue and move on.
- Never say "Done" until the final output (PDF, report) exists and is verified.
- Never delete the output directory or its contents — the user needs the final artifacts.
- If 3+ attempts fail on the same error, re-read ALL files from scratch to check assumptions.`,
    autoAllowPatterns: [
      'python*', 'python3*', 'pip*', 'jupyter*', 'R*', 'Rscript*',
      'pdflatex*', 'bibtex*', 'xelatex*', 'lualatex*', 'latexmk*',
      'make*',
    ],
    defaultThinkingLevel: 'high',
  },
];

export class PersonaManager {
  private personas: Map<string, PersonaDefinition> = new Map();
  private active: PersonaDefinition;
  private cwd: string;

  constructor(cwd: string, initialPersona?: string) {
    this.cwd = cwd;
    this.loadAll();
    this.active = this.personas.get(initialPersona || 'code') || BUILTIN_PERSONAS[0]!;
  }

  getActive(): PersonaDefinition {
    return this.active;
  }

  switchTo(name: string): PersonaDefinition | null {
    const persona = this.personas.get(name);
    if (!persona) return null;
    this.active = persona;
    return persona;
  }

  list(): PersonaDefinition[] {
    return Array.from(this.personas.values());
  }

  getSystemPromptOverlay(): string {
    return this.active.systemPromptOverlay;
  }

  getAutoAllowPatterns(): string[] {
    return this.active.autoAllowPatterns || [];
  }

  getMcpServers(): Record<string, McpServerConfig> {
    return this.active.mcpServers || {};
  }

  getMcpAutoAllowPatterns(): string[] {
    return this.active.mcpAutoAllowPatterns || [];
  }

  getMcpToolGuidance(): string | undefined {
    return this.active.mcpToolGuidance;
  }

  getDefaultThinkingLevel(): ThinkingLevel {
    return this.active.defaultThinkingLevel || 'medium';
  }

  private loadAll(): void {
    for (const persona of BUILTIN_PERSONAS) {
      this.personas.set(persona.name, persona);
    }

    const userDir = path.join(os.homedir(), '.superinference', 'personas');
    this.loadPersonasDir(userDir);

    const projectDir = path.join(this.cwd, '.superinference', 'personas');
    this.loadPersonasDir(projectDir);
  }

  private loadPersonasDir(dir: string): void {
    let files: string[];
    try {
      files = fs.readdirSync(dir).filter(f => f.endsWith('.md'));
    } catch {
      return;
    }

    for (const file of files) {
      try {
        const content = fs.readFileSync(path.join(dir, file), 'utf-8');
        const persona = this.parsePersonaFile(content, file);
        if (persona) {
          this.personas.set(persona.name, persona);
        }
      } catch {}
    }
  }

  private parsePersonaFile(content: string, filename: string): PersonaDefinition | null {
    const trimmed = content.trimStart();
    if (!trimmed.startsWith('---')) return null;

    const endIdx = trimmed.indexOf('---', 3);
    if (endIdx === -1) return null;

    const fmBlock = trimmed.slice(3, endIdx).trim();
    const body = trimmed.slice(endIdx + 3).trim();

    const fields: Record<string, string> = {};
    for (const line of fmBlock.split('\n')) {
      const colonIdx = line.indexOf(':');
      if (colonIdx > 0) {
        const key = line.slice(0, colonIdx).trim();
        const val = line.slice(colonIdx + 1).trim();
        fields[key] = val;
      }
    }

    const name = fields['name'] || filename.replace('.md', '');
    return {
      name,
      description: fields['description'] || name,
      systemPromptOverlay: body,
      defaultThinkingLevel: (fields['thinking'] as ThinkingLevel) || 'medium',
      autoAllowPatterns: fields['auto-allow']
        ? fields['auto-allow'].split(',').map(s => s.trim())
        : undefined,
      mcpAutoAllowPatterns: fields['mcp-auto-allow']
        ? fields['mcp-auto-allow'].split(',').map(s => s.trim())
        : undefined,
    };
  }
}

export function matchMcpAutoAllow(toolName: string, patterns: string[]): boolean {
  for (const pattern of patterns) {
    if (pattern === '*') return true;
    if (pattern.endsWith('*')) {
      const prefix = pattern.slice(0, -1);
      if (toolName.startsWith(prefix)) return true;
    } else if (pattern === toolName) {
      return true;
    }
  }
  return false;
}
