import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import type { PermissionRule } from './permissions';
import type { ThinkingLevel } from './model-capabilities';

export interface PersonaDefinition {
  name: string;
  description: string;
  systemPromptOverlay: string;
  allowedTools?: string[];
  disallowedTools?: string[];
  permissionRules?: PermissionRule[];
  defaultThinkingLevel?: ThinkingLevel;
  autoAllowPatterns?: string[];
}

const BUILTIN_PERSONAS: PersonaDefinition[] = [
  {
    name: 'code',
    description: 'Code assistant — generation, debugging, refactoring, testing',
    systemPromptOverlay: `You are an expert AI coding assistant. You help with writing code, debugging, refactoring, explaining code, answering questions about the codebase, and running commands. You have direct access to the user's filesystem through tools.`,
    defaultThinkingLevel: 'medium',
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
- Use bash to run security tools: nmap, nikto, sqlmap, gobuster, ffuf, curl, openssl, etc.
- Read configuration files to identify misconfigurations.
- Analyze source code for common vulnerability patterns (OWASP Top 10).
- Document findings in a structured format: severity, description, impact, remediation.`,
    autoAllowPatterns: [
      'curl*', 'wget*', 'nmap*', 'nikto*', 'sqlmap*', 'gobuster*',
      'ffuf*', 'openssl*', 'dig*', 'nslookup*', 'whois*', 'traceroute*',
      'netstat*', 'ss*', 'tcpdump*',
    ],
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
    };
  }
}
