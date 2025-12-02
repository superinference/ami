import * as vscode from 'vscode';
import { SchemaCache } from '../core/cache/schema-cache';

export interface CodeContext {
    file: {
        path: string;
        name: string;
        language: string;
        isDirty: boolean;
        relativePath: string;
    };
    selection: {
        text: string;
        range: vscode.Range;
        isEmpty: boolean;
        lineCount: number;
    };
    diagnostics: vscode.Diagnostic[];
    workspace: {
        folders: string[];
        openEditors: string[];
        gitStatus?: string;
    };
    relevanceScore: number;
}

export class EnhancedContextAnalyzer {
    private cache: SchemaCache;

    constructor(cache: SchemaCache) {
        this.cache = cache;
    }

    gatherContext(_customPrompt?: string, _diagnostic?: vscode.Diagnostic): CodeContext | null {
        const editor = vscode.window.activeTextEditor;
        if (!editor) return null;

        const document = editor.document;
        const selection = editor.selection;

        // Get effective range (selection or cursor area)
        const effectiveRange = this.getEffectiveRange(document, selection);
        const rangeText = document.getText(effectiveRange);

        // Gather diagnostics in range
        const diagnostics = this.getDiagnosticsInRange(document.uri, effectiveRange);

        const context: CodeContext = {
            file: {
                path: document.uri.fsPath,
                name: this.getFileName(document.uri.fsPath),
                language: document.languageId,
                isDirty: document.isDirty,
                relativePath: this.getRelativePath(document.uri)
            },
            selection: {
                text: rangeText,
                range: effectiveRange,
                isEmpty: selection.isEmpty,
                lineCount: effectiveRange.end.line - effectiveRange.start.line + 1
            },
            diagnostics,
            workspace: {
                folders: vscode.workspace.workspaceFolders?.map(f => f.uri.fsPath) || [],
                openEditors: vscode.window.visibleTextEditors.map(e => e.document.uri.fsPath)
            },
            relevanceScore: 0
        };

        // Calculate relevance score
        context.relevanceScore = this.calculateContextRelevance(context);

        return context;
    }

    buildIntelligentPrompt(context: CodeContext, userPrompt?: string, diagnostic?: vscode.Diagnostic): string {
        // Cache key for similar prompts
        const cacheKey = this.generatePromptCacheKey(context, userPrompt, diagnostic);
        const cached = this.cache.get<string>(cacheKey);
        if (cached) {
            return cached;
        }

        let prompt: string;

        if (diagnostic) {
            prompt = this.buildDiagnosticPrompt(context, diagnostic);
        } else if (userPrompt) {
            prompt = this.buildCustomPrompt(context, userPrompt);
        } else {
            prompt = this.buildAnalysisPrompt(context);
        }

        // Cache the generated prompt
        this.cache.set(cacheKey, prompt);

        return prompt;
    }

    private calculateContextRelevance(context: CodeContext): number {
        let score = 0;

        // File type relevance
        if (this.isSourceCode(context.file.language)) score += 10;

        // Selection size optimization
        if (context.selection.text.length > 5 && context.selection.text.length < 1000) score += 5;

        // Diagnostic density
        if (context.selection.lineCount > 0) {
            const issuesPerLine = context.diagnostics.length / context.selection.lineCount;
            if (issuesPerLine > 0.2) score += 8; // High issue density
        }

        // Recency of changes
        if (context.file.isDirty) score += 3; // Unsaved changes

        // Error severity
        const hasErrors = context.diagnostics.some(d => d.severity === vscode.DiagnosticSeverity.Error);
        if (hasErrors) score += 5;

        return score;
    }

    private getEffectiveRange(document: vscode.TextDocument, selection: vscode.Selection): vscode.Range {
        if (!selection.isEmpty) {
            return selection;
        }

        // Create 7-line context around cursor (3 lines before/after)
        const currentLine = selection.active.line;
        const startLine = Math.max(0, currentLine - 3);
        const endLine = Math.min(document.lineCount - 1, currentLine + 3);

        return new vscode.Range(
            new vscode.Position(startLine, 0),
            new vscode.Position(endLine, document.lineAt(endLine).text.length)
        );
    }

    private getDiagnosticsInRange(uri: vscode.Uri, range: vscode.Range): vscode.Diagnostic[] {
        return vscode.languages.getDiagnostics(uri)
            .filter(diagnostic => range.intersection(diagnostic.range) !== undefined)
            .sort((a, b) => a.severity - b.severity); // Errors first
    }

    private buildDiagnosticPrompt(context: CodeContext, diagnostic: vscode.Diagnostic): string {
        const severity = this.getSeverityString(diagnostic.severity);
        const lineNumber = diagnostic.range.start.line + 1;

        return `Fix: ${severity} in ${context.file.name} line ${lineNumber}: ${diagnostic.message}

Context: ${context.file.name} (${context.file.language}), ${this.getRangeDescription(context.selection.range)}:
${this.formatCodeContext(context)}

${this.formatDiagnostics(context.diagnostics)}`;
    }

    private buildCustomPrompt(context: CodeContext, userPrompt: string): string {
        return `${userPrompt}

Context: ${context.file.name} (${context.file.language}), ${this.getRangeDescription(context.selection.range)}:
${this.formatCodeContext(context)}

${this.formatDiagnostics(context.diagnostics)}`;
    }

    private buildAnalysisPrompt(context: CodeContext): string {
        return `Please analyze and fix issues in ${context.file.name} (${context.file.language}), ${this.getRangeDescription(context.selection.range)}:
${this.formatCodeContext(context)}

${this.formatDiagnostics(context.diagnostics)}`;
    }

    private formatCodeContext(context: CodeContext): string {
        const lines = context.selection.text.split('\n');
        if (lines.length === 1) {
            return `\`${lines[0]}\``;
        } else {
            const firstLine = lines[0].trim();
            const lastLine = lines[lines.length - 1].trim();
            return `First line: \`${firstLine}\`
Last line: \`${lastLine}\``;
        }
    }

    private formatDiagnostics(diagnostics: vscode.Diagnostic[]): string {
        if (diagnostics.length === 0) return '';

        const issueWord = diagnostics.length === 1 ? 'issue' : 'issues';
        let result = `\nFound ${diagnostics.length} ${issueWord}:`;

        diagnostics.forEach(diagnostic => {
            const severity = this.getSeverityString(diagnostic.severity);
            const line = diagnostic.range.start.line + 1;
            result += `\n- ${severity} at line ${line}: ${diagnostic.message}`;
        });

        return result;
    }

    private getSeverityString(severity: vscode.DiagnosticSeverity): string {
        switch (severity) {
            case vscode.DiagnosticSeverity.Error: return "Error";
            case vscode.DiagnosticSeverity.Warning: return "Warning";
            case vscode.DiagnosticSeverity.Information: return "Info";
            case vscode.DiagnosticSeverity.Hint: return "Hint";
            default: return "Issue";
        }
    }

    private getRangeDescription(range: vscode.Range): string {
        if (range.start.line === range.end.line) {
            return `line ${range.start.line + 1}`;
        } else {
            return `lines ${range.start.line + 1}-${range.end.line + 1}`;
        }
    }

    private isSourceCode(language: string): boolean {
        const sourceLanguages = [
            'typescript', 'javascript', 'python', 'java', 'cpp', 'c', 'csharp',
            'go', 'rust', 'php', 'ruby', 'swift', 'kotlin', 'scala', 'dart'
        ];
        return sourceLanguages.includes(language);
    }

    private getFileName(path: string): string {
        return path.split(/[/\\]/).pop() || 'unknown';
    }

    private getRelativePath(uri: vscode.Uri): string {
        const workspaceFolder = vscode.workspace.getWorkspaceFolder(uri);
        if (workspaceFolder) {
            return vscode.workspace.asRelativePath(uri, false);
        }
        return uri.fsPath;
    }

    private generatePromptCacheKey(context: CodeContext, userPrompt?: string, diagnostic?: vscode.Diagnostic): string {
        const elements = [
            context.file.name,
            context.file.language,
            context.selection.range.start.line,
            context.selection.range.end.line,
            context.diagnostics.length,
            userPrompt || '',
            diagnostic?.message || ''
        ];
        return elements.join(':');
    }
} 