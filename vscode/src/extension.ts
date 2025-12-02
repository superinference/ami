/**
 * Copyright (C) 2025 SuperInference contributors
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

import * as vscode from 'vscode';
import { SuperInferenceChatProvider } from './chatProvider';
import { SchemaCache } from './core/cache/schema-cache';
import { PerformanceMonitor } from './performance/monitor';
import { CircuitBreaker, TimeoutManager } from './utils/resilience';
import { EnhancedContextAnalyzer } from './context/enhanced-analyzer';
import { MultiStepProcessor } from './reasoning/multi-step-processor';
import { getInitializedMCPClient } from './common/mcp-client';

// Add these interfaces for embeddings
interface EmbeddingContent {
    content: string;
    metadata: {
        type: string;
        name: string;
        uri: string;
        language: string;
        relativePath: string;
        lastModified: number;
    };
}

class WorkspaceEmbeddingsManager {
    private readonly backendUrl = 'http://localhost:3000/mcp';
    private readonly fileWatcher: vscode.FileSystemWatcher;
    private readonly indexedFiles = new Set<string>();
    private isIndexing = false;
    private performanceMonitor: PerformanceMonitor;
    private circuitBreaker: CircuitBreaker;
    private cache: SchemaCache;

    constructor(performanceMonitor: PerformanceMonitor, circuitBreaker: CircuitBreaker, cache: SchemaCache) {
        this.performanceMonitor = performanceMonitor;
        this.circuitBreaker = circuitBreaker;
        this.cache = cache;

        // Watch for file changes
        this.fileWatcher = vscode.workspace.createFileSystemWatcher('**/*', false, false, false);
        this.setupFileWatchers();
    }

    private setupFileWatchers() {
        // Handle file creation
        this.fileWatcher.onDidCreate(async (uri) => {
            if (this.shouldIndexFile(uri)) {
                await this.indexFile(uri, 'File created');
            }
        });

        // Handle file changes (saves)
        this.fileWatcher.onDidChange(async (uri) => {
            if (this.shouldIndexFile(uri)) {
                await this.indexFile(uri, 'File modified');
            }
        });

        // Handle file deletion
        this.fileWatcher.onDidDelete(async (uri) => {
            await this.removeFileFromEmbeddings(uri);
        });

        // Watch for document saves (more reliable than file watcher for open files)
        vscode.workspace.onDidSaveTextDocument(async (document) => {
            if (this.shouldIndexFile(document.uri)) {
                await this.indexFile(document.uri, 'File saved');
            }
        });
    }

    private shouldIndexFile(uri: vscode.Uri): boolean {
        const path = uri.fsPath.toLowerCase();

        // Skip certain directories and file types
        const skipPatterns = [
            '/node_modules/',
            '/.git/',
            '/dist/',
            '/build/',
            '/out/',
            '/target/',
            '/.vscode/',
            '/coverage/',
            '/.next/',
            '/.nuxt/',
            '/vendor/',
            '__pycache__',
            // Python virtual environments
            '/venv/',
            '/.venv/',
            '/env/',
            '/.env/',
            '/.pytest_cache/',
            '/.mypy_cache/',
            '/.tox/',
            // Common virtual env patterns (e.g., guidellm_env, myenv, etc.)
            /\/\w+_env\//,
            // Binary/library directories
            '/.coverage/',
            '/.eggs/',
            '/.installed.cfg',
            '/lib/python',
            '/site-packages/'
        ];

        const skipExtensions = [
            '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico',
            '.mp4', '.avi', '.mov', '.mp3', '.wav',
            '.zip', '.tar', '.gz', '.7z', '.rar',
            '.exe', '.dll', '.so', '.dylib',
            '.min.js', '.min.css'
        ];

        // Check skip patterns (handle both strings and regex)
        if (skipPatterns.some(pattern => {
            if (pattern instanceof RegExp) {
                return pattern.test(path);
            }
            return path.includes(pattern);
        })) {
            return false;
        }

        // Check skip extensions
        if (skipExtensions.some(ext => path.endsWith(ext))) {
            return false;
        }

        // Only index text files that are reasonable size (< 500KB)
        try {
            const stat = vscode.workspace.fs.stat(uri);
            return true; // We'll check size when reading the file
        } catch {
            return false;
        }
    }

    public async indexFile(uri: vscode.Uri, action: string = 'File indexed'): Promise<void> {
        const tracker = this.performanceMonitor.startOperation('indexFile', { path: uri.fsPath, action }) as any;

        try {
            console.log(`🔍 Indexing file: ${uri.fsPath} (${action})`);

            // Check cache first
            const cacheKey = `index:${uri.fsPath}`;
            const cached = this.cache.get<{ timestamp: number; indexed: boolean }>(cacheKey);
            if (cached && Date.now() - cached.timestamp < 300000) { // 5 minute cache
                console.log(`📋 Using cached index for: ${uri.fsPath}`);
                if (tracker.complete) tracker.complete();
                return;
            }

            // Read file content with circuit breaker protection
            const fileContent = await this.circuitBreaker.execute(async () => {
                const fileBytes = await vscode.workspace.fs.readFile(uri);
                return Buffer.from(fileBytes).toString('utf8');
            });

            // Skip very large files (> 500KB)
            if (fileContent.length > 500000) {
                console.log(`⚠️ Skipping large file: ${uri.fsPath} (${fileContent.length} bytes)`);
                if (tracker.complete) tracker.complete();
                return;
            }

            // Skip empty files
            if (fileContent.trim().length === 0) {
                console.log(`⚠️ Skipping empty file: ${uri.fsPath}`);
                if (tracker.complete) tracker.complete();
                return;
            }

            const workspaceFolder = vscode.workspace.getWorkspaceFolder(uri);
            const relativePath = workspaceFolder ?
                vscode.workspace.asRelativePath(uri, false) :
                uri.fsPath;

            const embeddingContent: EmbeddingContent = {
                content: fileContent,
                metadata: {
                    type: 'workspace_file',
                    name: uri.fsPath.split('/').pop() || 'unknown',
                    uri: uri.toString(),
                    language: this.getLanguageFromPath(uri.fsPath),
                    relativePath: relativePath,
                    lastModified: Date.now()
                }
            };

            // Send to embeddings API with circuit breaker
            await this.circuitBreaker.execute(async () => {
                await this.createEmbedding(embeddingContent);
            });

            this.indexedFiles.add(uri.toString());

            // Cache successful result
            this.cache.set(cacheKey, { timestamp: Date.now(), indexed: true });

            console.log(`✅ Successfully indexed: ${relativePath}`);
            if (tracker.complete) tracker.complete();

        } catch (error) {
            console.error(`❌ Failed to index file ${uri.fsPath}:`, error);
            if (tracker.complete) tracker.complete();
        }
    }

    private async removeFileFromEmbeddings(uri: vscode.Uri): Promise<void> {
        try {
            // Since we don't have a direct remove-by-uri endpoint, we'll clear and reindex
            // This is a simple approach - in production you might want a more sophisticated solution
            console.log(`🗑️ File deleted: ${uri.fsPath}`);
            this.indexedFiles.delete(uri.toString());
        } catch (error) {
            console.error(`❌ Failed to remove file from embeddings ${uri.fsPath}:`, error);
        }
    }

    private async createEmbedding(embeddingContent: EmbeddingContent): Promise<void> {
        try {
            // Use MCP client instead of direct HTTP call
            const mcpClient = await getInitializedMCPClient();
            const result = await mcpClient.createEmbeddings(
                embeddingContent.content,
                embeddingContent.metadata,
                embeddingContent.metadata.type || 'general',
                embeddingContent.metadata.relativePath,
                undefined, // function_name - will be extracted from content if needed
                undefined, // class_name - will be extracted from content if needed
                undefined, // start_line
                undefined  // end_line
            );

            console.log(`🧠 Created embedding for ${embeddingContent.metadata.name}:`, result);

        } catch (error: any) {
            if (error?.name === 'AbortError') {
                console.warn(`⏰ Embedding creation timed out for ${embeddingContent.metadata.name}`);
            } else if (error?.message?.includes('fetch')) {
                console.warn(`🔌 Backend server not available for embeddings (${embeddingContent.metadata.name})`);
            } else {
                console.error(`❌ Failed to create embedding for ${embeddingContent.metadata.name}:`, error);
            }
            // Don't throw - allow indexing to continue even if embeddings fail
        }
    }

    // NOTE: Simple extension mapping - kept for fast file type hints
    // For deep language analysis, use MCP analyze_language_features tool
    private getLanguageFromPath(filePath: string): string {
        const ext = filePath.split('.').pop()?.toLowerCase() || '';

        const languageMap: { [key: string]: string } = {
            'ts': 'typescript',
            'tsx': 'typescriptreact',
            'js': 'javascript',
            'jsx': 'javascriptreact',
            'py': 'python',
            'java': 'java',
            'cpp': 'cpp',
            'c': 'c',
            'cs': 'csharp',
            'go': 'go',
            'rs': 'rust',
            'php': 'php',
            'rb': 'ruby',
            'swift': 'swift',
            'kt': 'kotlin',
            'html': 'html',
            'css': 'css',
            'scss': 'scss',
            'json': 'json',
            'yaml': 'yaml',
            'yml': 'yaml',
            'md': 'markdown',
            'txt': 'plaintext'
        };

        return languageMap[ext] || 'plaintext';
    }

    public async indexWorkspace(): Promise<void> {
        if (this.isIndexing) {
            console.log('⏳ Workspace indexing already in progress...');
            return;
        }

        this.isIndexing = true;
        console.log('🚀 Starting workspace indexing for embeddings...');

        try {
            const workspaceFolders = vscode.workspace.workspaceFolders;
            if (!workspaceFolders || workspaceFolders.length === 0) {
                console.log('⚠️ No workspace folders found');
                return;
            }

            let totalFiles = 0;
            let indexedCount = 0;
            let skippedCount = 0;

            for (const folder of workspaceFolders) {
                console.log(`📁 Indexing workspace folder: ${folder.uri.fsPath}`);

                // Find all files in workspace (exclude common build/virtual env directories)
                const files = await vscode.workspace.findFiles(
                    new vscode.RelativePattern(folder, '**/*'),
                    new vscode.RelativePattern(folder, '{**/node_modules/**,**/.git/**,**/dist/**,**/build/**,**/out/**,**/target/**,**/venv/**,**/.venv/**,**/*_env/**,**/env/**,**/.env/**,**/__pycache__/**,**/.pytest_cache/**,**/.mypy_cache/**,**/.tox/**,**/.coverage/**}'),
                    10000 // Max 10k files to avoid overwhelming the system
                );

                totalFiles += files.length;
                console.log(`📊 Found ${files.length} files in ${folder.name}`);

                // Process files in batches to avoid overwhelming the API
                const batchSize = 5; // Reduced from 10 to avoid rate limits
                for (let i = 0; i < files.length; i += batchSize) {
                    const batch = files.slice(i, i + batchSize);

                    await Promise.all(batch.map(async (file) => {
                        if (this.shouldIndexFile(file)) {
                            try {
                                await this.indexFile(file, 'Workspace scan');
                                indexedCount++;
                            } catch (error: any) {
                                // Handle rate limit errors gracefully
                                if (error?.message?.includes('429') || error?.message?.includes('rate limit')) {
                                    console.warn(`⚠️ Rate limit hit, pausing indexing for 5 seconds...`);
                                    await new Promise(resolve => setTimeout(resolve, 5000));
                                    skippedCount++;
                                } else {
                                    skippedCount++;
                                }
                            }
                        } else {
                            skippedCount++;
                        }
                    }));

                    // Increased delay between batches to avoid rate limits
                    await new Promise(resolve => setTimeout(resolve, 500));
                }
            }

            console.log(`✅ Workspace indexing completed!`);
            console.log(`📊 Total files: ${totalFiles}, Indexed: ${indexedCount}, Skipped: ${skippedCount}`);

            // Show completion notification
            vscode.window.showInformationMessage(
                `🧠 SuperInference: Indexed ${indexedCount} files for AI context. Files will auto-update on save.`
            );

        } catch (error) {
            console.error('❌ Error during workspace indexing:', error);
            vscode.window.showErrorMessage(`Failed to index workspace: ${error}`);
        } finally {
            this.isIndexing = false;
        }
    }

    public dispose(): void {
        this.fileWatcher.dispose();
    }

    public getStatus(): { totalIndexed: number; isIndexing: boolean } {
        return {
            totalIndexed: this.indexedFiles.size,
            isIndexing: this.isIndexing
        };
    }
}

export function activate(context: vscode.ExtensionContext) {
    console.log('SuperInference extension is now active!');

    // Set the context to enable SuperInference menus
    vscode.commands.executeCommand('setContext', 'superinference.enabled', true);

    // Initialize MCP client early during extension startup
    console.log('🔌 Initializing MCP client during extension startup...');
    getInitializedMCPClient().then(() => {
        console.log('✅ MCP client initialized successfully during startup');
    }).catch((error) => {
        console.error('❌ Failed to initialize MCP client during startup:', error);
        console.log('⚠️ MCP client will be initialized on first use');
    });

    // Initialize core systems
    const cache = new SchemaCache(1000, 30 * 60 * 1000);
    const performanceMonitor = new PerformanceMonitor();
    const circuitBreaker = new CircuitBreaker({
        failureThreshold: 5,
        recoveryTimeout: 60000,
        monitoringPeriod: 10000
    });
    const enhancedContextAnalyzer = new EnhancedContextAnalyzer(cache);

    // Initialize multi-step processor for complex reasoning (without backend service initially)
    const multiStepProcessor = new MultiStepProcessor(
        (step) => {
            // Step update callback - send to webview via chat provider
            console.log('🧠 Extension: Step update:', step);
        },
        (plan) => {
            // Plan update callback - send to webview via chat provider  
            console.log('🧠 Extension: Plan update:', plan);
        }
        // No backend service yet - will be added after chatProvider is created
    );

    // Start performance monitoring
    performanceMonitor.startMonitoring();

    // Initialize session start time
    context.globalState.update('superinference.sessionStart', Date.now());

    // Initialize embeddings manager with enhanced components
    const embeddingsManager = new WorkspaceEmbeddingsManager(performanceMonitor, circuitBreaker, cache);
    context.subscriptions.push(embeddingsManager);

    // Register the custom chat provider
    const chatProvider = new SuperInferenceChatProvider(context, multiStepProcessor);

    // Now update the multiStepProcessor with the chatProvider as backend service
    (multiStepProcessor as any).backendService = chatProvider;
    console.log('✅ MultiStepProcessor configured with real backend service');

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(
            'superinference-assistant-view',
            chatProvider,
            {
                webviewOptions: {
                    retainContextWhenHidden: true
                }
            }
        )
    );

    // Auto-index workspace when a folder is opened
    if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
        // Small delay to let everything initialize
        setTimeout(() => {
            embeddingsManager.indexWorkspace();
        }, 2000);
    }

    // Also index when a new workspace folder is added
    vscode.workspace.onDidChangeWorkspaceFolders((event) => {
        if (event.added.length > 0) {
            console.log('📁 New workspace folders added, starting indexing...');
            setTimeout(() => {
                embeddingsManager.indexWorkspace();
            }, 1000);
        }
    });

    // Register commands with enhanced components
    registerCommands(context, chatProvider, embeddingsManager, enhancedContextAnalyzer, performanceMonitor, cache, multiStepProcessor);

    // Create status bar item
    const statusBarItem = vscode.window.createStatusBarItem(
        vscode.StatusBarAlignment.Right,
        100
    );

    // Use Unicode R character which maps to SuperInference font logo
    statusBarItem.text = "AMI - SuperInference"; // Unicode R maps to SuperInference logo

    statusBarItem.tooltip = "Open SuperInference Assistant - Click to start chatting with AI";
    statusBarItem.command = "superinference.openChat";
    statusBarItem.show();
    context.subscriptions.push(statusBarItem);

    // Auto-open the assistant panel after a delay
    setTimeout(async () => {
        try {
            // First ensure the auxiliary bar is visible and SuperInference is primary
            await vscode.commands.executeCommand('workbench.action.toggleAuxiliaryBar');
            await new Promise(resolve => setTimeout(resolve, 500)); // Wait for bar to show

            // Open SuperInference assistant specifically
            await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
            await vscode.commands.executeCommand('superinference-assistant-view.focus');

            // Hide any competing views in the auxiliary bar
            try {
                await vscode.commands.executeCommand('workbench.action.closeAuxiliaryBar');
                await new Promise(resolve => setTimeout(resolve, 300));
                await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
            } catch (error) {
                console.log('Could not manage auxiliary bar views:', error);
            }

            vscode.window.showInformationMessage(
                'SuperInference Assistant is ready! 🚀\n\n' +
                'Your AI assistant now owns the right sidebar.\n\n' +
                '🧠 New: Toggle between Simple and PRE Loop reasoning modes in the chat interface!',
                'Show Framework Help',
                'Got it!'
            ).then(result => {
                if (result === 'Show Framework Help') {
                    vscode.commands.executeCommand('superinference.showFrameworkHelp');
                }
            });
        } catch (error) {
            console.log('Could not auto-open assistant:', error);
            // Fallback message
            vscode.window.showInformationMessage(
                'SuperInference Assistant is ready! 🚀\n\nLook for the sparkle icon in the right sidebar or use Ctrl+Alt+P',
                'Got it!'
            );
        }
    }, 2000);

    // Add cleanup to context
    context.subscriptions.push({
        dispose: () => {
            performanceMonitor.stopMonitoring();
            cache.clear();
            console.log('SuperInference extension cleaned up successfully');
        }
    });

    console.log('SuperInference extension fully activated with enhanced performance monitoring and caching!');
}

function registerCommands(
    context: vscode.ExtensionContext,
    chatProvider: SuperInferenceChatProvider,
    embeddingsManager: WorkspaceEmbeddingsManager,
    enhancedContextAnalyzer: EnhancedContextAnalyzer,
    performanceMonitor: PerformanceMonitor,
    cache: SchemaCache,
    multiStepProcessor: MultiStepProcessor
) {
    const commands = [
        // Main chat commands
        vscode.commands.registerCommand('superinference.openChat', async () => {
            try {
                // Show the auxiliary bar first
                await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
                // Then focus on our specific view
                await vscode.commands.executeCommand('superinference-assistant-view.focus');
            } catch (error) {
                console.log('Could not open SuperInference Assistant:', error);
                // Fallback
                vscode.commands.executeCommand('superinference-assistant-view.focus');
            }
        }),

        vscode.commands.registerCommand('superinference.clearChat', () => {
            chatProvider.clearChat();
        }),

        vscode.commands.registerCommand('superinference.focusChat', async () => {
            try {
                await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
                await vscode.commands.executeCommand('superinference-assistant-view.focus');
            } catch (error) {
                vscode.commands.executeCommand('superinference-assistant-view.focus');
            }
        }),

        // Code action commands with enhanced context
        vscode.commands.registerCommand('superinference.explainCode', () => {
            const tracker = performanceMonitor.startOperation('explainCode') as any;

            try {
                const context = enhancedContextAnalyzer.gatherContext("Please explain this code");
                if (context) {
                    const intelligentPrompt = enhancedContextAnalyzer.buildIntelligentPrompt(
                        context,
                        "Please explain this code in detail, including its purpose, functionality, and any potential issues"
                    );

                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(intelligentPrompt);
                } else {
                    vscode.window.showInformationMessage('Please open a file and select some code to explain.');
                }
            } catch (error) {
                console.error('Failed to explain code:', error);
                vscode.window.showErrorMessage('Failed to explain code. Please try again.');
            } finally {
                if (tracker.complete) tracker.complete();
            }
        }),

        vscode.commands.registerCommand('superinference.fixCode', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please fix any issues in this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to fix.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.reviewCode', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please review this code for improvements:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to review.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.generateTest', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please generate tests for this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to generate tests for.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.optimizeCode', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please optimize this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to optimize.');
                }
            }
        }),

        // Add the missing commands
        vscode.commands.registerCommand('superinference.editCode', async () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    // Get user input for edit instruction
                    const editInstruction = await vscode.window.showInputBox({
                        prompt: 'What changes would you like to make to the selected code?',
                        placeHolder: 'e.g., Add error handling, optimize performance, add comments...'
                    });

                    if (editInstruction) {
                        vscode.commands.executeCommand('superinference-assistant-view.focus');
                        // Call streamEdit directly through public method
                        await chatProvider.triggerStreamEdit(editInstruction, editor.document.uri.fsPath, editor.selection);
                    }
                } else {
                    vscode.window.showInformationMessage('Please select some code to edit.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.explain', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please explain this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to explain.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.edit', async () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    // Get user input for edit instruction
                    const editInstruction = await vscode.window.showInputBox({
                        prompt: 'What changes would you like to make to the selected code?',
                        placeHolder: 'e.g., Add error handling, optimize performance, add comments...'
                    });

                    if (editInstruction) {
                        vscode.commands.executeCommand('superinference-assistant-view.focus');
                        // Call streamEdit directly through public method
                        await chatProvider.triggerStreamEdit(editInstruction, editor.document.uri.fsPath, editor.selection);
                    }
                } else {
                    vscode.window.showInformationMessage('Please select some code to edit.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.fix', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please fix this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to fix.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.generate', () => {
            const input = vscode.window.showInputBox({
                prompt: "What code would you like me to generate?",
                placeHolder: "e.g., Create a function to sort an array"
            });

            if (input) {
                input.then(value => {
                    if (value) {
                        vscode.commands.executeCommand('superinference-assistant-view.focus');
                        chatProvider.sendMessage(`Please generate: ${value}`);
                    }
                });
            }
        }),

        vscode.commands.registerCommand('superinference.chat.review', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please review this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to review.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.test', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please generate tests for this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to generate tests for.');
                }
            }
        }),

        vscode.commands.registerCommand('superinference.chat.optimize', () => {
            const editor = vscode.window.activeTextEditor;
            if (editor) {
                const selection = editor.document.getText(editor.selection);
                if (selection) {
                    vscode.commands.executeCommand('superinference-assistant-view.focus');
                    chatProvider.sendMessage(`Please optimize this code:\n\n\`\`\`${editor.document.languageId}\n${selection}\n\`\`\``);
                } else {
                    vscode.window.showInformationMessage('Please select some code to optimize.');
                }
            }
        }),

        // Embeddings management commands
        vscode.commands.registerCommand('superinference.indexWorkspace', async () => {
            const result = await vscode.window.showInformationMessage(
                'Index all workspace files for AI context? This will create embeddings for better AI responses.',
                'Yes, Index Now',
                'Cancel'
            );

            if (result === 'Yes, Index Now') {
                await embeddingsManager.indexWorkspace();
            }
        }),

        vscode.commands.registerCommand('superinference.clearEmbeddings', async () => {
            const result = await vscode.window.showWarningMessage(
                'Clear all embeddings? This will remove AI context for all files.',
                'Yes, Clear All',
                'Cancel'
            );

            if (result === 'Yes, Clear All') {
                try {
                    const mcpClient = await getInitializedMCPClient();
                    const result = await mcpClient.clearEmbeddings();
                    vscode.window.showInformationMessage('🧠 All embeddings cleared successfully!');
                } catch (error) {
                    vscode.window.showErrorMessage(`Failed to clear embeddings: ${error}`);
                }
            }
        }),

        vscode.commands.registerCommand('superinference.embeddingsStatus', async () => {
            try {
                const mcpClient = await getInitializedMCPClient();
                const status = await mcpClient.getEmbeddingsStatus();
                const localStatus = embeddingsManager.getStatus();

                const message = `🧠 Embeddings Status:
                    
• Model: ${status.model || 'Unknown'}
• Total Entries: ${status.vector_store?.total_entries || 0}
• Locally Tracked: ${localStatus.totalIndexed}
• Currently Indexing: ${localStatus.isIndexing ? 'Yes' : 'No'}
• Smart Context: ${status.smart_context_enabled ? 'Enabled' : 'Disabled'}

Entry Types:
${Object.entries(status.vector_store?.entry_types || {})
                        .map(([type, count]) => `• ${type}: ${count}`)
                        .join('\n') || '• No entries found'}`;

                vscode.window.showInformationMessage(message, 'Refresh', 'Index Workspace').then(result => {
                    if (result === 'Refresh') {
                        vscode.commands.executeCommand('superinference.embeddingsStatus');
                    } else if (result === 'Index Workspace') {
                        vscode.commands.executeCommand('superinference.indexWorkspace');
                    }
                });
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to get embeddings status: ${error}`);
            }
        }),

        vscode.commands.registerCommand('superinference.forceReindexFile', async () => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) {
                vscode.window.showInformationMessage('No active file to reindex.');
                return;
            }

            try {
                const fileName = editor.document.fileName.split('/').pop() || 'current file';
                vscode.window.showInformationMessage(`🔄 Reindexing ${fileName}...`);

                // Force reindex by calling the indexFile method directly
                await embeddingsManager.indexFile(editor.document.uri, 'Manual reindex');

                vscode.window.showInformationMessage(`✅ Successfully reindexed ${fileName}`);
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to reindex file: ${error}`);
            }
        }),

        // Utility commands
        vscode.commands.registerCommand('superinference.enable', () => {
            vscode.commands.executeCommand('setContext', 'superinference.enabled', true);
            vscode.window.showInformationMessage('SuperInference enabled!');
        }),

        vscode.commands.registerCommand('superinference.showWelcome', () => {
            vscode.window.showInformationMessage(
                'Welcome to SuperInference! 🚀\n\nYour AI coding assistant is ready to help you with:\n\n' +
                '• Code explanation and analysis\n' +
                '• Bug fixes and improvements\n' +
                '• Code generation and completion\n' +
                '• Code reviews and best practices\n' +
                '• Test generation\n' +
                '• Performance optimization\n\n' +
                'Click the sparkle icon or use Ctrl+Alt+P to open the assistant!'
            );
        }),

        // Command to show auxiliary bar and SuperInference
        vscode.commands.registerCommand('superinference.showAuxBar', async () => {
            try {
                // Force show the auxiliary bar
                await vscode.commands.executeCommand('workbench.action.toggleAuxiliaryBar');
                await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
                await vscode.commands.executeCommand('superinference-assistant-view.focus');
                vscode.window.showInformationMessage('SuperInference Assistant opened in right sidebar!');
            } catch (error) {
                console.log('Could not show auxiliary bar:', error);
                vscode.window.showInformationMessage('Please manually open the right sidebar and look for SuperInference Assistant.');
            }
        }),

        // Command to take over the entire right sidebar
        vscode.commands.registerCommand('superinference.takeOverRightSidebar', async () => {
            try {
                vscode.window.showInformationMessage('Taking over right sidebar... 🚀');

                // Step 1: Close the auxiliary bar completely
                await vscode.commands.executeCommand('workbench.action.closeAuxiliaryBar');
                await new Promise(resolve => setTimeout(resolve, 500));

                // Step 2: Try to disable/hide Copilot Chat views aggressively
                const copilotCommands = [
                    'workbench.view.extension.github-copilot-chat.removeView',
                    'workbench.view.extension.copilot-chat.removeView',
                    'workbench.view.extension.ms-vscode.copilot-chat.removeView',
                    'github.copilot.chat.close',
                    'github.copilot.chat.hide',
                    'workbench.action.closePanel'
                ];

                for (const cmd of copilotCommands) {
                    try {
                        await vscode.commands.executeCommand(cmd);
                        await new Promise(resolve => setTimeout(resolve, 100));
                    } catch (error) {
                        // Ignore errors - these commands may not exist
                    }
                }

                // Step 3: Set context to disable Copilot chat UI
                try {
                    await vscode.commands.executeCommand('setContext', 'github.copilot.chat.enabled', false);
                    await vscode.commands.executeCommand('setContext', 'copilot.chat.enabled', false);
                    await vscode.commands.executeCommand('setContext', 'superinference.isOnlyRightSidebar', true);
                } catch (error) {
                    console.log('Could not set contexts:', error);
                }

                // Step 4: Wait and then open ONLY SuperInference
                await new Promise(resolve => setTimeout(resolve, 700));

                // Step 5: Open SuperInference as the sole occupant
                await vscode.commands.executeCommand('workbench.view.extension.superinference-assistant');
                await new Promise(resolve => setTimeout(resolve, 300));
                await vscode.commands.executeCommand('superinference-assistant-view.focus');

                // Step 6: Force auxiliary bar to show only SuperInference
                await vscode.commands.executeCommand('workbench.action.focusAuxiliaryBar');

                // Step 7: Try to move any remaining Copilot views to the panel area
                try {
                    const moveCommands = [
                        'workbench.action.moveViewToPanel',
                        'workbench.view.extension.github-copilot-chat.moveToPanel',
                        'workbench.view.extension.copilot-chat.moveToPanel'
                    ];

                    for (const cmd of moveCommands) {
                        try {
                            await vscode.commands.executeCommand(cmd);
                        } catch (error) {
                            // Ignore
                        }
                    }
                } catch (error) {
                    console.log('Could not move Copilot views:', error);
                }

                vscode.window.showInformationMessage(
                    '✅ SuperInference Assistant now exclusively owns the right sidebar! Copilot moved to bottom panel.',
                    'Perfect!'
                );

            } catch (error) {
                console.log('Could not completely take over right sidebar:', error);
                vscode.window.showInformationMessage(
                    'SuperInference Assistant is active in right sidebar. You may need to manually close Copilot tabs.',
                    'OK'
                );
            }
        }),

        // Add a command to restore normal layout if needed
        vscode.commands.registerCommand('superinference.restoreLayout', async () => {
            try {
                await vscode.commands.executeCommand('setContext', 'github.copilot.chat.enabled', true);
                await vscode.commands.executeCommand('setContext', 'copilot.chat.enabled', true);
                await vscode.commands.executeCommand('setContext', 'superinference.isOnlyRightSidebar', false);
                vscode.window.showInformationMessage('Layout restored! Both SuperInference and Copilot are available.');
            } catch (error) {
                console.log('Could not restore layout:', error);
            }
        }),

        // Performance and diagnostics commands
        vscode.commands.registerCommand('superinference.showPerformanceMetrics', async () => {
            const metrics = performanceMonitor.getMetrics();
            const cacheStats = cache.getStats();
            const embeddingsStatus = embeddingsManager.getStatus();

            // Calculate additional metrics
            const sessionStart = context.globalState.get<number>('superinference.sessionStart') || Date.now();
            const sessionDuration = Date.now() - sessionStart;
            const tokensPerMinute = metrics.totalOperations > 0 ? (metrics.totalOperations * 60000) / sessionDuration : 0;
            const cacheHitRate = cacheStats.size > 0 ? (cacheStats.averageAccessCount / cacheStats.size) * 100 : 0;

            const message = `📊 SuperInference Extension Performance Metrics:

🔄 Operations (Last 5 min):
• Total: ${metrics.totalOperations}
• Average Duration: ${Math.round(metrics.averageDuration)}ms
• Max Duration: ${Math.round(metrics.maxDuration)}ms
• Min Duration: ${Math.round(metrics.minDuration)}ms
• Ops/min: ${Math.round(tokensPerMinute)}

💾 Cache Performance:
• Size: ${cacheStats.size} entries
• Average Access: ${Math.round(cacheStats.averageAccessCount)} hits/entry
• Hit Rate: ${Math.round(cacheHitRate)}%
• Memory Usage: ${cacheStats.memoryUsage} items

🧠 System Memory:
• Heap Used: ${Math.round(metrics.memoryUsage.heapUsed / 1024 / 1024)}MB
• Heap Total: ${Math.round(metrics.memoryUsage.heapTotal / 1024 / 1024)}MB
• External: ${Math.round(metrics.memoryUsage.external / 1024 / 1024)}MB
• RSS: ${Math.round(metrics.memoryUsage.rss / 1024 / 1024)}MB

📂 Embeddings:
• Indexed Files: ${embeddingsStatus.totalIndexed}
• Currently Indexing: ${embeddingsStatus.isIndexing ? 'Yes' : 'No'}

⏱️ Session Duration: ${Math.round(sessionDuration / 1000 / 60)} minutes`;

            vscode.window.showInformationMessage(message,
                'Clear Cache',
                'View Slow Operations',
                'Copy to Clipboard',
                'Reset Session'
            ).then(result => {
                if (result === 'Clear Cache') {
                    cache.clear();
                    vscode.window.showInformationMessage('✅ Cache cleared successfully!');
                } else if (result === 'View Slow Operations') {
                    const slowOps = performanceMonitor.getSlowOperations(3000);
                    if (slowOps.length > 0) {
                        const slowOpsList = slowOps.map(op => `• ${op.name}: ${op.duration}ms`).join('\n');
                        vscode.window.showInformationMessage(`🐌 Slow Operations (>3s):\n\n${slowOpsList}`);
                    } else {
                        vscode.window.showInformationMessage('✅ No slow operations detected!');
                    }
                } else if (result === 'Copy to Clipboard') {
                    vscode.env.clipboard.writeText(message.replace(/[📊🔄💾🧠📂⏱️]/g, ''));
                    vscode.window.showInformationMessage('✅ Metrics copied to clipboard!');
                } else if (result === 'Reset Session') {
                    context.globalState.update('superinference.sessionStart', Date.now());
                    cache.clear();
                    vscode.window.showInformationMessage('✅ Session metrics reset!');
                }
            });
        }),

        // Multi-step reasoning test command
        vscode.commands.registerCommand('superinference.testMultiStepReasoning', async () => {
            try {
                const instruction = await vscode.window.showInputBox({
                    prompt: 'Enter a complex instruction to test multi-step reasoning',
                    placeHolder: 'e.g., "Implement a new component with error handling and tests"'
                });

                if (instruction) {
                    vscode.window.showInformationMessage('🧠 Starting multi-step reasoning process...');

                    const plan = await multiStepProcessor.processComplexInstruction(instruction);

                    const duration = plan.completedAt ? (plan.completedAt.getTime() - plan.createdAt.getTime()) / 1000 : 0;
                    const completedSteps = plan.steps.filter(s => s.status === 'completed').length;

                    vscode.window.showInformationMessage(
                        `✅ Multi-step reasoning completed!\n` +
                        `📋 ${completedSteps}/${plan.steps.length} steps completed\n` +
                        `⏱️ Duration: ${duration.toFixed(1)}s\n` +
                        `Status: ${plan.status}`,
                        'View Details'
                    ).then(result => {
                        if (result === 'View Details') {
                            console.log('Full reasoning plan:', plan);
                            const stepDetails = plan.steps.map(s =>
                                `${s.status === 'completed' ? '✅' : s.status === 'failed' ? '❌' : '⏳'} ${s.title}: ${s.output || s.error || 'In progress'}`
                            ).join('\n');
                            vscode.window.showInformationMessage(`Step Details:\n\n${stepDetails}`);
                        }
                    });
                }
            } catch (error) {
                vscode.window.showErrorMessage(`Multi-step reasoning failed: ${error}`);
            }
        }),

        // New: Theoretical framework configuration commands
        vscode.commands.registerCommand('superinference.enablePRELoop', async () => {
            try {
                const result = await vscode.window.showInformationMessage(
                    '🧠 Enable SuperInference PRE Loop?\n\n' +
                    'This activates the theoretical event-driven Planner-Retriever-Executor framework ' +
                    'from the paper for more sophisticated reasoning with belief states and critic-gated memory.',
                    'Enable PRE Loop',
                    'Keep Simple Mode'
                );

                if (result === 'Enable PRE Loop') {
                    // Send message to chat provider to enable PRE loop
                    chatProvider.updateReasoningMode('pre_loop');
                    vscode.window.showInformationMessage('✅ PRE Loop enabled! Your next requests will use event-driven reasoning.');
                }
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to enable PRE Loop: ${error}`);
            }
        }),

        vscode.commands.registerCommand('superinference.configurePlanningParameters', async () => {
            try {
                const mcpClient = await getInitializedMCPClient();

                // Get current configuration
                const currentConfig = await mcpClient.callTool({
                    name: 'get_performance_metrics',
                    arguments: {}
                });

                const config = currentConfig?.content?.[0] ? 
                    JSON.parse(currentConfig.content[0].text || '{}')?.config?.planning_config || {} : {};

                // Show configuration dialog
                const tau = await vscode.window.showInputBox({
                    prompt: 'Event Threshold (τ) - Lower values trigger more reasoning events',
                    value: (config.tau_event_threshold || 0.05).toString(),
                    validateInput: (value) => {
                        const num = parseFloat(value);
                        return (isNaN(num) || num < 0 || num > 1) ? 'Must be a number between 0 and 1' : undefined;
                    }
                });

                if (tau === undefined) return;

                const kappa = await vscode.window.showInputBox({
                    prompt: 'Confidence Stop (κ) - Stop when belief exceeds this threshold',
                    value: (config.kappa_confidence_stop || 0.85).toString(),
                    validateInput: (value) => {
                        const num = parseFloat(value);
                        return (isNaN(num) || num < 0 || num > 1) ? 'Must be a number between 0 and 1' : undefined;
                    }
                });

                if (kappa === undefined) return;

                const epsilon = await vscode.window.showInputBox({
                    prompt: 'Minimum EIG (ε) - Stop when information gain falls below this',
                    value: (config.epsilon_min_eig || 0.015).toString(),
                    validateInput: (value) => {
                        const num = parseFloat(value);
                        return (isNaN(num) || num < 0 || num > 1) ? 'Must be a number between 0 and 1' : undefined;
                    }
                });

                if (epsilon === undefined) return;

                const maxEvents = await vscode.window.showInputBox({
                    prompt: 'Maximum Events - Budget limit for reasoning events',
                    value: (config.max_events || 6).toString(),
                    validateInput: (value) => {
                        const num = parseInt(value);
                        return (isNaN(num) || num < 1 || num > 20) ? 'Must be a number between 1 and 20' : undefined;
                    }
                });

                if (maxEvents === undefined) return;

                // Update configuration
                const updateResult = await mcpClient.updatePlanningConfig({
                    tau_event_threshold: parseFloat(tau),
                    kappa_confidence_stop: parseFloat(kappa),
                    epsilon_min_eig: parseFloat(epsilon),
                    max_events: parseInt(maxEvents)
                });

                if (updateResult.success) {
                    vscode.window.showInformationMessage(
                        `✅ Planning parameters updated!\n` +
                        `τ = ${tau}, κ = ${kappa}, ε = ${epsilon}, max_events = ${maxEvents}`
                    );
                } else {
                    vscode.window.showErrorMessage(`Failed to update parameters: ${updateResult.error}`);
                }

            } catch (error) {
                vscode.window.showErrorMessage(`Failed to configure parameters: ${error}`);
            }
        }),

        vscode.commands.registerCommand('superinference.showTheoreticalMetrics', async () => {
            try {
                const mcpClient = await getInitializedMCPClient();
                const metrics = await mcpClient.getPerformanceMetrics();
                
                const planningConfig = metrics?.config?.planning_config || {};
                
                const message = `🧠 SuperInference Theoretical Parameters:

📊 **Current Configuration:**
• Event Threshold (τ): ${planningConfig.tau_event_threshold || 'N/A'}
• Confidence Stop (κ): ${planningConfig.kappa_confidence_stop || 'N/A'}  
• Min EIG (ε): ${planningConfig.epsilon_min_eig || 'N/A'}
• Max Events: ${planningConfig.max_events || 'N/A'}
• Max Steps: ${planningConfig.max_steps || 'N/A'}
• Critic Threshold: ${planningConfig.critic_threshold || 'N/A'}

🔬 **Framework Status:**
• Benchmark Mode: ${metrics?.config?.benchmark_mode ? 'Enabled' : 'Disabled'}
• Vector Store Entries: ${metrics?.server?.vector_store_entries || 0}
• Active Requests: ${metrics?.server?.active_requests || 0}`;

                vscode.window.showInformationMessage(message,
                    'Configure Parameters',
                    'Enable PRE Loop',
                    'View Paper'
                ).then(result => {
                    if (result === 'Configure Parameters') {
                        vscode.commands.executeCommand('superinference.configurePlanningParameters');
                    } else if (result === 'Enable PRE Loop') {
                        vscode.commands.executeCommand('superinference.enablePRELoop');
                    } else if (result === 'View Paper') {
                        vscode.env.openExternal(vscode.Uri.parse('https://arxiv.org/abs/2501.00000')); // Update with actual paper URL
                    }
                });
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to get metrics: ${error}`);
            }
        }),

        vscode.commands.registerCommand('superinference.interactivePRELoop', async () => {
            try {
                const instruction = await vscode.window.showInputBox({
                    prompt: 'Enter instruction for interactive PRE loop execution',
                    placeHolder: 'e.g., "Implement user authentication with validation and tests"'
                });

                if (instruction) {
                    vscode.window.showInformationMessage('🧠 Starting interactive PRE loop...');
                    
                    // Focus on the assistant and trigger interactive execution
                    await vscode.commands.executeCommand('superinference-assistant-view.focus');
                    
                    // Send the instruction to the chat provider for interactive PRE loop
                    chatProvider.triggerInteractivePRELoop(instruction);
                }
            } catch (error) {
                vscode.window.showErrorMessage(`Interactive PRE loop failed: ${error}`);
            }
        }),

        // PRE loop is now MANDATORY (paper compliance) - no toggle needed
        vscode.commands.registerCommand('superinference.showPRELoopInfo', async () => {
            vscode.window.showInformationMessage(
                '🧠 SuperInference PRE Loop Mode (MANDATORY)\n\n' +
                'Always using the theoretical event-driven framework with:\n' +
                '• Subquestion/Subanswer decomposition (SQ→SA→SQ→SA)\n' +
                '• Planner-Retriever-Executor-Critic architecture\n' +
                '• Belief state tracking with success probabilities\n' +
                '• Expected Information Gain (EIG) triggering\n' +
                '• Critic-gated memory updates (M_t → M_{t+1})\n' +
                '• POMDP-based reasoning\n\n' +
                'This mode cannot be disabled to ensure paper compliance.',
                'View Paper',
                'Configure Parameters'
            ).then(result => {
                if (result === 'View Paper') {
                    vscode.env.openExternal(vscode.Uri.parse('https://arxiv.org/abs/2501.00000'));
                } else if (result === 'Configure Parameters') {
                    vscode.commands.executeCommand('superinference.configurePlanningParameters');
                }
            });
        }),

        // Add command for quick access to theoretical framework
        vscode.commands.registerCommand('superinference.showFrameworkHelp', async () => {
            const message = `🧠 **SuperInference Theoretical Framework**

**PRE Loop Mode (MANDATORY):**
• Event-driven Planner-Retriever-Executor loop
• Belief state tracking with success probabilities
• Expected Information Gain (EIG) based triggering
• Critic-gated memory for approved artifacts
• POMDP formulation with noisy channels

**Key Parameters:**
• τ (tau): Event threshold for triggering reasoning
• κ (kappa): Confidence threshold for stopping
• ε (epsilon): Minimum EIG for continued reasoning

**Usage:**
1. PRE loop is always active (cannot be disabled)
2. Use "Configure Planning Parameters" for tuning τ, κ, ε
3. Try "Interactive PRE Loop" for step-by-step execution

Based on the SuperInference paper's theoretical framework.`;

            vscode.window.showInformationMessage(message,
                'Configure Parameters',
                'Interactive Demo',
                'Show PRE Loop Info'
            ).then(result => {
                if (result === 'Configure Parameters') {
                    vscode.commands.executeCommand('superinference.configurePlanningParameters');
                } else if (result === 'Interactive Demo') {
                    vscode.commands.executeCommand('superinference.interactivePRELoop');
                }
            });
        })
    ];

    context.subscriptions.push(...commands);
}

export function deactivate() {
    vscode.commands.executeCommand('setContext', 'superinference.enabled', false);
    console.log('SuperInference extension is now deactivated!');
} 