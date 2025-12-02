import { IChatService } from '../../common/chatService';
import { MessageItemModel } from '../../common/chatService/model';
import { BACKEND_CONFIG } from '../../common/chatService';
import * as vscode from 'vscode';
import * as path from 'path';
import { getSnapshotManager } from '../snapshotManager';
import { getInitializedMCPClient } from '../../common/mcp-client';

export interface ChatServiceClient {
    handleReadyStateChange(isReady: boolean): void;
    handleNewMessage(msg: MessageItemModel): void;
    handleMessageChange(msg: MessageItemModel): void;
    handleClearMessage(): void;
    handleCheckpointRestored?(snapshotId: string): void;
    handleClearMessagesAfterCheckpoint?(checkpointMessageId: string): void;
}

// Helper function to remove a method from code
// DEPRECATED: Hardwired Python method removal - should use dynamic MCP tools
// TODO: Replace all calls with: await mcpClient.streamEdit(code, `remove ${methodName}`, filePath, language)
function removeMethodFromCode_DEPRECATED(code: string, methodName: string): string {
    if (!code || !methodName) return code;

    // Simple regex to remove method - this is basic and could be improved
    // NOTE: This only works for Python 'def' statements
    // For language-agnostic removal, use MCP stream_edit with analyze_language_features
    const methodRegex = new RegExp(`\\s*def\\s+${methodName}\\s*\\([^)]*\\):\\s*[\\s\\S]*?(?=\\n\\s*def|\\n\\s*class|\\n\\s*$|$)`, 'g');
    return code.replace(methodRegex, '').trim();
}

// DEPRECATED: Hardwired language mapping - should use MCP analyze_language_features for accurate detection
// Keeping as fast fallback for simple extension hints
function getLanguageFromExtension_DEPRECATED(extension: string): string {
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
        'scala': 'scala',
        'sh': 'shellscript',
        'bash': 'shellscript',
        'ps1': 'powershell',
        'sql': 'sql',
        'html': 'html',
        'css': 'css',
        'scss': 'scss',
        'less': 'less',
        'xml': 'xml',
        'json': 'json',
        'yaml': 'yaml',
        'yml': 'yaml',
        'md': 'markdown',
        'txt': 'plaintext'
    };

    return languageMap[extension.toLowerCase()] || 'plaintext';
}

// DEPRECATED: Hardwired code change detection - should use MCP analyze_request_intent and analyze_code_structure
// This function uses hardcoded patterns for detecting code changes
// TODO: Replace with async MCP call to analyze_code_structure when streaming supports it
function detectCodeChanges_DEPRECATED(content: string, contextFiles?: any[]): MessageItemModel['codeEdit'] | null {
    // Look for patterns that indicate code changes
    // NOTE: This is a synchronous fallback. Ideally should be replaced with:
    // const analysis = await mcpClient.analyzeCodeStructure(content, filePath);
    // const hasCodeChanges = analysis.analysis.structure.functions.length > 0;
    const patterns = [
        // Pattern 0: "File: relative/path" followed by code fence (typical create response)
        /^\s*File:\s*([^\n]+)\n```[\w-]*\n([\s\S]*?)\n```/i,
        // Pattern 1: "Here's the modified code for X" format
        /(?:Here's|Here is)\s+the\s+modified\s+code\s+for\s+([^:\n]+):\s*\n+```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 2: File path with code blocks
        /(?:Here's|Here is|I'll|I will|Let me|Let's)\s+(?:update|modify|change|edit|fix|remove|add to|replace)\s+(?:the|this|that)?\s*(?:method|function|code|file|line|lines?)\s*(?:in|from|at|to)?\s*[`"']?([^`"'\n]+\.(ts|js|tsx|jsx|py|java|cpp|c|cs|go|rs|php|rb|swift|kt|html|css|scss|less|sql|json|xml|yaml|yml|toml|ini|cfg|conf|txt|md))[`"']?[:\s]*\n+```\w*\n([\s\S]*?)\n```/i,

        // Pattern 3: Diff-like format
        /(?:File|Path|In):\s*[`"']?([^`"'\n]+\.(ts|js|tsx|jsx|py|java|cpp|c|cs|go|rs|php|rb|swift|kt|html|css|scss|less|sql|json|xml|yaml|yml|toml|ini|cfg|conf|txt|md))[`"']?[:\s]*\n+```\w*\n([\s\S]*?)\n```/i,

        // Pattern 4: Simple "remove X from Y" patterns
        /(?:Remove|Delete|Take out|Eliminate)\s+(?:the|this|that)?\s*(?:method|function|code|line|lines?)\s*[`"']?([^`"'\n]+)[`"']?\s*(?:from|in)\s*[`"']?([^`"'\n]+\.(ts|js|tsx|jsx|py|java|cpp|c|cs|go|rs|php|rb|swift|kt|html|css|scss|less|sql|json|xml|yaml|yml|toml|ini|cfg|conf|txt|md))[`"']?/i,

        // Pattern 5: Generic code block with file mention
        /```(\w+)?\n([\s\S]*?)\n```[\s\S]*?(?:file|in|to|from|update|modify|change|edit|fix|remove|add to|replace)[\s\S]*?[`"']?([^`"'\n]+\.(ts|js|tsx|jsx|py|java|cpp|c|cs|go|rs|php|rb|swift|kt|html|css|scss|less|sql|json|xml|yaml|yml|toml|ini|cfg|conf|txt|md))[`"']?/i,

        // Pattern 6: Simple file mention with code block
        /([^`"'\n]+\.(ts|js|tsx|jsx|py|java|cpp|c|cs|go|rs|php|rb|swift|kt|html|css|scss|less|sql|json|xml|yaml|yml|toml|ini|cfg|conf|txt|md))[\s\S]*?```\w*\n([\s\S]*?)\n```/i,

        // Pattern 7: Code block with modification context (removed/updated/modified)
        /(?:removed?|updated?|modified?|changed?|fixed?|edited?)[\s\S]*?```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 8: Code block after removal/modification request
        /(?:remove|delete|take out|eliminate|update|modify|change|edit|fix)[\s\S]*?```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 9: Specific removal patterns (remove X commands/statements)
        /(?:remove|delete|take out|eliminate)\s+(?:all\s+)?(?:the\s+)?(?:`[^`]+`|[a-zA-Z_][a-zA-Z0-9_]*)\s*(?:commands?|statements?|calls?|functions?|methods?|lines?)(?:\s+(?:from|in)\s+(?:the\s+)?(?:context\s+)?file)?[\s\S]*?```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 10: Code response with file modification indicators
        /(?:I\s+(?:have\s+)?(?:will\s+)?(?:removed?|updated?|modified?|changed?|fixed?|edited?))[\s\S]*?```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 11: Applied changes format
        /(?:I\s+have\s+applied\s+the\s+requested\s+changes?)[\s\S]*?```(\w+)?\n([\s\S]*?)\n```/i,

        // Pattern 12: Just a code block (when context files are available)
        /```(\w+)?\n([\s\S]*?)\n```/i
    ];

    for (const pattern of patterns) {
        const match = content.match(pattern);
        if (match) {
            let filePath: string | null = null;
            let newCode: string;
            let methodName: string | null = null;
            let language: string | null = null;

            if (pattern === patterns[0]) {
                // Pattern 0: File header + code block
                filePath = match[1];
                newCode = match[2];
            } else if (pattern === patterns[1]) {
                // Pattern 1: "Here's the modified code for X" format
                filePath = match[1];
                language = match[2];
                newCode = match[3];
            } else if (pattern === patterns[2] || pattern === patterns[3]) {
                // Pattern 2 & 3: File path with code block
                filePath = match[1];
                newCode = match[3] || match[2];
            } else if (pattern === patterns[4]) {
                // Pattern 4: Remove method from file
                methodName = match[1];
                filePath = match[2];
                newCode = `// ${methodName} has been removed`;
            } else if (pattern === patterns[5]) {
                // Pattern 5: Generic code block with file mention
                language = match[1];
                newCode = match[2];
                filePath = match[3];
            } else if (pattern === patterns[6]) {
                // Pattern 6: Simple file mention with code block
                filePath = match[1];
                newCode = match[3];
            } else if (pattern === patterns[7] || pattern === patterns[8]) {
                // Pattern 7 & 8: Code block with modification context
                language = match[1];
                newCode = match[2];

                // Try to get file path from context
                if (contextFiles && contextFiles.length > 0) {
                    const contextFile = contextFiles[0];
                    filePath = contextFile.name || contextFile.uri || 'unknown';
                    if (!language && contextFile.language) {
                        language = contextFile.language;
                    }
                }
            } else if (pattern === patterns[9] || pattern === patterns[10] || pattern === patterns[11]) {
                // Pattern 9, 10, 11: Specific removal patterns and code response with file modification
                language = match[1];
                newCode = match[2];

                // Try to get file path from context
                if (contextFiles && contextFiles.length > 0) {
                    const contextFile = contextFiles[0];
                    filePath = contextFile.name || contextFile.uri || 'unknown';
                    if (!language && contextFile.language) {
                        language = contextFile.language;
                    }
                }
            } else if (pattern === patterns[12]) {
                // Pattern 12: Just a code block (fallback with context)
                language = match[1];
                newCode = match[2];

                // Only use this pattern if we have context files
                if (contextFiles && contextFiles.length > 0) {
                    const contextFile = contextFiles[0];
                    filePath = contextFile.name || contextFile.uri || 'unknown';
                    if (!language && contextFile.language) {
                        language = contextFile.language;
                    }
                } else {
                    continue; // Skip this pattern if no context
                }
            } else {
                continue;
            }

            // Skip if we couldn't determine a file path
            if (!filePath) {
                continue;
            }

            // Clean up the code
            newCode = newCode.trim();

            // Detect language from file extension or provided language
            const fileExtension = filePath.split('.').pop()?.toLowerCase() || '';
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
                'less': 'less',
                'sql': 'sql',
                'json': 'json',
                'xml': 'xml',
                'yaml': 'yaml',
                'yml': 'yaml',
                'md': 'markdown'
            };

            const detectedLanguage = language || languageMap[fileExtension] || 'plaintext';

            // Get original code from context if available
            let originalCode = '// Original code';
            if (contextFiles && contextFiles.length > 0 && filePath) {
                const contextFile = contextFiles.find(f =>
                    f.name === filePath ||
                    (f.uri && f.uri.includes(filePath)) ||
                    (filePath && filePath.includes(f.name))
                );
                if (contextFile && contextFile.content) {
                    originalCode = contextFile.content;
                }
            }

            // Special handling for removal requests - now that originalCode is available
            if (!newCode || newCode.length < 10) {
                // If newCode is empty or very short, try to generate it from original code
                if (methodName && originalCode) {
                    // For removal requests, create the modified code by removing the method
                    newCode = removeMethodFromCode_DEPRECATED(originalCode, methodName);
                } else if (originalCode && (
                    content.toLowerCase().includes('remove') ||
                    content.toLowerCase().includes('delete')
                )) {
                    // For general removal requests, try to extract what to remove
                    const removeMatch = content.match(/(?:remove|delete).*?([a-zA-Z_][a-zA-Z0-9_]*)/i);
                    if (removeMatch) {
                        const elementToRemove = removeMatch[1];
                        newCode = removeMethodFromCode_DEPRECATED(originalCode, elementToRemove);
                    } else {
                        newCode = originalCode; // Fallback to original if we can't determine what to remove
                    }
                } else {
                    newCode = originalCode; // Fallback for other cases
                }
            }

            return {
                filePath: filePath || 'unknown',
                originalCode,
                newCode,
                startLine: 1,
                endLine: Math.max(1, newCode.split('\n').length),
                explanation: methodName ? `Removed ${methodName} method` : 'Code updated'
            };
        }
    }

    return null;
}

// NOTE: Keep for text cleaning - not language-specific
function sanitizeAnalysis(input: string): string {
    try {
        let text = input || '';
        // Remove repeated "File: path" headers that are duplicated
        text = text.replace(/(^|\n)File:\s+[^\n]+/g, (match: string, _ignored: string, offset: number, full: string) => {
            // Keep the first occurrence, drop immediate duplicates
            return (offset > 0 && full.substring(0, offset).includes(match.trim())) ? '' : match;
        });
        // Collapse 3+ blank lines
        text = text.replace(/\n{3,}/g, '\n\n');
        return text.trimStart();
    } catch {
        return input || '';
    }
}

export class ChatServiceImpl implements IChatService {
    get name(): string {
        return 'SuperInference Chat Service';
    }

    #clients: ChatServiceClient[] = [];
    #messages: MessageItemModel[] = [];
    #messageIndex = new Map<string, MessageItemModel>();
    #currentMessageId = 0;
    #currentAbortController: AbortController | null = null;
    #isReady = true;
    #messageCheckpoints = new Map<string, string>(); // messageId -> snapshotId
    #restoredSnapshots = new Set<string>(); // Track which snapshots have been restored

    constructor() {
        console.log('ChatServiceImpl: Initializing with backend URL:', BACKEND_CONFIG.baseUrl);
    }

    // Client management
    attachClient(client: ChatServiceClient): void {
        this.#clients.push(client);
        client.handleReadyStateChange(this.#isReady);
    }

    detachClient(client: ChatServiceClient): void {
        const index = this.#clients.indexOf(client);
        if (index >= 0) {
            this.#clients.splice(index, 1);
        }
    }

    // Add message to timeline (used by provider to forward structured edits)
    async addMessage(
        contents: string,
        isReply: boolean = false,
        codeEdit?: MessageItemModel['codeEdit'],
        messageType?: 'text' | 'code' | 'diff' | 'file-edit'
    ): Promise<string> {
        const messageId = (++this.#currentMessageId).toString();
        const message: MessageItemModel = {
            id: messageId,
            contents,
            isReply,
            isFinished: true,
            codeEdit,
            messageType,
            timestamp: Date.now()
        };

        // Auto-checkpoint for file edits
        let snapshotId: string | undefined;
        if (codeEdit && codeEdit.filePath) {
            try {
                const snapshotManager = getSnapshotManager();
                const checkpointLabel = `before-edit-${Date.now()}`;
                snapshotId = await snapshotManager.createSnapshot(
                    checkpointLabel,
                    `Before applying edit to ${path.basename(codeEdit.filePath)}: ${contents.slice(0, 100)}...`
                );
                this.#messageCheckpoints.set(messageId, snapshotId);
                this.#clients.forEach(client => {
                    if ('handleSnapshotCreated' in client) {
                        (client as any).handleSnapshotCreated(messageId, snapshotId);
                    }
                });
            } catch (error) {
                console.error('Failed to create auto-checkpoint:', error);
            }
        }

        this.#messages.push(message);
        this.#messageIndex.set(messageId, message);
        this.#clients.forEach(client => client.handleNewMessage(message));
        return messageId;
    }

    // Ready state management
    setReady(isReady: boolean): void {
        this.#isReady = isReady;
        this.#clients.forEach(client => client.handleReadyStateChange(isReady));
    }

    isReady(): boolean {
        return this.#isReady;
    }

    // Message management
    async confirmPrompt(prompt: string): Promise<void> {
        console.log('ChatServiceImpl: confirmPrompt called with:', prompt);
        await this.streamChat(prompt);
    }

    async syncState(): Promise<void> {
        console.log('ChatServiceImpl: syncState called');
        // Sync with backend if needed
    }

    async insertCodeSnippet(contents: string): Promise<void> {
        console.log('ChatServiceImpl: insertCodeSnippet called with:', contents);
        // This would typically insert code into the active editor
        // For now, we'll just log it
    }

    clearSession(): void {
        console.log('ChatServiceImpl: clearSession called');
        this.#messages = [];
        this.#messageIndex.clear();
        this.#currentMessageId = 0;
        this.#messageCheckpoints.clear();
        this.#restoredSnapshots.clear();

        if (this.#currentAbortController) {
            this.#currentAbortController.abort();
            this.#currentAbortController = null;
        }

        this.#clients.forEach(client => client.handleClearMessage());
    }

    // Check if a snapshot has been restored
    isSnapshotRestored(snapshotId: string): boolean {
        return this.#restoredSnapshots.has(snapshotId);
    }

    // Add checkpoint mapping (called when snapshots are created externally)
    addCheckpointMapping(messageId: string, snapshotId: string): void {
        console.log('📸 ChatService: *** ADDING *** checkpoint mapping:', messageId, '→', snapshotId);
        this.#messageCheckpoints.set(messageId, snapshotId);
    }

    // Enhanced streaming implementation for Python backend
    async streamChat(prompt: string, contextFiles?: any[]): Promise<void> {
        try {
            console.log('🚀 ChatServiceImpl: Starting streamChat');

            // Cancel any ongoing request
            if (this.#currentAbortController) {
                this.#currentAbortController.abort();
            }
            this.#currentAbortController = new AbortController();

            // Prepare request payload for Python backend
            const payload = {
                prompt: prompt,
                contextFiles: contextFiles || [],
                apiKey: 'your-api-key-here' // Would come from configuration
            };

            // Add user message
            const userMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: prompt,
                isReply: false,
                isFinished: true,
                timestamp: Date.now()
            };

            this.#messages.push(userMessage);
            this.#messageIndex.set(userMessage.id, userMessage);
            this.#clients.forEach(client => client.handleNewMessage(userMessage));

            // Start with an assistant message for streaming
            const assistantMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: '',
                isReply: true,
                isFinished: false,
                timestamp: Date.now()
            };

            this.#messages.push(assistantMessage);
            this.#messageIndex.set(assistantMessage.id, assistantMessage);
            console.log('🚀 ChatServiceImpl: *** SENDING INITIAL ASSISTANT MESSAGE ***', {
                id: assistantMessage.id,
                contents: assistantMessage.contents,
                isReply: assistantMessage.isReply,
                isFinished: assistantMessage.isFinished
            });
            this.#clients.forEach(client => client.handleNewMessage(assistantMessage));

            // Make streaming request to Python backend
            const response = await fetch(`${BACKEND_CONFIG.baseUrl}${BACKEND_CONFIG.endpoints.streamChat}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(payload),
                signal: this.#currentAbortController.signal
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            // Handle streaming response
            const reader = response.body?.getReader();
            if (!reader) {
                throw new Error('No response body reader available');
            }

            const decoder = new TextDecoder();
            let buffer = '';
            let accumulatedContent = '';

            while (true) {
                const { done, value } = await reader.read();

                if (done) {
                    console.log('🚀 ChatServiceImpl: Stream reader finished');
                    break;
                }

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop() || ''; // Keep incomplete line in buffer

                for (const line of lines) {
                    if (line.trim() === '') continue;

                    if (line.startsWith('data: ')) {
                        const dataStr = line.substring(6);

                        try {
                            const data = JSON.parse(dataStr);

                            // Handle different response types
                            if (data.type === 'codeEdit') {
                                console.log('🚀 ChatServiceImpl: Received structured code edit response');

                                // Create a NEW message for each codeEdit instead of overwriting
                                const codeEditMessage: MessageItemModel = {
                                    id: (++this.#currentMessageId).toString(),
                                    contents: data.content || 'Code edit ready to apply',
                                    isReply: true,
                                    isFinished: true,
                                    codeEdit: data.codeEdit,
                                    messageType: 'file-edit',
                                    timestamp: Date.now()
                                };

                                // Create snapshot BEFORE applying code edit
                                let snapshotId: string | undefined;
                                try {
                                    const snapshotManager = getSnapshotManager();
                                    const checkpointLabel = `before-edit-${Date.now()}`;
                                    snapshotId = await snapshotManager.createSnapshot(
                                        checkpointLabel,
                                        data.content || 'LLM code edit'
                                    );
                                    console.log('🔖 Auto-created snapshot for code edit:', snapshotId);

                                    // Notify clients about the snapshot creation
                                    this.#clients.forEach(client => {
                                        if ('handleSnapshotCreated' in client) {
                                            (client as any).handleSnapshotCreated(codeEditMessage.id, snapshotId);
                                        }
                                    });
                                } catch (error) {
                                    console.error('Failed to create checkpoint:', error);
                                }

                                // Add the new message to our collections
                                this.#messages.push(codeEditMessage);
                                this.#messageIndex.set(codeEditMessage.id, codeEditMessage);
                                this.#clients.forEach(client => client.handleNewMessage(codeEditMessage));

                                console.log(`🚀 ChatServiceImpl: Created separate message for file: ${data.codeEdit?.fileName || 'unknown'}`);
                                // REMOVED the break; statement - continue processing stream for more codeEdit responses
                            } else if (data.type === 'analysis_start' || data.type === 'analysis' || data.type === 'analysis_complete' || data.type === 'execution_start') {
                                // Handle analysis workflow messages - update the same assistant message
                                console.log(`🧠 ChatServiceImpl: Received ${data.type} message`);

                                // Update the existing assistant message with analysis workflow information
                                if (data.type === 'analysis_start') {
                                    assistantMessage.contents = '🧠 Analyzing your request...';
                                } else if (data.type === 'analysis_complete') {
                                    assistantMessage.contents = '🚀 Preparing response...';
                                } else if (data.type === 'execution_start') {
                                    assistantMessage.contents = '⚙️ Processing...';
                                } else if (data.type === 'analysis') {
                                    // Keep the current analysis message, don't change it for analysis chunks
                                    // Just continue accumulating analysis content silently
                                }

                                // CRITICAL: Add the type field to the message object so frontend can identify it
                                (assistantMessage as any).type = data.type;
                                (assistantMessage as any).analysisData = data.content; // Also pass the raw analysis content

                                console.log('🚀 ChatServiceImpl: *** SENDING MESSAGE CHANGE - WORKFLOW ***', {
                                    messageId: assistantMessage.id,
                                    type: data.type,
                                    contents: assistantMessage.contents,
                                    dataContent: data.content?.substring(0, 50) || 'empty'
                                });
                                this.#clients.forEach(client => client.handleMessageChange(assistantMessage));
                            } else if (data.content) {
                                accumulatedContent += data.content;
                                assistantMessage.contents = accumulatedContent;

                                // CRITICAL: Clear the type field for regular content so it doesn't interfere with workflow detection
                                (assistantMessage as any).type = undefined;

                                console.log('🚀 ChatServiceImpl: *** SENDING MESSAGE CHANGE - CONTENT ***', {
                                    messageId: assistantMessage.id,
                                    contentLength: accumulatedContent.length,
                                    contentPreview: data.content?.substring(0, 50) || 'empty',
                                    totalContentLength: assistantMessage.contents.length
                                });
                                this.#clients.forEach(client => client.handleMessageChange(assistantMessage));
                            }

                            if (data.done) {
                                console.log('🚀 ChatServiceImpl: Stream completed');
                                // Mark message as finished
                                assistantMessage.isFinished = true;

                                // If we have accumulated content but no codeEdit, this is a regular text response
                                if (accumulatedContent && !assistantMessage.codeEdit) {
                                    assistantMessage.contents = accumulatedContent;

                                    // Try to detect code changes if we don't have structured response
                                    const codeEdit = detectCodeChanges_DEPRECATED(accumulatedContent, contextFiles);
                                    if (codeEdit) {
                                        console.log('🚀 ChatServiceImpl: Detected code changes from content:', codeEdit);

                                        // Create snapshot for detected code changes
                                        let snapshotId: string | undefined;
                                        try {
                                            const snapshotManager = getSnapshotManager();
                                            const checkpointLabel = `before-edit-${Date.now()}`;
                                            snapshotId = await snapshotManager.createSnapshot(
                                                checkpointLabel,
                                                `Before applying detected changes to ${path.basename(codeEdit.filePath || 'file')}`
                                            );
                                            console.log('🔖 Auto-created snapshot for detected code changes:', snapshotId);

                                            // Notify clients about the snapshot creation
                                            this.#clients.forEach(client => {
                                                if ('handleSnapshotCreated' in client) {
                                                    (client as any).handleSnapshotCreated(assistantMessage.id, snapshotId);
                                                }
                                            });
                                        } catch (error) {
                                            console.error('Failed to create checkpoint:', error);
                                        }

                                        assistantMessage.codeEdit = codeEdit;
                                        assistantMessage.messageType = 'file-edit';
                                    }
                                } else if (!accumulatedContent) {
                                    // If there's no accumulated content, set a default message (for multi-file edits)
                                    assistantMessage.contents = 'Multi-file changes processed. See individual file edit messages above.';
                                }

                                this.#clients.forEach(client => client.handleMessageChange(assistantMessage));
                                break;
                            }
                        } catch (parseError) {
                            console.log('🚀 ChatServiceImpl: Could not parse JSON chunk:', dataStr);
                        }
                    }
                }
            }

            console.log('🚀 ChatServiceImpl: Stream processing completed');

        } catch (error: any) {
            console.error('ChatServiceImpl: Error in streamChat:', error);
            throw error;
        } finally {
            // Clean up
            if (this.#currentAbortController) {
                this.#currentAbortController = null;
            }
        }
    }

    async streamEdit(prompt: string, filePath: string, selection?: any): Promise<void> {
        try {
            console.log('ChatServiceImpl: Starting streamEdit for file:', filePath);

            // Cancel any ongoing request
            if (this.#currentAbortController) {
                this.#currentAbortController.abort();
            }
            this.#currentAbortController = new AbortController();

            // Read the actual file content to send to backend
            let fileContent = '';
            try {
                const fileUri = filePath.startsWith('file://') ? vscode.Uri.parse(filePath) : vscode.Uri.file(filePath);
                const bytes = await vscode.workspace.fs.readFile(fileUri);
                fileContent = Buffer.from(bytes).toString('utf8');
            } catch (readErr) {
                console.warn('🚀 ChatServiceImpl: Could not read file content for streamEdit; proceeding with empty content');
            }

            // Prepare request payload for Python backend
            const payload = {
                prompt: prompt,
                filePath: filePath,
                fileContent: fileContent,
                selection: selection
            };

            // Add user message
            const userMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: `Edit request for ${filePath}: ${prompt}`,
                isReply: false,
                isFinished: true,
                timestamp: Date.now()
            };

            this.#messages.push(userMessage);
            this.#messageIndex.set(userMessage.id, userMessage);
            this.#clients.forEach(client => client.handleNewMessage(userMessage));

            // Start with an assistant message for streaming
            const assistantMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: '',
                isReply: true,
                isFinished: false,
                timestamp: Date.now()
            };

            this.#messages.push(assistantMessage);
            this.#messageIndex.set(assistantMessage.id, assistantMessage);
            this.#clients.forEach(client => client.handleNewMessage(assistantMessage));

            // Use MCP client instead of direct HTTP
            const mcpClient = await getInitializedMCPClient();
            console.log('🔧 Making MCP edit request via client...');

            const editResponse = await mcpClient.streamEdit(
                payload.fileContent,
                payload.prompt,
                payload.filePath.split('/').pop() || 'file',
                'python' // Default language, could be detected from file extension
            );

            console.log(`✅ Got MCP edit response: ${editResponse.length} characters`);

            // Use MCP to analyze language dynamically
            let language = 'plaintext';
            try {
                const langAnalysis = await mcpClient.analyzeLanguageFeatures(
                    editResponse,
                    payload.filePath,
                    payload.filePath.split('.').pop() || ''
                );
                if (langAnalysis.success) {
                    language = langAnalysis.analysis.language;
                    console.log('✅ Dynamic language detection:', language);
                } else {
                    // Fallback to simple extension hint only if MCP fails
                    const fileExtension = payload.filePath.split('.').pop() || 'txt';
                    language = getLanguageFromExtension_DEPRECATED(fileExtension);
                }
            } catch (error) {
                console.warn('Language analysis failed, using fallback:', error);
                const fileExtension = payload.filePath.split('.').pop() || 'txt';
                language = getLanguageFromExtension_DEPRECATED(fileExtension);
            }
            
            const originalLines = payload.fileContent.split('\n').length;
            const newLines = editResponse.split('\n').length;

            const codeEdit = {
                filePath: payload.filePath,
                originalCode: payload.fileContent,
                newCode: editResponse,
                startLine: 1,
                endLine: originalLines,
                explanation: `Applied edit: ${payload.prompt}`,
                // Additional fields for webview compatibility
                language: language,
                fileName: payload.filePath.split('/').pop() || payload.filePath,
                linesChanged: Math.abs(newLines - originalLines)
            };

            // Update message with code edit data
            assistantMessage.contents = 'Code edit ready to apply';
            assistantMessage.codeEdit = codeEdit;
            assistantMessage.messageType = 'file-edit';
            assistantMessage.isFinished = true;

            // Create snapshot for this edit (like streamCreate does)
            try {
                const snapshotManager = getSnapshotManager();
                const checkpointLabel = `before-edit-${Date.now()}`;
                const snapshotId = await snapshotManager.createSnapshot(
                    checkpointLabel,
                    `Edit file: ${payload.filePath}`
                );
                console.log('✅ Created snapshot for edit:', snapshotId);

                // Notify clients about the snapshot creation
                this.#clients.forEach(client => {
                    if ('handleSnapshotCreated' in client) {
                        (client as any).handleSnapshotCreated(assistantMessage.id, snapshotId);
                    }
                });
            } catch (error) {
                console.error('Failed to create snapshot for edit:', error);
            }

            this.#clients.forEach(client => client.handleMessageChange(assistantMessage));

            console.log('ChatServiceImpl: StreamEdit completed');

        } catch (error: any) {
            console.error('ChatServiceImpl: Error in streamEdit:', error);
            throw error;
        } finally {
            // Clean up
            if (this.#currentAbortController) {
                this.#currentAbortController = null;
            }
        }
    }

    async streamEditDirect(prompt: string, filePath: string, selection?: any): Promise<void> {
        // Same as streamEdit but without creating user messages (to avoid duplication)
        try {
            console.log('ChatServiceImpl: Starting streamEditDirect for file:', filePath);

            // Cancel any ongoing request
            if (this.#currentAbortController) {
                this.#currentAbortController.abort();
            }
            this.#currentAbortController = new AbortController();

            // Read the actual file content to send to backend
            let fileContent = '';
            try {
                const fileUri = filePath.startsWith('file://') ? vscode.Uri.parse(filePath) : vscode.Uri.file(filePath);
                const bytes = await vscode.workspace.fs.readFile(fileUri);
                fileContent = Buffer.from(bytes).toString('utf8');
            } catch (readErr) {
                console.warn('🚀 ChatServiceImpl: Could not read file content for streamEditDirect; proceeding with empty content');
            }

            // Prepare request payload for Python backend
            const payload = {
                prompt: prompt,
                filePath: filePath,
                fileContent: fileContent,
                selection: selection
            };

            // Skip user message creation - go directly to assistant message
            const assistantMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: '',
                isReply: true,
                isFinished: false,
                timestamp: Date.now()
            };

            this.#messages.push(assistantMessage);
            this.#messageIndex.set(assistantMessage.id, assistantMessage);
            this.#clients.forEach(client => client.handleNewMessage(assistantMessage));

            // Use MCP client instead of direct HTTP
            const mcpClient = await getInitializedMCPClient();
            console.log('🔧 Making MCP edit request via client (direct)...');

            const editResponse = await mcpClient.streamEdit(
                payload.fileContent,
                payload.prompt,
                payload.filePath.split('/').pop() || 'file',
                getLanguageFromExtension_DEPRECATED(payload.filePath.split('.').pop() || 'txt')
            );

            console.log(`✅ Got MCP edit response (direct): ${editResponse.length} characters`);

            // Use MCP to analyze language dynamically
            let language = 'plaintext';
            try {
                const langAnalysis = await mcpClient.analyzeLanguageFeatures(
                    editResponse,
                    payload.filePath,
                    payload.filePath.split('.').pop() || ''
                );
                if (langAnalysis.success) {
                    language = langAnalysis.analysis.language;
                    console.log('✅ Dynamic language detection (direct):', language);
                } else {
                    // Fallback only if MCP fails
                    const fileExtension = payload.filePath.split('.').pop() || 'txt';
                    language = getLanguageFromExtension_DEPRECATED(fileExtension);
                }
            } catch (error) {
                console.warn('Language analysis failed (direct), using fallback:', error);
                const fileExtension = payload.filePath.split('.').pop() || 'txt';
                language = getLanguageFromExtension_DEPRECATED(fileExtension);
            }
            
            const originalLines = payload.fileContent.split('\n').length;
            const newLines = editResponse.split('\n').length;

            const codeEdit = {
                filePath: payload.filePath,
                originalCode: payload.fileContent,
                newCode: editResponse,
                startLine: 1,
                endLine: originalLines,
                explanation: `Applied edit: ${payload.prompt}`,
                // Additional fields for webview compatibility
                language: language,
                fileName: payload.filePath.split('/').pop() || payload.filePath,
                linesChanged: Math.abs(newLines - originalLines)
            };

            // Update message with code edit data
            assistantMessage.contents = 'Code edit ready to apply';
            assistantMessage.codeEdit = codeEdit;
            assistantMessage.messageType = 'file-edit';
            assistantMessage.isFinished = true;

            // Debug logging for codeEdit structure
            console.log('🔍 streamEditDirect: Created codeEdit structure:', {
                hasCodeEdit: !!assistantMessage.codeEdit,
                messageType: assistantMessage.messageType,
                filePath: codeEdit.filePath,
                originalCodeLength: codeEdit.originalCode.length,
                newCodeLength: codeEdit.newCode.length,
                language: codeEdit.language,
                fileName: codeEdit.fileName
            });

            // Create snapshot for this edit
            try {
                const snapshotManager = getSnapshotManager();
                const checkpointLabel = `before-edit-${Date.now()}`;
                const snapshotId = await snapshotManager.createSnapshot(
                    checkpointLabel,
                    `Edit file: ${payload.filePath}`
                );
                console.log('✅ Created snapshot for edit (direct):', snapshotId);

                // Notify clients about the snapshot creation
                this.#clients.forEach(client => {
                    if ('handleSnapshotCreated' in client) {
                        (client as any).handleSnapshotCreated(assistantMessage.id, snapshotId);
                    }
                });
            } catch (error) {
                console.error('Failed to create snapshot for edit (direct):', error);
            }

            // Debug logging before sending to webview
            console.log('🔍 streamEditDirect: Sending message to webview:', {
                messageId: assistantMessage.id,
                contents: assistantMessage.contents,
                messageType: assistantMessage.messageType,
                hasCodeEdit: !!assistantMessage.codeEdit,
                isFinished: assistantMessage.isFinished
            });

            this.#clients.forEach(client => client.handleMessageChange(assistantMessage));

            console.log('ChatServiceImpl: StreamEditDirect completed');

        } catch (error: any) {
            console.error('ChatServiceImpl: Error in streamEditDirect:', error);
            throw error;
        } finally {
            // Clean up
            if (this.#currentAbortController) {
                this.#currentAbortController = null;
            }
        }
    }

    async streamGenerate(prompt: string, contextFiles?: any[]): Promise<void> {
        try {
            console.log('ChatServiceImpl: Starting streamGenerate');

            // Cancel any ongoing request
            if (this.#currentAbortController) {
                this.#currentAbortController.abort();
            }
            this.#currentAbortController = new AbortController();

            // Prepare request payload for Python backend
            const payload = {
                prompt: prompt,
                contextFiles: contextFiles || []
            };

            // Add user message
            const userMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: `Generate: ${prompt}`,
                isReply: false,
                isFinished: true
            };

            this.#messages.push(userMessage);
            this.#messageIndex.set(userMessage.id, userMessage);
            this.#clients.forEach(client => client.handleNewMessage(userMessage));

            // Use MCP client instead of direct HTTP
            const mcpClient = await getInitializedMCPClient();
            console.log('🔧 Making MCP generate request via client...');

            // Start assistant response
            const assistantMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: '',
                isReply: true,
                isFinished: false
            };

            this.#messages.push(assistantMessage);
            this.#messageIndex.set(assistantMessage.id, assistantMessage);
            this.#clients.forEach(client => client.handleNewMessage(assistantMessage));

            const generateResponse = await mcpClient.streamGenerate(
                payload.prompt,
                undefined, // current file content
                'python', // default language
                '' // workspace path
            );

            console.log(`✅ Got MCP generate response: ${generateResponse.length} characters`);

            // Update message with generated content
            assistantMessage.contents = generateResponse;
            assistantMessage.isFinished = true;
            this.#clients.forEach(client => client.handleMessageChange(assistantMessage));

            // MCP call is already complete, no streaming needed

            console.log('ChatServiceImpl: StreamGenerate completed');

        } catch (error) {
            console.error('ChatServiceImpl: StreamGenerate error:', error);

            const errorMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: `Generate error: ${error instanceof Error ? error.message : 'Unknown error occurred'}`,
                isReply: true,
                isFinished: true
            };

            this.#messages.push(errorMessage);
            this.#messageIndex.set(errorMessage.id, errorMessage);
            this.#clients.forEach(client => client.handleNewMessage(errorMessage));
        } finally {
            this.#currentAbortController = null;
        }
    }

    // New: StreamCreate for creating new files with LLM-inferred file names
    async streamCreate(prompt: string, contextFiles?: any[]): Promise<void> {
        try {
            console.log('ChatServiceImpl: Starting streamCreate');

            if (this.#currentAbortController) {
                this.#currentAbortController.abort();
            }
            this.#currentAbortController = new AbortController();

            const payload = {
                prompt: prompt,
                contextFiles: contextFiles || []
            };

            const userMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: `Create: ${prompt}`,
                isReply: false,
                isFinished: true
            };

            this.#messages.push(userMessage);
            this.#messageIndex.set(userMessage.id, userMessage);
            this.#clients.forEach(client => client.handleNewMessage(userMessage));

            // Use MCP client instead of direct HTTP
            const mcpClient = await getInitializedMCPClient();
            console.log('🔧 Making MCP create request via client...');

            // Assistant message for status
            const assistantMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: 'Creating project...',
                isReply: true,
                isFinished: false
            };
            this.#messages.push(assistantMessage);
            this.#messageIndex.set(assistantMessage.id, assistantMessage);
            this.#clients.forEach(client => client.handleNewMessage(assistantMessage));

            const createResponse = await mcpClient.streamCreate(
                payload.prompt,
                'new_project',
                'Generated by SuperInference'
            );

            console.log(`✅ Got MCP create response: ${createResponse.length} files`);

            // Update message with created files information
            assistantMessage.contents = `Created ${createResponse.length} files:\n\n` +
                createResponse.map((file: any) => `📄 ${file.path}`).join('\n');
            assistantMessage.isFinished = true;
            this.#clients.forEach(client => client.handleMessageChange(assistantMessage));

            // Process the created files from MCP response
            if (createResponse && createResponse.length > 0) {
                // Create code edit messages for each file
                for (const file of createResponse) {
                    const codeEditMessage: MessageItemModel = {
                        id: (++this.#currentMessageId).toString(),
                        contents: `Created file: ${file.path}`,
                        isReply: true,
                        isFinished: true,
                        codeEdit: {
                            filePath: file.path,
                            newCode: file.content,
                            originalCode: '',
                            startLine: 1,
                            endLine: file.content.split('\n').length,
                            explanation: `Created new file: ${file.path}`
                        },
                        messageType: 'file-edit',
                        timestamp: Date.now()
                    };

                    try {
                        const snapshotManager = getSnapshotManager();
                        const checkpointLabel = `before-create-${Date.now()}`;
                        const snapshotId = await snapshotManager.createSnapshot(
                            checkpointLabel,
                            `Create file: ${file.path}`
                        );
                        // Notify clients about the snapshot creation
                        this.#clients.forEach(client => {
                            if ('handleSnapshotCreated' in client) {
                                (client as any).handleSnapshotCreated(codeEditMessage.id, snapshotId);
                            }
                        });
                    } catch (error) {
                        console.error('Failed to create checkpoint:', error);
                    }

                    this.#messages.push(codeEditMessage);
                    this.#messageIndex.set(codeEditMessage.id, codeEditMessage);
                    this.#clients.forEach(client => client.handleNewMessage(codeEditMessage));
                }
            }

            // Ensure we always stop analysis spinner even if backend didn't emit a final done event
            // Ensure assistant message is marked as finished
            if (!assistantMessage.isFinished) {
                assistantMessage.isFinished = true;
                this.#clients.forEach(client => client.handleMessageChange(assistantMessage));
            }

            console.log('ChatServiceImpl: StreamCreate completed');

        } catch (error) {
            console.error('ChatServiceImpl: StreamCreate error:', error);
            const errorMessage: MessageItemModel = {
                id: (++this.#currentMessageId).toString(),
                contents: `Create error: ${error instanceof Error ? error.message : 'Unknown error occurred'}`,
                isReply: true,
                isFinished: true
            };
            this.#messages.push(errorMessage);
            this.#messageIndex.set(errorMessage.id, errorMessage);
            this.#clients.forEach(client => client.handleNewMessage(errorMessage));
        } finally {
            this.#currentAbortController = null;
        }
    }

    async restoreCheckpoint(snapshotId: string): Promise<void> {
        console.log('📸 ChatService: Starting restoreCheckpoint with snapshotId:', snapshotId);
        console.log('📸 ChatService: snapshotId type:', typeof snapshotId);
        console.log('📸 ChatService: snapshotId length:', snapshotId?.length);

        try {
            console.log('📸 ChatService: Getting snapshot manager instance');
            const snapshotManager = getSnapshotManager();
            console.log('📸 ChatService: Got snapshot manager:', !!snapshotManager);

            console.log('📸 ChatService: Calling snapshotManager.restoreSnapshotById with:', snapshotId);
            await snapshotManager.restoreSnapshotById(snapshotId);
            console.log('📸 ChatService: snapshotManager.restoreSnapshotById completed successfully');

            // Clear chat messages created after this checkpoint
            this.clearMessagesAfterCheckpoint(snapshotId);

            // Mark this snapshot as restored
            this.#restoredSnapshots.add(snapshotId);

            // Notify clients about successful restoration
            console.log('📸 ChatService: Notifying clients about checkpoint restoration');
            this.#clients.forEach((client: ChatServiceClient) => {
                try {
                    if (client.handleCheckpointRestored) {
                        console.log('📸 ChatService: Calling handleCheckpointRestored on client');
                        client.handleCheckpointRestored(snapshotId);
                    }
                } catch (clientError) {
                    console.error('📸 ChatService: Error notifying client:', clientError);
                }
            });

            console.log('📸 ChatService: Checkpoint restoration completed successfully');

        } catch (error) {
            console.error('📸 ChatService: Error in restoreCheckpoint:', error);
            console.error('📸 ChatService: Error details:', error);
            throw error;
        }
    }

    private clearMessagesAfterCheckpoint(snapshotId: string): void {
        console.log('📸 ChatService: Clearing messages after checkpoint:', snapshotId);
        console.log('📸 ChatService: Current messages count:', this.#messages.length);
        console.log('📸 ChatService: Current message IDs:', this.#messages.map(m => m.id));
        console.log('📸 ChatService: Current checkpoint mappings:', Array.from(this.#messageCheckpoints.entries()));

        // Find the message associated with this checkpoint
        let checkpointMessageId: string | null = null;
        let checkpointMessageIndex = -1;

        for (let i = 0; i < this.#messages.length; i++) {
            const message = this.#messages[i];
            const messageSnapshotId = this.#messageCheckpoints.get(message.id);
            console.log('📸 ChatService: Checking message', i, 'ID:', message.id, 'snapshotId:', messageSnapshotId);

            if (messageSnapshotId === snapshotId) {
                checkpointMessageId = message.id;
                checkpointMessageIndex = i;
                console.log('📸 ChatService: *** FOUND *** checkpoint message at index:', i, 'messageId:', checkpointMessageId);
                break;
            }
        }

        if (checkpointMessageIndex >= 0 && checkpointMessageId) {
            // Remove all messages after the checkpoint from our internal state
            const messagesToRemove = this.#messages.slice(checkpointMessageIndex + 1);
            this.#messages = this.#messages.slice(0, checkpointMessageIndex + 1);

            // Clean up the message index and checkpoints for removed messages
            messagesToRemove.forEach(message => {
                this.#messageIndex.delete(message.id);
                this.#messageCheckpoints.delete(message.id);
            });

            console.log('📸 ChatService: Removed', messagesToRemove.length, 'messages after checkpoint');

            // Send targeted clear command to webview instead of clearing all and re-adding
            console.log('📸 ChatService: Notifying', this.#clients.length, 'clients about message clearing for checkpoint:', checkpointMessageId);
            this.#clients.forEach((client, index) => {
                console.log('📸 ChatService: Checking client', index, 'for handleClearMessagesAfterCheckpoint method');
                if ('handleClearMessagesAfterCheckpoint' in client && typeof (client as any).handleClearMessagesAfterCheckpoint === 'function') {
                    console.log('📸 ChatService: Calling handleClearMessagesAfterCheckpoint on client', index);
                    (client as any).handleClearMessagesAfterCheckpoint(checkpointMessageId);
                } else {
                    console.log('📸 ChatService: Client', index, 'does not have handleClearMessagesAfterCheckpoint method');
                }
            });
        } else {
            console.log('📸 ChatService: *** FALLBACK *** No messages found to clear for checkpoint:', snapshotId);
            console.log('📸 ChatService: Attempting fallback - sending snapshotId to webview for clearing');

            // Fallback: Send the snapshotId directly to webview to let it handle the clearing
            this.#clients.forEach((client, index) => {
                console.log('📸 ChatService: Checking client', index, 'for handleClearMessagesAfterCheckpoint method (fallback)');
                if ('handleClearMessagesAfterCheckpoint' in client && typeof (client as any).handleClearMessagesAfterCheckpoint === 'function') {
                    console.log('📸 ChatService: Calling handleClearMessagesAfterCheckpoint with snapshotId as fallback');
                    (client as any).handleClearMessagesAfterCheckpoint(snapshotId); // Use snapshotId as fallback
                }
            });
        }
    }

    // Remove the complex captureFileSnapshots and restoreFileSnapshots methods
    // They are replaced by the native snapshot manager

    // NOTE: Simple extension mapping - kept for fast file type hints
    // For deep language analysis, use MCP analyze_language_features tool
    private getLanguageFromFileName(fileName: string): string {
        const ext = path.extname(fileName).toLowerCase();
        const languageMap: { [key: string]: string } = {
            '.ts': 'typescript',
            '.tsx': 'typescriptreact',
            '.js': 'javascript',
            '.jsx': 'javascriptreact',
            '.py': 'python',
            '.java': 'java',
            '.cpp': 'cpp',
            '.c': 'c',
            '.cs': 'csharp',
            '.go': 'go',
            '.rs': 'rust',
            '.php': 'php',
            '.rb': 'ruby',
            '.swift': 'swift',
            '.kt': 'kotlin',
            '.html': 'html',
            '.css': 'css',
            '.scss': 'scss',
            '.less': 'less',
            '.sql': 'sql',
            '.json': 'json',
            '.xml': 'xml',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            '.md': 'markdown',
            '.txt': 'plaintext'
        };
        return languageMap[ext] || 'plaintext';
    }
}

// Singleton instance
let chatServiceInstance: ChatServiceImpl | null = null;

export function sharedChatServiceImpl(): ChatServiceImpl {
    if (!chatServiceInstance) {
        chatServiceInstance = new ChatServiceImpl();
    }
    return chatServiceInstance;
}
