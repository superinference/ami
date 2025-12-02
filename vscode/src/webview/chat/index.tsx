import * as React from 'react';
import { useState, useEffect, useRef, useCallback } from 'react';
import {
    VSCodeButton,
    VSCodeTextArea,
    VSCodeProgressRing,
    VSCodeTag,
    VSCodeDropdown,
    VSCodeOption,
    VSCodeDivider,
    VSCodeBadge,
    VSCodePanels,
    VSCodePanelTab,
    VSCodePanelView
} from '@vscode/webview-ui-toolkit/react';
import {
    getServiceManager
} from '../../common/ipc/webview';
import {
    IChatService,
    CHAT_SERVICE_NAME,
    IChatViewService,
    CHAT_VIEW_SERVICE_NAME
} from '../../common/chatService';
import { MessageItemModel } from '../../common/chatService/model';
import { ChatViewServiceImpl } from './chatViewServiceImpl';
import { SimpleCheckpointControls } from '../components/SimpleCheckpointControls';

// Enhanced interfaces features
interface ChatMessage {
    id: string;
    role: 'user' | 'assistant' | 'system';
    content: string;
    timestamp: Date;
    isStreaming?: boolean;
    messageType?: 'text' | 'code' | 'diff' | 'file-edit';
    references?: Array<{
        uri: string;
        range?: {
            start: { line: number; character: number };
            end: { line: number; character: number };
        };
    }>;
    toolCalls?: Array<{
        name: string;
        args: any;
        result?: string;
    }>;
    codeEdit?: {
        filePath: string;
        language: string;
        originalCode: string;
        newCode: string;
        startLine: number;
        endLine: number;
        linesChanged: number;
        fileName?: string; // Added for better display
        explanation?: string; // Added for explanation
    };
    canApply?: boolean;
    isApplied?: boolean;
    streamingPhase?: 'analysis' | 'execution' | 'response';
    analysisContent?: string;
    executionContent?: string;
    executionStarted?: boolean;
    analysisComplete?: boolean;
}

interface ContextFile {
    uri: string;
    name: string;
    type: 'file' | 'selection' | 'docs' | 'web' | 'chat';
    content?: string;
    range?: {
        start: { line: number; character: number };
        end: { line: number; character: number };
    };
    icon?: string;
}



interface ChatPageProps {
    showContextPanel?: boolean;
    contextFiles?: any[];
    onClearChat?: () => void;
    selectedModel?: string;
    onModelChange?: (model: string) => void;
    // Removed: executionMode - plan/act mode eliminated, PRE loop handles planning internally
}

// Global VS Code API instance
let vscodeApi: any = null;

function getVsCodeApi(): any {
    if (!vscodeApi) {
        try {
            vscodeApi = (window as any).acquireVsCodeApi();
            // Set it on window for other components to access
            (window as any).vscode = vscodeApi;
            console.log('🔵 Webview: VS Code API acquired successfully');
        } catch (error) {
            console.error('🔵 Webview: Failed to acquire VS Code API:', error);
            return null;
        }
    }
    return vscodeApi;
}

function getSafeServiceManager() {
    try {
        return (window as any).serviceManager || getServiceManager();
    } catch (error) {
        console.warn('Service manager not available:', error);
        return null;
    }
}

function renderMarkdown(content: string): string {
    // Enhanced markdown renderer with comprehensive formatting support
    if (!content) {
        return '';
    }
    let result = content;

    // First, handle code blocks while preserving whitespace
    result = result.replace(/```(\w+)?\n?([\s\S]*?)```/g, (_, language, code) => {
        // Preserve the original indentation and formatting
        const cleanCode = code.replace(/^\n/, '').replace(/\n$/, '');
        const langClass = language ? `language-${language}` : '';
        return `<pre class="code-block"><code class="${langClass}">${cleanCode}</code></pre>`;
    });

    // Handle inline code
    result = result.replace(/`([^`]+)`/g, '<code class="inline-code">$1</code>');

    // Handle headers (### ## #) - must be at start of line
    result = result.replace(/^### (.*$)/gm, '<h3 class="markdown-h3">$1</h3>');
    result = result.replace(/^## (.*$)/gm, '<h2 class="markdown-h2">$1</h2>');
    result = result.replace(/^# (.*$)/gm, '<h1 class="markdown-h1">$1</h1>');

    // Handle bold and italic (more robust patterns)
    result = result.replace(/\*\*(.*?)\*\*/g, '<strong class="markdown-bold">$1</strong>');
    result = result.replace(/(?<!\*)\*(?!\*)([^*]+)\*(?!\*)/g, '<em class="markdown-italic">$1</em>');

    // Handle unordered lists - group consecutive items
    result = result.replace(/^- (.*$)/gm, '<li class="markdown-li">$1</li>');
    result = result.replace(/(<li class="markdown-li">.*?<\/li>(\s*<li class="markdown-li">.*?<\/li>)*)/gs, '<ul class="markdown-ul">$1</ul>');

    // Handle ordered lists - group consecutive items  
    result = result.replace(/^\d+\. (.*$)/gm, '<li class="markdown-oli">$1</li>');
    result = result.replace(/(<li class="markdown-oli">.*?<\/li>(\s*<li class="markdown-oli">.*?<\/li>)*)/gs, '<ol class="markdown-ol">$1</ol>');

    // Handle line breaks (but not inside code blocks or other block elements)
    result = result.replace(/\n(?!<\/?(h[1-6]|li|ul|ol|pre|code))/g, '<br>');

    return result;
}

// Search Result Item Interface
interface SearchResultItem {
    uri: string;
    name: string;
    path: string;
    type: string;
}

// Context Files Panel Component
const ContextFilesPanel: React.FC<{
    contextFiles: ContextFile[];
    onRemoveFile: (uri: string) => void;
    onAddFile: () => void;
    onAddFilesByPath: () => void;
    onAddAllOpenFiles: () => void;
}> = ({ contextFiles, onRemoveFile, onAddFile, onAddFilesByPath, onAddAllOpenFiles }) => {
    const [showActions, setShowActions] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState<SearchResultItem[]>([]);
    const [showSearchResults, setShowSearchResults] = useState(false);
    const [menuPosition, setMenuPosition] = useState<{ top: number; left: number } | null>(null);
    const actionsRef = useRef<HTMLDivElement>(null);
    const searchRef = useRef<HTMLInputElement>(null);

    // Close actions menu when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (actionsRef.current && !actionsRef.current.contains(event.target as Node)) {
                setShowActions(false);
                setShowSearchResults(false);
                setSearchQuery('');
                setSearchResults([]);
                setMenuPosition(null);
            }
        };

        if (showActions) {
            document.addEventListener('mousedown', handleClickOutside);
        }

        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [showActions]);

    // Handle search query changes
    const handleSearchChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
        const query = e.target.value;
        setSearchQuery(query);

        if (query.trim().length > 0) {
            setShowSearchResults(true);
        } else {
            setShowSearchResults(false);
            setSearchResults([]);
        }
    }, []);

    // Debounced search effect
    useEffect(() => {
        if (searchQuery.trim().length > 0) {
            const timeoutId = setTimeout(() => {
                const vscode = getVsCodeApi();
                if (vscode) {
                    vscode.postMessage({
                        type: 'searchFiles',
                        query: searchQuery
                    });
                }
            }, 300);

            return () => clearTimeout(timeoutId);
        }
        return undefined;
    }, [searchQuery]);

    // Handle search result selection
    const handleSearchResultClick = useCallback((result: SearchResultItem) => {
        const vscode = getVsCodeApi();
        if (vscode) {
            vscode.postMessage({
                type: 'addFileByUri',
                uri: result.uri
            });
        }
        setSearchQuery('');
        setShowSearchResults(false);
        setSearchResults([]);
        setShowActions(false);
        setMenuPosition(null);
    }, []);

    // Handle search input key events
    const handleSearchKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && searchResults.length > 0) {
            // Select the first result
            handleSearchResultClick(searchResults[0]);
        } else if (e.key === 'Escape') {
            setSearchQuery('');
            setShowSearchResults(false);
            setSearchResults([]);
        }
    }, [searchResults, handleSearchResultClick]);

    // Handle search result messages from VS Code
    useEffect(() => {
        const handleMessage = (event: MessageEvent) => {
            const { type, results } = event.data;
            if (type === 'searchResults') {
                setSearchResults(results || []);
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, []);

    const getContextIcon = (type: string) => {
        switch (type) {
            case 'file': return 'file';
            case 'selection': return 'selection';
            case 'docs': return 'book';
            case 'web': return 'globe';
            case 'chat': return 'comment-discussion';
            default: return 'file';
        }
    };

    const formatFileSize = (content: string) => {
        if (!content || typeof content !== 'string') return '';
        const lines = content.split('\n').length;
        const chars = content.length;
        if (lines > 1) return `${lines} lines`;
        if (chars > 100) return `${chars} chars`;
        return '';
    };

    return (
        <div className="context-files-panel">
            <div className="context-header">
                <h3>Context Files ({contextFiles.length})</h3>
                <div className="context-actions" ref={actionsRef}>
                    <VSCodeButton
                        appearance="icon"
                        onClick={() => {
                            const willShow = !showActions;
                            setShowActions(willShow);
                            if (willShow && actionsRef.current) {
                                // Calculate position for fixed positioning
                                const rect = actionsRef.current.getBoundingClientRect();
                                setMenuPosition({
                                    top: rect.bottom + 4,
                                    left: rect.left
                                });
                                // Focus search input when opening
                                setTimeout(() => searchRef.current?.focus(), 100);
                            } else {
                                setMenuPosition(null);
                            }
                        }}
                        title="Add files to context - search for files or use quick actions"
                    >
                        <i className="codicon codicon-add"></i>
                    </VSCodeButton>
                    {showActions && menuPosition && (
                        <div
                            className="context-actions-menu-fixed"
                            style={{
                                position: 'fixed',
                                top: `${menuPosition.top}px`,
                                left: `${menuPosition.left}px`,
                                zIndex: 99999
                            }}
                        >
                            <div className="search-input-container">
                                <input
                                    ref={searchRef}
                                    type="text"
                                    value={searchQuery}
                                    onChange={handleSearchChange}
                                    onKeyDown={handleSearchKeyDown}
                                    placeholder="Search workspace files..."
                                    className="search-input"
                                />
                                <i className="codicon codicon-search search-icon"></i>
                            </div>

                            {showSearchResults && searchResults.length > 0 && (
                                <div className="search-results">
                                    {searchResults.map((result) => (
                                        <div
                                            key={result.uri}
                                            className="search-result-item"
                                            onClick={() => handleSearchResultClick(result)}
                                        >
                                            <i className="codicon codicon-file"></i>
                                            <div className="search-result-content">
                                                <div className="search-result-name">{result.name}</div>
                                                <div className="search-result-path">{result.path}</div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}

                            <div className="context-actions-divider"></div>

                            <VSCodeButton
                                appearance="secondary"
                                onClick={() => { onAddFile(); setShowActions(false); setMenuPosition(null); }}
                                title="Add current file to context"
                            >
                                <i className="codicon codicon-file"></i>
                                Current File
                            </VSCodeButton>
                            <VSCodeButton
                                appearance="secondary"
                                onClick={() => { onAddFilesByPath(); setShowActions(false); setMenuPosition(null); }}
                                title="Browse for files in file picker"
                            >
                                <i className="codicon codicon-folder-opened"></i>
                                Browse Files
                            </VSCodeButton>
                            <VSCodeButton
                                appearance="secondary"
                                onClick={() => { onAddAllOpenFiles(); setShowActions(false); setMenuPosition(null); }}
                                title="Add all currently open files to context"
                            >
                                <i className="codicon codicon-files"></i>
                                All Open Files
                            </VSCodeButton>
                        </div>
                    )}
                </div>
            </div>
            <div className="context-files">
                {contextFiles.map((file, index) => (
                    <div key={index} className="context-file-item">
                        <div className="context-file-info">
                            <i className={`codicon codicon-${getContextIcon(file.type)}`}></i>
                            <div className="context-file-details">
                                <span className="context-file-name" title={file.uri}>
                                    {file.name}
                                </span>
                                {file.range && (
                                    <span className="context-file-range">
                                        Lines {file.range.start.line + 1}-{file.range.end.line + 1}
                                    </span>
                                )}
                                {file.content && (
                                    <span className="context-file-size">
                                        {formatFileSize(file.content)}
                                    </span>
                                )}
                            </div>
                        </div>
                        <VSCodeButton
                            appearance="icon"
                            onClick={() => onRemoveFile(file.uri)}
                            title="Remove from context"
                            className="context-file-remove"
                        >
                            <i className="codicon codicon-x"></i>
                        </VSCodeButton>
                    </div>
                ))}
                {contextFiles.length === 0 && (
                    <div className="no-context-files">
                        <p>No context files added</p>
                        <p>Click + to add files to provide context for your questions</p>
                        <div className="context-help">
                            <p><strong>Context files help SuperInference:</strong></p>
                            <ul>
                                <li>Understand your codebase structure</li>
                                <li>Provide more accurate suggestions</li>
                                <li>Reference existing code patterns</li>
                                <li>Give contextual explanations</li>
                            </ul>
                        </div>
                    </div>
                )}
            </div>
            {contextFiles.length > 0 && (
                <div className="context-summary">
                    <p>
                        <strong>{contextFiles.length}</strong> file{contextFiles.length > 1 ? 's' : ''} in context
                    </p>
                    <VSCodeButton
                        appearance="secondary"
                        onClick={() => contextFiles.forEach(f => onRemoveFile(f.uri))}
                        title="Clear all context files"
                    >
                        <i className="codicon codicon-clear-all"></i>
                        Clear All
                    </VSCodeButton>
                </div>
            )}
        </div>
    );
};

// Code Edit Component with Apply/Reject buttons
const CodeEditMessage: React.FC<{
    message: ChatMessage;
    onApply: (messageId: string) => void;
    onReject: (messageId: string) => void;
}> = ({ message, onApply, onReject }) => {
    const { codeEdit } = message;
    if (!codeEdit) return null;

    // Generate a clean diff display from original and new code
    const generateCleanDiff = (original: string, newCode: string) => {
        // Use a more sophisticated diff algorithm that shows only changes with context
        const originalLines = (original || '').split('\n');
        const newLines = (newCode || '').split('\n');

        // First, generate a basic line-by-line diff to identify changes
        const diffOperations = generateLineDiff(originalLines, newLines);

        // Then, group changes and add context
        return generateContextualDiff(diffOperations);
    };

    // Generate basic line-by-line diff operations
    const generateLineDiff = (originalLines: string[], newLines: string[]) => {
        const operations: Array<{ type: 'add' | 'remove' | 'equal', originalIndex?: number, newIndex?: number, line: string }> = [];

        // Use a simple LCS-based approach for better diff quality
        const lcs = longestCommonSubsequence(originalLines, newLines);

        let origIndex = 0;
        let newIndex = 0;
        let lcsIndex = 0;

        while (origIndex < originalLines.length || newIndex < newLines.length) {
            if (lcsIndex < lcs.length &&
                origIndex < originalLines.length &&
                originalLines[origIndex] === lcs[lcsIndex] &&
                newIndex < newLines.length &&
                newLines[newIndex] === lcs[lcsIndex]) {
                // Lines match
                operations.push({
                    type: 'equal',
                    originalIndex: origIndex,
                    newIndex: newIndex,
                    line: originalLines[origIndex]
                });
                origIndex++;
                newIndex++;
                lcsIndex++;
            } else if (origIndex < originalLines.length &&
                (lcsIndex >= lcs.length || originalLines[origIndex] !== lcs[lcsIndex])) {
                // Line was removed
                operations.push({
                    type: 'remove',
                    originalIndex: origIndex,
                    line: originalLines[origIndex]
                });
                origIndex++;
            } else if (newIndex < newLines.length) {
                // Line was added
                operations.push({
                    type: 'add',
                    newIndex: newIndex,
                    line: newLines[newIndex]
                });
                newIndex++;
            }
        }

        return operations;
    };

    // Simple LCS implementation for better diff quality
    const longestCommonSubsequence = (a: string[], b: string[]): string[] => {
        const dp: number[][] = Array(a.length + 1).fill(null).map(() => Array(b.length + 1).fill(0));

        for (let i = 1; i <= a.length; i++) {
            for (let j = 1; j <= b.length; j++) {
                if (a[i - 1] === b[j - 1]) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        // Reconstruct LCS
        const result: string[] = [];
        let i = a.length, j = b.length;
        while (i > 0 && j > 0) {
            if (a[i - 1] === b[j - 1]) {
                result.unshift(a[i - 1]);
                i--;
                j--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }

        return result;
    };

    // Generate contextual diff showing only changes with surrounding context
    const generateContextualDiff = (operations: any[]) => {
        const result: Array<{ line: string, type: 'removed' | 'added' | 'unchanged', lineNumber?: number, isContext?: boolean }> = [];
        const contextLines = 3; // Number of context lines to show around changes

        // Find change groups (consecutive changes)
        const changeGroups: Array<{ start: number, end: number }> = [];
        let currentGroup: { start: number, end: number } | null = null;

        operations.forEach((op, index) => {
            if (op.type !== 'equal') {
                if (!currentGroup) {
                    currentGroup = { start: index, end: index };
                } else {
                    currentGroup.end = index;
                }
            } else if (currentGroup) {
                changeGroups.push(currentGroup);
                currentGroup = null;
            }
        });

        if (currentGroup) {
            changeGroups.push(currentGroup);
        }

        // If no changes, show a summary
        if (changeGroups.length === 0) {
            result.push({
                line: '// No significant changes detected',
                type: 'unchanged',
                isContext: true
            });
            return result;
        }

        // For each change group, add context and changes
        changeGroups.forEach((group, groupIndex) => {
            const groupStart = Math.max(0, group.start - contextLines);
            const groupEnd = Math.min(operations.length - 1, group.end + contextLines);

            // Add context before changes
            for (let i = groupStart; i < group.start; i++) {
                const op = operations[i];
                if (op.type === 'equal') {
                    result.push({
                        line: op.line,
                        type: 'unchanged',
                        lineNumber: (op.originalIndex ?? 0) + 1,
                        isContext: true
                    });
                }
            }

            // Add the actual changes
            for (let i = group.start; i <= group.end; i++) {
                const op = operations[i];
                if (op.type === 'remove') {
                    result.push({
                        line: op.line,
                        type: 'removed',
                        lineNumber: (op.originalIndex ?? 0) + 1
                    });
                } else if (op.type === 'add') {
                    result.push({
                        line: op.line,
                        type: 'added',
                        lineNumber: (op.newIndex ?? 0) + 1
                    });
                } else if (op.type === 'equal') {
                    result.push({
                        line: op.line,
                        type: 'unchanged',
                        lineNumber: (op.originalIndex ?? 0) + 1,
                        isContext: true
                    });
                }
            }

            // Add context after changes
            for (let i = group.end + 1; i <= groupEnd; i++) {
                const op = operations[i];
                if (op.type === 'equal') {
                    result.push({
                        line: op.line,
                        type: 'unchanged',
                        lineNumber: (op.originalIndex ?? 0) + 1,
                        isContext: true
                    });
                }
            }

            // Add separator between groups (if not the last group)
            if (groupIndex < changeGroups.length - 1) {
                result.push({
                    line: '...',
                    type: 'unchanged',
                    isContext: true
                });
            }
        });

        return result;
    };

    const diffLines = generateCleanDiff(codeEdit.originalCode, codeEdit.newCode);

    // Calculate statistics
    const addedLines = diffLines.filter(line => line.type === 'added').length;
    const removedLines = diffLines.filter(line => line.type === 'removed').length;
    const changedLines = Math.max(addedLines, removedLines);

    return (
        <div className="code-edit-message">
            <div className="code-edit-header">
                <div className="code-edit-info">
                    <i className="codicon codicon-file-code"></i>
                    <span className="code-edit-filename">{codeEdit.fileName || codeEdit.filePath}</span>
                    <div className="code-edit-stats">
                        {addedLines > 0 && (
                            <VSCodeBadge appearance="success">+{addedLines}</VSCodeBadge>
                        )}
                        {removedLines > 0 && (
                            <VSCodeBadge appearance="warning">-{removedLines}</VSCodeBadge>
                        )}
                        {changedLines === 0 && (
                            <VSCodeBadge>No changes</VSCodeBadge>
                        )}
                    </div>
                </div>
                <div className="code-edit-actions">
                    {!message.isApplied && message.canApply && (
                        <>
                            <VSCodeButton
                                appearance="secondary"
                                onClick={() => onReject(message.id)}
                                title="Reject changes"
                            >
                                <i className="codicon codicon-x"></i>
                                Reject
                            </VSCodeButton>
                            <VSCodeButton
                                appearance="primary"
                                onClick={() => onApply(message.id)}
                                title="Apply changes to file"
                            >
                                <i className="codicon codicon-check"></i>
                                Apply
                            </VSCodeButton>
                        </>
                    )}
                    {message.isApplied && (
                        <VSCodeBadge appearance="success">
                            <i className="codicon codicon-check"></i>
                            Applied
                        </VSCodeBadge>
                    )}
                    {!message.canApply && !message.isApplied && (
                        <VSCodeBadge>
                            <i className="codicon codicon-x"></i>
                            Rejected
                        </VSCodeBadge>
                    )}
                </div>
            </div>

            {codeEdit.explanation && (
                <div className="code-edit-explanation">
                    <p>{codeEdit.explanation}</p>
                </div>
            )}

            <div className="code-edit-diff">
                <div className="diff-header">
                    <span>Changes Preview: </span>
                    <span className="diff-stats">
                        {changedLines > 0 ? `${changedLines} line${changedLines > 1 ? 's' : ''} changed` : 'No changes'}
                    </span>
                </div>

                {changedLines > 0 && (
                    <div className="diff-unified">
                        <pre className="diff-code">
                            {diffLines.map((diffLine, index) => (
                                <div
                                    key={index}
                                    className={`diff-line diff-line-${diffLine.type} ${diffLine.isContext ? 'diff-context' : ''}`}
                                >
                                    <span className="diff-marker">
                                        {diffLine.type === 'added' ? '+' : diffLine.type === 'removed' ? '-' : ' '}
                                    </span>
                                    {diffLine.lineNumber && (
                                        <span className="diff-line-number">
                                            {diffLine.lineNumber}
                                        </span>
                                    )}
                                    <code>{diffLine.line}</code>
                                </div>
                            ))}
                        </pre>
                    </div>
                )}

                {changedLines === 0 && (
                    <div className="no-changes">
                        <p>No changes detected in the file.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

// Enhanced Message Component with inline checkpoints
const MessageItem: React.FC<{
    message: ChatMessage;
    messageCheckpoints: Map<string, string>;
    restoredSnapshots: Set<string>;
    onApply: (messageId: string) => void;
    onReject: (messageId: string) => void;
    onCreateCheckpoint: (description: string, messageId: string) => void;
    onRestoreToCheckpoint: (checkpointId: string) => void;
    // Removed: executionMode - plan/act mode eliminated
    analysisEvents?: any[];
}> = ({
    message,
    messageCheckpoints,
    restoredSnapshots,
    onApply,
    onReject,
    onCreateCheckpoint,
    onRestoreToCheckpoint,
    analysisEvents
}) => {
        const isUser = message.role === 'user';
        const isStreaming = message.isStreaming;
        const [showCopy, setShowCopy] = useState(false);
        const [copySuccess, setCopySuccess] = useState(false);
        const [showAnalysisCopy, setShowAnalysisCopy] = useState(false);
        const [analysisCopySuccess, setAnalysisCopySuccess] = useState(false);

        const handleCopyMessage = useCallback(async () => {
            try {
                // Build complete message content including analysis
                let fullContent = message.content;

                // If there's analysis content, include it
                if (message.analysisContent && message.analysisContent.trim()) {
                    const analysisText = message.analysisContent
                        .replace(/<[^>]*>/g, '') // Remove HTML tags
                        .replace(/&nbsp;/g, ' ') // Replace HTML entities
                        .replace(/&lt;/g, '<')
                        .replace(/&gt;/g, '>')
                        .replace(/&amp;/g, '&')
                        .trim();

                    if (analysisText) {
                        fullContent = `🧠 AI Reasoning & Analysis Process:\n\n${analysisText}\n\n---\n\n${message.content}`;
                    }
                }

                await navigator.clipboard.writeText(fullContent);
                setCopySuccess(true);
                setTimeout(() => setCopySuccess(false), 2000);
            } catch (err) {
                console.error('Failed to copy message:', err);
            }
        }, [message.content, message.analysisContent]);

        const handleCopyAnalysis = useCallback(async () => {
            try {
                if (message.analysisContent && message.analysisContent.trim()) {
                    const analysisText = message.analysisContent
                        .replace(/<[^>]*>/g, '') // Remove HTML tags
                        .replace(/&nbsp;/g, ' ') // Replace HTML entities
                        .replace(/&lt;/g, '<')
                        .replace(/&gt;/g, '>')
                        .replace(/&amp;/g, '&')
                        .trim();

                    await navigator.clipboard.writeText(`🧠 AI Reasoning & Analysis Process:\n\n${analysisText}`);
                    setAnalysisCopySuccess(true);
                    setTimeout(() => setAnalysisCopySuccess(false), 2000);
                }
            } catch (err) {
                console.error('Failed to copy analysis:', err);
            }
        }, [message.analysisContent]);

        const handleCreateCheckpoint = useCallback((description: string) => {
            onCreateCheckpoint(description, message.id);
        }, [onCreateCheckpoint, message.id]);

        return (
            <div className={`message ${isUser ? 'user' : 'assistant'} ${isStreaming ? 'streaming' : ''}`}>
                {/* Always show simple checkpoint controls for code edit messages */}
                {message.messageType === 'file-edit' && message.codeEdit && (() => {
                    const snapshotId = messageCheckpoints.get(message.id);
                    const isRestored = snapshotId ? restoredSnapshots.has(snapshotId) : false;
                    console.log(`🔄 MessageItem: Rendering checkpoint controls for message ${message.id}, snapshotId: ${snapshotId}, isRestored: ${isRestored}`);
                    console.log(`🔄 MessageItem: messageCheckpoints has:`, Array.from(messageCheckpoints.entries()));
                    return (
                        <SimpleCheckpointControls
                            snapshotId={snapshotId}
                            onRestore={onRestoreToCheckpoint}
                            alwaysShow={true}
                            isRestored={isRestored}
                            key={`checkpoint-${message.id}-${snapshotId || 'none'}`}
                        />
                    );
                })()}

                {message.messageType === 'file-edit' && message.codeEdit ? (
                    <CodeEditMessage
                        message={message}
                        onApply={onApply}
                        onReject={onReject}
                        // Removed: executionMode prop
                    />
                ) : (
                    <div className="message-text">
                        {/* Analysis Content - Complete LLM Thinking Process - Only for AI messages */}
                        {!isUser && ((message.analysisContent && message.analysisContent.trim()) || message.streamingPhase === 'analysis') ? (
                            <details
                                className="analysis-section my-2 px-2"
                                open
                                onMouseEnter={() => setShowAnalysisCopy(true)}
                                onMouseLeave={() => setShowAnalysisCopy(false)}
                            >
                                <summary className="analysis-toggle">
                                    <span className="analysis-title">
                                        🧠 AI Reasoning & Analysis Process
                                    </span>
                                    <div className="analysis-actions">
                                        <span className="analysis-meta">
                                            {message.streamingPhase === 'analysis' ? 'Analyzing...' : message.analysisContent ? 'Analysis Complete' : 'Initializing...'}
                                            {message.streamingPhase === 'analysis' && (
                                                <span className="analysis-spinner ml-2">🔄</span>
                                            )}
                                        </span>
                                        {showAnalysisCopy && message.analysisContent && message.analysisContent.trim() && (
                                            <button
                                                className="analysis-copy-button ml-2"
                                                onClick={(e) => {
                                                    e.preventDefault();
                                                    e.stopPropagation();
                                                    handleCopyAnalysis();
                                                }}
                                                title="Copy analysis only"
                                            >
                                                {analysisCopySuccess ? (
                                                    <i className="codicon codicon-check"></i>
                                                ) : (
                                                    <i className="codicon codicon-copy"></i>
                                                )}
                                            </button>
                                        )}
                                    </div>
                                </summary>
                                {/* Live analysis steps feed */}
                                {analysisEvents && analysisEvents.length > 0 && (
                                    <div className="analysis-steps mt-2 text-xs text-vscode-description">
                                        {analysisEvents.map((e, idx) => {
                                            const t = e.type || '';
                                            const text = t === 'search_start' ? `Searching workspace (${(e.queries || []).length} terms)...`
                                                : t === 'search_complete' ? `Workspace search complete — ${e.total || 0} hits`
                                                    : t === 'semantic_search_start' ? 'Semantic search (embeddings)...'
                                                        : t === 'semantic_search_complete' ? `Semantic search complete — ${e.total || 0} results`
                                                            : t === 'analysis' ? 'Analyzing...'
                                                                : t === 'analysis_complete' ? 'Analysis complete'
                                                                    : t === 'execution_start' ? 'Executing...'
                                                                        : (e.content ? String(e.content).slice(0, 80) : '');
                                            const icon = t.includes('complete') ? '✅' : (t.includes('start') ? '🟡' : '•');
                                            return (
                                                <div key={idx} className="flex items-center gap-2 py-0.5">
                                                    <span>{icon}</span>
                                                    <span>{text}</span>
                                                </div>
                                            );
                                        })}
                                    </div>
                                )}
                                {/* Remove unintended background highlights from inline code in analysis markdown */}
                                <style>
                                    {`
                                    .analysis-markdown code,
                                    .analysis-markdown pre,
                                    .analysis-markdown pre code,
                                    .analysis-markdown .inline-code,
                                    .analysis-markdown .code-block {
                                        background: transparent !important;
                                        box-shadow: none !important;
                                    }
                                    .analysis-markdown mark,
                                    .analysis-markdown kbd {
                                        background: transparent !important;
                                    }
                                    `}
                                </style>
                                {message.analysisContent ? (
                                    <>
                                        <div className="analysis-markdown mt-2" dangerouslySetInnerHTML={{
                                            __html: renderMarkdown((() => {
                                                const raw = message.analysisContent || '';
                                                const fenceCount = (raw.match(/```/g) || []).length;
                                                if (fenceCount % 2 !== 0) {
                                                    return raw + '\n```';
                                                }
                                                return raw;
                                            })())
                                        }} />
                                        {message.streamingPhase === 'analysis' && (
                                            <div className="analysis-thinking mt-2">
                                                ✍️ Still thinking...
                                            </div>
                                        )}
                                    </>
                                ) : (
                                    <div className="analysis-loading mt-2">
                                        <VSCodeProgressRing className="loading-spinner" />
                                        AI is analyzing your request and thinking through the approach...
                                    </div>
                                )}
                            </details>
                        ) : null}

                        {/* Main Message Content - different structure for user vs AI */}
                        {isUser ? (
                            /* User message: simple structure with copy on hover */
                            <div
                                className="user-message-container"
                                onMouseEnter={() => setShowCopy(true)}
                                onMouseLeave={() => setShowCopy(false)}
                            >
                                <div className="message-markdown" dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }} />
                                {showCopy && (
                                    <button
                                        className="copy-button user-copy-button"
                                        onClick={handleCopyMessage}
                                        title="Copy message"
                                    >
                                        {copySuccess ? (
                                            <i className="codicon codicon-check"></i>
                                        ) : (
                                            <i className="codicon codicon-copy"></i>
                                        )}
                                    </button>
                                )}
                            </div>
                        ) : (
                            /* AI message: main content with copy on hover (separate from analysis) */
                            <div
                                className="ai-message-container"
                                onMouseEnter={() => setShowCopy(true)}
                                onMouseLeave={() => setShowCopy(false)}
                            >
                                <div className="message-markdown" dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }} />
                                {showCopy && (
                                    <button
                                        className="copy-button ai-copy-button"
                                        onClick={handleCopyMessage}
                                        title="Copy complete message (includes analysis + response)"
                                    >
                                        {copySuccess ? (
                                            <i className="codicon codicon-check"></i>
                                        ) : (
                                            <i className="codicon codicon-copy"></i>
                                        )}
                                    </button>
                                )}
                            </div>
                        )}
                    </div>
                )}

                {/* Removed individual message streaming indicator - using global one instead */}
            </div>
        );
    };





const ModelSelector: React.FC<{
    selectedModel: string;
    onModelChange: (model: string) => void;
}> = ({ selectedModel }) => {
    const models = [
        { value: 'gpt-4', label: 'GPT-4' },
        { value: 'gpt-4-turbo', label: 'GPT-4 Turbo' },
        { value: 'gpt-3.5-turbo', label: 'GPT-3.5 Turbo' },
        { value: 'claude-3-sonnet', label: 'Claude 3 Sonnet' },
        { value: 'claude-3-haiku', label: 'Claude 3 Haiku' },
    ];

    return (
        <div className="model-selector">
            <label>Model:</label>
            <VSCodeDropdown value={selectedModel}>
                {models.map(model => (
                    <VSCodeOption key={model.value} value={model.value}>
                        {model.label}
                    </VSCodeOption>
                ))}
            </VSCodeDropdown>
        </div>
    );
};

const AUTO_SCROLL_FLAG_NONE = 0;
const AUTO_SCROLL_FLAG_FORCED = 1;
const AUTO_SCROLL_FLAG_AUTOMATIC = 2;

export function ChatPage(props: ChatPageProps = {}) {
    const { showContextPanel = true } = props;
    
    // PRE loop mode is now MANDATORY (paper compliance)
    const reasoningMode = 'pre_loop';  // No longer toggleable

    // Enhanced state management
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [conversationId] = useState<string>(() => `conv_${Date.now()}`);
    const [availableCheckpoints, setAvailableCheckpoints] = useState<any[]>([]);
    const [messageCheckpoints, setMessageCheckpoints] = useState<Map<string, string>>(new Map());
    const [restoredSnapshots, setRestoredSnapshots] = useState<Set<string>>(new Set());

    // Debug effect to track messageCheckpoints changes
    useEffect(() => {
        console.log('🔄 ChatPage: messageCheckpoints updated, size:', messageCheckpoints.size);
        console.log('🔄 ChatPage: messageCheckpoints entries:', Array.from(messageCheckpoints.entries()));
    }, [messageCheckpoints]);

    const [isReady, setIsReady] = useState(false);
    const [prompt, setPrompt] = useState("");
    const [autoScrollFlag, setAutoScrollFlag] = useState(AUTO_SCROLL_FLAG_NONE);

    const [selectedModel, setSelectedModel] = useState(props.selectedModel || 'gpt-4');
    const [isStreaming, setIsStreaming] = useState(false);
    const [contextFiles, setContextFiles] = useState<ContextFile[]>(props.contextFiles || []);
    const [showInputActions, setShowInputActions] = useState(false);
    const [inputSearchQuery, setInputSearchQuery] = useState('');
    const [inputSearchResults, setInputSearchResults] = useState<SearchResultItem[]>([]);
    const [showInputSearchResults, setShowInputSearchResults] = useState(false);
    const chatListRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLTextAreaElement>(null);

    // Simple flags to prevent duplicate analysis processing
    const analysisPhaseProcessed = useRef<boolean>(false);
    const executionPhaseProcessed = useRef<boolean>(false);

    // Effect to ensure proper padding for send button
    useEffect(() => {
        const textarea = inputRef.current;
        if (textarea) {
            textarea.style.paddingRight = '48px';
            // Also try to find the VSCode textarea element
            const vscodeTextarea = textarea.closest('vscode-text-area')?.shadowRoot?.querySelector('textarea');
            if (vscodeTextarea) {
                (vscodeTextarea as HTMLElement).style.paddingRight = '48px';
            }
        }
    }, []);
    const inputActionsRef = useRef<HTMLDivElement>(null);
    const inputSearchRef = useRef<HTMLInputElement>(null);

    // Analysis progress events per assistant message id
    const [analysisEventsByMessage, setAnalysisEventsByMessage] = useState<Map<string, any[]>>(new Map());
    const currentAnalysisMessageIdRef = useRef<string | null>(null);

    // Listen for reasoningUpdate events from provider and store per-message
    useEffect(() => {
        const onReasoningUpdate = (evt: any) => {
            const detail = evt?.detail || {};
            const msgId = detail.messageId || currentAnalysisMessageIdRef.current;
            if (!msgId) return;
            setAnalysisEventsByMessage(prev => {
                const newMap = new Map(prev);
                const arr = newMap.get(msgId) ? [...(newMap.get(msgId) as any[])] : [];
                arr.push({ ...detail, timestamp: Date.now() });
                newMap.set(msgId, arr);
                return newMap;
            });
        };
        window.addEventListener('reasoningUpdate', onReasoningUpdate as EventListener);
        return () => window.removeEventListener('reasoningUpdate', onReasoningUpdate as EventListener);
    }, []);

    // Checkpoint management functions
    const fetchCheckpoints = useCallback(async () => {
        // Backend checkpoint fetching disabled - using VS Code native snapshots only
        console.log('🔖 Backend checkpoint fetching disabled - using VS Code native snapshots');
        setAvailableCheckpoints([]);
    }, []);

    const handleCreateCheckpoint = useCallback(async (_description: string, _messageId: string) => {
        // Backend checkpoint creation disabled - using VS Code native snapshots only
        console.log('🔖 Manual checkpoint creation disabled - using automatic VS Code native snapshots');
        return;
    }, []);

    const handleRestoreCheckpoint = useCallback(async (checkpointId: string, _restoreType: 'code' | 'conversation' | 'both') => {
        // Note: The actual restore command is already sent by SimpleCheckpointControls.handleRestore()
        // This callback is just for any additional UI updates or logging
        console.log('🔄 Restore checkpoint callback triggered for:', checkpointId);
        console.log('🔄 Restore command already sent by SimpleCheckpointControls - no duplicate needed');
    }, []);

    const handleCompareCheckpoint = useCallback(async (checkpointId: string) => {
        try {
            // Send message to VS Code extension to handle checkpoint comparison via MCP
            const vscode = getVsCodeApi();
            if (!vscode) {
                console.error('VS Code API not available for checkpoint comparison.');
                return;
            }
            vscode.postMessage({
                type: 'compareCheckpoint',
                checkpointId: checkpointId,
                conversationId: conversationId
            });
        } catch (error) {
            console.error('Failed to compare checkpoint:', error);
        }
    }, [conversationId]);

    // Load checkpoints on component mount
    useEffect(() => {
        fetchCheckpoints();
    }, [fetchCheckpoints]);

    // Add CSS styles for the improved diff display
    React.useEffect(() => {
        const styles = `
            .diff-context {
                opacity: 0.6;
                background-color: var(--vscode-editor-background) !important;
            }
            
            .diff-line-unchanged.diff-context {
                color: var(--vscode-descriptionForeground);
            }
            
            .diff-line-added {
                background-color: var(--vscode-diffEditor-insertedTextBackground);
                border-left: 3px solid var(--vscode-diffEditor-insertedLineBackground);
            }
            
            .diff-line-removed {
                background-color: var(--vscode-diffEditor-removedTextBackground);
                border-left: 3px solid var(--vscode-diffEditor-removedLineBackground);
            }
            
            .diff-line-unchanged {
                background-color: var(--vscode-editor-background);
            }
            
            .diff-marker {
                display: inline-block;
                width: 20px;
                text-align: center;
                font-weight: bold;
                margin-right: 8px;
            }
            
            .diff-line-number {
                display: inline-block;
                width: 60px;
                text-align: right;
                margin-right: 8px;
                color: var(--vscode-editorLineNumber-foreground);
                font-size: 0.9em;
            }
            
            .diff-code {
                font-family: var(--vscode-editor-font-family);
                font-size: var(--vscode-editor-font-size);
                line-height: 1.4;
                margin: 0;
                padding: 8px;
                border: 1px solid var(--vscode-panel-border);
                border-radius: 4px;
            }
            
            .diff-line {
                padding: 2px 8px;
                margin: 0;
                white-space: pre-wrap;
                word-break: break-all;
            }
            
            .code-edit-stats {
                display: flex;
                gap: 8px;
                align-items: center;
            }
            
            .code-edit-explanation {
                background-color: var(--vscode-textBlockQuote-background);
                border-left: 4px solid var(--vscode-textBlockQuote-border);
                padding: 12px;
                margin: 8px 0;
                font-style: italic;
                color: var(--vscode-descriptionForeground);
            }
            
            .no-changes {
                text-align: center;
                padding: 20px;
                color: var(--vscode-descriptionForeground);
                background-color: var(--vscode-textBlockQuote-background);
                border-radius: 4px;
            }
            
            /* Analysis Panel (squared, Tailwind-like) */
            .analysis-section {
                border: 1px solid var(--vscode-panel-border);
                border-radius: 4px; /* squared look consistent with editor */
                background: var(--vscode-editor-background);
                margin: 8px 0 12px 0;
                overflow: hidden; /* square corners for inner content */
                box-shadow: 0 0 0 1px transparent;
            }
            .analysis-section[open] {
                border-color: var(--vscode-widget-border);
            }
            .analysis-section > .analysis-toggle {
                list-style: none;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 8px;
                padding: 10px 12px;
                background: var(--vscode-editorWidget-background);
                border-bottom: 1px solid var(--vscode-panel-border);
                cursor: pointer;
                user-select: none;
            }
            .analysis-section > .analysis-toggle::-webkit-details-marker {
                display: none; /* remove default marker for squared header */
            }
            .analysis-title {
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-weight: 600;
                color: var(--vscode-editor-foreground);
            }
            .analysis-actions {
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .analysis-meta {
                color: var(--vscode-descriptionForeground);
                font-size: 12px;
            }
            .analysis-copy-button {
                background: #FFA500; /* Yellow/Orange background */
                color: white;
                border: 1px solid #FF8C00;
                border-radius: 3px;
                padding: 6px;
                font-size: 14px;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
                transition: all 0.2s ease;
                width: 28px;
                height: 28px;
            }
            .analysis-copy-button:hover {
                background: #FF8C00;
                transform: scale(1.1);
            }
            .analysis-copy-button:active {
                transform: scale(0.95);
            }
            
            /* Message container styling with inline copy buttons */
            .user-message-container,
            .ai-message-container {
                position: relative;
                margin-top: 8px;
            }
            
            /* Copy buttons - icon-only with specific colors */
            .user-message-container .copy-button,
            .ai-message-container .copy-button {
                position: absolute;
                top: 12px;
                right: 12px;
                border: none;
                border-radius: 50%;
                padding: 8px;
                font-size: 14px;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
                transition: all 0.2s ease;
                z-index: 10;
                width: 32px;
                height: 32px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
            }
            
            /* User copy button - Blue */
            .user-copy-button {
                background: #007ACC !important;
                color: white !important;
            }
            .user-copy-button:hover {
                background: #005A9E !important;
                transform: scale(1.1);
            }
            
            /* AI response copy button - Green */
            .ai-copy-button {
                background: #28A745 !important;
                color: white !important;
            }
            .ai-copy-button:hover {
                background: #1E7E34 !important;
                transform: scale(1.1);
            }
            
            .user-copy-button:active,
            .ai-copy-button:active {
                transform: scale(0.95);
            }
            
            /* Ensure message content has padding for copy button */
            .user-message-container .message-markdown,
            .ai-message-container .message-markdown {
                padding-right: 80px; /* Make room for copy button */
                min-height: 40px; /* Ensure minimum height for button placement */
            }
            .analysis-spinner {
                margin-left: 8px;
                opacity: 0.85;
            }
            .analysis-content {
                background: var(--vscode-editor-background);
                padding: 10px 12px;
            }
            .analysis-content-inner {
                border-left: 3px solid var(--vscode-textLink-foreground);
                padding-left: 10px;
            }
            .analysis-thinking {
                margin-top: 8px;
                color: var(--vscode-descriptionForeground);
                font-style: italic;
            }

            /* Enhanced Markdown Styles */
            .markdown-h1 {
                font-size: 1.5em;
                font-weight: bold;
                color: var(--vscode-editor-foreground);
                margin: 16px 0 12px 0;
                line-height: 1.2;
                border-bottom: 2px solid var(--vscode-textLink-foreground);
                padding-bottom: 6px;
            }
            
            .markdown-h2 {
                font-size: 1.3em;
                font-weight: bold;
                color: var(--vscode-editor-foreground);
                margin: 14px 0 10px 0;
                line-height: 1.2;
                border-bottom: 1px solid var(--vscode-widget-border);
                padding-bottom: 4px;
            }
            
            .markdown-h3 {
                font-size: 1.1em;
                font-weight: bold;
                color: var(--vscode-editor-foreground);
                margin: 12px 0 8px 0;
                line-height: 1.2;
            }
            
            .markdown-bold {
                font-weight: bold;
                color: var(--vscode-textPreformat-foreground);
            }
            
            .markdown-italic {
                font-style: italic;
                color: var(--vscode-descriptionForeground);
            }
            
            .markdown-ul {
                margin: 8px 0;
                padding-left: 20px;
            }
            
            .markdown-ol {
                margin: 8px 0;
                padding-left: 20px;
            }
            
            .markdown-li {
                margin: 4px 0;
                color: var(--vscode-editor-foreground);
                line-height: 1.4;
            }
            
            /* Enhanced code styles */
            .inline-code {
                background-color: var(--vscode-textCodeBlock-background);
                color: var(--vscode-textPreformat-foreground);
                padding: 2px 4px;
                border-radius: 3px;
                font-family: var(--vscode-editor-font-family);
                font-size: 0.9em;
                border: 1px solid var(--vscode-widget-border);
            }
            
            .code-block {
                background-color: var(--vscode-textCodeBlock-background);
                border: 1px solid var(--vscode-panel-border);
                border-radius: 4px;
                padding: 12px;
                margin: 8px 0;
                overflow-x: auto;
                font-family: var(--vscode-editor-font-family);
                font-size: var(--vscode-editor-font-size);
                line-height: 1.4;
            }
            
            .code-block code {
                background: none;
                padding: 0;
                border: none;
                color: var(--vscode-textPreformat-foreground);
            }

            /* ReactMarkdown Analysis Styles */
            .analysis-markdown h1,
            .analysis-markdown h2,
            .analysis-markdown h3,
            .analysis-markdown h4 {
                margin: 12px 0 6px 0;
                font-weight: 600;
                color: var(--vscode-symbolIcon-colorForeground);
            }
            
            .analysis-markdown h1 {
                font-size: 14px;
                border-bottom: 2px solid var(--vscode-textLink-foreground);
                padding-bottom: 4px;
            }
            
            .analysis-markdown h2 {
                font-size: 13px;
                border-bottom: 1px solid var(--vscode-widget-border);
                padding-bottom: 2px;
            }
            
            .analysis-markdown h3 {
                font-size: 12px;
            }
            
            .analysis-markdown p {
                margin: 6px 0;
                line-height: 1.4;
                color: var(--vscode-editor-foreground);
            }
            
            .analysis-markdown ul,
            .analysis-markdown ol {
                margin: 6px 0;
                padding-left: 16px;
            }
            
            .analysis-markdown li {
                margin: 3px 0;
                line-height: 1.3;
                color: var(--vscode-editor-foreground);
            }
            
            .analysis-markdown strong {
                color: var(--vscode-textLink-foreground);
                font-weight: 600;
            }
            
            .analysis-markdown em {
                color: var(--vscode-descriptionForeground);
                font-style: italic;
            }
            
            .analysis-markdown code {
                background-color: var(--vscode-textCodeBlock-background);
                color: var(--vscode-textPreformat-foreground);
                padding: 1px 4px;
                border-radius: 3px;
                font-family: var(--vscode-editor-font-family);
                font-size: 11px;
                border: 1px solid var(--vscode-widget-border);
            }
            
            .analysis-markdown pre {
                background-color: var(--vscode-textCodeBlock-background);
                border: 1px solid var(--vscode-panel-border);
                border-radius: 4px;
                padding: 8px;
                margin: 8px 0;
                overflow-x: auto;
                font-size: 11px;
                line-height: 1.3;
            }
            
            .analysis-markdown pre code {
                background: none;
                border: none;
                padding: 0;
                font-size: inherit;
            }
            
            /* ReactMarkdown Message Styles */
            .message-markdown h1,
            .message-markdown h2,
            .message-markdown h3,
            .message-markdown h4 {
                margin: 16px 0 8px 0;
                font-weight: 600;
                color: var(--vscode-editor-foreground);
            }
            
            .message-markdown p {
                margin: 8px 0;
                line-height: 1.5;
                color: var(--vscode-editor-foreground);
            }
            
            .message-markdown ul,
            .message-markdown ol {
                margin: 8px 0;
                padding-left: 20px;
            }
            
            .message-markdown li {
                margin: 4px 0;
                line-height: 1.4;
                color: var(--vscode-editor-foreground);
            }
            
            .message-markdown strong {
                color: var(--vscode-textLink-foreground);
                font-weight: 600;
            }
            
            .message-markdown em {
                color: var(--vscode-descriptionForeground);
                font-style: italic;
            }
            
            .message-markdown code {
                background-color: var(--vscode-textCodeBlock-background);
                color: var(--vscode-textPreformat-foreground);
                padding: 2px 4px;
                border-radius: 3px;
                font-family: var(--vscode-editor-font-family);
                font-size: 0.9em;
                border: 1px solid var(--vscode-widget-border);
            }
            
            .message-markdown pre {
                background-color: var(--vscode-textCodeBlock-background);
                border: 1px solid var(--vscode-panel-border);
                border-radius: 4px;
                padding: 12px;
                margin: 12px 0;
                overflow-x: auto;
                font-family: var(--vscode-editor-font-family);
                line-height: 1.4;
            }
            
            .message-markdown pre code {
                background: none;
                border: none;
                padding: 0;
            }
        `;

        const styleSheet = document.createElement('style');
        styleSheet.type = 'text/css';
        styleSheet.innerText = styles;
        document.head.appendChild(styleSheet);

        return () => {
            document.head.removeChild(styleSheet);
        };
    }, []);

    // Enhanced message handling with support for code edits and auto-checkpoints
    const addMessage = useCallback(async (content: string, role: 'user' | 'assistant' | 'system', messageType: 'text' | 'code' | 'diff' | 'file-edit' = 'text', codeEdit?: ChatMessage['codeEdit'], isFinished = false, messageId?: string, streamingPhase?: 'analysis' | 'execution' | 'response') => {
        console.log('🔵 addMessage called:', {
            role,
            messageType,
            isFinished,
            messageId,
            streamingPhase,
            contentLength: content?.length || 0,
            contentPreview: content?.substring(0, 50) || 'empty'
        });

        const message: ChatMessage = {
            id: messageId || Date.now().toString(),  // Use extension ID if provided, otherwise generate timestamp
            role,
            content,
            timestamp: new Date(),
            isStreaming: role === 'assistant' && !isFinished,  // Only stream if assistant message and not finished
            messageType,
            codeEdit,
            canApply: !!codeEdit,
            isApplied: false,
            streamingPhase: role === 'assistant' && !isFinished ? streamingPhase || 'analysis' : undefined,
            analysisContent: '',
            executionContent: '',
            analysisComplete: false,
            executionStarted: false
        };

        console.log('🔵 Created message:', {
            id: message.id,
            role: message.role,
            isStreaming: message.isStreaming,
            streamingPhase: message.streamingPhase,
            messageType: message.messageType
        });

        // Auto-create checkpoint before code edit messages
        if (messageType === 'file-edit' && role === 'assistant' && isFinished) {
            try {
                const checkpointDescription = `Before code change #${messages.length + 1}`;
                const messageIndex = messages.length; // Current position before adding this message

                console.log('🔖 Creating auto-checkpoint:', checkpointDescription);

                // File snapshots are automatically handled by VS Code native snapshot manager
                console.log('🔖 VS Code native snapshots handle file capture automatically');

                // Backend checkpoint creation removed - now using VS Code native snapshots
                // Snapshots are automatically created by chatServiceImpl.ts and linked via snapshotCreated message
                console.log('🔖 Backend checkpoint creation disabled - using VS Code native snapshots');
            } catch (error) {
                console.error('Failed to create auto-checkpoint:', error);
            }
        }

        setMessages(prev => [...prev, message]);
        console.log('🔵 Messages updated, new count:', messages.length + 1);
        console.log('🔵 New message analysis details:', {
            id: message.id,
            role: message.role,
            isStreaming: message.isStreaming,
            streamingPhase: message.streamingPhase,
            analysisContent: message.analysisContent || 'empty',
            shouldShowAnalysisPanel: (message.analysisContent && message.analysisContent.trim()) || message.streamingPhase === 'analysis'
        });
        setAutoScrollFlag(AUTO_SCROLL_FLAG_FORCED);

        return message.id;
    }, [conversationId, messages, fetchCheckpoints, contextFiles]);

    // Handle apply code changes
    const handleApplyCode = useCallback(async (messageId: string) => {
        const message = messages.find(msg => msg.id === messageId);
        if (!message || !message.codeEdit) return;

        try {
            // Send message to VS Code extension to apply the code edit
            const vscode = getVsCodeApi();
            if (!vscode) {
                console.error('VS Code API not available to apply code edit.');
                return;
            }
            vscode.postMessage({
                type: 'applyCodeEdit',
                messageId: messageId,
                codeEdit: message.codeEdit
            });

            // Mark message as applied optimistically
            setMessages(prev => prev.map(msg =>
                msg.id === messageId
                    ? { ...msg, isApplied: true }
                    : msg
            ));
        } catch (error) {
            console.error('Failed to apply code:', error);
        }
    }, [messages]);

    // Handle reject code changes
    const handleRejectCode = useCallback((messageId: string) => {
        try {
            // Send message to VS Code extension
            const vscode = getVsCodeApi();
            if (!vscode) {
                console.error('VS Code API not available to reject code edit.');
                return;
            }
            vscode.postMessage({
                type: 'rejectCodeEdit',
                messageId: messageId
            });

            // Mark message as rejected
            setMessages(prev => prev.map(msg =>
                msg.id === messageId
                    ? { ...msg, canApply: false }
                    : msg
            ));
        } catch (error) {
            console.error('Failed to reject code:', error);
        }
    }, []);

    // Context file management
    const addContextFile = useCallback((file: ContextFile) => {
        setContextFiles(prev => {
            const exists = prev.find(f => f.uri === file.uri);
            if (exists) return prev;
            return [...prev, file];
        });
    }, []);

    const removeContextFile = useCallback((uri: string) => {
        setContextFiles(prev => prev.filter(f => f.uri !== uri));
    }, []);

    const handleAddFileToContext = useCallback(async () => {
        try {
            console.log('🔵 Webview: Adding file to context...');
            const vscode = getVsCodeApi();
            if (vscode) {
                // Request VS Code to add the current file to context
                vscode.postMessage({ type: 'addFileToContext' });
            } else {
                console.error('VS Code API not available to add file to context');
            }
        } catch (error) {
            console.error('Failed to add file to context:', error);
        }
    }, []);

    const handleAddFilesByPath = useCallback(async () => {
        try {
            console.log('🔵 Webview: Adding files by path...');
            const vscode = getVsCodeApi();
            if (vscode) {
                // Request VS Code to show file picker
                vscode.postMessage({ type: 'showFilePicker' });
            } else {
                console.error('VS Code API not available to show file picker');
            }
        } catch (error) {
            console.error('Failed to show file picker:', error);
        }
    }, []);

    const handleAddAllOpenFiles = useCallback(async () => {
        try {
            console.log('🔵 Webview: Adding all open files...');
            const vscode = getVsCodeApi();
            if (vscode) {
                // Request VS Code to add all open files
                vscode.postMessage({ type: 'addAllOpenFiles' });
            } else {
                console.error('VS Code API not available to add all open files');
            }
        } catch (error) {
            console.error('Failed to add all open files:', error);
        }
    }, []);

    // Input search handlers (similar to context panel search)
    const handleInputSearchChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
        const query = e.target.value;
        setInputSearchQuery(query);

        if (query.trim().length > 0) {
            setShowInputSearchResults(true);
        } else {
            setShowInputSearchResults(false);
            setInputSearchResults([]);
        }
    }, []);

    // Debounced input search effect
    useEffect(() => {
        if (inputSearchQuery.trim().length > 0) {
            const timeoutId = setTimeout(() => {
                const vscode = getVsCodeApi();
                if (vscode) {
                    vscode.postMessage({
                        type: 'searchFiles',
                        query: inputSearchQuery
                    });
                }
            }, 300);

            return () => clearTimeout(timeoutId);
        }
        return undefined;
    }, [inputSearchQuery]);

    // Handle input search result selection
    const handleInputSearchResultClick = useCallback((result: SearchResultItem) => {
        const vscode = getVsCodeApi();
        if (vscode) {
            vscode.postMessage({
                type: 'addFileByUri',
                uri: result.uri
            });
        }
        setInputSearchQuery('');
        setShowInputSearchResults(false);
        setInputSearchResults([]);
        setShowInputActions(false);
    }, []);

    // Handle input search input key events
    const handleInputSearchKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' && inputSearchResults.length > 0) {
            // Select the first result
            handleInputSearchResultClick(inputSearchResults[0]);
        } else if (e.key === 'Escape') {
            setInputSearchQuery('');
            setShowInputSearchResults(false);
            setInputSearchResults([]);
        }
    }, [inputSearchResults, handleInputSearchResultClick]);

    // Close input actions menu when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (inputActionsRef.current && !inputActionsRef.current.contains(event.target as Node)) {
                setShowInputActions(false);
                setShowInputSearchResults(false);
                setInputSearchQuery('');
                setInputSearchResults([]);
            }
        };

        if (showInputActions) {
            document.addEventListener('mousedown', handleClickOutside);
        }

        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [showInputActions]);

    // Function to request context file refresh
    const refreshContextFiles = useCallback(async () => {
        console.log('🔵 Webview: Requesting context files refresh...');
        const vscode = getVsCodeApi();
        if (vscode) {
            vscode.postMessage({ type: 'refreshContextFiles' });
        }
    }, []);

    // Error message generator for backend connection issues
    const generateErrorMessage = (error: string) => {
        return `❌ **Connection Error**\n\nCannot connect to SuperInference backend.\n\n**Error:** ${error}\n\n**Please ensure:**\n1. Backend is running on localhost:3000\n2. Run: \`python3 mock_cursor_backend.py --port 3000 --debug\`\n3. Check VS Code Developer Tools for more details`;
    };

    // Enhanced message handling with real backend streaming
    const handleAskAction = useCallback(async () => {
        if (!prompt.trim() || !isReady) return;

        const userMessage = prompt;
        setPrompt("");
        setIsStreaming(true);

        // Reset analysis flags for new conversation
        analysisPhaseProcessed.current = false;
        executionPhaseProcessed.current = false;

        // Refresh context files before sending the request
        await refreshContextFiles();

        // Small delay to ensure context is refreshed
        await new Promise(resolve => setTimeout(resolve, 100));

        // Determine which streaming operation to use based on execution mode and context
        const currentFile = null;
        const hasSelection = false;

        try {
            const vscode = getVsCodeApi();
            if (!vscode) {
                console.error('VS Code API not available for streaming.');
                // Show error message if VS Code API is not available
                const errorMessage = generateErrorMessage('VS Code API not available');
                addMessage(errorMessage, 'assistant', 'text', undefined, true);
                setIsStreaming(false);
                return;
            }
            console.log('🔵 Webview: Acquired VS Code API successfully');
            console.log('🔵 Webview: Current reasoning mode:', reasoningMode);

            // Use intelligent routing to determine the appropriate endpoint
            if (currentFile && hasSelection && (userMessage.includes('edit') || userMessage.includes('change') || userMessage.includes('modify'))) {
                // Use streamEdit for file editing operations
                console.log('🔵 Webview: Sending streamEdit message:', userMessage);
                vscode.postMessage({
                    type: 'streamEdit',
                    prompt: userMessage,
                    filePath: currentFile,
                    selection: null
                });
            } else if (userMessage.includes('generate') || userMessage.includes('create')) {
                // Use streamCreate for file creation if prompt includes 'create', else streamGenerate
                console.log('🔵 Webview: Sending streamGenerate message:', userMessage);
                if (userMessage.toLowerCase().includes('create')) {
                    vscode.postMessage({
                        type: 'streamCreate',
                        prompt: userMessage,
                        contextFiles: contextFiles
                    });
                } else {
                    vscode.postMessage({
                        type: 'streamGenerate',
                        prompt: userMessage,
                        contextFiles: contextFiles
                    });
                }
            } else {
                // Use streamChat for general conversations
                console.log('🔵 Webview: Sending streamChat message:', userMessage);
                vscode.postMessage({
                    type: 'streamChat',
                    prompt: userMessage,
                    contextFiles: contextFiles
                });
            }

        } catch (error) {
            console.error('🔵 Webview: Backend streaming failed:', error);

            // Show error message if backend connection fails
            const errorMessage = generateErrorMessage(error instanceof Error ? error.message : 'Unknown error');
            addMessage(errorMessage, 'assistant', 'text', undefined, true);
            setIsStreaming(false);
        }
    }, [prompt, isReady, reasoningMode, contextFiles, addMessage, refreshContextFiles]);



    const clearChat = useCallback(async () => {
        setMessages([]);
        // Note: clearSession is not available in the interface, so we just clear the UI
    }, []);

    // Keyboard shortcut handler
    const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
        if (e.key === 'Enter') {
            if (e.ctrlKey || e.metaKey) {
                // Ctrl+Enter or Cmd+Enter sends message
                e.preventDefault();
                handleAskAction();
            } else if (!e.shiftKey) {
                // Enter alone sends message (Shift+Enter for new line)
                e.preventDefault();
                handleAskAction();
            }
            // Shift+Enter allows new line (default behavior)
        }
        if (e.key === 'Escape') {
            setPrompt("");
            inputRef.current?.focus();
        }
        if (e.key === 'a' && e.ctrlKey && e.shiftKey) {
            e.preventDefault();
            handleAddFileToContext();
        }
    }, [handleAskAction, handleAddFileToContext]);

    // Auto-scroll functionality
    useEffect(() => {
        if (!autoScrollFlag) return;

        const chatListEl = chatListRef.current;
        if (!chatListEl) return;

        setAutoScrollFlag(AUTO_SCROLL_FLAG_NONE);

        const targetScrollTop = chatListEl.scrollHeight - chatListEl.clientHeight;

        if (autoScrollFlag === AUTO_SCROLL_FLAG_FORCED) {
            chatListEl.scrollTop = targetScrollTop;
        } else if (autoScrollFlag === AUTO_SCROLL_FLAG_AUTOMATIC) {
            // Smooth scroll for automatic updates
            chatListEl.scrollTo({
                top: targetScrollTop,
                behavior: 'smooth'
            });
        }
    }, [messages, autoScrollFlag]);

    // Listen for messages from VS Code extension
    useEffect(() => {
        console.log('🔵 Webview: Setting up message listener...');
        const handleMessage = async (event: MessageEvent) => {
            console.log('🔵 Webview: Received message from extension:', event.data);
            const { type, ...data } = event.data;

            switch (type) {
                case 'newMessage':
                    console.log('🔵 Webview: Processing newMessage:', data.message);
                    console.log('🔵 Webview: Message details:', {
                        isReply: data.message.isReply,
                        isFinished: data.message.isFinished,
                        contents: data.message.contents?.substring(0, 50) || 'empty',
                        hasCodeEdit: !!data.message?.codeEdit
                    });

                    if (data.message?.codeEdit) {
                        // Handle structured code edit messages
                        console.log('🔵 Webview: Adding code edit message');
                        addMessage(
                            data.message.contents || 'Code edit ready to apply',
                            data.message.isReply ? 'assistant' : 'user',
                            'file-edit',
                            data.message.codeEdit,
                            data.message.isFinished,
                            data.message.id,  // Pass the extension message ID
                            data.message.isReply && !data.message.isFinished ? 'analysis' : undefined // Start in analysis phase for streaming assistant messages
                        );
                    } else {
                        console.log('🔵 Webview: Adding regular message');

                        // Use default text message type
                        const messageType = 'text';

                        addMessage(
                            data.message.contents,
                            data.message.isReply ? 'assistant' : 'user',
                            messageType,
                            undefined,
                            data.message.isFinished,
                            data.message.id,  // Pass the extension message ID
                            data.message.isReply && !data.message.isFinished ? 'analysis' : undefined // Start in analysis phase for streaming assistant messages
                        );
                    }
                    if (data.message.isFinished) {
                        console.log('🔵 Webview: Message finished, setting streaming to false');
                        setIsStreaming(false);
                    } else if (data.message.isReply) {
                        console.log('🔵 Webview: Assistant message not finished, ensuring streaming is true');
                        setIsStreaming(true);
                    }
                    break;

                case 'messageChange':
                    console.log('🔵 Webview: Processing messageChange:', data.message);

                    // Check if this is a special analysis/execution message type
                    const messageData = data.message;
                    const messageType = messageData.type; // This should now work correctly
                    const content = messageData.content || messageData.contents;
                    const analysisData = messageData.analysisData; // New field for analysis content
                    const analysisContent = messageData.analysisContent; // Direct analysis content from extension
                    const streamingPhase = messageData.streamingPhase; // Streaming phase from extension

                    console.log('🔵 Webview: Message details:', {
                        type: messageType,
                        contentLength: content?.length || 0,
                        contentPreview: content?.substring(0, 50) || 'empty',
                        analysisDataLength: analysisData?.length || 0,
                        analysisDataPreview: analysisData?.substring(0, 50) || 'empty',
                        analysisContentLength: analysisContent?.length || 0,
                        analysisContentPreview: analysisContent?.substring(0, 50) || 'empty',
                        streamingPhase: streamingPhase,
                        isFinished: messageData.isFinished,
                        rawContents: messageData.contents?.substring(0, 50) || 'none',
                        rawContent: messageData.content?.substring(0, 50) || 'none'
                    });

                    // ===== DIRECT ANALYSIS CONTENT UPDATE (from simplified flow) =====
                    if (analysisContent !== undefined || streamingPhase !== undefined || (content !== undefined && messageData.isFinished) || messageData.codeEdit !== undefined || messageData.messageType !== undefined) {
                        console.log('🧠 Direct analysis content update from extension');
                        console.log('🧠 Update conditions:', {
                            hasAnalysisContent: analysisContent !== undefined,
                            hasStreamingPhase: streamingPhase !== undefined,
                            hasContent: content !== undefined,
                            isFinished: messageData.isFinished,
                            contentValue: content || 'no content',
                            hasCodeEdit: !!messageData.codeEdit,
                            messageType: messageData.messageType
                        });
                        setMessages(prev => {
                            const messageIndex = prev.findIndex(msg =>
                                msg.id === messageData.id || (msg.role === 'assistant' && msg.isStreaming)
                            );
                            if (messageIndex >= 0) {
                                const updatedMessages = [...prev];
                                const existingMessage = updatedMessages[messageIndex];

                                updatedMessages[messageIndex] = {
                                    ...existingMessage,
                                    ...(analysisContent !== undefined && { analysisContent: analysisContent }),
                                    ...(streamingPhase !== undefined && { streamingPhase: streamingPhase }),
                                    ...(content !== undefined && { content: content }),
                                    ...(messageData.isFinished !== undefined && {
                                        isStreaming: !messageData.isFinished
                                    }),
                                    // ✅ ADD MISSING FIELDS FOR CODEEDIT SUPPORT
                                    ...(messageData.codeEdit !== undefined && {
                                        codeEdit: messageData.codeEdit,
                                        messageType: 'file-edit',
                                        canApply: true,
                                        isApplied: false
                                    }),
                                    ...(messageData.messageType !== undefined && { messageType: messageData.messageType })
                                };

                                console.log('🧠 Updated message with analysis content:', {
                                    id: updatedMessages[messageIndex].id,
                                    analysisContentLength: updatedMessages[messageIndex].analysisContent?.length || 0,
                                    contentLength: updatedMessages[messageIndex].content?.length || 0,
                                    contentPreview: updatedMessages[messageIndex].content?.substring(0, 50) || 'empty',
                                    streamingPhase: updatedMessages[messageIndex].streamingPhase,
                                    isStreaming: updatedMessages[messageIndex].isStreaming,
                                    // ✅ ADD DEBUG FOR CODEEDIT FIELDS
                                    messageType: updatedMessages[messageIndex].messageType,
                                    hasCodeEdit: !!updatedMessages[messageIndex].codeEdit,
                                    canApply: updatedMessages[messageIndex].canApply,
                                    isApplied: updatedMessages[messageIndex].isApplied
                                });

                                return updatedMessages;
                            }
                            return prev;
                        });

                        // Update streaming state if message is finished
                        if (messageData.isFinished) {
                            setIsStreaming(false);
                        }

                        return;
                    }

                    // ===== ANALYSIS PHASE: LLM THINKING/REASONING =====
                    if (messageType === 'analysis_start') {
                        console.log('🧠 Analysis phase started - LLM is thinking...');
                        setMessages(prev => {
                            const messageIndex = prev.findIndex(msg => msg.role === 'assistant' && msg.isStreaming);
                            if (messageIndex >= 0) {
                                const existingMessage = prev[messageIndex];
                                // Only update if not already in analysis phase
                                if (existingMessage.streamingPhase !== 'analysis') {
                                    const updatedMessages = [...prev];
                                    updatedMessages[messageIndex] = {
                                        ...existingMessage,
                                        streamingPhase: 'analysis',
                                        analysisContent: '', // Reset analysis content
                                        content: '' // Keep main content empty during analysis - final result will go here
                                    };
                                    return updatedMessages;
                                }
                            }
                            return prev;
                        });
                        return;
                    }

                    if (messageType === 'analysis') {
                        console.log('🧠 Adding analysis content (LLM thinking)');
                        console.log('🧠 Analysis data to add:', analysisData?.substring(0, 100) || 'empty');
                        // Accumulate the LLM's thinking process in analysisContent
                        setMessages(prev => {
                            const messageIndex = prev.findIndex(msg => msg.role === 'assistant' && msg.isStreaming);
                            if (messageIndex >= 0) {
                                const updatedMessages = [...prev];
                                const existingMessage = updatedMessages[messageIndex];
                                const newAnalysisContent = (existingMessage.analysisContent || '') + (analysisData || '');
                                console.log('🧠 Updated analysis content length:', newAnalysisContent.length);
                                console.log('🧠 Total analysis content preview:', newAnalysisContent.substring(0, 200) + '...');

                                updatedMessages[messageIndex] = {
                                    ...existingMessage,
                                    analysisContent: newAnalysisContent,
                                    streamingPhase: 'analysis'
                                };
                                return updatedMessages;
                            }
                            return prev;
                        });
                        return;
                    }

                    if (messageType === 'analysis_complete') {
                        console.log('🧠 Analysis phase completed - moving to response generation');
                        setMessages(prev => {
                            const messageIndex = prev.findIndex(msg => msg.role === 'assistant' && msg.isStreaming);
                            if (messageIndex >= 0) {
                                const existingMessage = prev[messageIndex];
                                if (!existingMessage.analysisComplete) {
                                    const updatedMessages = [...prev];
                                    updatedMessages[messageIndex] = {
                                        ...existingMessage,
                                        analysisComplete: true,
                                        streamingPhase: 'execution',
                                        content: '' // Keep main content empty until final result
                                    };
                                    return updatedMessages;
                                }
                            }
                            return prev;
                        });
                        return;
                    }

                    // ===== EXECUTION PHASE: RESPONSE GENERATION =====
                    if (messageType === 'execution_start') {
                        console.log('⚙️ Execution phase started - generating response');
                        setMessages(prev => {
                            const messageIndex = prev.findIndex(msg => msg.role === 'assistant' && msg.isStreaming);
                            if (messageIndex >= 0) {
                                const existingMessage = prev[messageIndex];
                                if (!existingMessage.executionStarted) {
                                    const updatedMessages = [...prev];
                                    updatedMessages[messageIndex] = {
                                        ...existingMessage,
                                        executionContent: content || 'Planning approach...',
                                        executionStarted: true,
                                        streamingPhase: 'response',
                                        content: '' // Keep main content empty until final result
                                    };
                                    return updatedMessages;
                                }
                            }
                            return prev;
                        });
                        return;
                    }

                    // ===== REGULAR CONTENT: FINAL ANSWER STREAMING =====
                    // Handle streaming message updates and structured responses
                    setMessages(prev => {
                        const messageIndex = prev.findIndex(msg =>
                            msg.role === 'assistant' && msg.isStreaming
                        );

                        if (messageIndex >= 0) {
                            const updatedMessages = [...prev];
                            const existingMessage = updatedMessages[messageIndex];

                            // Handle structured code edit response
                            if (data.message.codeEdit) {
                                const updatedMessage = {
                                    ...existingMessage,
                                    content: data.message.contents || 'Code edit ready to apply',
                                    isStreaming: !data.message.isFinished,
                                    messageType: 'file-edit' as const,
                                    codeEdit: data.message.codeEdit,
                                    canApply: true,
                                    isApplied: false,
                                    // Preserve analysis content if flag is set
                                    ...(data.message.preserveAnalysis && existingMessage.analysisContent && {
                                        analysisContent: existingMessage.analysisContent
                                    })
                                };

                                updatedMessages[messageIndex] = updatedMessage;

                                // Auto-create checkpoint when code edit message finishes
                                if (data.message.isFinished && !messageCheckpoints.has(updatedMessage.id)) {
                                    console.log('🔖 Creating auto-checkpoint for finished code edit message');
                                    console.log('🔖 Message details:', {
                                        id: updatedMessage.id,
                                        messageType: updatedMessage.messageType,
                                        hasCodeEdit: !!updatedMessage.codeEdit,
                                        isFinished: data.message.isFinished
                                    });

                                    // Use setTimeout to ensure state update happens first
                                    setTimeout(async () => {
                                        try {
                                            const checkpointDescription = `Before code change #${messageIndex + 1}`;

                                            // File snapshots are automatically handled by VS Code native snapshot manager
                                            console.log('🔖 VS Code native snapshots handle file capture automatically for streaming message:', updatedMessage.id);

                                            // Backend checkpoint creation removed - now using VS Code native snapshots
                                            // Snapshots are automatically created by chatServiceImpl.ts
                                            console.log('🔖 Backend checkpoint creation disabled for streaming message:', updatedMessage.id);
                                        } catch (error) {
                                            console.error('Failed to create auto-checkpoint:', error);
                                        }
                                    }, 100);
                                }
                            } else {
                                // Regular message update
                                updatedMessages[messageIndex] = {
                                    ...existingMessage,
                                    content: data.message.contents,
                                    isStreaming: !data.message.isFinished,
                                    messageType: data.message.messageType || 'text',
                                    codeEdit: data.message.codeEdit
                                };
                            }

                            return updatedMessages;
                        }
                        return prev;
                    });

                    if (data.message.isFinished) {
                        setIsStreaming(false);
                    }
                    break;

                case 'codeEditApplied':
                    console.log('🔵 Webview: Processing codeEditApplied:', data);
                    setMessages(prev => prev.map(msg =>
                        msg.id === data.messageId
                            ? { ...msg, isApplied: data.success, canApply: !data.success }
                            : msg
                    ));

                    if (data.success) {
                        // Show success message or notification
                        console.log('🔵 Webview: Code edit applied successfully');
                    } else {
                        console.error('🔵 Webview: Code edit failed:', data.error);
                    }
                    break;

                case 'codeEditRejected':
                    console.log('🔵 Webview: Processing codeEditRejected:', data);
                    setMessages(prev => prev.map(msg =>
                        msg.id === data.messageId
                            ? { ...msg, canApply: false }
                            : msg
                    ));
                    break;

                case 'snapshotCreated':
                    console.log('🔖 Webview: Processing snapshotCreated:', data);
                    if (data.messageId && data.snapshotId) {
                        setMessageCheckpoints(prev => {
                            const newCheckpoints = new Map(prev);
                            newCheckpoints.set(data.messageId, data.snapshotId);
                            console.log('🔖 Updated message checkpoints:', Array.from(newCheckpoints.entries()));
                            return newCheckpoints;
                        });
                    }
                    break;

                case 'clearMessage':
                    setMessages([]);
                    break;

                case 'fileAdded':
                    console.log('🔵 Webview: Processing fileAdded:', data.file);
                    if (data.file) {
                        addContextFile(data.file);
                    }
                    break;

                case 'filesAdded':
                    console.log('🔵 Webview: Processing filesAdded:', data.files);
                    if (data.files && Array.isArray(data.files)) {
                        data.files.forEach((file: ContextFile) => addContextFile(file));
                    }
                    break;

                case 'contextFileUpdated':
                    console.log('🔵 Webview: Processing contextFileUpdated:', data.file);
                    if (data.file) {
                        // Update the specific context file with new content
                        setContextFiles(prev => prev.map(file =>
                            file.uri === data.file.uri
                                ? { ...file, content: data.file.content }
                                : file
                        ));
                        console.log('🔵 Webview: Updated context file with', data.file.content?.length || 0, 'characters');
                    }
                    break;

                case 'searchResults':
                    console.log('🔵 Webview: Processing searchResults:', data.results);
                    // Update both context panel and input search results
                    setInputSearchResults(data.results || []);
                    break;

                case 'fileSnapshotsCaptured':
                    console.log('🔵 Webview: Processing fileSnapshotsCaptured (legacy - ignored):', data);
                    // Backend file snapshot updates disabled - VS Code native snapshots handle this
                    break;

                case 'snapshotCreated':
                    console.log('🔵 Webview: Processing snapshotCreated:', data);
                    console.log('🔵 Webview: snapshotCreated messageId:', data.messageId, 'type:', typeof data.messageId);
                    console.log('🔵 Webview: snapshotCreated snapshotId:', data.snapshotId, 'type:', typeof data.snapshotId);
                    console.log('🔵 Webview: Current messages:', messages.map(m => ({ id: m.id, type: typeof m.id })));

                    // Link the snapshot to the message
                    if (data.messageId && data.snapshotId) {
                        console.log('🔵 Webview: Before update - messageCheckpoints size:', messageCheckpoints.size);
                        console.log('🔵 Webview: Before update - existing checkpoints:', Array.from(messageCheckpoints.entries()));

                        setMessageCheckpoints(prev => {
                            const newMap = new Map(prev);
                            newMap.set(data.messageId, data.snapshotId);
                            console.log('🔖 Linked snapshot to message:', data.messageId, '→', data.snapshotId);
                            console.log('🔖 New messageCheckpoints:', Array.from(newMap.entries()));
                            return newMap;
                        });

                        // Force a re-render to update the component
                        console.log('🔵 Webview: Triggering re-render for snapshot link');
                    } else {
                        console.error('🔵 Webview: Missing messageId or snapshotId:', { messageId: data.messageId, snapshotId: data.snapshotId });
                    }
                    break;

                case 'workspaceContext':
                    console.log('🔵 Webview: Processing workspaceContext:', data);
                    // Handle workspace context if needed
                    break;

                case 'checkpointRestored':
                    console.log('🔵 Webview: Processing checkpointRestored:', data.snapshotId);
                    // Mark this snapshot as restored
                    if (data.snapshotId) {
                        setRestoredSnapshots(prev => {
                            const newSet = new Set(prev);
                            newSet.add(data.snapshotId);
                            console.log('🔄 Marked snapshot as restored:', data.snapshotId);
                            return newSet;
                        });
                    }
                    break;

                case 'clearMessagesAfterCheckpoint':
                    console.log('🔵 Webview: *** RECEIVED *** clearMessagesAfterCheckpoint');
                    console.log('🔵 Webview: checkpointMessageId:', data.checkpointMessageId);
                    console.log('🔵 Webview: checkpointSnapshotId:', data.checkpointSnapshotId);
                    console.log('🔵 Webview: Current messages count:', messages.length);
                    console.log('🔵 Webview: Current message IDs:', messages.map(m => m.id));
                    console.log('🔵 Webview: Current messageCheckpoints:', Array.from(messageCheckpoints.entries()));

                    // Clear all messages after the specified checkpoint message or snapshot
                    if (data.checkpointMessageId) {
                        // Clear by message ID (direct approach)
                        setMessages(prev => {
                            console.log('🔵 Webview: Looking for checkpoint message ID:', data.checkpointMessageId);
                            console.log('🔵 Webview: Available message IDs:', prev.map(m => m.id));
                            const checkpointIndex = prev.findIndex(msg => msg.id === data.checkpointMessageId);
                            console.log('🔵 Webview: Found checkpoint at index:', checkpointIndex);
                            if (checkpointIndex >= 0) {
                                const remainingMessages = prev.slice(0, checkpointIndex + 1);
                                console.log('🔄 *** SUCCESS *** Cleared', prev.length - remainingMessages.length, 'messages after checkpoint (by messageId)');
                                console.log('🔄 Remaining messages:', remainingMessages.length);
                                console.log('🔄 Remaining message IDs:', remainingMessages.map(m => m.id));
                                return remainingMessages;
                            }
                            console.error('🔄 *** ERROR *** Checkpoint message not found, no messages cleared');
                            return prev;
                        });
                    } else if (data.checkpointSnapshotId) {
                        // Clear by snapshot ID (fallback approach)
                        setMessages(prev => {
                            console.log('🔵 Webview: Looking for checkpoint snapshot ID:', data.checkpointSnapshotId);

                            // Find message with this snapshot ID
                            let checkpointMessageId: string | null = null;
                            for (const [msgId, snapId] of messageCheckpoints.entries()) {
                                if (snapId === data.checkpointSnapshotId) {
                                    checkpointMessageId = msgId;
                                    console.log('🔵 Webview: Found message', msgId, 'with snapshot', snapId);
                                    break;
                                }
                            }

                            if (checkpointMessageId) {
                                const checkpointIndex = prev.findIndex(msg => msg.id === checkpointMessageId);
                                console.log('🔵 Webview: Found checkpoint message at index:', checkpointIndex);
                                if (checkpointIndex >= 0) {
                                    const remainingMessages = prev.slice(0, checkpointIndex + 1);
                                    console.log('🔄 *** SUCCESS *** Cleared', prev.length - remainingMessages.length, 'messages after checkpoint (by snapshotId)');
                                    console.log('🔄 Remaining messages:', remainingMessages.length);
                                    console.log('🔄 Remaining message IDs:', remainingMessages.map(m => m.id));
                                    return remainingMessages;
                                }
                            }

                            console.error('🔄 *** ERROR *** Checkpoint snapshot not found, no messages cleared');
                            return prev;
                        });
                    } else {
                        console.error('🔄 *** ERROR *** No checkpointMessageId or checkpointSnapshotId provided');
                    }
                    break;

                // NEW: Embeddings message handlers
                case 'embeddingsStatusUpdate':
                    console.log('🔵 Webview: Processing embeddingsStatusUpdate:', data.status);
                    // These will be handled by the EnhancedChatPage parent component
                    // Forward the message to window for parent to catch
                    window.dispatchEvent(new CustomEvent('embeddingsStatusUpdate', { detail: data.status }));
                    break;

                case 'indexingProgress':
                    console.log('🔵 Webview: Processing indexingProgress:', data.progress);
                    // Forward to parent component
                    window.dispatchEvent(new CustomEvent('indexingProgress', { detail: data.progress }));
                    break;

                case 'indexingComplete':
                    console.log('🔵 Webview: Processing indexingComplete');
                    // Forward to parent component
                    window.dispatchEvent(new CustomEvent('indexingComplete', { detail: true }));
                    break;

                case 'reasoningUpdate':
                    console.log('🧠 Webview: Processing reasoningUpdate:', data);
                    // Forward reasoning updates to parent component
                    window.dispatchEvent(new CustomEvent('reasoningUpdate', { detail: data }));
                    break;

                case 'reasoningModeSync':
                    console.log('🔵 Webview: PRE loop mode is mandatory (ignoring sync message)');
                    // PRE loop is now mandatory - no mode switching allowed
                    break;

                case 'streamingComplete':
                    console.log('🏁 Webview: Streaming completed for message:', data.messageId);
                    console.log('🏁 Webview: Force clearing streaming state');
                    setIsStreaming(false);
                    break;

                default:
                    console.log('Unknown message type:', type);
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, []); // Remove addMessage dependency to prevent re-registration

    // Request workspace context on mount (ONCE only)
    useEffect(() => {
        console.log('🔵 Webview: Initializing chat page...');
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                console.log('🔵 Webview: VS Code API acquired, sending getWorkspaceContext');
                vscode.postMessage({ type: 'getWorkspaceContext' });
            }
        } catch (error) {
            console.log('🔵 Webview: VS Code API not available:', error);
        }

        // Initialize ready state
        setIsReady(true);
        console.log('🔵 Webview: Chat ready');
    }, []); // Empty dependencies - run ONCE on mount only

    // Debug isStreaming changes
    useEffect(() => {
        console.log('🔵 isStreaming state changed to:', isStreaming);
    }, [isStreaming]);

    return (
        <div className="chat-root">
            <div className="chat-layout">
                {props.showContextPanel && (
                    <div className="chat-sidebar">
                        <ContextFilesPanel
                            contextFiles={contextFiles}
                            onRemoveFile={removeContextFile}
                            onAddFile={handleAddFileToContext}
                            onAddFilesByPath={handleAddFilesByPath}
                            onAddAllOpenFiles={handleAddAllOpenFiles}
                        />
                    </div>
                )}

                <div className="chat-main">
                    <div ref={chatListRef} className="chat-list">
                        {messages.map((message) => (
                            <MessageItem
                                key={message.id}
                                message={message}
                                messageCheckpoints={messageCheckpoints}
                                restoredSnapshots={restoredSnapshots}
                                onApply={handleApplyCode}
                                onReject={handleRejectCode}
                                onCreateCheckpoint={handleCreateCheckpoint}
                                onRestoreToCheckpoint={(checkpointId) => handleRestoreCheckpoint(checkpointId, 'both')}
                                // Removed: executionMode prop
                                analysisEvents={analysisEventsByMessage.get(message.id) || []}
                            />
                        ))}

                        {isStreaming && (
                            <div className="streaming-indicator">
                                <VSCodeProgressRing />
                                <span>{(() => {
                                    console.log('🔵 Rendering streaming indicator, isStreaming:', isStreaming);
                                    console.log('🔵 Current messages:', messages.map(m => ({ id: m.id, role: m.role, isStreaming: m.isStreaming, phase: m.streamingPhase })));

                                    // Find the currently streaming message to get its phase
                                    const streamingMessage = messages.find(msg => msg.role === 'assistant' && msg.isStreaming);
                                    console.log('🔵 Found streaming message:', streamingMessage ? { id: streamingMessage.id, phase: streamingMessage.streamingPhase } : 'none');

                                    if (streamingMessage) {
                                        switch (streamingMessage.streamingPhase) {
                                            case 'analysis':
                                                return '🧠 Analyzing your request and thinking through the problem...';
                                            case 'execution':
                                                return '⚙️ Planning approach and determining next steps...';
                                            case 'response':
                                                return '🚀 Executing plan and generating final result...';
                                            default:
                                                return 'SuperInference is working...';
                                        }
                                    }
                                    return 'SuperInference is thinking...';
                                })()}</span>
                            </div>
                        )}

                        {messages.length === 0 && (
                            <div className="empty-state">
                                <div className="empty-icon">
                                    <span className="icon-superinference"></span>
                                </div>
                                <p>Start talking with AMI (SuperInference)</p>
                            </div>
                        )}
                    </div>

                    <div className="chat-input-container" style={{ position: 'sticky', bottom: 0, zIndex: 10 }}>
                        {/* Reasoning Mode Indicator */}
                        {(
                            <div className="flex items-center justify-center py-1 px-3 text-xs bg-blue-900 bg-opacity-10 border-t border-blue-500 border-opacity-30">
                                <div className="flex items-center gap-2">
                                    {/* PRE Loop active indicator (not clickable) */}
                                    <div 
                                        className="relative inline-flex h-4 w-8 items-center rounded-full bg-blue-500"
                                        title="PRE Loop mode is mandatory for paper compliance"
                                    >
                                        <span className="inline-block h-3 w-3 transform rounded-full bg-white translate-x-4" />
                                    </div>
                                    <span className="text-blue-400 text-xs font-semibold">
                                        🧠 PRE Loop Mode
                                    </span>
                                    <span className="text-vscode-descriptionForeground text-xs opacity-60">
                                        (Mandatory - Paper Compliant)
                                    </span>
                                </div>
                            </div>
                        )}
                        <div className="chat-input-wrapper">
                            <div className="input-actions" ref={inputActionsRef}>
                                <VSCodeButton
                                    appearance="icon"
                                    onClick={() => {
                                        setShowInputActions(!showInputActions);
                                        if (!showInputActions) {
                                            // Focus search input when opening
                                            setTimeout(() => inputSearchRef.current?.focus(), 100);
                                        }
                                    }}
                                    title="Add files to context - search for files or use quick actions"
                                >
                                    <i className="codicon codicon-add"></i>
                                </VSCodeButton>
                                {showInputActions && (
                                    <div className="input-actions-menu">
                                        <div className="search-input-container">
                                            <input
                                                ref={inputSearchRef}
                                                type="text"
                                                value={inputSearchQuery}
                                                onChange={handleInputSearchChange}
                                                onKeyDown={handleInputSearchKeyDown}
                                                placeholder="Search workspace files..."
                                                className="search-input"
                                            />
                                            <i className="codicon codicon-search search-icon"></i>
                                        </div>

                                        {showInputSearchResults && inputSearchResults.length > 0 && (
                                            <div className="search-results">
                                                {inputSearchResults.map((result) => (
                                                    <div
                                                        key={result.uri}
                                                        className="search-result-item"
                                                        onClick={() => handleInputSearchResultClick(result)}
                                                    >
                                                        <i className="codicon codicon-file"></i>
                                                        <div className="search-result-content">
                                                            <div className="search-result-name">{result.name}</div>
                                                            <div className="search-result-path">{result.path}</div>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        )}

                                        <div className="context-actions-divider"></div>

                                        <VSCodeButton
                                            appearance="secondary"
                                            onClick={() => { handleAddFileToContext(); setShowInputActions(false); }}
                                            title="Add current file to context"
                                        >
                                            <i className="codicon codicon-file"></i>
                                            Current File
                                        </VSCodeButton>
                                        <VSCodeButton
                                            appearance="secondary"
                                            onClick={() => { handleAddFilesByPath(); setShowInputActions(false); }}
                                            title="Browse for files in file picker"
                                        >
                                            <i className="codicon codicon-folder-opened"></i>
                                            Browse Files
                                        </VSCodeButton>
                                        <VSCodeButton
                                            appearance="secondary"
                                            onClick={() => { handleAddAllOpenFiles(); setShowInputActions(false); }}
                                            title="Add all currently open files to context"
                                        >
                                            <i className="codicon codicon-files"></i>
                                            All Open Files
                                        </VSCodeButton>
                                    </div>
                                )}
                            </div>
                            <div className="textarea-container">
                                <VSCodeTextArea
                                    ref={inputRef}
                                    value={prompt}
                                    onInput={(e: any) => setPrompt((e.target as HTMLTextAreaElement).value)}
                                    placeholder="Ask SuperInference anything... Use @ to reference files"
                                    onKeyDown={handleKeyDown}
                                    resize="vertical"
                                    rows={3}
                                    className="textarea-with-send-button"
                                />
                                <VSCodeButton
                                    appearance="icon"
                                    onClick={handleAskAction}
                                    disabled={!prompt.trim() || !isReady}
                                    title="Send message (Enter or Ctrl+Enter)"
                                    className="send-button-inside"
                                >
                                    <i className="codicon codicon-send"></i>
                                </VSCodeButton>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
