import * as React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { ChatPage } from './index';
import { TaskPanel } from '../components/TaskPanel';
import { AutoApproveControls } from '../components/AutoApproveControls';
import { CommandExecution } from '../components/CommandExecution';
import { EnhancedDiffView } from '../components/EnhancedDiffView';
import { EnhancedChatContainer } from '../components/EnhancedChatContainer';
import { VSCodeButton, VSCodeBadge } from '@vscode/webview-ui-toolkit/react';


// Use the shared VS Code API instance (already acquired by ChatPage)
function getVsCodeApi(): any {
    // First try to get from window (set by ChatPage)
    if ((window as any).vscode) {
        return (window as any).vscode;
    }

    // Fallback: try to get from global scope
    try {
        if (typeof (window as any).acquireVsCodeApi === 'function') {
            const api = (window as any).acquireVsCodeApi();
            (window as any).vscode = api;
            console.log('🔵 EnhancedChatPage: VS Code API acquired successfully');
            return api;
        }
    } catch (error) {
        console.error('🔵 EnhancedChatPage: Failed to acquire VS Code API:', error);
    }

    console.error('🔵 EnhancedChatPage: VS Code API not available');
    return null;
}

interface AutoApprovalSettings {
    enabled: boolean;
    actions: {
        readFiles: boolean;
        editFiles: boolean;
        executeCommands: boolean;
        useBrowser: boolean;
        useMcp: boolean;
    };
    maxRequests: number;
    enableNotifications: boolean;
}

interface TaskMetrics {
    tokensIn: number;
    tokensOut: number;
    cacheWrites?: number;
    cacheReads?: number;
    totalCost: number;
    contextWindow: number;
    lastApiReqTotalTokens?: number;
}

interface TaskStep {
    id: string;
    description: string;
    status: 'pending' | 'running' | 'completed' | 'error';
    timestamp?: Date;
}

interface CurrentTask {
    id: string;
    description: string;
    steps: TaskStep[];
    metrics: TaskMetrics;
    isRunning: boolean;
}

interface CommandExecution {
    id: string;
    command: string;
    output: string[];
    isRunning: boolean;
    exitCode?: number;
    requiresApproval: boolean;
    isCompleted: boolean;
}

interface FileDiff {
    filePath: string;
    changes: Array<{
        type: 'added' | 'removed' | 'context' | 'header';
        content: string;
        lineNumber?: { old?: number; new?: number };
    }>;
    isNew?: boolean;
    isDeleted?: boolean;
}

// Add interfaces for embeddings
interface EmbeddingsStatus {
    status: string;
    model: string;
    vector_store: {
        total_entries: number;
        entry_types: Record<string, number>;
    };
    smart_context_enabled: boolean;
    embedding_dimensions: number | null;
    last_update: number | null;
}

interface EmbeddingsSettings {
    enabled: boolean;
    autoIndex: boolean;
    autoReindex: boolean;
    maxFileSize: number;
    indexOnSave: boolean;
}

export function EnhancedChatPage() {
    const [currentTask, setCurrentTask] = useState<CurrentTask | undefined>();
    const [currentMode, setCurrentMode] = useState<'plan' | 'act'>('act');
    const [autoApprovalSettings, setAutoApprovalSettings] = useState<AutoApprovalSettings>({
        enabled: false,
        actions: {
            readFiles: true,
            editFiles: false,
            executeCommands: false,
            useBrowser: false,
            useMcp: false,
        },
        maxRequests: 20,
        enableNotifications: false,
    });
    const [currentAutoApprovalCount, setCurrentAutoApprovalCount] = useState(0);
    const [showAutoApproveControls, setShowAutoApproveControls] = useState(false);
    const [showTaskDetails, setShowTaskDetails] = useState(false);
    const [showContextPanel, setShowContextPanel] = useState(false);
    const [contextFiles, setContextFiles] = useState<any[]>([]);
    const [selectedModel, setSelectedModel] = useState('gpt-4');
    const [chatHistory, setChatHistory] = useState<any[]>([]);

    // New state for advanced components
    const [activeCommand, setActiveCommand] = useState<CommandExecution | undefined>();
    const [showCommandExecution, setShowCommandExecution] = useState(false);
    const [showDiffView, setShowDiffView] = useState(false);
    const [currentDiffs, setCurrentDiffs] = useState<FileDiff[]>([]);

    // State for clear chat confirmation dialog
    const [showClearConfirmation, setShowClearConfirmation] = useState(false);

    // NEW: Embeddings state
    const [showEmbeddingsPanel, setShowEmbeddingsPanel] = useState(false);
    const [embeddingsStatus, setEmbeddingsStatus] = useState<EmbeddingsStatus | null>(null);
    const [embeddingsSettings, setEmbeddingsSettings] = useState<EmbeddingsSettings>({
        enabled: true,
        autoIndex: true,
        autoReindex: true,
        maxFileSize: 500000, // 500KB
        indexOnSave: true,
    });
    const [isIndexing, setIsIndexing] = useState(false);
    const [indexingProgress, setIndexingProgress] = useState<{
        current: number;
        total: number;
        currentFile: string;
    } | null>(null);

    // NEW: Header menu states
    const [showPerformanceMetrics, setShowPerformanceMetrics] = useState(false);
    const [showExtensionSettings, setShowExtensionSettings] = useState(false);



    // NEW: Extension settings state
    const [extensionSettings, setExtensionSettings] = useState({
        // Conversation Settings
        autosave: false,
        autorun: false,

        // Backend Configuration
        backendHost: 'localhost',
        backendPort: 3000,
        backendProtocol: 'http',
        backendTimeout: 30000,
        backendRetryAttempts: 3,
        healthCheckInterval: 60000,

        // AI Provider Configuration
        aiProvider: 'openai',
        openaiApiKey: '',
        openaiBaseUrl: 'https://api.openai.com/v1',
        openaiModel: 'gpt-4',
        openaiMaxTokens: 4000,
        geminiApiKey: '',
        geminiModel: 'gemini-pro',
        geminiMaxTokens: 8000,
        anthropicApiKey: '',
        anthropicModel: 'claude-3-sonnet-20240229',
        anthropicMaxTokens: 4000,
        localEndpoint: 'http://localhost:11434',
        localModel: 'llama2',

        // Embeddings Configuration
        embeddingsProvider: 'openai',
        embeddingsModel: 'text-embedding-3-small',
        embeddingsDimensions: 1536,

        // API Endpoints
        chatEndpoint: '/api/chat',
        embeddingsEndpoint: '/api/embeddings',
        contextEndpoint: '/api/context',
        healthEndpoint: '/api/health',

        // Rate Limiting
        requestsPerMinute: 60,
        concurrentRequests: 5,

        // Performance & Monitoring
        enableAutoIndex: true,
        enablePerformanceMonitoring: true,
        maxCacheSize: 1000,
        cacheTimeout: 30,
        enableCircuitBreaker: true,
        enableDiagnostics: true,
        maxFileSize: 500,
        autoReindexOnSave: true,
        enableSmartContext: true,
        debugMode: false,
    });

    // NEW: Performance metrics state
    const [performanceMetrics, setPerformanceMetrics] = useState({
        sessionStartTime: Date.now(),
        totalTokensIn: 0,
        totalTokensOut: 0,
        totalRequests: 0,
        avgResponseTime: 0,
        cacheHits: 0,
        cacheMisses: 0,
        totalCost: 0,
        tokensPerSecond: 0,
        lastUpdateTime: Date.now(),
    });

    // Function to update performance metrics
    const updatePerformanceMetrics = (update: Partial<typeof performanceMetrics>) => {
        setPerformanceMetrics(prev => {
            const updated = { ...prev, ...update, lastUpdateTime: Date.now() };

            // Calculate tokens per second
            if (updated.totalTokensIn > 0 || updated.totalTokensOut > 0) {
                const sessionDurationSeconds = (Date.now() - updated.sessionStartTime) / 1000;
                updated.tokensPerSecond = ((updated.totalTokensIn + updated.totalTokensOut) / sessionDurationSeconds) || 0;
            }

            return updated;
        });
    };

    // Initialize task metrics on mount and setup message handling
    useEffect(() => {
        console.log('🟢 EnhancedChatPage: Initializing with embeddings status fetch');

        // Fetch initial embeddings status
        fetchEmbeddingsStatus();

        // Simulate some initial performance data for demonstration
        setTimeout(() => {
            updatePerformanceMetrics({
                totalTokensIn: 1250,
                totalTokensOut: 890,
                totalRequests: 5,
                avgResponseTime: 1200,
                cacheHits: 8,
                cacheMisses: 2,
                totalCost: 0.0234
            });
        }, 2000);

        // Set up periodic metrics updates (every 5 seconds)
        const metricsInterval = setInterval(() => {
            // Simulate gradual metric increases
            updatePerformanceMetrics({
                totalTokensIn: performanceMetrics.totalTokensIn + Math.floor(Math.random() * 50),
                totalTokensOut: performanceMetrics.totalTokensOut + Math.floor(Math.random() * 30),
                cacheHits: performanceMetrics.cacheHits + Math.floor(Math.random() * 2),
                avgResponseTime: 800 + Math.floor(Math.random() * 800), // 800-1600ms
            });
        }, 5000);

        return () => {
            clearInterval(metricsInterval);
        };

        // NEW: Listen for embeddings events from ChatPage
        const handleEmbeddingsStatusUpdate = (event: CustomEvent) => {
            console.log('🟢 EnhancedChatPage: Received embeddings status update:', event.detail);
            setEmbeddingsStatus(event.detail);
        };

        const handleIndexingProgress = (event: CustomEvent) => {
            console.log('🟢 EnhancedChatPage: Received indexing progress:', event.detail);
            setIndexingProgress(event.detail);
            setIsIndexing(true);
        };

        const handleIndexingComplete = (_event: CustomEvent) => {
            console.log('🟢 EnhancedChatPage: Indexing complete');
            setIsIndexing(false);
            setIndexingProgress(null);
            // Refresh status after completion
            setTimeout(fetchEmbeddingsStatus, 1000);
        };



        // Add event listeners
        window.addEventListener('embeddingsStatusUpdate', handleEmbeddingsStatusUpdate as EventListener);
        window.addEventListener('indexingProgress', handleIndexingProgress as EventListener);
        window.addEventListener('indexingComplete', handleIndexingComplete as EventListener);

        // Add click outside listener to close menus
        const handleClickOutside = (_event: MouseEvent) => {
            // Close all dropdown menus when clicking outside
            setShowEmbeddingsPanel(false);
            setShowPerformanceMetrics(false);
            setShowContextPanel(false);
            setShowExtensionSettings(false);
        };

        document.addEventListener('click', handleClickOutside);

        // *** DISABLED MESSAGE LISTENER TO PREVENT INTERFERENCE ***
        // The main ChatPage handles all messages - this was causing duplication

        // Listen for messages from the underlying ChatPage to update our chat history
        // const handleMessage = (event: MessageEvent) => {
        //     const { type, ...data } = event.data;

        //     console.log('🟢 EnhancedChatPage: Received message:', { type, hasMessage: !!data.message });

        //     // ONLY track chat history - DO NOT interfere with main message processing
        //     if (type === 'newMessage' || type === 'messageChange') {
        //         if (data.message) {
        //             console.log('🟢 EnhancedChatPage: Updating chat history for message:', {
        //                 id: data.message.id,
        //                 isReply: data.message.isReply,
        //                 isFinished: data.message.isFinished,
        //                 messageType: data.message.type || 'no-type'
        //             });

        //             setChatHistory(prev => {
        //                 const newMessage = {
        //                     id: data.message.id || Date.now().toString(),
        //                     role: data.message.isReply ? 'assistant' : 'user',
        //                     content: data.message.contents || '',
        //                     timestamp: new Date(),
        //                     isFinished: data.message.isFinished || false
        //                 };

        //                 // Update existing message or add new one
        //                 const existingIndex = prev.findIndex(msg => msg.id === newMessage.id);
        //                 if (existingIndex >= 0) {
        //                     const updated = [...prev];
        //                     updated[existingIndex] = newMessage;
        //                     console.log('🟢 EnhancedChatPage: Updated existing chat history message:', newMessage.id);
        //                     return updated;
        //                 } else {
        //                     console.log('🟢 EnhancedChatPage: Added new chat history message:', newMessage.id);
        //                     return [...prev, newMessage];
        //                 }
        //             });
        //         } else {
        //             console.log('🟢 EnhancedChatPage: Message event without message data:', { type, data });
        //         }
        //     } else {
        //         console.log('🟢 EnhancedChatPage: Ignoring non-message event:', type);
        //     }
        // };

        // console.log('🟢 EnhancedChatPage: Adding message event listener...');
        // window.addEventListener('message', handleMessage);

        // Simulate a task after 2 seconds
        setTimeout(() => {
            setCurrentTask({
                id: 'task_1',
                description: 'Remove print statements from practica3.py',
                steps: [
                    {
                        id: 'step_1',
                        description: 'Analyze code for print statements',
                        status: 'completed',
                        timestamp: new Date(Date.now() - 30000)
                    },
                    {
                        id: 'step_2',
                        description: 'Remove debug print statements',
                        status: 'running',
                        timestamp: new Date()
                    },
                    {
                        id: 'step_3',
                        description: 'Verify code functionality',
                        status: 'pending'
                    }
                ],
                metrics: {
                    tokensIn: 1247,
                    tokensOut: 892,
                    cacheWrites: 156,
                    cacheReads: 334,
                    totalCost: 0.0156,
                    contextWindow: 200000,
                    lastApiReqTotalTokens: 2139
                },
                isRunning: true
            });

            // Simulate task completion after another 5 seconds
            setTimeout(() => {
                setCurrentTask(prev => prev ? {
                    ...prev,
                    isRunning: false,
                    steps: prev.steps.map(step =>
                        step.status === 'running' ? { ...step, status: 'completed' } :
                            step.status === 'pending' ? { ...step, status: 'completed', timestamp: new Date() } :
                                step
                    )
                } : undefined);
            }, 5000);
        }, 2000);

        return () => {
            console.log('🟢 EnhancedChatPage: Cleaning up embeddings event listeners');
            // Clean up event listeners
            window.removeEventListener('embeddingsStatusUpdate', handleEmbeddingsStatusUpdate as EventListener);
            window.removeEventListener('indexingProgress', handleIndexingProgress as EventListener);
            window.removeEventListener('indexingComplete', handleIndexingComplete as EventListener);
            document.removeEventListener('click', handleClickOutside);
        };
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    // Effect to sync performance metrics with current task
    useEffect(() => {
        if (currentTask && currentTask.metrics) {
            updatePerformanceMetrics({
                totalTokensIn: performanceMetrics.totalTokensIn + (currentTask.metrics.tokensIn || 0),
                totalTokensOut: performanceMetrics.totalTokensOut + (currentTask.metrics.tokensOut || 0),
                totalCost: performanceMetrics.totalCost + (currentTask.metrics.totalCost || 0),
                totalRequests: performanceMetrics.totalRequests + 1,
                cacheHits: performanceMetrics.cacheHits + (currentTask.metrics.cacheReads || 0),
                cacheMisses: performanceMetrics.cacheMisses + (currentTask.metrics.cacheWrites || 0),
            });
        }
    }, [currentTask?.metrics]);

    const handleModeChange = async (mode: 'plan' | 'act') => {
        setCurrentMode(mode);
        console.log(`Mode changed to: ${mode}`);
    };

    const handleAutoApprovalSettingsChange = (settings: AutoApprovalSettings) => {
        setAutoApprovalSettings(settings);
        console.log('Auto-approval settings updated:', settings);
    };

    const handleTaskCancel = () => {
        setCurrentTask(prev => prev ? { ...prev, isRunning: false } : undefined);
        console.log('Task cancelled');
    };

    const handleTaskClose = () => {
        setCurrentTask(undefined);
        setShowTaskDetails(false);
        console.log('Task closed');
    };

    const clearChat = useCallback(() => {
        setShowClearConfirmation(true);
    }, []);

    const handleConfirmClearChat = useCallback(() => {
        // Send clear message to VS Code extension
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({ type: 'clearMessage' });
                console.log('Clear message sent to VS Code extension');
            } else {
                console.error('VS Code API not available to clear chat');
            }
        } catch (error) {
            console.error('Failed to send clear message to extension:', error);
        }

        setShowClearConfirmation(false);
        console.log('Chat cleared');
    }, []);

    const handleCancelClearChat = useCallback(() => {
        setShowClearConfirmation(false);
    }, []);

    const handleModelChange = (model: string) => {
        setSelectedModel(model);
        console.log('Model changed to:', model);
    };

    // Command execution handlers
    const handleCommandApprove = () => {
        if (activeCommand) {
            setActiveCommand(prev => prev ? { ...prev, requiresApproval: false, isRunning: true } : undefined);
            console.log('Command approved:', activeCommand.command);
        }
    };

    const handleCommandReject = () => {
        if (activeCommand) {
            setActiveCommand(prev => prev ? { ...prev, isCompleted: true, exitCode: 1 } : undefined);
            console.log('Command rejected:', activeCommand.command);
        }
    };

    const handleCommandCancel = () => {
        setActiveCommand(undefined);
        setShowCommandExecution(false);
        console.log('Command cancelled');
    };

    // Diff view handlers
    const handleDiffApply = (filePath: string) => {
        console.log('Applying diff for:', filePath);
        setShowDiffView(false);
        // Implementation would apply the diff to the file
    };

    const handleDiffReject = (filePath: string) => {
        console.log('Rejecting diff for:', filePath);
        setShowDiffView(false);
    };

    // Simulate command execution
    const executeCommand = (command: string) => {
        const newCommand: CommandExecution = {
            id: `cmd_${Date.now()}`,
            command,
            output: [],
            isRunning: false,
            requiresApproval: true,
            isCompleted: false
        };
        setActiveCommand(newCommand);
        setShowCommandExecution(true);
    };

    // NEW: Embeddings handlers
    const fetchEmbeddingsStatus = useCallback(async () => {
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({ type: 'getEmbeddingsStatus' });
            }
        } catch (error) {
            console.error('Failed to fetch embeddings status:', error);
        }
    }, []);

    const handleIndexWorkspace = useCallback(async () => {
        try {
            setIsIndexing(true);
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({ type: 'indexWorkspace' });
            }
        } catch (error) {
            console.error('Failed to index workspace:', error);
            setIsIndexing(false);
        }
    }, []);

    const handleClearEmbeddings = useCallback(async () => {
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({ type: 'clearEmbeddings' });
            }
            // Refresh status after clearing
            setTimeout(fetchEmbeddingsStatus, 1000);
        } catch (error) {
            console.error('Failed to clear embeddings:', error);
        }
    }, [fetchEmbeddingsStatus]);

    const handleReindexCurrentFile = useCallback(async () => {
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({ type: 'reindexCurrentFile' });
            }
        } catch (error) {
            console.error('Failed to reindex current file:', error);
        }
    }, []);

    const handleEmbeddingsSettingsChange = useCallback((newSettings: EmbeddingsSettings) => {
        setEmbeddingsSettings(newSettings);
        // Optionally send to extension
        try {
            const vscode = getVsCodeApi();
            if (vscode) {
                vscode.postMessage({
                    type: 'updateEmbeddingsSettings',
                    settings: newSettings
                });
            }
        } catch (error) {
            console.error('Failed to update embeddings settings:', error);
        }
    }, []);

    const toggleEmbeddingsPanel = useCallback((e?: React.MouseEvent) => {
        e?.stopPropagation();
        setShowEmbeddingsPanel(!showEmbeddingsPanel);
        // Close other menus
        setShowPerformanceMetrics(false);
        setShowContextPanel(false);
        setShowExtensionSettings(false);
        if (!showEmbeddingsPanel) {
            // Refresh status when opening panel
            fetchEmbeddingsStatus();
        }
    }, [showEmbeddingsPanel, fetchEmbeddingsStatus]);

    // NEW: Header menu handlers



    const togglePerformanceMetrics = useCallback((e?: React.MouseEvent) => {
        e?.stopPropagation();
        setShowPerformanceMetrics(!showPerformanceMetrics);
        // Close other menus
        setShowEmbeddingsPanel(false);
        setShowContextPanel(false);
        setShowExtensionSettings(false);
    }, [showPerformanceMetrics]);

    const toggleContextPanel = useCallback((e?: React.MouseEvent) => {
        e?.stopPropagation();
        setShowContextPanel(!showContextPanel);
        // Close other menus
        setShowEmbeddingsPanel(false);
        setShowPerformanceMetrics(false);
        setShowExtensionSettings(false);
    }, [showContextPanel]);

    const toggleExtensionSettings = useCallback((e?: React.MouseEvent) => {
        e?.stopPropagation();
        setShowExtensionSettings(!showExtensionSettings);
        // Close other menus
        setShowEmbeddingsPanel(false);
        setShowPerformanceMetrics(false);
        setShowContextPanel(false);
    }, [showExtensionSettings]);



    return (
        <div className="enhanced-chat-page h-full flex flex-col bg-vscode-bg text-vscode-fg">
            {/* NEW: Reorganized Header Bar */}
            <div className="chat-header border-b border-vscode-border relative">
                {/* Main toolbar bar */}
                <div className="flex items-center justify-between px-4 py-2 bg-vscode-sidebar-bg w-full">
                    {/* Left side: AMI from SuperInference */}
                    <div className="flex items-center gap-2 flex-shrink-0">
                        <span className="title-icon icon-superinference text-xl font-bold"></span>
                        <span className="text-sm font-semibold text-vscode-sideBarSectionHeader-foreground uppercase tracking-wide">AMI from SuperInference</span>
                    </div>

                    {/* Right side: All menu options */}
                    <div className="flex items-center gap-2 flex-shrink-0 ml-auto">






                        {/* Context Files Panel Toggle */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={toggleContextPanel}
                            title="Toggle context panel"
                        >
                            <i className="codicon codicon-list-unordered"></i>
                            {contextFiles.length > 0 && (
                                <VSCodeBadge>{contextFiles.length}</VSCodeBadge>
                            )}
                        </VSCodeButton>

                        {/* Indexes Menu */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={toggleEmbeddingsPanel}
                            title="Embeddings & AI Context Management"
                        >
                            <i className="codicon codicon-database"></i>
                            {embeddingsStatus && (
                                <VSCodeBadge>{embeddingsStatus.vector_store?.total_entries || 0}</VSCodeBadge>
                            )}
                            {isIndexing && (
                                <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse ml-1"></div>
                            )}
                        </VSCodeButton>



                        {/* Performance Metrics Panel Toggle */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={togglePerformanceMetrics}
                            title="Performance Metrics & Statistics"
                        >
                            <i className="codicon codicon-graph"></i>
                        </VSCodeButton>

                        {/* Extension Settings */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={toggleExtensionSettings}
                            title="Extension Settings & Configuration"
                        >
                            <i className="codicon codicon-settings-gear"></i>
                        </VSCodeButton>

                        {/* Remove Conversation Icon */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={clearChat}
                            title="Clear chat"
                        >
                            <i className="codicon codicon-trash"></i>
                        </VSCodeButton>
                    </div>
                </div>

                {/* NEW: Execution Mode Options Panel */}



                {/* NEW: Layover Panel: Embeddings Controls */}
                {showEmbeddingsPanel && (
                    <div className="absolute top-full left-0 right-0 z-50 border-b border-vscode-border bg-vscode-input-bg shadow-lg">
                        <div
                            className="p-4 space-y-4"
                            onClick={(e) => e.stopPropagation()}
                        >
                            {/* Header */}
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <i className="codicon codicon-database text-lg"></i>
                                    <h3 className="text-sm font-semibold text-vscode-fg">AI Context Indexes</h3>
                                </div>
                                <button
                                    onClick={() => setShowEmbeddingsPanel(false)}
                                    className="text-vscode-description hover:text-vscode-fg"
                                >
                                    <i className="codicon codicon-close"></i>
                                </button>
                            </div>

                            {/* Status Section */}
                            <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                <div className="grid grid-cols-2 gap-4 text-xs">
                                    <div>
                                        <span className="text-vscode-description">Status:</span>
                                        <div className="flex items-center gap-1 mt-1">
                                            <div className={`w-2 h-2 rounded-full ${embeddingsStatus?.status === 'active' ? 'bg-green-500' : 'bg-gray-500'}`}></div>
                                            <span className="text-vscode-fg">{embeddingsStatus?.status || 'Unknown'}</span>
                                        </div>
                                    </div>
                                    <div>
                                        <span className="text-vscode-description">Total Entries:</span>
                                        <div className="text-vscode-fg mt-1">{embeddingsStatus?.vector_store?.total_entries || 0}</div>
                                    </div>
                                    <div>
                                        <span className="text-vscode-description">Model:</span>
                                        <div className="text-vscode-fg mt-1">{embeddingsStatus?.model || 'gemini-embedding-001'}</div>
                                    </div>
                                    <div>
                                        <span className="text-vscode-description">Smart Context:</span>
                                        <div className="flex items-center gap-1 mt-1">
                                            <div className={`w-2 h-2 rounded-full ${embeddingsStatus?.smart_context_enabled ? 'bg-green-500' : 'bg-gray-500'}`}></div>
                                            <span className="text-vscode-fg">{embeddingsStatus?.smart_context_enabled ? 'Enabled' : 'Disabled'}</span>
                                        </div>
                                    </div>
                                </div>

                                {/* Entry Types Breakdown */}
                                {embeddingsStatus?.vector_store?.entry_types && Object.keys(embeddingsStatus.vector_store.entry_types).length > 0 && (
                                    <div className="mt-3 pt-3 border-t border-vscode-border">
                                        <span className="text-vscode-description text-xs">Entry Types:</span>
                                        <div className="flex flex-wrap gap-2 mt-1">
                                            {Object.entries(embeddingsStatus.vector_store.entry_types).map(([type, count]) => (
                                                <span key={type} className="inline-flex items-center gap-1 px-2 py-1 bg-vscode-button-background text-vscode-button-foreground rounded text-xs">
                                                    {type}: {count}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Indexing Progress */}
                            {isIndexing && indexingProgress && (
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-2">
                                        <div className="w-3 h-3 bg-blue-500 rounded-full animate-pulse"></div>
                                        <span className="text-sm text-vscode-fg">Indexing in progress...</span>
                                    </div>
                                    <div className="text-xs text-vscode-description mb-2">
                                        {indexingProgress.current}/{indexingProgress.total} files processed
                                    </div>
                                    <div className="text-xs text-vscode-description truncate">
                                        Current: {indexingProgress.currentFile}
                                    </div>
                                    <div className="w-full bg-vscode-input-bg rounded-full h-2 mt-2">
                                        <div
                                            className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                                            style={{ width: `${(indexingProgress.current / indexingProgress.total) * 100}%` }}
                                        ></div>
                                    </div>
                                </div>
                            )}

                            {/* Action Buttons */}
                            <div className="flex flex-wrap gap-2">
                                <VSCodeButton
                                    appearance="primary"
                                    onClick={handleIndexWorkspace}
                                    disabled={isIndexing}
                                >
                                    <i className="codicon codicon-database mr-1"></i>
                                    {isIndexing ? 'Indexing...' : 'Index Workspace'}
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={fetchEmbeddingsStatus}
                                >
                                    <i className="codicon codicon-refresh mr-1"></i>
                                    Refresh Status
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={handleReindexCurrentFile}
                                >
                                    <i className="codicon codicon-file mr-1"></i>
                                    Reindex Current File
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={handleClearEmbeddings}
                                    title="Clear all embeddings"
                                >
                                    <i className="codicon codicon-trash mr-1"></i>
                                    Clear All
                                </VSCodeButton>
                            </div>

                            {/* Settings Section */}
                            <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                <h4 className="text-xs font-semibold text-vscode-fg mb-3">Settings</h4>
                                <div className="space-y-2">
                                    <label className="flex items-center gap-2 text-xs">
                                        <input
                                            type="checkbox"
                                            checked={embeddingsSettings.enabled}
                                            onChange={(e) => handleEmbeddingsSettingsChange({
                                                ...embeddingsSettings,
                                                enabled: e.target.checked
                                            })}
                                            className="w-3 h-3"
                                        />
                                        <span className="text-vscode-fg">Enable embeddings</span>
                                    </label>

                                    <label className="flex items-center gap-2 text-xs">
                                        <input
                                            type="checkbox"
                                            checked={embeddingsSettings.autoIndex}
                                            onChange={(e) => handleEmbeddingsSettingsChange({
                                                ...embeddingsSettings,
                                                autoIndex: e.target.checked
                                            })}
                                            className="w-3 h-3"
                                        />
                                        <span className="text-vscode-fg">Auto-index workspace files</span>
                                    </label>

                                    <label className="flex items-center gap-2 text-xs">
                                        <input
                                            type="checkbox"
                                            checked={embeddingsSettings.indexOnSave}
                                            onChange={(e) => handleEmbeddingsSettingsChange({
                                                ...embeddingsSettings,
                                                indexOnSave: e.target.checked
                                            })}
                                            className="w-3 h-3"
                                        />
                                        <span className="text-vscode-fg">Auto-reindex on file save</span>
                                    </label>

                                    <label className="flex items-center gap-2 text-xs">
                                        <input
                                            type="checkbox"
                                            checked={embeddingsSettings.autoReindex}
                                            onChange={(e) => handleEmbeddingsSettingsChange({
                                                ...embeddingsSettings,
                                                autoReindex: e.target.checked
                                            })}
                                            className="w-3 h-3"
                                        />
                                        <span className="text-vscode-fg">Auto-reindex modified files</span>
                                    </label>
                                </div>
                            </div>

                            {/* Help Text */}
                            <div className="text-xs text-vscode-description">
                                <p className="mb-1">
                                    <strong>Embeddings</strong> enable semantic understanding of your codebase for better AI responses.
                                </p>
                                <p>
                                    Files are automatically indexed when you open a workspace and updated when you save changes.
                                </p>
                            </div>
                        </div>
                    </div>
                )}

                {/* NEW: Performance Metrics Panel */}
                {showPerformanceMetrics && (
                    <div className="absolute top-full left-0 right-0 z-50 border-b border-vscode-border bg-vscode-input-bg shadow-lg">
                        <div
                            className="p-4 space-y-4"
                            onClick={(e) => e.stopPropagation()}
                        >
                            {/* Header */}
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <i className="codicon codicon-graph text-lg"></i>
                                    <h3 className="text-sm font-semibold text-vscode-fg">Performance Metrics</h3>
                                </div>
                                <button
                                    onClick={() => setShowPerformanceMetrics(false)}
                                    className="text-vscode-description hover:text-vscode-fg"
                                >
                                    <i className="codicon codicon-close"></i>
                                </button>
                            </div>

                            {/* Metrics Grid */}
                            <div className="grid grid-cols-3 gap-4">
                                {/* Token Metrics */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-2">
                                        <i className="codicon codicon-symbol-text text-blue-400"></i>
                                        <span className="text-xs font-semibold text-vscode-fg">Token Usage</span>
                                    </div>
                                    <div className="space-y-1 text-xs">
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Input:</span>
                                            <span className="text-vscode-fg">{(performanceMetrics.totalTokensIn || 0).toLocaleString()}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Output:</span>
                                            <span className="text-vscode-fg">{(performanceMetrics.totalTokensOut || 0).toLocaleString()}</span>
                                        </div>
                                        <div className="flex justify-between font-semibold">
                                            <span className="text-vscode-description">Total:</span>
                                            <span className="text-vscode-fg">{((performanceMetrics.totalTokensIn || 0) + (performanceMetrics.totalTokensOut || 0)).toLocaleString()}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Rate:</span>
                                            <span className="text-green-400 font-medium">{Math.round(performanceMetrics.tokensPerSecond || 0)} t/s</span>
                                        </div>
                                    </div>
                                </div>

                                {/* Request Metrics */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-2">
                                        <i className="codicon codicon-pulse text-green-400"></i>
                                        <span className="text-xs font-semibold text-vscode-fg">Request Stats</span>
                                    </div>
                                    <div className="space-y-1 text-xs">
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Total:</span>
                                            <span className="text-vscode-fg">{performanceMetrics.totalRequests || 0}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Avg Time:</span>
                                            <span className="text-vscode-fg">{Math.round(performanceMetrics.avgResponseTime || 0)}ms</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Session:</span>
                                            <span className="text-vscode-fg">{Math.round((Date.now() - performanceMetrics.sessionStartTime) / 1000 / 60)}m</span>
                                        </div>
                                    </div>
                                </div>

                                {/* Cache & Cost Metrics */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-2">
                                        <i className="codicon codicon-database text-orange-400"></i>
                                        <span className="text-xs font-semibold text-vscode-fg">Cache & Cost</span>
                                    </div>
                                    <div className="space-y-1 text-xs">
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Cache Hits:</span>
                                            <span className="text-green-400">{performanceMetrics.cacheHits || 0}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Cache Misses:</span>
                                            <span className="text-orange-400">{performanceMetrics.cacheMisses || 0}</span>
                                        </div>
                                        <div className="flex justify-between">
                                            <span className="text-vscode-description">Hit Rate:</span>
                                            <span className="text-vscode-fg">
                                                {performanceMetrics.cacheHits > 0 || performanceMetrics.cacheMisses > 0
                                                    ? Math.round((performanceMetrics.cacheHits / (performanceMetrics.cacheHits + performanceMetrics.cacheMisses)) * 100)
                                                    : 0}%
                                            </span>
                                        </div>
                                        <div className="flex justify-between font-semibold">
                                            <span className="text-vscode-description">Total Cost:</span>
                                            <span className="text-vscode-fg">${(performanceMetrics.totalCost || 0).toFixed(4)}</span>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Current Task Metrics */}
                            {currentTask && currentTask.metrics && (
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-2">
                                        <i className="codicon codicon-clock text-purple-400"></i>
                                        <span className="text-xs font-semibold text-vscode-fg">Current Task</span>
                                    </div>
                                    <div className="grid grid-cols-4 gap-4 text-xs">
                                        <div>
                                            <span className="text-vscode-description">Task Tokens In:</span>
                                            <div className="text-vscode-fg mt-1">{currentTask.metrics.tokensIn.toLocaleString()}</div>
                                        </div>
                                        <div>
                                            <span className="text-vscode-description">Task Tokens Out:</span>
                                            <div className="text-vscode-fg mt-1">{currentTask.metrics.tokensOut.toLocaleString()}</div>
                                        </div>
                                        <div>
                                            <span className="text-vscode-description">Context Usage:</span>
                                            <div className="text-vscode-fg mt-1">
                                                {currentTask.metrics.lastApiReqTotalTokens && currentTask.metrics.contextWindow
                                                    ? Math.round((currentTask.metrics.lastApiReqTotalTokens / currentTask.metrics.contextWindow) * 100)
                                                    : 0}%
                                            </div>
                                        </div>
                                        <div>
                                            <span className="text-vscode-description">Task Cost:</span>
                                            <div className="text-vscode-fg mt-1">${currentTask.metrics.totalCost.toFixed(4)}</div>
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* Action Buttons */}
                            <div className="flex flex-wrap gap-2">
                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => {
                                        // Reset session metrics
                                        setPerformanceMetrics({
                                            ...performanceMetrics,
                                            sessionStartTime: Date.now(),
                                            totalTokensIn: 0,
                                            totalTokensOut: 0,
                                            totalRequests: 0,
                                            avgResponseTime: 0,
                                            cacheHits: 0,
                                            cacheMisses: 0,
                                            totalCost: 0,
                                            tokensPerSecond: 0,
                                        });
                                    }}
                                >
                                    <i className="codicon codicon-refresh mr-1"></i>
                                    Reset Session
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => {
                                        // Copy metrics to clipboard
                                        const metricsText = `SuperInference Performance Metrics:
Token Usage: ${(performanceMetrics.totalTokensIn + performanceMetrics.totalTokensOut).toLocaleString()} total (${performanceMetrics.totalTokensIn.toLocaleString()} in, ${performanceMetrics.totalTokensOut.toLocaleString()} out)
Request Stats: ${performanceMetrics.totalRequests} requests, ${Math.round(performanceMetrics.avgResponseTime)}ms avg
Cache Performance: ${performanceMetrics.cacheHits} hits, ${performanceMetrics.cacheMisses} misses (${performanceMetrics.cacheHits > 0 || performanceMetrics.cacheMisses > 0 ? Math.round((performanceMetrics.cacheHits / (performanceMetrics.cacheHits + performanceMetrics.cacheMisses)) * 100) : 0}% hit rate)
Total Cost: $${performanceMetrics.totalCost.toFixed(4)}
Tokens/sec: ${Math.round(performanceMetrics.tokensPerSecond)}
Session Duration: ${Math.round((Date.now() - performanceMetrics.sessionStartTime) / 1000 / 60)} minutes`;
                                        navigator.clipboard.writeText(metricsText);
                                    }}
                                >
                                    <i className="codicon codicon-copy mr-1"></i>
                                    Copy Report
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => {
                                        // Trigger VS Code command to show extension performance metrics
                                        window.vscode?.postMessage({
                                            type: 'command',
                                            command: 'superinference.showPerformanceMetrics'
                                        });
                                    }}
                                >
                                    <i className="codicon codicon-graph mr-1"></i>
                                    Extension Metrics
                                </VSCodeButton>
                            </div>

                            {/* Help Text */}
                            <div className="text-xs text-vscode-description">
                                <p className="mb-1">
                                    <strong>Performance Metrics</strong> track token usage, request latency, cache efficiency, and costs.
                                </p>
                                <p>
                                    Metrics reset each session. Use "Extension Metrics" for detailed performance data from the VS Code extension.
                                </p>
                            </div>
                        </div>
                    </div>
                )}

                {/* NEW: Extension Settings Panel */}
                {showExtensionSettings && (
                    <div className="absolute top-full left-0 right-0 z-50 border-b border-vscode-border bg-vscode-input-bg shadow-lg max-h-96 overflow-y-auto">
                        <div
                            className="p-4 space-y-4"
                            onClick={(e) => e.stopPropagation()}
                        >
                            {/* Header */}
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <i className="codicon codicon-settings-gear text-lg"></i>
                                    <h3 className="text-sm font-semibold text-vscode-fg">Extension Settings</h3>
                                </div>
                                <button
                                    onClick={() => setShowExtensionSettings(false)}
                                    className="text-vscode-description hover:text-vscode-fg"
                                >
                                    <i className="codicon codicon-close"></i>
                                </button>
                            </div>

                            {/* Settings Sections */}
                            <div className="space-y-4">
                                {/* Conversation Settings */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-comment-discussion text-blue-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">Conversation Settings</span>
                                    </div>
                                    <div className="space-y-3">
                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-save"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Autosave</span>
                                                    <div className="text-xs text-vscode-description">Automatically approve code changes</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.autosave}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    autosave: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-play"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Autorun</span>
                                                    <div className="text-xs text-vscode-description">Automatically execute proposed commands</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.autorun}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    autorun: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>
                                    </div>
                                </div>

                                {/* Backend Configuration */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-server-environment text-blue-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">Backend Configuration</span>
                                    </div>
                                    <div className="space-y-3">
                                        <div className="grid grid-cols-2 gap-3">
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Host/IP Address</label>
                                                <input
                                                    type="text"
                                                    placeholder="localhost"
                                                    value={extensionSettings.backendHost}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        backendHost: e.target.value
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Port</label>
                                                <input
                                                    type="number"
                                                    min="1"
                                                    max="65535"
                                                    value={extensionSettings.backendPort}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        backendPort: parseInt(e.target.value) || 3000
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                        </div>

                                        <div className="grid grid-cols-2 gap-3">
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Protocol</label>
                                                <select
                                                    value={extensionSettings.backendProtocol}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        backendProtocol: e.target.value
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                >
                                                    <option value="http">HTTP</option>
                                                    <option value="https">HTTPS</option>
                                                </select>
                                            </div>
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Timeout (ms)</label>
                                                <input
                                                    type="number"
                                                    min="5000"
                                                    max="120000"
                                                    step="1000"
                                                    value={extensionSettings.backendTimeout}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        backendTimeout: parseInt(e.target.value) || 30000
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                        </div>

                                        <div className="grid grid-cols-2 gap-3">
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Retry Attempts</label>
                                                <input
                                                    type="number"
                                                    min="0"
                                                    max="10"
                                                    value={extensionSettings.backendRetryAttempts}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        backendRetryAttempts: parseInt(e.target.value) || 3
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Health Check (ms)</label>
                                                <input
                                                    type="number"
                                                    min="10000"
                                                    max="300000"
                                                    step="5000"
                                                    value={extensionSettings.healthCheckInterval}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        healthCheckInterval: parseInt(e.target.value) || 60000
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                {/* AI Provider Configuration */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-copilot text-purple-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">AI Provider Configuration</span>
                                    </div>
                                    <div className="space-y-3">
                                        <div>
                                            <label className="text-xs text-vscode-description mb-1 block">Primary AI Provider</label>
                                            <select
                                                value={extensionSettings.aiProvider}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    aiProvider: e.target.value
                                                }))}
                                                className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                            >
                                                <option value="openai">OpenAI (ChatGPT)</option>
                                                <option value="gemini">Google Gemini</option>
                                                <option value="anthropic">Anthropic Claude</option>
                                                <option value="local">Local Model</option>
                                            </select>
                                        </div>

                                        {/* OpenAI Settings */}
                                        {extensionSettings.aiProvider === 'openai' && (
                                            <div className="space-y-2 border-l-2 border-blue-400 pl-3">
                                                <div>
                                                    <label className="text-xs text-vscode-description mb-1 block">OpenAI API Key</label>
                                                    <input
                                                        type="password"
                                                        placeholder="sk-..."
                                                        value={extensionSettings.openaiApiKey}
                                                        onChange={(e) => setExtensionSettings(prev => ({
                                                            ...prev,
                                                            openaiApiKey: e.target.value
                                                        }))}
                                                        className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                    />
                                                </div>
                                                <div className="grid grid-cols-2 gap-2">
                                                    <div>
                                                        <label className="text-xs text-vscode-description mb-1 block">Model</label>
                                                        <select
                                                            value={extensionSettings.openaiModel}
                                                            onChange={(e) => setExtensionSettings(prev => ({
                                                                ...prev,
                                                                openaiModel: e.target.value
                                                            }))}
                                                            className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                        >
                                                            <option value="gpt-4">GPT-4</option>
                                                            <option value="gpt-4-turbo">GPT-4 Turbo</option>
                                                            <option value="gpt-4o">GPT-4o</option>
                                                            <option value="gpt-3.5-turbo">GPT-3.5 Turbo</option>
                                                        </select>
                                                    </div>
                                                    <div>
                                                        <label className="text-xs text-vscode-description mb-1 block">Max Tokens</label>
                                                        <input
                                                            type="number"
                                                            min="100"
                                                            max="8000"
                                                            value={extensionSettings.openaiMaxTokens}
                                                            onChange={(e) => setExtensionSettings(prev => ({
                                                                ...prev,
                                                                openaiMaxTokens: parseInt(e.target.value) || 4000
                                                            }))}
                                                            className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                        )}

                                        {/* Gemini Settings */}
                                        {extensionSettings.aiProvider === 'gemini' && (
                                            <div className="space-y-2 border-l-2 border-green-400 pl-3">
                                                <div>
                                                    <label className="text-xs text-vscode-description mb-1 block">Gemini API Key</label>
                                                    <input
                                                        type="password"
                                                        placeholder="API Key"
                                                        value={extensionSettings.geminiApiKey}
                                                        onChange={(e) => setExtensionSettings(prev => ({
                                                            ...prev,
                                                            geminiApiKey: e.target.value
                                                        }))}
                                                        className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                    />
                                                </div>
                                                <div className="grid grid-cols-2 gap-2">
                                                    <div>
                                                        <label className="text-xs text-vscode-description mb-1 block">Model</label>
                                                        <select
                                                            value={extensionSettings.geminiModel}
                                                            onChange={(e) => setExtensionSettings(prev => ({
                                                                ...prev,
                                                                geminiModel: e.target.value
                                                            }))}
                                                            className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                        >
                                                            <option value="gemini-pro">Gemini Pro</option>
                                                            <option value="gemini-pro-vision">Gemini Pro Vision</option>
                                                            <option value="gemini-1.5-pro">Gemini 1.5 Pro</option>
                                                        </select>
                                                    </div>
                                                    <div>
                                                        <label className="text-xs text-vscode-description mb-1 block">Max Tokens</label>
                                                        <input
                                                            type="number"
                                                            min="100"
                                                            max="30000"
                                                            value={extensionSettings.geminiMaxTokens}
                                                            onChange={(e) => setExtensionSettings(prev => ({
                                                                ...prev,
                                                                geminiMaxTokens: parseInt(e.target.value) || 8000
                                                            }))}
                                                            className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                        )}

                                        {/* Local Model Settings */}
                                        {extensionSettings.aiProvider === 'local' && (
                                            <div className="space-y-2 border-l-2 border-orange-400 pl-3">
                                                <div>
                                                    <label className="text-xs text-vscode-description mb-1 block">Local Endpoint</label>
                                                    <input
                                                        type="text"
                                                        placeholder="http://localhost:11434"
                                                        value={extensionSettings.localEndpoint}
                                                        onChange={(e) => setExtensionSettings(prev => ({
                                                            ...prev,
                                                            localEndpoint: e.target.value
                                                        }))}
                                                        className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                    />
                                                </div>
                                                <div>
                                                    <label className="text-xs text-vscode-description mb-1 block">Model Name</label>
                                                    <input
                                                        type="text"
                                                        placeholder="llama2"
                                                        value={extensionSettings.localModel}
                                                        onChange={(e) => setExtensionSettings(prev => ({
                                                            ...prev,
                                                            localModel: e.target.value
                                                        }))}
                                                        className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                    />
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {/* API Endpoints & Rate Limiting */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-globe text-cyan-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">API Endpoints & Rate Limiting</span>
                                    </div>
                                    <div className="space-y-3">
                                        <div className="grid grid-cols-2 gap-3">
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Chat Endpoint</label>
                                                <input
                                                    type="text"
                                                    value={extensionSettings.chatEndpoint}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        chatEndpoint: e.target.value
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Embeddings Endpoint</label>
                                                <input
                                                    type="text"
                                                    value={extensionSettings.embeddingsEndpoint}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        embeddingsEndpoint: e.target.value
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                        </div>

                                        <div className="grid grid-cols-2 gap-3">
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Requests/Minute</label>
                                                <input
                                                    type="number"
                                                    min="1"
                                                    max="1000"
                                                    value={extensionSettings.requestsPerMinute}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        requestsPerMinute: parseInt(e.target.value) || 60
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                            <div>
                                                <label className="text-xs text-vscode-description mb-1 block">Concurrent Requests</label>
                                                <input
                                                    type="number"
                                                    min="1"
                                                    max="20"
                                                    value={extensionSettings.concurrentRequests}
                                                    onChange={(e) => setExtensionSettings(prev => ({
                                                        ...prev,
                                                        concurrentRequests: parseInt(e.target.value) || 5
                                                    }))}
                                                    className="w-full px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                {/* Performance Settings */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-pulse text-green-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">Performance & Monitoring</span>
                                    </div>
                                    <div className="space-y-3">
                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-graph text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Performance Monitoring</span>
                                                    <div className="text-xs text-vscode-description">Track operation timing and resource usage</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.enablePerformanceMonitoring}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    enablePerformanceMonitoring: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-shield text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Circuit Breaker</span>
                                                    <div className="text-xs text-vscode-description">Prevent cascading failures with resilience patterns</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.enableCircuitBreaker}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    enableCircuitBreaker: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-database text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Max Cache Size</span>
                                                    <div className="text-xs text-vscode-description">Maximum number of cache entries</div>
                                                </div>
                                            </div>
                                            <input
                                                type="number"
                                                min="100"
                                                max="10000"
                                                step="100"
                                                value={extensionSettings.maxCacheSize}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    maxCacheSize: parseInt(e.target.value) || 1000
                                                }))}
                                                className="w-20 px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                            />
                                        </div>

                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-clock text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Cache Timeout (minutes)</span>
                                                    <div className="text-xs text-vscode-description">How long to keep items in cache</div>
                                                </div>
                                            </div>
                                            <input
                                                type="number"
                                                min="5"
                                                max="120"
                                                step="5"
                                                value={extensionSettings.cacheTimeout}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    cacheTimeout: parseInt(e.target.value) || 30
                                                }))}
                                                className="w-20 px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                            />
                                        </div>
                                    </div>
                                </div>

                                {/* AI & Embeddings Settings */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <span className="icon-superinference text-purple-400 text-lg"></span>
                                        <span className="text-sm font-semibold text-vscode-fg">AI & Context Management</span>
                                    </div>
                                    <div className="space-y-3">
                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-search text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Auto-index Workspace</span>
                                                    <div className="text-xs text-vscode-description">Automatically create embeddings for workspace files</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.enableAutoIndex}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    enableAutoIndex: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-save text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Auto-reindex on Save</span>
                                                    <div className="text-xs text-vscode-description">Update embeddings when files are saved</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.autoReindexOnSave}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    autoReindexOnSave: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-lightbulb text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Smart Context</span>
                                                    <div className="text-xs text-vscode-description">Enhanced context gathering with intelligent chunking</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.enableSmartContext}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    enableSmartContext: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-file text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Max File Size (KB)</span>
                                                    <div className="text-xs text-vscode-description">Skip files larger than this size</div>
                                                </div>
                                            </div>
                                            <input
                                                type="number"
                                                min="100"
                                                max="2000"
                                                step="50"
                                                value={extensionSettings.maxFileSize}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    maxFileSize: parseInt(e.target.value) || 500
                                                }))}
                                                className="w-20 px-2 py-1 text-xs bg-vscode-input-bg border border-vscode-border rounded"
                                            />
                                        </div>
                                    </div>
                                </div>

                                {/* Diagnostics & Debug Settings */}
                                <div className="bg-vscode-sidebar-bg border border-vscode-border rounded p-3">
                                    <div className="flex items-center gap-2 mb-3">
                                        <i className="codicon codicon-bug text-orange-400"></i>
                                        <span className="text-sm font-semibold text-vscode-fg">Diagnostics & Debug</span>
                                    </div>
                                    <div className="space-y-3">
                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-info text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Enable Diagnostics</span>
                                                    <div className="text-xs text-vscode-description">Show diagnostic information and error details</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.enableDiagnostics}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    enableDiagnostics: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>

                                        <label className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <i className="codicon codicon-debug-console text-xs"></i>
                                                <div>
                                                    <span className="text-sm text-vscode-fg">Debug Mode</span>
                                                    <div className="text-xs text-vscode-description">Enable verbose logging and debug features</div>
                                                </div>
                                            </div>
                                            <input
                                                type="checkbox"
                                                checked={extensionSettings.debugMode}
                                                onChange={(e) => setExtensionSettings(prev => ({
                                                    ...prev,
                                                    debugMode: e.target.checked
                                                }))}
                                                className="w-4 h-4"
                                            />
                                        </label>
                                    </div>
                                </div>
                            </div>

                            {/* Action Buttons */}
                            <div className="flex flex-wrap gap-2">
                                <VSCodeButton
                                    appearance="primary"
                                    onClick={() => {
                                        // Apply settings to VS Code extension
                                        window.vscode?.postMessage({
                                            type: 'updateExtensionSettings',
                                            settings: extensionSettings
                                        });
                                    }}
                                >
                                    <i className="codicon codicon-save mr-1"></i>
                                    Apply Settings
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => {
                                        // Reset to defaults
                                        setExtensionSettings({
                                            // Conversation Settings
                                            autosave: false,
                                            autorun: false,

                                            // Backend Configuration
                                            backendHost: 'localhost',
                                            backendPort: 3000,
                                            backendProtocol: 'http',
                                            backendTimeout: 30000,
                                            backendRetryAttempts: 3,
                                            healthCheckInterval: 60000,

                                            // AI Provider Configuration
                                            aiProvider: 'openai',
                                            openaiApiKey: '',
                                            openaiBaseUrl: 'https://api.openai.com/v1',
                                            openaiModel: 'gpt-4',
                                            openaiMaxTokens: 4000,
                                            geminiApiKey: '',
                                            geminiModel: 'gemini-pro',
                                            geminiMaxTokens: 8000,
                                            anthropicApiKey: '',
                                            anthropicModel: 'claude-3-sonnet-20240229',
                                            anthropicMaxTokens: 4000,
                                            localEndpoint: 'http://localhost:11434',
                                            localModel: 'llama2',

                                            // Embeddings Configuration
                                            embeddingsProvider: 'openai',
                                            embeddingsModel: 'text-embedding-3-small',
                                            embeddingsDimensions: 1536,

                                            // API Endpoints
                                            chatEndpoint: '/api/chat',
                                            embeddingsEndpoint: '/api/embeddings',
                                            contextEndpoint: '/api/context',
                                            healthEndpoint: '/api/health',

                                            // Rate Limiting
                                            requestsPerMinute: 60,
                                            concurrentRequests: 5,

                                            // Performance & Monitoring
                                            enableAutoIndex: true,
                                            enablePerformanceMonitoring: true,
                                            maxCacheSize: 1000,
                                            cacheTimeout: 30,
                                            enableCircuitBreaker: true,
                                            enableDiagnostics: true,
                                            maxFileSize: 500,
                                            autoReindexOnSave: true,
                                            enableSmartContext: true,
                                            debugMode: false,
                                        });
                                    }}
                                >
                                    <i className="codicon codicon-refresh mr-1"></i>
                                    Reset to Defaults
                                </VSCodeButton>

                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => {
                                        // Open VS Code settings
                                        window.vscode?.postMessage({
                                            type: 'command',
                                            command: 'workbench.action.openSettings',
                                            args: ['@ext:superinference']
                                        });
                                    }}
                                >
                                    <i className="codicon codicon-gear mr-1"></i>
                                    Open VS Code Settings
                                </VSCodeButton>
                            </div>

                            {/* Help Text */}
                            <div className="text-xs text-vscode-description">
                                <p className="mb-1">
                                    <strong>Extension Settings</strong> control SuperInference's behavior and performance. Changes take effect immediately.
                                </p>
                                <p>
                                    Use "Apply Settings" to save changes or "Open VS Code Settings" to access the full extension configuration.
                                </p>
                            </div>
                        </div>
                    </div>
                )}

            </div>

            {/* Command Execution Overlay */}
            {showCommandExecution && activeCommand && (
                <div className="absolute top-16 left-4 right-4 z-60 bg-vscode-input-bg border border-vscode-border rounded shadow-lg">
                    <CommandExecution
                        command={activeCommand.command}
                        output={activeCommand.output}
                        isRunning={activeCommand.isRunning}
                        exitCode={activeCommand.exitCode}
                        onApprove={handleCommandApprove}
                        onReject={handleCommandReject}
                        onCancel={handleCommandCancel}
                        requiresApproval={activeCommand.requiresApproval}
                        isCompleted={activeCommand.isCompleted}
                    />
                </div>
            )}

            {/* Diff View Overlay */}
            {showDiffView && currentDiffs.length > 0 && (
                <div className="absolute top-16 left-4 right-4 bottom-4 z-60 bg-vscode-input-bg border border-vscode-border rounded shadow-lg">
                    <EnhancedDiffView
                        fileDiffs={currentDiffs}
                        onApply={handleDiffApply}
                        onReject={handleDiffReject}
                        onClose={() => setShowDiffView(false)}
                    />
                </div>
            )}

            {/* Restore Progress Overlay */}
            {/* This section is removed as restore progress is no longer tracked */}

            {/* Clear Chat Confirmation Dialog */}
            {showClearConfirmation && (
                <div
                    className="fixed inset-0 flex items-center justify-center bg-black bg-opacity-50"
                    style={{ zIndex: 9999 }}
                >
                    <div className="bg-vscode-input-bg border border-vscode-border rounded-lg p-6 max-w-md mx-4 shadow-2xl">
                        <div className="flex items-center gap-3 mb-4">
                            <i className="codicon codicon-warning text-yellow-500 text-xl"></i>
                            <h3 className="text-lg font-semibold text-vscode-fg">Clear Chat History</h3>
                        </div>
                        <p className="text-vscode-description mb-6">
                            Are you sure you want to clear the entire chat history? This action cannot be undone.
                        </p>
                        <div className="flex justify-end gap-2">
                            <VSCodeButton
                                appearance="secondary"
                                onClick={handleCancelClearChat}
                            >
                                Cancel
                            </VSCodeButton>
                            <VSCodeButton
                                appearance="primary"
                                onClick={handleConfirmClearChat}
                                className="bg-red-600 hover:bg-red-700"
                            >
                                <i className="codicon codicon-trash mr-1"></i>
                                Clear Chat
                            </VSCodeButton>
                        </div>
                    </div>
                </div>
            )}



            {/* Main Chat Content - Full height */}
            <div className="flex-1 overflow-hidden">
                <ChatPage
                    showContextPanel={showContextPanel}
                    contextFiles={contextFiles}
                    onClearChat={clearChat}
                    selectedModel={selectedModel}
                    onModelChange={handleModelChange}
                />
            </div>
        </div>
    );
} 