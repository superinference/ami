import * as React from 'react';
import { useState, useEffect } from 'react';
import { MessageItem } from '../chat/MessageItem';
import { TaskPanel } from './TaskPanel';
import { AutoApproveControls } from './AutoApproveControls';
import { PlanActModeToggle, ModeIndicator } from './PlanActModeToggle';
import { MessageItemModel } from '../../common/chatService/model';

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

interface EnhancedChatContainerProps {
    messages: MessageItemModel[];
    currentTask?: CurrentTask;
    currentMode: 'plan' | 'act';
    autoApprovalSettings: AutoApprovalSettings;
    currentAutoApprovalCount: number;
    onSendMessage?: (message: string) => void;
    onModeChange?: (mode: 'plan' | 'act') => void;
    onAutoApprovalSettingsChange?: (settings: AutoApprovalSettings) => void;
    onTaskCancel?: () => void;
    onTaskClose?: () => void;
    className?: string;
}

export const EnhancedChatContainer: React.FC<EnhancedChatContainerProps> = ({
    messages,
    currentTask,
    currentMode,
    autoApprovalSettings,
    currentAutoApprovalCount,
    onSendMessage,
    onModeChange,
    onAutoApprovalSettingsChange,
    onTaskCancel,
    onTaskClose,
    className = ''
}) => {
    const [showAutoApproveControls, setShowAutoApproveControls] = useState(false);
    const [inputValue, setInputValue] = useState('');

    const handleSendMessage = () => {
        if (inputValue.trim() && onSendMessage) {
            onSendMessage(inputValue.trim());
            setInputValue('');
        }
    };

    const handleKeyPress = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSendMessage();
        }
    };

    return (
        <div className={`enhanced-chat-container flex flex-col h-full ${className}`}>
            {/* Header with Mode Toggle and Controls */}
            <div className="chat-header bg-vscode-sidebar-bg border-b border-vscode-border p-4 space-y-4">
                {/* Mode Toggle */}
                <div className="flex items-center justify-between">
                    <ModeIndicator currentMode={currentMode} compact />
                    <PlanActModeToggle
                        currentMode={currentMode}
                        onModeChange={onModeChange || (() => {})}
                        isDisabled={currentTask?.isRunning}
                    />
                </div>

                {/* Auto-Approve Controls */}
                <AutoApproveControls
                    settings={autoApprovalSettings}
                    onSettingsChange={onAutoApprovalSettingsChange || (() => {})}
                    currentCount={currentAutoApprovalCount}
                    isVisible={showAutoApproveControls}
                    onToggleVisibility={() => setShowAutoApproveControls(!showAutoApproveControls)}
                />
            </div>

            {/* Task Panel */}
            {currentTask && (
                <TaskPanel
                    task={currentTask}
                    onClose={onTaskClose}
                    onCancel={onTaskCancel}
                />
            )}

            {/* Messages Area */}
            <div className="messages-area flex-1 overflow-y-auto p-4 space-y-4">
                {messages.length === 0 ? (
                    <div className="empty-state text-center py-12">
                        <div className="mb-6">
                            <i className="codicon codicon-comment-discussion text-6xl text-vscode-description opacity-50"></i>
                        </div>
                        <h3 className="text-lg font-medium text-vscode-fg mb-2">
                            Start a conversation
                        </h3>
                        <p className="text-vscode-description mb-6 max-w-md mx-auto">
                            {currentMode === 'plan' 
                                ? 'Ask questions, discuss your approach, or create a plan for your task.'
                                : 'Request file changes, run commands, or implement your solution.'
                            }
                        </p>
                        <div className="flex flex-wrap gap-2 justify-center">
                            <button 
                                onClick={() => setInputValue("Help me plan how to implement a new feature")}
                                className="px-3 py-2 text-sm bg-vscode-button-background text-vscode-button-foreground rounded hover:bg-vscode-button-hoverBackground transition-colors"
                            >
                                Plan a feature
                            </button>
                            <button 
                                onClick={() => setInputValue("Review my code and suggest improvements")}
                                className="px-3 py-2 text-sm bg-vscode-button-background text-vscode-button-foreground rounded hover:bg-vscode-button-hoverBackground transition-colors"
                            >
                                Code review
                            </button>
                            <button 
                                onClick={() => setInputValue("Help me debug this issue")}
                                className="px-3 py-2 text-sm bg-vscode-button-background text-vscode-button-foreground rounded hover:bg-vscode-button-hoverBackground transition-colors"
                            >
                                Debug issue
                            </button>
                        </div>
                    </div>
                ) : (
                    messages.map((message, index) => (
                        <MessageItem
                            key={index}
                            model={message}
                            // Add enhanced props based on message content
                            messageType={detectMessageType(message)}
                            commandData={extractCommandData(message)}
                            checkpointData={extractCheckpointData(message)}
                            diffData={extractDiffData(message)}
                        />
                    ))
                )}
            </div>

            {/* Input Area */}
            <div className="input-area bg-vscode-sidebar-bg border-t border-vscode-border p-4">
                <div className="flex items-end gap-3">
                    <div className="flex-1">
                        <textarea
                            value={inputValue}
                            onChange={(e) => setInputValue(e.target.value)}
                            onKeyPress={handleKeyPress}
                            placeholder={currentMode === 'plan' 
                                ? "Ask a question, discuss your approach..."
                                : "Request changes, run commands, implement features..."
                            }
                            className="w-full p-3 bg-vscode-input-bg border border-vscode-input-border rounded resize-none focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                            rows={Math.min((inputValue || '').split('\n').length + 1, 6)}
                            disabled={currentTask?.isRunning}
                        />
                        <div className="flex justify-between items-center mt-2 text-xs text-vscode-description">
                            <span>
                                {currentTask?.isRunning ? 'Task is running...' : 'Press Enter to send, Shift+Enter for new line'}
                            </span>
                            <span>{inputValue.length} characters</span>
                        </div>
                    </div>
                    
                    <button
                        onClick={handleSendMessage}
                        disabled={!inputValue.trim() || currentTask?.isRunning}
                        className="px-4 py-3 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                        <i className="codicon codicon-send"></i>
                    </button>
                </div>
            </div>
        </div>
    );
};

// Helper functions to detect message types and extract data
function detectMessageType(message: MessageItemModel): 'text' | 'command' | 'file_edit' | 'checkpoint' | 'error' | 'api_request' {
    const content = message.contents.toLowerCase();
    
    if (content.includes('executing command') || content.includes('$ ')) {
        return 'command';
    }
    if (content.includes('writing to file') || content.includes('editing file') || content.includes('diff')) {
        return 'file_edit';
    }
    if (content.includes('checkpoint') || content.includes('saving state')) {
        return 'checkpoint';
    }
    if (content.includes('error') || content.includes('failed')) {
        return 'error';
    }
    if (content.includes('api request') || content.includes('tokens:')) {
        return 'api_request';
    }
    
    return 'text';
}

function extractCommandData(message: MessageItemModel) {
    // Extract command execution data from message
    const content = message.contents;
    const commandMatch = content.match(/\$\s+(.+)$/m);
    
    if (commandMatch) {
        return {
            command: commandMatch[1],
            output: [],
            isRunning: content.includes('running') && !message.isFinished,
            requiresApproval: content.includes('requires approval')
        };
    }
    
    return undefined;
}

function extractCheckpointData(message: MessageItemModel) {
    // Extract checkpoint data from message
    if (message.contents.toLowerCase().includes('checkpoint')) {
        return {
            messageId: `msg_${Date.now()}`,
            isCheckedOut: message.contents.includes('restored')
        };
    }
    
    return undefined;
}

function extractDiffData(message: MessageItemModel) {
    // Extract diff data from message
    const content = message.contents;
    if (content.includes('diff') || content.includes('---') || content.includes('+++')) {
        return {
            filePath: 'example.ts', // Would extract from actual content
            changes: [] // Would parse actual diff
        };
    }
    
    return undefined;
} 