import * as React from 'react';
import { useState, useEffect, useRef } from 'react';
import { VSCodeButton, VSCodeProgressRing } from '@vscode/webview-ui-toolkit/react';

interface CommandExecutionProps {
    command: string;
    output?: string[];
    isRunning?: boolean;
    exitCode?: number;
    onContinue?: () => void;
    onCancel?: () => void;
    onApprove?: () => void;
    onReject?: () => void;
    requiresApproval?: boolean;
    isCompleted?: boolean;
}

export const CommandExecution: React.FC<CommandExecutionProps> = ({
    command,
    output = [],
    isRunning = false,
    exitCode,
    onContinue,
    onCancel,
    onApprove,
    onReject,
    requiresApproval = false,
    isCompleted = false
}) => {
    const [isExpanded, setIsExpanded] = useState(true);
    const [autoScroll, setAutoScroll] = useState(true);
    const outputRef = useRef<HTMLDivElement>(null);
    const bottomRef = useRef<HTMLDivElement>(null);

    // Auto-scroll to bottom when new output arrives
    useEffect(() => {
        if (autoScroll && bottomRef.current) {
            bottomRef.current.scrollIntoView({ behavior: 'smooth' });
        }
    }, [output, autoScroll]);

    // Check if user has scrolled up
    const handleScroll = () => {
        if (!outputRef.current) return;
        
        const { scrollTop, scrollHeight, clientHeight } = outputRef.current;
        const isAtBottom = scrollTop + clientHeight >= scrollHeight - 10;
        setAutoScroll(isAtBottom);
    };

    const getStatusIcon = () => {
        if (isRunning) {
            return <VSCodeProgressRing style={{ transform: 'scale(0.6)' }} />;
        } else if (isCompleted) {
            return exitCode === 0 
                ? <i className="codicon codicon-check text-green-500"></i>
                : <i className="codicon codicon-error text-red-500"></i>;
        } else {
            return <i className="codicon codicon-terminal text-vscode-description"></i>;
        }
    };

    const getStatusText = () => {
        if (requiresApproval && !isRunning && !isCompleted) {
            return 'Waiting for approval';
        } else if (isRunning) {
            return 'Running...';
        } else if (isCompleted) {
            return exitCode === 0 ? 'Completed successfully' : `Failed (exit code: ${exitCode})`;
        } else {
            return 'Ready to execute';
        }
    };

    const formatOutputLine = (line: string, index: number) => {
        // Detect different types of output
        let className = "font-mono text-sm leading-relaxed py-1";
        
        if (line.includes('ERROR') || line.includes('error:')) {
            className += " text-red-500";
        } else if (line.includes('WARNING') || line.includes('warning:')) {
            className += " text-yellow-500";
        } else if (line.includes('SUCCESS') || line.includes('✓')) {
            className += " text-green-500";
        } else {
            className += " text-vscode-fg";
        }

        return (
            <div key={index} className={className}>
                <span className="text-vscode-description mr-3 select-none text-xs">
                    {String(index + 1).padStart(3, ' ')}
                </span>
                {line}
            </div>
        );
    };

    return (
        <div className="command-execution bg-vscode-sidebar-bg border border-vscode-border rounded-lg overflow-hidden animate-slide-up">
            {/* Header */}
            <div className="command-header bg-vscode-input-bg border-b border-vscode-border p-3">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="flex-shrink-0">
                            {getStatusIcon()}
                        </div>
                        <div className="flex-1">
                            <div className="text-sm font-medium text-vscode-fg">Execute Command</div>
                            <div className="text-xs text-vscode-description">{getStatusText()}</div>
                        </div>
                    </div>
                    
                    <div className="flex items-center gap-2">
                        {/* Copy command button */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={() => navigator.clipboard.writeText(command)}
                            title="Copy command"
                        >
                            <i className="codicon codicon-copy"></i>
                        </VSCodeButton>
                        
                        {/* Expand/collapse button */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={() => setIsExpanded(!isExpanded)}
                            title={isExpanded ? "Collapse" : "Expand"}
                        >
                            <i className={`codicon ${isExpanded ? 'codicon-chevron-up' : 'codicon-chevron-down'}`}></i>
                        </VSCodeButton>
                    </div>
                </div>

                {/* Command display */}
                <div className="mt-3 p-3 bg-vscode-bg rounded border border-vscode-border">
                    <div className="flex items-start gap-2">
                        <span className="text-green-500 font-mono text-sm">$</span>
                        <code className="text-sm text-vscode-fg font-mono break-all">{command}</code>
                    </div>
                </div>

                {/* Approval buttons */}
                {requiresApproval && !isRunning && !isCompleted && (
                    <div className="mt-3 flex gap-2">
                        <VSCodeButton
                            appearance="primary"
                            onClick={onApprove}
                            className="flex-1"
                        >
                            <i className="codicon codicon-check mr-2"></i>
                            Approve & Execute
                        </VSCodeButton>
                        <VSCodeButton
                            appearance="secondary"
                            onClick={onReject}
                        >
                            <i className="codicon codicon-close mr-2"></i>
                            Reject
                        </VSCodeButton>
                    </div>
                )}
            </div>

            {/* Output section */}
            {isExpanded && (
                <div className="command-output">
                    {/* Output controls */}
                    {output.length > 0 && (
                        <div className="output-controls flex items-center justify-between p-2 bg-vscode-input-bg border-b border-vscode-border">
                            <div className="flex items-center gap-2 text-xs text-vscode-description">
                                <i className="codicon codicon-output"></i>
                                <span>{output.length} lines</span>
                                {!autoScroll && (
                                    <span className="text-yellow-500">(scroll paused)</span>
                                )}
                            </div>
                            
                            <div className="flex items-center gap-1">
                                {/* Auto-scroll toggle */}
                                <VSCodeButton
                                    appearance="icon"
                                    onClick={() => setAutoScroll(!autoScroll)}
                                    title={autoScroll ? "Disable auto-scroll" : "Enable auto-scroll"}
                                >
                                    <i className={`codicon ${autoScroll ? 'codicon-arrow-down' : 'codicon-arrow-circle-down'}`}></i>
                                </VSCodeButton>
                                
                                {/* Clear output */}
                                <VSCodeButton
                                    appearance="icon"
                                    onClick={() => {/* Clear output logic */}}
                                    title="Clear output"
                                >
                                    <i className="codicon codicon-clear-all"></i>
                                </VSCodeButton>
                                
                                {/* Copy output */}
                                <VSCodeButton
                                    appearance="icon"
                                    onClick={() => navigator.clipboard.writeText(output.join('\n'))}
                                    title="Copy output"
                                >
                                    <i className="codicon codicon-copy"></i>
                                </VSCodeButton>
                            </div>
                        </div>
                    )}

                    {/* Output content */}
                    <div 
                        ref={outputRef}
                        onScroll={handleScroll}
                        className="output-content bg-vscode-bg p-3 max-h-96 overflow-y-auto"
                    >
                        {output.length === 0 ? (
                            <div className="text-center text-vscode-description py-8">
                                {isRunning ? (
                                    <div className="flex items-center justify-center gap-2">
                                        <VSCodeProgressRing style={{ transform: 'scale(0.7)' }} />
                                        <span>Waiting for output...</span>
                                    </div>
                                ) : (
                                    <div>
                                        <i className="codicon codicon-terminal text-2xl mb-2 block"></i>
                                        <span>No output yet</span>
                                    </div>
                                )}
                            </div>
                        ) : (
                            <div className="output-lines">
                                {output.map((line, index) => formatOutputLine(line, index))}
                                
                                {/* Typing indicator for running commands */}
                                {isRunning && (
                                    <div className="flex items-center gap-2 mt-2 text-vscode-description">
                                        <div className="flex gap-1">
                                            <div className="w-1 h-1 bg-current rounded-full animate-bounce" style={{ animationDelay: '0ms' }}></div>
                                            <div className="w-1 h-1 bg-current rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
                                            <div className="w-1 h-1 bg-current rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
                                        </div>
                                        <span className="text-xs">Command running...</span>
                                    </div>
                                )}
                                
                                <div ref={bottomRef}></div>
                            </div>
                        )}
                    </div>

                    {/* Action buttons for running commands */}
                    {isRunning && (
                        <div className="command-actions flex justify-between items-center p-3 bg-vscode-sidebar-bg border-t border-vscode-border">
                            <div className="text-xs text-vscode-description">
                                Command is running. You can continue or cancel.
                            </div>
                            <div className="flex gap-2">
                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={onCancel}
                                >
                                    <i className="codicon codicon-debug-stop mr-2"></i>
                                    Cancel
                                </VSCodeButton>
                                <VSCodeButton
                                    appearance="primary"
                                    onClick={onContinue}
                                >
                                    <i className="codicon codicon-arrow-right mr-2"></i>
                                    Continue
                                </VSCodeButton>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}; 