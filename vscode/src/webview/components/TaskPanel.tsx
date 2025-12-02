import * as React from 'react';
import { useState, useEffect } from 'react';
import { VSCodeButton, VSCodeProgressRing, VSCodeBadge } from '@vscode/webview-ui-toolkit/react';

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

interface TaskPanelProps {
    task?: {
        id: string;
        description: string;
        steps: TaskStep[];
        metrics: TaskMetrics;
        isRunning: boolean;
    };
    onClose?: () => void;
    onCancel?: () => void;
}

export const TaskPanel: React.FC<TaskPanelProps> = ({ task, onClose, onCancel }) => {
    const [isExpanded, setIsExpanded] = useState(true);

    if (!task) return null;

    const formatNumber = (num: number) => {
        if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'k';
        }
        return num.toString();
    };

    const formatCost = (cost: number) => {
        return `$${cost.toFixed(4)}`;
    };

    const getContextUsagePercentage = () => {
        if (!task.metrics.lastApiReqTotalTokens || !task.metrics.contextWindow) return 0;
        return Math.round((task.metrics.lastApiReqTotalTokens / task.metrics.contextWindow) * 100);
    };

    const completedSteps = task.steps.filter(step => step.status === 'completed').length;
    const currentStep = task.steps.find(step => step.status === 'running');

    return (
        <div className="task-panel bg-vscode-sidebar-bg border border-vscode-border rounded-lg mb-4 overflow-hidden animate-slide-up">
            {/* Task Header */}
            <div className="task-header flex justify-between items-center p-4 border-b border-vscode-border">
                <div className="flex items-center gap-3">
                    <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse"></div>
                    <div>
                        <h3 className="text-sm font-semibold text-vscode-fg">Task</h3>
                        <p className="text-xs text-vscode-description line-clamp-2">{task.description}</p>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <VSCodeButton 
                        appearance="icon" 
                        onClick={() => setIsExpanded(!isExpanded)}
                        title={isExpanded ? "Collapse" : "Expand"}
                    >
                        <i className={`codicon ${isExpanded ? 'codicon-chevron-up' : 'codicon-chevron-down'}`}></i>
                    </VSCodeButton>
                    <VSCodeButton 
                        appearance="icon" 
                        onClick={onClose}
                        title="Close task"
                    >
                        <i className="codicon codicon-close"></i>
                    </VSCodeButton>
                </div>
            </div>

            {isExpanded && (
                <div className="task-content">
                    {/* Metrics Section */}
                    <div className="metrics-section p-4 border-b border-vscode-border">
                        <div className="grid grid-cols-2 gap-4 mb-4">
                            {/* Token Usage */}
                            <div className="metric-card bg-vscode-input-bg rounded p-3">
                                <div className="flex justify-between items-center mb-2">
                                    <span className="text-xs font-medium text-vscode-description">Tokens</span>
                                    <VSCodeBadge>
                                        ↑ {formatNumber(task.metrics.tokensIn)} ↓ {formatNumber(task.metrics.tokensOut)}
                                    </VSCodeBadge>
                                </div>
                                <div className="text-lg font-bold text-vscode-fg">
                                    {formatNumber(task.metrics.tokensIn + task.metrics.tokensOut)}
                                </div>
                            </div>

                            {/* Cache Usage */}
                            {(task.metrics.cacheReads || task.metrics.cacheWrites) && (
                                <div className="metric-card bg-vscode-input-bg rounded p-3">
                                    <div className="flex justify-between items-center mb-2">
                                        <span className="text-xs font-medium text-vscode-description">Cache</span>
                                        <VSCodeBadge>
                                            ⊕ {formatNumber(task.metrics.cacheWrites || 0)} → {formatNumber(task.metrics.cacheReads || 0)}
                                        </VSCodeBadge>
                                    </div>
                                    <div className="text-lg font-bold text-vscode-fg">
                                        {((task.metrics.cacheReads || 0) / (task.metrics.tokensIn + task.metrics.tokensOut) * 100).toFixed(1)}%
                                    </div>
                                </div>
                            )}

                            {/* Context Window */}
                            <div className="metric-card bg-vscode-input-bg rounded p-3">
                                <div className="flex justify-between items-center mb-2">
                                    <span className="text-xs font-medium text-vscode-description">Context Window</span>
                                    <VSCodeBadge>{getContextUsagePercentage()}%</VSCodeBadge>
                                </div>
                                <div className="w-full bg-vscode-border rounded-full h-2 mb-2">
                                    <div 
                                        className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                                        style={{ width: `${getContextUsagePercentage()}%` }}
                                    ></div>
                                </div>
                                <div className="text-xs text-vscode-description">
                                    {formatNumber(task.metrics.lastApiReqTotalTokens || 0)} / {formatNumber(task.metrics.contextWindow)}
                                </div>
                            </div>

                            {/* API Cost */}
                            <div className="metric-card bg-vscode-input-bg rounded p-3">
                                <div className="flex justify-between items-center mb-2">
                                    <span className="text-xs font-medium text-vscode-description">API Cost</span>
                                    <i className="codicon codicon-graph text-vscode-description"></i>
                                </div>
                                <div className="text-lg font-bold text-vscode-fg">
                                    {formatCost(task.metrics.totalCost)}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Steps Section */}
                    <div className="steps-section p-4">
                        <div className="flex justify-between items-center mb-3">
                            <h4 className="text-sm font-medium text-vscode-fg">Progress</h4>
                            <span className="text-xs text-vscode-description">
                                {completedSteps} of {task.steps.length} steps
                            </span>
                        </div>

                        <div className="steps-list space-y-2">
                            {task.steps.map((step, index) => (
                                <div key={step.id} className="step-item flex items-start gap-3 p-2 rounded hover:bg-vscode-input-bg transition-colors">
                                    <div className="step-number flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-xs font-medium border border-vscode-border">
                                        {step.status === 'completed' ? (
                                            <i className="codicon codicon-check text-green-500"></i>
                                        ) : step.status === 'running' ? (
                                            <VSCodeProgressRing style={{ transform: 'scale(0.6)' }} />
                                        ) : step.status === 'error' ? (
                                            <i className="codicon codicon-error text-red-500"></i>
                                        ) : (
                                            <span className="text-vscode-description">{index + 1}</span>
                                        )}
                                    </div>
                                    <div className="flex-1">
                                        <div className="text-sm text-vscode-fg">{step.description}</div>
                                        {step.timestamp && (
                                            <div className="text-xs text-vscode-description mt-1">
                                                {step.timestamp.toLocaleTimeString()}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>

                        {/* Current Action */}
                        {currentStep && (
                            <div className="current-action mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded">
                                <div className="flex items-center gap-2 text-sm">
                                    <VSCodeProgressRing style={{ transform: 'scale(0.7)' }} />
                                    <span className="text-vscode-fg font-medium">Currently: {currentStep.description}</span>
                                </div>
                            </div>
                        )}

                        {/* Action Buttons */}
                        {task.isRunning && (
                            <div className="actions mt-4 flex gap-2">
                                <VSCodeButton 
                                    appearance="secondary"
                                    onClick={onCancel}
                                    className="flex-1"
                                >
                                    <i className="codicon codicon-debug-stop mr-1"></i>
                                    Cancel
                                </VSCodeButton>
                            </div>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}; 