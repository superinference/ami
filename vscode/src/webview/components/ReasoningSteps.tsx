import * as React from 'react';
import { ReasoningStep, TaskPlan } from '../../reasoning/multi-step-processor';

interface ReasoningStepsProps {
    plan?: TaskPlan;
    onStepClick?: (step: ReasoningStep) => void;
}

export const ReasoningSteps: React.FC<ReasoningStepsProps> = ({ plan, onStepClick }: ReasoningStepsProps) => {
    if (!plan) {
        return null;
    }

    const getStepIcon = (step: ReasoningStep) => {
        switch (step.type) {
            case 'thinking':
                return 'codicon-pulse';
            case 'planning':
                return 'codicon-timeline';
            case 'analysis':
                return 'codicon-search';
            case 'execution':
                return 'codicon-gear';
            case 'validation':
                return 'codicon-check';
            case 'error_handling':
                return 'codicon-warning';
            default:
                return 'codicon-circle-filled';
        }
    };

    const getStepColor = (step: ReasoningStep) => {
        switch (step.status) {
            case 'completed':
                return 'text-green-400';
            case 'in_progress':
                return 'text-blue-400';
            case 'failed':
                return 'text-red-400';
            case 'pending':
                return 'text-vscode-description';
            default:
                return 'text-vscode-description';
        }
    };

    const formatDuration = (duration?: number) => {
        if (!duration) return '';
        return duration < 1000 ? `${duration}ms` : `${Math.round(duration / 1000)}s`;
    };

    const formatThinkingMessage = (step: ReasoningStep) => {
        const duration = step.duration ? formatDuration(step.duration) : '';
        const files = step.files?.length ? `, read ${step.files.length} files` : '';

        let action = '';
        switch (step.type) {
            case 'analysis':
                action = ', searched, and analyzed';
                break;
            case 'planning':
                action = ' and planned';
                break;
            case 'execution':
                action = ' and executed';
                break;
            case 'validation':
                action = ' and validated';
                break;
            default:
                action = '';
        }

        if (step.status === 'in_progress') {
            return `Thinking${action}...`;
        }

        return duration ? `Thought for ${duration}${action}${files}` : step.title;
    };

    return (
        <div className="reasoning-steps p-4 border border-vscode-border rounded bg-vscode-sidebar-bg">
            <div className="flex items-center gap-2 mb-4">
                <i className="codicon codicon-timeline text-blue-400"></i>
                <h3 className="text-sm font-semibold text-vscode-fg">Multi-Step Reasoning</h3>
                <div className="flex-1"></div>
                <div className="text-xs text-vscode-description">
                    {plan.status === 'completed' ? '✅ Complete' :
                        plan.status === 'failed' ? '❌ Failed' :
                            plan.status === 'in_progress' ? '🔄 In Progress' : '📋 Planning'}
                </div>
            </div>

            <div className="space-y-2">
                {plan.steps.map((step: ReasoningStep, _index: number) => (
                    <div
                        key={step.id}
                        className={`reasoning-step p-3 rounded cursor-pointer transition-colors ${step.status === 'in_progress' ? 'bg-blue-900/20 border border-blue-500/30' :
                            step.status === 'completed' ? 'bg-green-900/20' :
                                step.status === 'failed' ? 'bg-red-900/20' :
                                    'bg-vscode-input-bg hover:bg-vscode-editor-hoverHighlightBg'
                            }`}
                        onClick={() => onStepClick?.(step)}
                    >
                        <div className="flex items-start gap-3">
                            {/* Step Icon */}
                            <div className={`flex-shrink-0 mt-0.5 ${getStepColor(step)}`}>
                                <i className={`codicon ${getStepIcon(step)} ${step.status === 'in_progress' ? 'animate-pulse' : ''
                                    }`}></i>
                            </div>

                            {/* Step Content */}
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1">
                                    <span className="text-sm font-medium text-vscode-fg">
                                        {formatThinkingMessage(step)}
                                    </span>
                                    {step.status === 'in_progress' && (
                                        <div className="flex gap-1">
                                            <div className="w-1 h-1 bg-blue-400 rounded-full animate-bounce"></div>
                                            <div className="w-1 h-1 bg-blue-400 rounded-full animate-bounce delay-100"></div>
                                            <div className="w-1 h-1 bg-blue-400 rounded-full animate-bounce delay-200"></div>
                                        </div>
                                    )}
                                </div>

                                <p className="text-xs text-vscode-description mb-2">
                                    {step.description}
                                </p>

                                {/* Files analyzed */}
                                {step.files && step.files.length > 0 && (
                                    <div className="mb-2">
                                        <div className="text-xs text-vscode-description mb-1">Files analyzed:</div>
                                        <div className="flex flex-wrap gap-1">
                                            {step.files.map((file: string, fileIndex: number) => (
                                                <span
                                                    key={fileIndex}
                                                    className="px-2 py-0.5 text-xs bg-vscode-button-hoverBg rounded"
                                                >
                                                    {file}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Step output */}
                                {step.output && (
                                    <div className="text-xs text-green-400 bg-green-900/20 p-2 rounded">
                                        {step.output}
                                    </div>
                                )}

                                {/* Step error */}
                                {step.error && (
                                    <div className="text-xs text-red-400 bg-red-900/20 p-2 rounded">
                                        ❌ {step.error}
                                    </div>
                                )}

                                {/* Progress indicator for current step */}
                                {step.status === 'in_progress' && (
                                    <div className="mt-2">
                                        <div className="w-full bg-vscode-progressBar-bg rounded-full h-1">
                                            <div className="bg-blue-400 h-1 rounded-full animate-pulse w-2/3"></div>
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Duration */}
                            <div className="flex-shrink-0 text-xs text-vscode-description">
                                {step.duration && formatDuration(step.duration)}
                            </div>
                        </div>
                    </div>
                ))}

                {/* Overall progress */}
                <div className="mt-4 pt-3 border-t border-vscode-border">
                    <div className="flex items-center justify-between text-xs text-vscode-description mb-2">
                        <span>Overall Progress</span>
                        <span>
                            {plan.steps.filter((s: ReasoningStep) => s.status === 'completed').length} / {plan.steps.length} steps
                        </span>
                    </div>
                    <div className="w-full bg-vscode-progressBar-bg rounded-full h-2">
                        <div
                            className="bg-blue-400 h-2 rounded-full transition-all duration-300"
                            style={{
                                width: `${(plan.steps.filter((s: ReasoningStep) => s.status === 'completed').length / plan.steps.length) * 100}%`
                            }}
                        ></div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ReasoningSteps; 