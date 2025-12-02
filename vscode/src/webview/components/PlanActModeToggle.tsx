import * as React from 'react';
import { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';

interface PlanActModeToggleProps {
    currentMode: 'plan' | 'act';
    onModeChange: (mode: 'plan' | 'act') => void;
    isDisabled?: boolean;
    className?: string;
}

export const PlanActModeToggle: React.FC<PlanActModeToggleProps> = ({
    currentMode,
    onModeChange,
    isDisabled = false,
    className = ''
}) => {
    const [isTransitioning, setIsTransitioning] = useState(false);

    const handleModeChange = async (newMode: 'plan' | 'act') => {
        if (newMode === currentMode || isTransitioning || isDisabled) return;

        setIsTransitioning(true);
        try {
            await onModeChange(newMode);
        } catch (error) {
            console.error('Failed to change mode:', error);
        } finally {
            setIsTransitioning(false);
        }
    };

    const getModeDescription = (mode: 'plan' | 'act') => {
        switch (mode) {
            case 'plan':
                return 'Discuss approach, ask questions, create plans';
            case 'act':
                return 'Execute actions, modify files, run commands';
        }
    };

    const getModeIcon = (mode: 'plan' | 'act') => {
        switch (mode) {
            case 'plan':
                return 'codicon-lightbulb';
            case 'act':
                return 'codicon-play';
        }
    };

    return (
        <div className={`plan-act-toggle ${className}`}>
            {/* Mode Toggle Buttons */}
            <div className="mode-buttons flex bg-vscode-input-bg rounded-lg p-1 border border-vscode-border">
                {(['plan', 'act'] as const).map((mode) => {
                    const isActive = currentMode === mode;
                    return (
                        <button
                            key={mode}
                            onClick={() => handleModeChange(mode)}
                            disabled={isDisabled || isTransitioning}
                            className={`
                                flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded transition-all duration-200
                                ${isActive 
                                    ? 'bg-vscode-button-background text-vscode-button-foreground shadow-sm' 
                                    : 'text-vscode-description hover:text-vscode-fg hover:bg-vscode-input-hoverBackground'
                                }
                                ${(isDisabled || isTransitioning) ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                            `}
                        >
                            {isTransitioning && isActive ? (
                                <i className="codicon codicon-loading codicon-modifier-spin"></i>
                            ) : (
                                <i className={getModeIcon(mode)}></i>
                            )}
                            <span className="font-medium capitalize">{mode}</span>
                        </button>
                    );
                })}
            </div>

            {/* Current Mode Description */}
            <div className="mode-description mt-3 p-3 bg-vscode-sidebar-bg rounded border border-vscode-border">
                <div className="flex items-start gap-3">
                    <div className="flex-shrink-0 mt-1">
                        <i className={`${getModeIcon(currentMode)} text-lg ${
                            currentMode === 'plan' ? 'text-yellow-500' : 'text-green-500'
                        }`}></i>
                    </div>
                    <div>
                        <div className="text-sm font-medium text-vscode-fg mb-1">
                            {currentMode === 'plan' ? 'Plan Mode' : 'Act Mode'}
                        </div>
                        <div className="text-xs text-vscode-description">
                            {getModeDescription(currentMode)}
                        </div>
                    </div>
                </div>

                {/* Mode-specific features */}
                <div className="mt-3">
                    <div className="text-xs font-medium text-vscode-description mb-2">Available in this mode:</div>
                    <div className="flex flex-wrap gap-1">
                        {currentMode === 'plan' ? (
                            <>
                                <span className="px-2 py-1 text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 rounded">
                                    Questions
                                </span>
                                <span className="px-2 py-1 text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 rounded">
                                    Discussion
                                </span>
                                <span className="px-2 py-1 text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 rounded">
                                    Planning
                                </span>
                                <span className="px-2 py-1 text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 rounded">
                                    Analysis
                                </span>
                            </>
                        ) : (
                            <>
                                <span className="px-2 py-1 text-xs bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 rounded">
                                    File editing
                                </span>
                                <span className="px-2 py-1 text-xs bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 rounded">
                                    Commands
                                </span>
                                <span className="px-2 py-1 text-xs bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 rounded">
                                    Browser
                                </span>
                                <span className="px-2 py-1 text-xs bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 rounded">
                                    Implementation
                                </span>
                            </>
                        )}
                    </div>
                </div>
            </div>

            {/* Mode switch hint */}
            {!isDisabled && (
                <div className="mode-hint mt-2 text-center">
                    <div className="text-xs text-vscode-description">
                        {currentMode === 'plan' 
                            ? 'Switch to Act mode when ready to implement your plan'
                            : 'Switch to Plan mode to discuss approach before making changes'
                        }
                    </div>
                </div>
            )}
        </div>
    );
};

interface ModeIndicatorProps {
    currentMode: 'plan' | 'act';
    compact?: boolean;
}

export const ModeIndicator: React.FC<ModeIndicatorProps> = ({
    currentMode,
    compact = false
}) => {
    const modeColor = currentMode === 'plan' ? 'yellow' : 'green';
    
    if (compact) {
        return (
            <div className={`mode-indicator-compact flex items-center gap-1 px-2 py-1 rounded text-xs bg-${modeColor}-100 dark:bg-${modeColor}-900/30 text-${modeColor}-800 dark:text-${modeColor}-200`}>
                <i className={currentMode === 'plan' ? 'codicon-lightbulb' : 'codicon-play'}></i>
                <span className="font-medium uppercase">{currentMode}</span>
            </div>
        );
    }

    return (
        <div className={`mode-indicator flex items-center gap-2 px-3 py-2 rounded-lg bg-${modeColor}-50 dark:bg-${modeColor}-900/20 border border-${modeColor}-200 dark:border-${modeColor}-800`}>
            <i className={`${currentMode === 'plan' ? 'codicon-lightbulb' : 'codicon-play'} text-${modeColor}-500`}></i>
            <div>
                <div className={`text-sm font-medium text-${modeColor}-800 dark:text-${modeColor}-200`}>
                    {currentMode === 'plan' ? 'Plan Mode' : 'Act Mode'}
                </div>
                <div className="text-xs text-vscode-description">
                    {currentMode === 'plan' 
                        ? 'Discussing and planning approach'
                        : 'Executing actions and implementing changes'
                    }
                </div>
            </div>
        </div>
    );
}; 