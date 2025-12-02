import * as React from 'react';
import { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';

interface CheckpointControlsProps {
    messageId: string;
    isCheckedOut?: boolean;
    onCompare?: (messageId: string) => void;
    onRestore?: (messageId: string, type: 'task' | 'workspace' | 'both') => void;
    className?: string;
}

export const CheckpointControls: React.FC<CheckpointControlsProps> = ({
    messageId,
    isCheckedOut = false,
    onCompare,
    onRestore,
    className = ''
}) => {
    const [showRestoreMenu, setShowRestoreMenu] = useState(false);
    const [isComparing, setIsComparing] = useState(false);
    const [isRestoring, setIsRestoring] = useState(false);

    const handleCompare = async () => {
        if (!onCompare || isComparing) return;
        
        setIsComparing(true);
        try {
            await onCompare(messageId);
        } catch (error) {
            console.error('Failed to compare checkpoint:', error);
        } finally {
            setIsComparing(false);
        }
    };

    const handleRestore = async (type: 'task' | 'workspace' | 'both') => {
        if (!onRestore || isRestoring) return;
        
        setIsRestoring(true);
        setShowRestoreMenu(false);
        try {
            await onRestore(messageId, type);
        } catch (error) {
            console.error('Failed to restore checkpoint:', error);
        } finally {
            setIsRestoring(false);
        }
    };

    return (
        <div className={`checkpoint-controls relative ${className}`}>
            <div className="flex items-center gap-2 group">
                {/* Checkpoint Indicator */}
                <div className="flex items-center gap-2 text-xs text-vscode-description">
                    <i 
                        className={`codicon codicon-bookmark ${
                            isCheckedOut ? 'text-blue-500' : 'text-vscode-description'
                        }`}
                    ></i>
                    <span className={isCheckedOut ? 'text-blue-500 font-medium' : ''}>
                        {isCheckedOut ? 'Checkpoint (restored)' : 'Checkpoint'}
                    </span>
                </div>

                {/* Dotted Line */}
                <div className="flex-1 border-t border-dotted border-vscode-border mx-2"></div>

                {/* Action Buttons */}
                <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                    {/* Compare Button */}
                    <VSCodeButton
                        appearance="secondary"
                        disabled={isComparing}
                        onClick={handleCompare}
                        title="Compare changes since this checkpoint"
                        className="text-xs px-2 py-1"
                    >
                        {isComparing ? (
                            <i className="codicon codicon-loading codicon-modifier-spin"></i>
                        ) : (
                            <>
                                <i className="codicon codicon-diff mr-1"></i>
                                Compare
                            </>
                        )}
                    </VSCodeButton>

                    {/* Restore Button */}
                    <div className="relative">
                        <VSCodeButton
                            appearance="secondary"
                            disabled={isRestoring}
                            onClick={() => setShowRestoreMenu(!showRestoreMenu)}
                            title="Restore to this checkpoint"
                            className="text-xs px-2 py-1"
                        >
                            {isRestoring ? (
                                <i className="codicon codicon-loading codicon-modifier-spin"></i>
                            ) : (
                                <>
                                    <i className="codicon codicon-discard mr-1"></i>
                                    Restore
                                </>
                            )}
                        </VSCodeButton>

                        {/* Restore Menu */}
                        {showRestoreMenu && (
                            <div className="absolute top-full right-0 mt-1 bg-vscode-dropdown-background border border-vscode-dropdown-border shadow-lg z-50 min-w-max">
                                <div className="p-2">
                                    <div className="text-xs font-medium text-vscode-fg mb-2 px-2">
                                        Restore Options
                                    </div>
                                    
                                    <button
                                        onClick={() => handleRestore('both')}
                                        className="w-full text-left px-3 py-2 text-xs hover:bg-vscode-list-hoverBackground transition-colors"
                                    >
                                        <div className="font-medium text-vscode-fg">Restore Task and Workspace</div>
                                        <div className="text-vscode-description">Reset both codebase and task to this point</div>
                                    </button>
                                    
                                    <button
                                                                onClick={() => handleRestore('task')}
                        className="w-full text-left px-3 py-2 text-xs hover:bg-vscode-list-hoverBackground transition-colors"
                                    >
                                        <div className="font-medium text-vscode-fg">Restore Task Only</div>
                                        <div className="text-vscode-description">Keep codebase changes but revert task context</div>
                                    </button>
                                    
                                    <button
                                                                onClick={() => handleRestore('workspace')}
                        className="w-full text-left px-3 py-2 text-xs hover:bg-vscode-list-hoverBackground transition-colors"
                                    >
                                        <div className="font-medium text-vscode-fg">Restore Workspace Only</div>
                                        <div className="text-vscode-description">Reset codebase while preserving task context</div>
                                    </button>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Click outside to close menu */}
            {showRestoreMenu && (
                <div 
                    className="fixed inset-0 z-40"
                    onClick={() => setShowRestoreMenu(false)}
                ></div>
            )}
        </div>
    );
};

interface CheckpointTimelineProps {
    checkpoints: Array<{
        id: string;
        timestamp: Date;
        description: string;
        isCheckedOut?: boolean;
    }>;
    onCompare?: (checkpointId: string) => void;
    onRestore?: (checkpointId: string, type: 'task' | 'workspace' | 'both') => void;
}

export const CheckpointTimeline: React.FC<CheckpointTimelineProps> = ({
    checkpoints,
    onCompare,
    onRestore
}) => {
    return (
        <div className="checkpoint-timeline space-y-3">
            {checkpoints.map((checkpoint, index) => (
                <div key={checkpoint.id} className="checkpoint-item">
                    <div className="flex items-start gap-3">
                        {/* Timeline indicator */}
                        <div className="flex flex-col items-center">
                            <div className={`w-3 h-3 border-2 ${
                                checkpoint.isCheckedOut 
                                    ? 'bg-blue-500 border-blue-500' 
                                    : 'bg-vscode-bg border-vscode-border'
                            }`}></div>
                            {index < checkpoints.length - 1 && (
                                <div className="w-px h-8 bg-vscode-border mt-1"></div>
                            )}
                        </div>

                        {/* Checkpoint content */}
                        <div className="flex-1 pb-4">
                            <div className="flex items-center justify-between">
                                <div>
                                    <div className="text-sm text-vscode-fg font-medium">
                                        {checkpoint.description}
                                    </div>
                                    <div className="text-xs text-vscode-description">
                                        {checkpoint.timestamp.toLocaleString()}
                                    </div>
                                </div>
                                
                                <CheckpointControls
                                    messageId={checkpoint.id}
                                    isCheckedOut={checkpoint.isCheckedOut}
                                    onCompare={onCompare}
                                    onRestore={onRestore}
                                />
                            </div>
                        </div>
                    </div>
                </div>
            ))}
        </div>
    );
}; 