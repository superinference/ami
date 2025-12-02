import * as React from 'react';
import { useState } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';

// Get VS Code API from global scope (should be acquired once in the main app)
declare global {
    interface Window {
        vscode?: any;
    }
}

function getVsCodeApi(): any {
    // Try to get from window first (set by main app)
    if (window.vscode) {
        return window.vscode;
    }

    // Fallback to global acquireVsCodeApi if available
    if (typeof (window as any).acquireVsCodeApi === 'function') {
        try {
            const api = (window as any).acquireVsCodeApi();
            window.vscode = api; // Cache it
            return api;
        } catch (error) {
            console.error('Failed to acquire VS Code API:', error);
            return null;
        }
    }

    return null;
}

interface SimpleCheckpointControlsProps {
    snapshotId?: string;
    onRestore?: (snapshotId: string) => void;
    className?: string;
    // Always show the component, even if snapshot is not ready yet
    alwaysShow?: boolean;
    // Track if this checkpoint has been restored
    isRestored?: boolean;
}

export const SimpleCheckpointControls: React.FC<SimpleCheckpointControlsProps> = ({
    snapshotId,
    onRestore,
    className = '',
    alwaysShow = true,
    isRestored = false
}) => {
    const [isRestoring, setIsRestoring] = useState(false);

    // Debug: Log the snapshotId prop changes
    React.useEffect(() => {
        console.log('🔄 SimpleCheckpointControls: Received snapshotId prop:', snapshotId, 'type:', typeof snapshotId);
        console.log('🔄 SimpleCheckpointControls: Will show restore button:', !!snapshotId);
        console.log('🔄 SimpleCheckpointControls: isRestored:', isRestored);
    }, [snapshotId, isRestored]);

    const handleRestore = async () => {
        console.log('🔄 SimpleCheckpointControls: Restore button clicked');
        console.log('🔄 SimpleCheckpointControls: snapshotId:', snapshotId);
        console.log('🔄 SimpleCheckpointControls: isRestoring:', isRestoring);
        console.log('🔄 SimpleCheckpointControls: isRestored:', isRestored);

        if (isRestoring || !snapshotId || isRestored) {
            console.log('🔄 SimpleCheckpointControls: Early return - isRestoring, no snapshotId, or already restored');
            return;
        }

        setIsRestoring(true);
        try {
            console.log('🔄 SimpleCheckpointControls: Getting VS Code API');
            const vscode = getVsCodeApi();

            if (!vscode) {
                console.error('🔄 SimpleCheckpointControls: Failed to get VS Code API');
                return;
            }

            console.log('🔄 SimpleCheckpointControls: Sending executeCommand message');
            console.log('🔄 SimpleCheckpointControls: Command: superinference.restoreSnapshot');
            console.log('🔄 SimpleCheckpointControls: Args:', [snapshotId]);

            vscode.postMessage({
                type: 'executeCommand',
                command: 'superinference.restoreSnapshot',
                args: [snapshotId]
            });

            console.log('🔄 SimpleCheckpointControls: executeCommand message sent successfully');

            // Call the callback if provided
            if (onRestore) {
                console.log('🔄 SimpleCheckpointControls: Calling onRestore callback');
                await onRestore(snapshotId);
                console.log('🔄 SimpleCheckpointControls: onRestore callback completed');
            }
        } catch (error) {
            console.error('🔄 SimpleCheckpointControls: Failed to restore snapshot:', error);
        } finally {
            console.log('🔄 SimpleCheckpointControls: Setting isRestoring to false');
            setIsRestoring(false);
        }
    };

    // Show if we always show or if we have a snapshot ID
    if (!alwaysShow && !snapshotId) {
        return null;
    }

    console.log('🔄 SimpleCheckpointControls: Rendering with snapshotId:', snapshotId, 'showing restore button:', !!snapshotId);

    return (
        <div className={`simple-checkpoint-controls bg-vscode-editorWidget-background border border-vscode-widget-border p-2 my-1 ${className}`}>
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <i className="codicon codicon-history text-vscode-symbolIcon-colorProperty"></i>
                    <span className="text-xs text-vscode-fg">
                        {snapshotId ? (
                            `Auto-checkpoint: ${snapshotId.slice(0, 12)}...`
                        ) : (
                            <>
                                <i className="codicon codicon-loading codicon-modifier-spin mr-1"></i>
                                Creating snapshot...
                            </>
                        )}
                    </span>
                </div>

                {snapshotId ? (
                    <VSCodeButton
                        appearance="secondary"
                        disabled={isRestoring || isRestored}
                        onClick={handleRestore}
                        title={isRestored ? "Checkpoint has been restored" : "Restore files to this snapshot"}
                        className="text-xs px-2 py-1"
                    >
                        {isRestored ? (
                            <>
                                <i className="codicon codicon-check mr-1"></i>
                                Restored
                            </>
                        ) : isRestoring ? (
                            <i className="codicon codicon-loading codicon-modifier-spin"></i>
                        ) : (
                            <>
                                <i className="codicon codicon-discard mr-1"></i>
                                Restore
                            </>
                        )}
                    </VSCodeButton>
                ) : (
                    <div className="text-xs text-vscode-descriptionForeground">
                        Snapshot will be ready shortly...
                    </div>
                )}
            </div>
        </div>
    );
}; 