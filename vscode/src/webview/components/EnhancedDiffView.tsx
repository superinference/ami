import * as React from 'react';
import { useState, useRef, useEffect } from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';

interface DiffLine {
    type: 'added' | 'removed' | 'context' | 'header';
    content: string;
    lineNumber?: {
        old?: number;
        new?: number;
    };
}

interface FileDiff {
    filePath: string;
    changes: DiffLine[];
    isNew?: boolean;
    isDeleted?: boolean;
    isBinary?: boolean;
}

interface EnhancedDiffViewProps {
    fileDiffs: FileDiff[];
    onApply?: (filePath: string) => void;
    onReject?: (filePath: string) => void;
    onClose?: () => void;
}

export const EnhancedDiffView: React.FC<EnhancedDiffViewProps> = ({
    fileDiffs,
    onApply,
    onReject,
    onClose
}) => {
    const [selectedFile, setSelectedFile] = useState(0);
    const [isFullScreen, setIsFullScreen] = useState(false);
    const diffContainerRef = useRef<HTMLDivElement>(null);

    const currentDiff = fileDiffs[selectedFile];

    const copyToClipboard = async (content: string) => {
        try {
            await navigator.clipboard.writeText(content);
        } catch (err) {
            console.error('Failed to copy to clipboard:', err);
        }
    };

    const getFileIcon = (filePath: string) => {
        if (!filePath || typeof filePath !== 'string') return 'codicon-file';
        const ext = filePath.split('.').pop()?.toLowerCase();
        switch (ext) {
            case 'ts': case 'tsx': return 'codicon-file-code';
            case 'js': case 'jsx': return 'codicon-file-code';
            case 'py': return 'codicon-file-code';
            case 'json': return 'codicon-file-code';
            case 'md': return 'codicon-file-text';
            case 'css': case 'scss': return 'codicon-file-code';
            case 'html': return 'codicon-file-code';
            default: return 'codicon-file';
        }
    };

    const getChangesSummary = (diff: FileDiff) => {
        const added = diff.changes.filter(line => line.type === 'added').length;
        const removed = diff.changes.filter(line => line.type === 'removed').length;
        return { added, removed };
    };

    const renderDiffLine = (line: DiffLine, index: number) => {
        const baseClasses = "diff-line font-mono text-sm leading-relaxed px-4 py-1 border-l-2 transition-colors";
        
        let lineClasses = baseClasses;
        let bgColor = "";
        let borderColor = "";
        let icon = "";

        switch (line.type) {
            case 'added':
                bgColor = "bg-green-50 dark:bg-green-900/20";
                borderColor = "border-green-500";
                lineClasses += " text-green-800 dark:text-green-200";
                icon = "+";
                break;
            case 'removed':
                bgColor = "bg-red-50 dark:bg-red-900/20";
                borderColor = "border-red-500";
                lineClasses += " text-red-800 dark:text-red-200";
                icon = "-";
                break;
            case 'context':
                bgColor = "bg-transparent";
                borderColor = "border-transparent";
                lineClasses += " text-vscode-fg";
                icon = " ";
                break;
            case 'header':
                bgColor = "bg-vscode-input-bg";
                borderColor = "border-vscode-border";
                lineClasses += " text-vscode-description font-medium";
                icon = "@";
                break;
        }

        return (
            <div 
                key={index}
                className={`${lineClasses} ${bgColor} ${borderColor} hover:bg-opacity-80 group`}
            >
                <div className="flex items-start">
                    <div className="flex-shrink-0 w-12 text-right text-xs text-vscode-description mr-3">
                        {line.lineNumber?.old && (
                            <span className="inline-block w-5">{line.lineNumber.old}</span>
                        )}
                        {line.lineNumber?.new && (
                            <span className="inline-block w-5 ml-1">{line.lineNumber.new}</span>
                        )}
                    </div>
                    <div className="flex-shrink-0 w-4 text-center text-xs font-bold mr-2">
                        {icon}
                    </div>
                    <div className="flex-1 whitespace-pre-wrap break-all">
                        {line.content}
                    </div>
                    <button
                        onClick={() => copyToClipboard(line.content)}
                        className="opacity-0 group-hover:opacity-100 ml-2 text-vscode-description hover:text-vscode-fg transition-opacity"
                        title="Copy line"
                    >
                        <i className="codicon codicon-copy text-xs"></i>
                    </button>
                </div>
            </div>
        );
    };

    if (!currentDiff) return null;

    return (
        <div className={`enhanced-diff-view bg-vscode-bg text-vscode-fg ${isFullScreen ? 'fixed inset-0 z-50' : 'rounded-lg border border-vscode-border'} overflow-hidden`}>
            {/* Header */}
            <div className="diff-header bg-vscode-sidebar-bg border-b border-vscode-border p-4">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="flex items-center gap-2">
                            <i className={`${getFileIcon(currentDiff.filePath)} text-vscode-description`}></i>
                            <h3 className="text-lg font-semibold text-vscode-fg">
                                {currentDiff.filePath}
                            </h3>
                        </div>
                        
                        {/* Status badges */}
                        <div className="flex gap-2">
                            {currentDiff.isNew && (
                                <span className="px-2 py-1 text-xs bg-green-500 text-white rounded">New</span>
                            )}
                            {currentDiff.isDeleted && (
                                <span className="px-2 py-1 text-xs bg-red-500 text-white rounded">Deleted</span>
                            )}
                            {currentDiff.isBinary && (
                                <span className="px-2 py-1 text-xs bg-gray-500 text-white rounded">Binary</span>
                            )}
                        </div>

                        {/* Changes summary */}
                        {(() => {
                            const { added, removed } = getChangesSummary(currentDiff);
                            return (
                                <div className="flex items-center gap-3 text-sm">
                                    <span className="text-green-500">+{added}</span>
                                    <span className="text-red-500">-{removed}</span>
                                </div>
                            );
                        })()}
                    </div>

                    <div className="flex items-center gap-2">
                        {/* File navigation */}
                        {fileDiffs.length > 1 && (
                            <div className="flex items-center gap-2 mr-4">
                                <VSCodeButton
                                    appearance="icon"
                                    disabled={selectedFile === 0}
                                    onClick={() => setSelectedFile(selectedFile - 1)}
                                    title="Previous file"
                                >
                                    <i className="codicon codicon-chevron-left"></i>
                                </VSCodeButton>
                                <span className="text-sm text-vscode-description">
                                    {selectedFile + 1} of {fileDiffs.length}
                                </span>
                                <VSCodeButton
                                    appearance="icon"
                                    disabled={selectedFile === fileDiffs.length - 1}
                                    onClick={() => setSelectedFile(selectedFile + 1)}
                                    title="Next file"
                                >
                                    <i className="codicon codicon-chevron-right"></i>
                                </VSCodeButton>
                            </div>
                        )}

                        {/* Action buttons */}
                        <VSCodeButton
                            appearance="icon"
                            onClick={() => setIsFullScreen(!isFullScreen)}
                            title={isFullScreen ? "Exit fullscreen" : "Fullscreen"}
                        >
                            <i className={`codicon ${isFullScreen ? 'codicon-screen-normal' : 'codicon-screen-full'}`}></i>
                        </VSCodeButton>
                        
                        <VSCodeButton
                            appearance="icon"
                            onClick={() => copyToClipboard(currentDiff.changes.map(line => line.content).join('\n'))}
                            title="Copy all changes"
                        >
                            <i className="codicon codicon-copy"></i>
                        </VSCodeButton>

                        {onClose && (
                            <VSCodeButton
                                appearance="icon"
                                onClick={onClose}
                                title="Close diff view"
                            >
                                <i className="codicon codicon-close"></i>
                            </VSCodeButton>
                        )}
                    </div>
                </div>

                {/* File tabs for multiple files */}
                {fileDiffs.length > 1 && (
                    <div className="file-tabs flex gap-1 mt-3 overflow-x-auto">
                        {fileDiffs.map((diff, index) => (
                            <button
                                key={index}
                                onClick={() => setSelectedFile(index)}
                                className={`
                                    px-3 py-2 text-sm rounded-t transition-colors whitespace-nowrap
                                    ${index === selectedFile 
                                        ? 'bg-vscode-bg text-vscode-fg border-b-2 border-blue-500' 
                                        : 'bg-vscode-input-bg text-vscode-description hover:bg-vscode-bg'
                                    }
                                `}
                            >
                                <i className={`${getFileIcon(diff.filePath)} mr-2`}></i>
                                {(diff.filePath || '').split('/').pop() || 'Unknown'}
                                {(diff.isNew || diff.isDeleted) && (
                                    <span className={`ml-2 w-2 h-2 rounded-full ${diff.isNew ? 'bg-green-500' : 'bg-red-500'}`}></span>
                                )}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {/* Diff content */}
            <div 
                ref={diffContainerRef}
                className="diff-content flex-1 overflow-auto bg-vscode-bg"
                style={{ maxHeight: isFullScreen ? 'calc(100vh - 140px)' : '500px' }}
            >
                {currentDiff.isBinary ? (
                    <div className="p-8 text-center text-vscode-description">
                        <i className="codicon codicon-file-binary text-4xl mb-4"></i>
                        <p>Binary file - cannot show diff</p>
                    </div>
                ) : currentDiff.changes.length === 0 ? (
                    <div className="p-8 text-center text-vscode-description">
                        <i className="codicon codicon-info text-4xl mb-4"></i>
                        <p>No changes to display</p>
                    </div>
                ) : (
                    <div className="diff-lines">
                        {currentDiff.changes.map((line, index) => renderDiffLine(line, index))}
                    </div>
                )}
            </div>

            {/* Footer with actions */}
            {(onApply || onReject) && (
                <div className="diff-footer bg-vscode-sidebar-bg border-t border-vscode-border p-4">
                    <div className="flex justify-between items-center">
                        <div className="text-sm text-vscode-description">
                            Review the changes and choose an action
                        </div>
                        <div className="flex gap-2">
                            {onReject && (
                                <VSCodeButton
                                    appearance="secondary"
                                    onClick={() => onReject(currentDiff.filePath)}
                                >
                                    <i className="codicon codicon-close mr-2"></i>
                                    Reject
                                </VSCodeButton>
                            )}
                            {onApply && (
                                <VSCodeButton
                                    appearance="primary"
                                    onClick={() => onApply(currentDiff.filePath)}
                                >
                                    <i className="codicon codicon-check mr-2"></i>
                                    Apply Changes
                                </VSCodeButton>
                            )}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}; 