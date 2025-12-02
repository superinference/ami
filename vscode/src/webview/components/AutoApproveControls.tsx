import * as React from 'react';
import { useState } from 'react';
import { VSCodeButton, VSCodeCheckbox, VSCodeDropdown, VSCodeOption } from '@vscode/webview-ui-toolkit/react';

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

interface AutoApproveControlsProps {
    settings: AutoApprovalSettings;
    onSettingsChange: (settings: AutoApprovalSettings) => void;
    currentCount?: number;
    isVisible?: boolean;
    onToggleVisibility?: () => void;
}

export const AutoApproveControls: React.FC<AutoApproveControlsProps> = ({
    settings,
    onSettingsChange,
    currentCount = 0,
    isVisible = false,
    onToggleVisibility
}) => {
    const [localSettings, setLocalSettings] = useState(settings);

    const updateSetting = (key: keyof AutoApprovalSettings['actions'], value: boolean) => {
        const newSettings = {
            ...localSettings,
            actions: {
                ...localSettings.actions,
                [key]: value
            }
        };
        setLocalSettings(newSettings);
        onSettingsChange(newSettings);
    };

    const updateMaxRequests = (value: number) => {
        const newSettings = {
            ...localSettings,
            maxRequests: value
        };
        setLocalSettings(newSettings);
        onSettingsChange(newSettings);
    };

    const toggleEnabled = () => {
        const newSettings = {
            ...localSettings,
            enabled: !localSettings.enabled
        };
        setLocalSettings(newSettings);
        onSettingsChange(newSettings);
    };

    const getStatusText = () => {
        if (!localSettings.enabled) return 'Disabled';
        const enabledActions = Object.values(localSettings.actions).filter(Boolean).length;
        const totalActions = Object.keys(localSettings.actions).length;
        return `${enabledActions}/${totalActions} permissions enabled`;
    };

    const getProgressPercentage = () => {
        if (localSettings.maxRequests === 0) return 0;
        return Math.min((currentCount / localSettings.maxRequests) * 100, 100);
    };

    return (
        <div className="auto-approve-controls bg-vscode-sidebar-bg border border-vscode-border rounded-lg overflow-hidden">
            {/* Header */}
            <div 
                className="auto-approve-header flex items-center justify-between p-3 border-b border-vscode-border cursor-pointer hover:bg-vscode-input-bg transition-colors"
                onClick={onToggleVisibility}
            >
                <div className="flex items-center gap-3">
                    <div className={`w-2 h-2 rounded-full ${localSettings.enabled ? 'bg-green-500' : 'bg-gray-500'}`}></div>
                    <div>
                        <div className="text-sm font-medium text-vscode-fg">Auto-approve</div>
                        <div className="text-xs text-vscode-description">{getStatusText()}</div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    {localSettings.enabled && (
                        <div className="text-xs text-vscode-description">
                            {currentCount}/{localSettings.maxRequests}
                        </div>
                    )}
                    <i className={`codicon ${isVisible ? 'codicon-chevron-up' : 'codicon-chevron-down'} text-vscode-description`}></i>
                </div>
            </div>

            {/* Content */}
            {isVisible && (
                <div className="auto-approve-content p-4 space-y-4">
                    {/* Enable/Disable Toggle */}
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-sm font-medium text-vscode-fg">Enable Auto-approve</div>
                            <div className="text-xs text-vscode-description">Allow automatic execution of approved actions</div>
                        </div>
                        <VSCodeCheckbox 
                            checked={localSettings.enabled}
                            onChange={toggleEnabled}
                        />
                    </div>

                    {localSettings.enabled && (
                        <>
                            {/* Request Counter Progress */}
                            <div className="request-counter">
                                <div className="flex justify-between items-center mb-2">
                                    <span className="text-xs font-medium text-vscode-description">Requests Used</span>
                                    <span className="text-xs text-vscode-description">
                                        {currentCount} / {localSettings.maxRequests}
                                    </span>
                                </div>
                                <div className="w-full bg-vscode-border rounded-full h-2">
                                    <div 
                                        className={`h-2 rounded-full transition-all duration-300 ${
                                            getProgressPercentage() > 80 ? 'bg-red-500' : 
                                            getProgressPercentage() > 60 ? 'bg-yellow-500' : 'bg-green-500'
                                        }`}
                                        style={{ width: `${getProgressPercentage()}%` }}
                                    ></div>
                                </div>
                            </div>

                            {/* Permission Settings */}
                            <div className="permissions-grid">
                                <div className="text-sm font-medium text-vscode-fg mb-3">Permissions</div>
                                
                                {/* Read Files */}
                                <div className="permission-item flex items-center justify-between py-2">
                                    <div className="flex items-center gap-2">
                                        <i className="codicon codicon-file text-vscode-description"></i>
                                        <div>
                                            <div className="text-sm text-vscode-fg">Read Files</div>
                                            <div className="text-xs text-vscode-description">Read project files and directories</div>
                                        </div>
                                    </div>
                                    <VSCodeCheckbox 
                                        checked={localSettings.actions.readFiles}
                                        onChange={(e: any) => updateSetting('readFiles', (e.target as HTMLInputElement).checked)}
                                    />
                                </div>

                                {/* Edit Files */}
                                <div className="permission-item flex items-center justify-between py-2">
                                    <div className="flex items-center gap-2">
                                        <i className="codicon codicon-edit text-vscode-description"></i>
                                        <div>
                                            <div className="text-sm text-vscode-fg">Edit Files</div>
                                            <div className="text-xs text-vscode-description">Modify and create files</div>
                                        </div>
                                    </div>
                                    <VSCodeCheckbox 
                                        checked={localSettings.actions.editFiles}
                                        onChange={(e: any) => updateSetting('editFiles', (e.target as HTMLInputElement).checked)}
                                    />
                                </div>

                                {/* Execute Commands */}
                                <div className="permission-item flex items-center justify-between py-2">
                                    <div className="flex items-center gap-2">
                                        <i className="codicon codicon-terminal text-vscode-description"></i>
                                        <div>
                                            <div className="text-sm text-vscode-fg">Execute Commands</div>
                                            <div className="text-xs text-vscode-description">Run terminal commands</div>
                                        </div>
                                    </div>
                                    <VSCodeCheckbox 
                                        checked={localSettings.actions.executeCommands}
                                        onChange={(e: any) => updateSetting('executeCommands', (e.target as HTMLInputElement).checked)}
                                    />
                                </div>

                                {/* Use Browser */}
                                <div className="permission-item flex items-center justify-between py-2">
                                    <div className="flex items-center gap-2">
                                        <i className="codicon codicon-browser text-vscode-description"></i>
                                        <div>
                                            <div className="text-sm text-vscode-fg">Use Browser</div>
                                            <div className="text-xs text-vscode-description">Browse and fetch web content</div>
                                        </div>
                                    </div>
                                    <VSCodeCheckbox 
                                        checked={localSettings.actions.useBrowser}
                                        onChange={(e: any) => updateSetting('useBrowser', (e.target as HTMLInputElement).checked)}
                                    />
                                </div>

                                {/* Use MCP */}
                                <div className="permission-item flex items-center justify-between py-2">
                                    <div className="flex items-center gap-2">
                                        <i className="codicon codicon-plug text-vscode-description"></i>
                                        <div>
                                            <div className="text-sm text-vscode-fg">Use MCP Servers</div>
                                            <div className="text-xs text-vscode-description">Connect to external services</div>
                                        </div>
                                    </div>
                                    <VSCodeCheckbox 
                                        checked={localSettings.actions.useMcp}
                                        onChange={(e: any) => updateSetting('useMcp', (e.target as HTMLInputElement).checked)}
                                    />
                                </div>
                            </div>

                            {/* Max Requests Setting */}
                            <div className="max-requests-setting">
                                <div className="text-sm font-medium text-vscode-fg mb-2">Maximum Requests</div>
                                <div className="flex items-center gap-2">
                                    <input
                                        type="range"
                                        min="5"
                                        max="100"
                                        step="5"
                                        value={localSettings.maxRequests}
                                        onChange={(e) => updateMaxRequests(parseInt(e.target.value))}
                                        className="flex-1"
                                    />
                                    <span className="text-sm text-vscode-description min-w-[3ch]">
                                        {localSettings.maxRequests}
                                    </span>
                                </div>
                                <div className="text-xs text-vscode-description mt-1">
                                    Number of consecutive auto-approved actions before requiring confirmation
                                </div>
                            </div>

                            {/* Notifications */}
                            <div className="flex items-center justify-between">
                                <div>
                                    <div className="text-sm text-vscode-fg">Notifications</div>
                                    <div className="text-xs text-vscode-description">Show notifications for auto-approved actions</div>
                                </div>
                                <VSCodeCheckbox 
                                    checked={localSettings.enableNotifications}
                                    onChange={(e: any) => setLocalSettings({
                                        ...localSettings,
                                        enableNotifications: (e.target as HTMLInputElement).checked
                                    })}
                                />
                            </div>

                            {/* Reset Button */}
                            {currentCount > 0 && (
                                <div className="pt-2 border-t border-vscode-border">
                                    <VSCodeButton 
                                        appearance="secondary"
                                        onClick={() => {/* Reset counter logic */}}
                                        className="w-full"
                                    >
                                        Reset Request Counter
                                    </VSCodeButton>
                                </div>
                            )}
                        </>
                    )}
                </div>
            )}
        </div>
    );
}; 