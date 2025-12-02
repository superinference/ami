import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';

const SNAPSHOT_DIR_NAME = '.vscode/llm-snapshots';

export interface FileSnapshot {
    filePath: string;
    content: string;
    language?: string;
}

export interface Snapshot {
    id: string;
    timestamp: string;
    label: string;
    llmMessage?: string;
    files: FileSnapshot[];
}

export class SnapshotManager {
    private getSnapshotDir(): string | null {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            return null;
        }
        return path.join(workspaceFolder.uri.fsPath, SNAPSHOT_DIR_NAME);
    }

    private ensureSnapshotDir(): string {
        const snapshotDir = this.getSnapshotDir();
        if (!snapshotDir) {
            throw new Error('No workspace folder found');
        }

        if (!fs.existsSync(snapshotDir)) {
            fs.mkdirSync(snapshotDir, { recursive: true });
        }

        return snapshotDir;
    }

    async createSnapshot(label: string = '', llmMessage?: string): Promise<string> {
        console.log('📸 Creating snapshot:', label);

        try {
            const snapshotDir = this.ensureSnapshotDir();

            // Get all open text documents
            const openDocuments = vscode.workspace.textDocuments.filter(doc =>
                !doc.isUntitled &&
                doc.uri.scheme === 'file'
            );

            const files: FileSnapshot[] = [];

            for (const doc of openDocuments) {
                try {
                    const content = doc.getText();
                    files.push({
                        filePath: doc.uri.fsPath,
                        content: content,
                        language: doc.languageId
                    });
                } catch (error) {
                    console.warn('Failed to capture document:', doc.uri.fsPath, error);
                }
            }

            // Include active editor if not already captured
            const activeEditor = vscode.window.activeTextEditor;
            if (activeEditor && !activeEditor.document.isUntitled) {
                const activeUri = activeEditor.document.uri;
                const alreadyCaptured = files.some(f => f.filePath === activeUri.fsPath);

                if (!alreadyCaptured) {
                    files.push({
                        filePath: activeUri.fsPath,
                        content: activeEditor.document.getText(),
                        language: activeEditor.document.languageId
                    });
                }
            }

            const timestamp = new Date().toISOString();
            const id = timestamp.replace(/[:.]/g, '-');

            const snapshot: Snapshot = {
                id,
                timestamp,
                label,
                llmMessage,
                files
            };

            const filename = path.join(snapshotDir, `${id}${label ? '-' + label.replace(/[^a-zA-Z0-9]/g, '-') : ''}.json`);
            fs.writeFileSync(filename, JSON.stringify(snapshot, null, 2));

            console.log(`📸 Snapshot created: ${filename} (${files.length} files)`);
            return id;

        } catch (error) {
            console.error('Failed to create snapshot:', error);
            vscode.window.showErrorMessage(`Failed to create snapshot: ${error instanceof Error ? error.message : 'Unknown error'}`);
            throw error;
        }
    }

    async restoreSnapshotById(snapshotId: string): Promise<void> {
        console.log('🔄 SnapshotManager: restoreSnapshotById called with:', snapshotId);
        console.log('🔄 SnapshotManager: snapshotId type:', typeof snapshotId);
        console.log('🔄 SnapshotManager: snapshotId length:', snapshotId?.length);

        // Direct restore without showing picker
        try {
            console.log('🔄 SnapshotManager: Calling restoreSnapshot method');
            const result = await this.restoreSnapshot(snapshotId);
            console.log('🔄 SnapshotManager: restoreSnapshot completed successfully');
            return result;
        } catch (error) {
            console.error('🔄 SnapshotManager: Error in restoreSnapshotById:', error);
            throw error;
        }
    }

    async restoreSnapshot(snapshotId: string): Promise<void> {
        console.log('🔄 SnapshotManager: Starting restoreSnapshot with snapshotId:', snapshotId);
        console.log('🔄 SnapshotManager: snapshotId value:', JSON.stringify(snapshotId));

        try {
            console.log('🔄 SnapshotManager: Getting snapshot directory');
            const snapshotDir = this.getSnapshotDir();
            console.log('🔄 SnapshotManager: Snapshot directory:', snapshotDir);

            if (!snapshotDir) {
                throw new Error('No workspace folder found');
            }

            console.log('🔄 SnapshotManager: Checking if snapshot directory exists');
            if (!fs.existsSync(snapshotDir)) {
                console.error('🔄 SnapshotManager: Snapshot directory does not exist:', snapshotDir);
                throw new Error(`Snapshot directory does not exist: ${snapshotDir}`);
            }

            // Find the snapshot file
            console.log('🔄 SnapshotManager: Reading snapshot directory contents');
            const files = fs.readdirSync(snapshotDir).filter(f => f.endsWith('.json'));
            console.log('🔄 SnapshotManager: Found snapshot files:', files);

            console.log('🔄 SnapshotManager: Looking for file starting with:', snapshotId);
            const snapshotFile = files.find(f => f.startsWith(snapshotId));
            console.log('🔄 SnapshotManager: Matched snapshot file:', snapshotFile);

            if (!snapshotFile) {
                console.error('🔄 SnapshotManager: Snapshot file not found for ID:', snapshotId);
                console.error('🔄 SnapshotManager: Available files:', files);
                throw new Error(`Snapshot ${snapshotId} not found`);
            }

            const snapshotPath = path.join(snapshotDir, snapshotFile);
            console.log('🔄 SnapshotManager: Reading snapshot file:', snapshotPath);

            let snapshotData: Snapshot;
            try {
                const fileContent = fs.readFileSync(snapshotPath, 'utf-8');
                console.log('🔄 SnapshotManager: Snapshot file content length:', fileContent.length);
                snapshotData = JSON.parse(fileContent);
                console.log('🔄 SnapshotManager: Parsed snapshot data:', {
                    id: snapshotData.id,
                    timestamp: snapshotData.timestamp,
                    label: snapshotData.label,
                    filesCount: snapshotData.files?.length
                });
            } catch (parseError) {
                console.error('🔄 SnapshotManager: Error parsing snapshot file:', parseError);
                throw new Error(`Failed to parse snapshot file: ${parseError}`);
            }

            let restoredCount = 0;
            const errors: string[] = [];

            console.log('🔄 SnapshotManager: Starting file restoration for', snapshotData.files.length, 'files');

            for (const file of snapshotData.files) {
                console.log('🔄 SnapshotManager: Restoring file:', file.filePath);
                try {
                    const fileUri = vscode.Uri.file(file.filePath);
                    console.log('🔄 SnapshotManager: File URI:', fileUri.toString());

                    // Check if file exists
                    try {
                        console.log('🔄 SnapshotManager: Checking if file exists');
                        await vscode.workspace.fs.stat(fileUri);
                        console.log('🔄 SnapshotManager: File exists, will update content');
                    } catch {
                        console.log('🔄 SnapshotManager: File does not exist, will create new file');
                        // File doesn't exist, create it
                        await vscode.workspace.fs.writeFile(fileUri, Buffer.from(file.content, 'utf8'));
                        restoredCount++;
                        console.log('🔄 SnapshotManager: Created new file successfully');
                        continue;
                    }

                    // Open the document and replace its content
                    console.log('🔄 SnapshotManager: Opening document for editing');
                    const doc = await vscode.workspace.openTextDocument(fileUri);
                    console.log('🔄 SnapshotManager: Document opened, showing in editor');
                    const editor = await vscode.window.showTextDocument(doc);

                    console.log('🔄 SnapshotManager: Creating edit range for entire document');
                    const fullRange = new vscode.Range(
                        doc.positionAt(0),
                        doc.positionAt(doc.getText().length)
                    );

                    console.log('🔄 SnapshotManager: Applying edit to replace content');
                    const success = await editor.edit(editBuilder => {
                        editBuilder.replace(fullRange, file.content);
                    });

                    if (success) {
                        restoredCount++;
                        console.log('🔄 SnapshotManager: File restored successfully:', path.basename(file.filePath));
                    } else {
                        const errorMsg = `Failed to apply changes to ${path.basename(file.filePath)}`;
                        console.error('🔄 SnapshotManager:', errorMsg);
                        errors.push(errorMsg);
                    }

                } catch (error) {
                    const fileName = path.basename(file.filePath);
                    const errorMsg = `${fileName}: ${error instanceof Error ? error.message : 'Unknown error'}`;
                    console.error('🔄 SnapshotManager: Error restoring file:', errorMsg);
                    errors.push(errorMsg);
                }
            }

            // Show results
            console.log('🔄 SnapshotManager: Restoration completed. Success:', restoredCount, 'Errors:', errors.length);
            if (errors.length === 0) {
                const message = `✅ Snapshot restored successfully (${restoredCount} files)`;
                console.log('🔄 SnapshotManager:', message);
                vscode.window.showInformationMessage(message);
            } else {
                const message = `⚠️ Snapshot partially restored (${restoredCount}/${snapshotData.files.length} files). Errors: ${errors.join(', ')}`;
                console.log('🔄 SnapshotManager:', message);
                vscode.window.showWarningMessage(message);
            }

        } catch (error) {
            console.error('🔄 SnapshotManager: Failed to restore snapshot:', error);
            console.error('🔄 SnapshotManager: Error details:', error);
            vscode.window.showErrorMessage(`Failed to restore snapshot: ${error instanceof Error ? error.message : 'Unknown error'}`);
            throw error;
        }
    }

    async listSnapshots(): Promise<Snapshot[]> {
        try {
            const snapshotDir = this.getSnapshotDir();
            if (!snapshotDir || !fs.existsSync(snapshotDir)) {
                return [];
            }

            const files = fs.readdirSync(snapshotDir)
                .filter(f => f.endsWith('.json'))
                .sort((a, b) => b.localeCompare(a)); // Most recent first

            const snapshots: Snapshot[] = [];

            for (const file of files) {
                try {
                    const filePath = path.join(snapshotDir, file);
                    const data: Snapshot = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
                    snapshots.push(data);
                } catch (error) {
                    console.warn('Failed to parse snapshot file:', file, error);
                }
            }

            return snapshots;

        } catch (error) {
            console.error('Failed to list snapshots:', error);
            return [];
        }
    }

    async showSnapshotPicker(): Promise<string | undefined> {
        const snapshots = await this.listSnapshots();

        if (snapshots.length === 0) {
            vscode.window.showInformationMessage('No snapshots found');
            return undefined;
        }

        const items = snapshots.map(snapshot => ({
            label: snapshot.label || `Snapshot ${snapshot.id}`,
            description: new Date(snapshot.timestamp).toLocaleString(),
            detail: snapshot.llmMessage || `${snapshot.files.length} files`,
            snapshotId: snapshot.id
        }));

        const selected = await vscode.window.showQuickPick(items, {
            placeHolder: 'Select a snapshot to restore'
        });

        return selected?.snapshotId;
    }

    async deleteSnapshot(snapshotId: string): Promise<void> {
        try {
            const snapshotDir = this.getSnapshotDir();
            if (!snapshotDir) {
                throw new Error('No workspace folder found');
            }

            const files = fs.readdirSync(snapshotDir).filter(f => f.endsWith('.json'));
            const snapshotFile = files.find(f => f.startsWith(snapshotId));

            if (snapshotFile) {
                const filePath = path.join(snapshotDir, snapshotFile);
                fs.unlinkSync(filePath);
                console.log('🗑️ Deleted snapshot:', snapshotId);
            }

        } catch (error) {
            console.error('Failed to delete snapshot:', error);
            throw error;
        }
    }
}

// Singleton instance
let snapshotManager: SnapshotManager | null = null;

export function getSnapshotManager(): SnapshotManager {
    if (!snapshotManager) {
        snapshotManager = new SnapshotManager();
    }
    return snapshotManager;
}

// Register VS Code commands
export function registerSnapshotCommands(context: vscode.ExtensionContext): void {
    const manager = getSnapshotManager();

    const createSnapshotCommand = vscode.commands.registerCommand(
        'superinference.createSnapshot',
        async () => {
            const label = await vscode.window.showInputBox({
                prompt: 'Enter a label for this snapshot (optional)',
                placeHolder: 'e.g., before-refactor, working-version'
            });

            if (label !== undefined) {
                await manager.createSnapshot(label || '');
            }
        }
    );

    const restoreSnapshotCommand = vscode.commands.registerCommand(
        'superinference.restoreSnapshot',
        async (snapshotId?: string) => {
            if (snapshotId) {
                // Direct restore by ID
                await manager.restoreSnapshotById(snapshotId);
            } else {
                // Show picker for manual selection
                const selectedSnapshotId = await manager.showSnapshotPicker();
                if (selectedSnapshotId) {
                    await manager.restoreSnapshot(selectedSnapshotId);
                }
            }
        }
    );

    const listSnapshotsCommand = vscode.commands.registerCommand(
        'superinference.listSnapshots',
        async () => {
            const snapshots = await manager.listSnapshots();
            if (snapshots.length === 0) {
                vscode.window.showInformationMessage('No snapshots found');
                return;
            }

            const output = vscode.window.createOutputChannel('SuperInference Snapshots');
            output.clear();
            output.appendLine('📸 Available Snapshots:\n');

            snapshots.forEach(snapshot => {
                output.appendLine(`ID: ${snapshot.id}`);
                output.appendLine(`Label: ${snapshot.label || '(no label)'}`);
                output.appendLine(`Time: ${new Date(snapshot.timestamp).toLocaleString()}`);
                output.appendLine(`Files: ${snapshot.files.length}`);
                if (snapshot.llmMessage) {
                    output.appendLine(`Message: ${snapshot.llmMessage}`);
                }
                output.appendLine('---');
            });

            output.show();
        }
    );

    context.subscriptions.push(
        createSnapshotCommand,
        restoreSnapshotCommand,
        listSnapshotsCommand
    );
} 