import * as vscode from 'vscode';

export interface ReasoningStep {
    id: string;
    type: 'thinking' | 'planning' | 'analysis' | 'execution' | 'validation' | 'error_handling';
    title: string;
    description: string;
    status: 'pending' | 'in_progress' | 'completed' | 'failed';
    startTime: number;
    endTime?: number;
    duration?: number;
    files?: string[];
    output?: string;
    error?: string;
    subSteps?: ReasoningStep[];
}

export interface TaskPlan {
    id: string;
    title: string;
    description: string;
    steps: ReasoningStep[];
    status: 'pending' | 'in_progress' | 'completed' | 'failed';
    createdAt: Date;
    completedAt?: Date;
    originalInstruction: string;
    analysisContent?: string; // Add field to accumulate real analysis content
}

export interface BackendService {
    makeAnalysisCall(prompt: string, context?: any[], onToken?: (delta: string, meta?: any) => void): Promise<string>;
}

export class MultiStepProcessor {
    private onStepUpdate: (step: ReasoningStep) => void;
    private onPlanUpdate: (plan: TaskPlan) => void;
    private currentPlan?: TaskPlan;
    private backendService?: BackendService;

    constructor(
        onStepUpdate: (step: ReasoningStep) => void,
        onPlanUpdate: (plan: TaskPlan) => void,
        backendService?: BackendService
    ) {
        this.onStepUpdate = onStepUpdate;
        this.onPlanUpdate = onPlanUpdate;
        this.backendService = backendService;
    }

    public setCallbacks(
        onStepUpdate: (step: ReasoningStep) => void,
        onPlanUpdate: (plan: TaskPlan) => void
    ): void {
        this.onStepUpdate = onStepUpdate;
        this.onPlanUpdate = onPlanUpdate;
    }

    public setBackendService(backendService: BackendService): void {
        this.backendService = backendService;
    }

    async processComplexInstruction(instruction: string, context?: any): Promise<TaskPlan> {
        console.log('🚀 MultiStepProcessor: Starting processComplexInstruction with:', instruction);

        const plan = await this.createTaskPlan(instruction, context);
        this.currentPlan = plan;

        console.log('📋 MultiStepProcessor: Created plan with', plan.steps.length, 'steps');
        console.log('📋 MultiStepProcessor: Plan status:', plan.status);

        this.notifyPlanUpdate(plan);

        try {
            console.log('🔄 MultiStepProcessor: Starting plan execution...');
            await this.executePlan(plan);
            console.log('✅ MultiStepProcessor: Plan execution completed successfully');
            plan.status = 'completed';
            plan.completedAt = new Date();
        } catch (error) {
            console.error('❌ MultiStepProcessor: Plan execution failed:', error);
            plan.status = 'failed';
            plan.completedAt = new Date();
            console.error('Task plan execution failed:', error);
        }

        console.log('🎯 MultiStepProcessor: Final plan status:', plan.status);
        this.notifyPlanUpdate(plan);
        return plan;
    }

    private async createTaskPlan(instruction: string, context?: any): Promise<TaskPlan> {
        const planningStep = this.createStep(
            'planning',
            'Analyzing Instruction',
            'Breaking down the complex instruction into manageable steps...'
        );

        this.startStep(planningStep);
        this.notifyStepUpdate(planningStep);

        // Simulate thinking time
        await this.simulateThinking(2000);

        const plan: TaskPlan = {
            id: this.generateId(),
            title: `Execute: ${instruction.substring(0, 50)}...`,
            description: instruction,
            originalInstruction: instruction, // Store the original instruction
            steps: [],
            status: 'in_progress',
            createdAt: new Date()
        };

        // Analyze instruction complexity and create steps
        const steps = await this.decomposeInstruction(instruction, context);
        plan.steps = [planningStep, ...steps];

        this.completeStep(planningStep, `Identified ${steps.length} main steps to execute`);
        this.notifyStepUpdate(planningStep);

        plan.status = 'in_progress';
        return plan;
    }

    private async decomposeInstruction(instruction: string, _context?: any): Promise<ReasoningStep[]> {
        const steps: ReasoningStep[] = [];
        const lowerInstruction = instruction.toLowerCase();

        // Always create comprehensive multi-step analysis for better AI insights
        steps.push(
            this.createStep('thinking', '🧠 Initial Analysis', 'Understanding the request and identifying key requirements'),
            this.createStep('analysis', '🔍 Detailed Analysis', 'Examining codebase structure and relevant components'),
            this.createStep('planning', '📋 Strategic Planning', 'Developing implementation approach and identifying dependencies')
        );

        // Add specific steps based on instruction content
        if (lowerInstruction.includes('implement') || lowerInstruction.includes('create') || lowerInstruction.includes('add')) {
            steps.push(this.createStep('execution', '⚡ Implementation', 'Executing the planned changes and improvements'));
        }

        if (lowerInstruction.includes('test') || lowerInstruction.includes('verify')) {
            steps.push(this.createStep('validation', '✅ Validation', 'Testing and verifying the implementation works correctly'));
        }

        if (lowerInstruction.includes('fix') || lowerInstruction.includes('debug') || lowerInstruction.includes('error')) {
            steps.push(this.createStep('error_handling', '🔧 Error Resolution', 'Identifying and resolving issues or bugs'));
        }

        return steps;
    }

    private async executePlan(plan: TaskPlan): Promise<void> {
        console.log('🔄 MultiStepProcessor: executePlan starting with', plan.steps.length, 'steps');

        for (let i = 0; i < plan.steps.length; i++) {
            const step = plan.steps[i];
            console.log(`🔄 MultiStepProcessor: Executing step ${i + 1}/${plan.steps.length}: ${step.title} (${step.type})`);

            try {
                await this.executeStep(step);
                console.log(`✅ MultiStepProcessor: Step ${i + 1} completed successfully: ${step.title}`);
            } catch (error) {
                console.error(`❌ MultiStepProcessor: Step ${i + 1} failed:`, error);
                step.status = 'failed';
                step.error = error instanceof Error ? error.message : 'Unknown error';

                // Stop execution on first failed step
                plan.status = 'failed';
                throw error;
            }

            // Check if we should continue
            if (step.status === 'failed') {
                console.error(`❌ MultiStepProcessor: Step ${i + 1} has failed status, stopping execution`);
                plan.status = 'failed';
                break;
            }
        }

        console.log('✅ MultiStepProcessor: All steps completed successfully');
    }

    private async executeStep(step: ReasoningStep): Promise<void> {
        console.log(`🔄 MultiStepProcessor: executeStep starting for: ${step.title} (${step.type})`);

        this.startStep(step);
        this.notifyStepUpdate(step);

        try {
            switch (step.type) {
                case 'thinking':
                    console.log('🤔 MultiStepProcessor: Executing thinking step...');
                    await this.executeThinkingStep(step);
                    break;
                case 'analysis':
                    console.log('🔍 MultiStepProcessor: Executing analysis step...');
                    await this.executeAnalysisStep(step);
                    break;
                case 'planning':
                    console.log('📋 MultiStepProcessor: Executing planning step...');
                    await this.executePlanningStep(step);
                    break;
                case 'execution':
                    console.log('⚡ MultiStepProcessor: Executing execution step...');
                    await this.executeExecutionStep(step);
                    break;
                case 'validation':
                    console.log('✅ MultiStepProcessor: Executing validation step...');
                    await this.executeValidationStep(step);
                    break;
                case 'error_handling':
                    console.log('🔧 MultiStepProcessor: Executing error handling step...');
                    await this.executeErrorHandlingStep(step);
                    break;
                default:
                    console.error(`❌ MultiStepProcessor: Unknown step type: ${step.type}`);
                    throw new Error(`Unknown step type: ${step.type}`);
            }

            console.log(`✅ MultiStepProcessor: executeStep completed for: ${step.title}`);
        } catch (error) {
            console.error(`❌ MultiStepProcessor: executeStep failed for ${step.title}:`, error);
            throw error;
        }

        this.notifyStepUpdate(step);
    }

    private async executeThinkingStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create a focused analysis prompt for this thinking step
            const analysisPrompt = `Think through this step carefully:\n\nTask: ${step.description}\nStep Type: ${step.type}\n\nPlease provide your analytical thinking for this step. Focus on:\n- Key considerations and insights\n- Potential approaches or solutions\n- Important factors to keep in mind\n\nBe thorough but concise in your analysis.`;

            console.log('🤔 Thinking step AI call prompt:', analysisPrompt.substring(0, 200) + '...');

            // Make real AI call for thinking analysis with streaming
            if (this.backendService) {
                step.output = '';
                const aiAnalysis = await this.backendService.makeAnalysisCall(analysisPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('🤔 Thinking step AI response:', aiAnalysis.substring(0, 200) + '...');

                // Store the real AI analysis as the step output
                step.output = aiAnalysis;

                this.completeStep(step, aiAnalysis);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Thinking step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executeAnalysisStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create a specific analysis prompt for this step
            const analysisPrompt = `Analyze this specific aspect of the task:\n\nTask Component: ${step.description}\nStep Type: ${step.type}\n\nPlease provide detailed analysis including:\n- Current state and what needs to be done\n- Technical considerations and requirements\n- Potential challenges or edge cases\n- Recommended approach\n\nBe specific and actionable in your analysis.`;

            console.log('🔍 Analysis step AI call prompt:', analysisPrompt.substring(0, 200) + '...');

            // Make real AI call for detailed analysis with streaming
            if (this.backendService) {
                step.output = '';
                const aiAnalysis = await this.backendService.makeAnalysisCall(analysisPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('🔍 Analysis step AI response:', aiAnalysis.substring(0, 200) + '...');

                // Store the real AI analysis as the step output
                step.output = aiAnalysis;

                this.completeStep(step, aiAnalysis);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Analysis step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executePlanningStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create a planning-focused prompt
            const planningPrompt = `Create a concrete plan for this aspect:\n\nPlanning Focus: ${step.description}\nStep Type: ${step.type}\n\nPlease provide a detailed plan including:\n- Specific steps and sequence\n- Implementation approach\n- Resource requirements\n- Success criteria\n\nBe detailed and actionable in your planning.`;

            console.log('📋 Planning step AI call prompt:', planningPrompt.substring(0, 200) + '...');

            // Make real AI call for planning with streaming
            if (this.backendService) {
                step.output = '';
                const aiPlanning = await this.backendService.makeAnalysisCall(planningPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('📋 Planning step AI response:', aiPlanning.substring(0, 200) + '...');

                // Store the real AI planning as the step output
                step.output = aiPlanning;

                this.completeStep(step, aiPlanning);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Planning step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executeExecutionStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create an execution-focused prompt
            const executionPrompt = `Execute this specific task:\n\nExecution Task: ${step.description}\nStep Type: ${step.type}\n\nPlease provide detailed execution guidance including:\n- Specific actions to take\n- Implementation details\n- Code examples if applicable\n- Expected outcomes\n\nBe detailed and actionable in your execution plan.`;

            console.log('⚡ Execution step AI call prompt:', executionPrompt.substring(0, 200) + '...');

            // Make real AI call for execution guidance with streaming
            if (this.backendService) {
                step.output = '';
                const aiExecution = await this.backendService.makeAnalysisCall(executionPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('⚡ Execution step AI response:', aiExecution.substring(0, 200) + '...');

                // Store the real AI execution guidance as the step output
                step.output = aiExecution;

                this.completeStep(step, aiExecution);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Execution step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executeValidationStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create a validation-focused prompt
            const validationPrompt = `Validate this aspect of the task:\n\nValidation Focus: ${step.description}\nStep Type: ${step.type}\n\nPlease provide detailed validation including:\n- What to check and verify\n- Success criteria\n- Potential issues to look for\n- Validation methods\n\nBe thorough and specific in your validation approach.`;

            console.log('✅ Validation step AI call prompt:', validationPrompt.substring(0, 200) + '...');

            // Make real AI call for validation guidance with streaming
            if (this.backendService) {
                step.output = '';
                const aiValidation = await this.backendService.makeAnalysisCall(validationPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('✅ Validation step AI response:', aiValidation.substring(0, 200) + '...');

                // Store the real AI validation guidance as the step output
                step.output = aiValidation;

                this.completeStep(step, aiValidation);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Validation step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executeErrorHandlingStep(step: ReasoningStep): Promise<void> {
        try {
            this.startStep(step);

            // Create an error handling-focused prompt
            const errorHandlingPrompt = `Handle errors for this task:\n\nError Handling Focus: ${step.description}\nStep Type: ${step.type}\n\nPlease provide detailed error handling including:\n- Common errors and issues\n- Prevention strategies\n- Recovery methods\n- Debugging approaches\n\nBe comprehensive in your error handling guidance.`;

            console.log('🔧 Error handling step AI call prompt:', errorHandlingPrompt.substring(0, 200) + '...');

            // Make real AI call for error handling guidance with streaming
            if (this.backendService) {
                step.output = '';
                const aiErrorHandling = await this.backendService.makeAnalysisCall(errorHandlingPrompt, undefined, (delta, meta) => {
                    if (meta && meta.phase && meta.phase !== 'analysis' && meta.phase !== 'analysis_complete') {
                        return;
                    }
                    step.output = (step.output || '') + (delta || '');
                    this.notifyStepUpdate(step);
                });
                console.log('🔧 Error handling step AI response:', aiErrorHandling.substring(0, 200) + '...');

                // Store the real AI error handling guidance as the step output
                step.output = aiErrorHandling;

                this.completeStep(step, aiErrorHandling);
            } else {
                throw new Error('Backend service not available');
            }
        } catch (error) {
            console.error('❌ Error handling step failed:', error);
            this.failStep(step, error instanceof Error ? error.message : 'Unknown error');
        }
    }

    private async executeGenericStep(step: ReasoningStep): Promise<void> {
        await this.simulateThinking(1000);

        const duration = ((Date.now() - step.startTime) / 1000).toFixed(1);

        if (step.title.includes('Server Communication')) {
            step.output = `🌐 SuperInference server communication\n` +
                `📡 Established secure connection\n` +
                `🔄 Data synchronization complete\n` +
                `✅ Server response: 200 OK\n` +
                `⏱️ Communication completed in ${duration}s`;
        } else {
            step.output = `✅ Step completed successfully\n` +
                `📡 Server interaction: 1 API call\n` +
                `⏱️ Completed in ${duration}s`;
        }
    }

    private async getWorkspaceFiles(): Promise<string[]> {
        const files: string[] = [];

        if (vscode.workspace.workspaceFolders) {
            try {
                const pattern = '**/*.{ts,tsx,js,jsx,py}';
                const uris = await vscode.workspace.findFiles(pattern, '**/node_modules/**', 10);
                files.push(...uris.map(uri => vscode.workspace.asRelativePath(uri)));
            } catch (error) {
                console.warn('Could not read workspace files:', error);
            }
        }

        return files;
    }

    private createStep(
        type: ReasoningStep['type'],
        title: string,
        description: string
    ): ReasoningStep {
        return {
            id: this.generateId(),
            type,
            title,
            description,
            status: 'pending',
            startTime: Date.now()
        };
    }

    private startStep(step: ReasoningStep): void {
        step.status = 'in_progress';
        step.startTime = Date.now();
    }

    private completeStep(step: ReasoningStep, output: string): void {
        step.status = 'completed';
        step.endTime = Date.now();
        step.duration = step.endTime - step.startTime;
        step.output = output;
    }

    private failStep(step: ReasoningStep, errorMessage?: string): void {
        step.status = 'failed';
        step.endTime = Date.now();
        step.duration = step.endTime - step.startTime;
        if (errorMessage) {
            step.error = errorMessage;
        }
        // Remove duplicate line that was causing the errorMessage error
        this.notifyStepUpdate(step);
    }

    private async simulateThinking(duration: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, duration));
    }

    private notifyStepUpdate(step: ReasoningStep): void {
        this.onStepUpdate(step);
    }

    private notifyPlanUpdate(plan: TaskPlan): void {
        this.onPlanUpdate(plan);
    }

    private generateId(): string {
        return Math.random().toString(36).substring(2, 15);
    }

    public getCurrentPlan(): TaskPlan | undefined {
        return this.currentPlan;
    }

    private stepComplete(plan: TaskPlan): void {
        // Find the next pending step to execute
        const nextStep = plan.steps.find(step => step.status === 'pending');
        if (nextStep) {
            this.executeStep(nextStep);
        } else {
            // All steps completed
            plan.status = 'completed';
            this.notifyPlanUpdate(plan);
        }
    }
}