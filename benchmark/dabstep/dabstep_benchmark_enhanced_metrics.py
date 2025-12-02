#!/usr/bin/env python3
# Copyright (C) 2025 SuperInference contributors
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see https://www.gnu.org/licenses/.

"""
Enhanced Metrics Collection for SuperInference Benchmark
Comprehensive tracking of all theoretical and practical metrics from the paper.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
import numpy as np
import math
import time

@dataclass
class GenerationConfig:
    """Model generation configuration used for this specific prediction."""
    temperature: float = 0.7
    max_tokens: int = 32768
    top_p: float = 0.8
    top_k: int = 40
    provider: str = "gemini"  # gemini, openai, vllm, deepseek
    model_name: str = "gemini-2.5-flash"
    critic_provider: str = ""
    critic_model: str = ""
    critic_threshold: float = 0.6

@dataclass
class InformationTheoreticMetrics:
    """Information-theoretic metrics from the SuperInference paper."""
    # Entropy measures
    initial_entropy: float = 0.0  # H(b_0) - initial belief entropy
    final_entropy: float = 0.0    # H(b_T) - final belief entropy
    entropy_reduction: float = 0.0  # H(b_0) - H(b_T)
    
    # Expected Information Gain
    avg_eig_per_step: float = 0.0  # Average EIG across steps
    total_eig: float = 0.0  # Sum of EIG across all steps
    eig_efficiency: float = 0.0  # Total EIG / time
    
    # Mutual Information
    mutual_information: float = 0.0  # I(G;M̃) - between goal and memory
    context_relevance_score: float = 0.0  # p(η) - retrieval quality
    
    # POMDP metrics
    belief_convergence_rate: float = 0.0  # How fast beliefs converge
    final_belief_probability: float = 0.0  # Final p(success)
    tau_threshold_met: bool = False  # Did we reach confidence threshold τ?
    
    # Stopping condition metrics
    stopped_by_confidence: bool = False  # Stopped by κ threshold
    stopped_by_eig: bool = False  # Stopped by ε threshold
    stopped_by_budget: bool = False  # Stopped by max iterations

@dataclass
class CalibrationMetrics:
    """Calibration and confidence metrics."""
    confidence_score: float = 0.0  # Predicted confidence
    brier_score: float = 0.0  # (confidence - actual)^2
    calibration_error: float = 0.0  # |confidence - accuracy|
    overconfidence_amount: float = 0.0  # How much overconfident (if any)
    underconfidence_amount: float = 0.0  # How much underconfident (if any)

@dataclass
class PlanMetrics:
    """Metrics about the planning process."""
    total_steps_planned: int = 0
    total_steps_completed: int = 0
    total_steps_failed: int = 0
    avg_step_success_probability: float = 0.0
    step_success_probabilities: List[float] = field(default_factory=list)
    
    # Step-level details
    step_titles: List[str] = field(default_factory=list)
    step_tools_used: List[List[str]] = field(default_factory=list)
    step_execution_times: List[float] = field(default_factory=list)
    step_critic_scores: List[float] = field(default_factory=list)
    
    # Dependency analysis
    total_dependencies: int = 0
    avg_dependencies_per_step: float = 0.0
    max_dependency_depth: int = 0

@dataclass
class ExecutionMetrics:
    """Code execution and computational metrics."""
    code_length_chars: int = 0
    code_length_lines: int = 0
    code_execution_time: float = 0.0
    
    # Retry and error metrics
    total_retries: int = 0
    total_execution_errors: int = 0
    error_types: List[str] = field(default_factory=list)
    
    # Resource usage
    events_fired: int = 0
    api_calls_made: int = 0
    tokens_used_estimate: int = 0  # Estimated from char count
    
    # Parallel execution (if enabled)
    parallel_steps_executed: int = 0
    sequential_steps_executed: int = 0

@dataclass
class CriticMetrics:
    """Critic evaluation metrics."""
    total_evaluations: int = 0
    total_approvals: int = 0
    total_rejections: int = 0
    approval_rate: float = 0.0
    avg_critic_score: float = 0.0
    critic_scores: List[float] = field(default_factory=list)
    
    # Precision and error rates
    critic_precision: float = 0.0  # P(correct|approve)
    false_positive_rate: float = 0.0  # α (approve wrong)
    false_negative_rate: float = 0.0  # β (reject correct)

@dataclass
class MemoryMetrics:
    """Memory and context metrics."""
    total_artifacts_stored: int = 0
    total_embeddings_created: int = 0
    total_context_retrievals: int = 0
    avg_retrieval_similarity: float = 0.0
    context_utilization_score: float = 0.0

@dataclass
class PerformanceMetrics:
    """Overall performance and efficiency metrics."""
    total_time: float = 0.0
    planning_time: float = 0.0
    execution_time: float = 0.0
    evaluation_time: float = 0.0
    
    # Efficiency ratios
    time_per_step: float = 0.0
    accuracy_per_second: float = 0.0  # Accuracy / time
    eig_per_second: float = 0.0  # Information gain / time
    
    # Iteration efficiency
    geometric_success_rate: float = 0.0  # 1 - (1-p)^N
    predicted_iterations_needed: int = 0  # From N ≥ ln(1-τ)/ln(1-p)
    actual_iterations_used: int = 0

@dataclass
class EnhancedDABStepResult:
    """Enhanced result with comprehensive metrics from SuperInference paper."""
    # Core result fields (original)
    task_id: str
    question: str
    level: str
    correct_answer: str
    predicted_answer: str
    correct: bool
    confidence: float
    response_time: float
    method: str
    reasoning_trace: str
    error_message: str = ""
    
    # Original enhanced metrics
    solution_quality: float = 0.0
    context_relevance: float = 0.0
    solution_length: int = 0
    reasoning_depth: float = 0.0
    code_quality: float = 0.0
    domain_expertise: float = 0.0
    
    # SUPER-INFERENCE metrics (original)
    supinf_rounds: int = 0
    supinf_verifier_calls: int = 0
    supinf_backtracks: int = 0
    supinf_mode_enabled: bool = False
    
    # NEW: Comprehensive metrics
    generation_config: GenerationConfig = field(default_factory=GenerationConfig)
    information_theory: InformationTheoreticMetrics = field(default_factory=InformationTheoreticMetrics)
    calibration: CalibrationMetrics = field(default_factory=CalibrationMetrics)
    plan_metrics: PlanMetrics = field(default_factory=PlanMetrics)
    execution: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    critic: CriticMetrics = field(default_factory=CriticMetrics)
    memory: MemoryMetrics = field(default_factory=MemoryMetrics)
    performance: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with nested metric structures."""
        result = asdict(self)
        return result

class MetricsCalculator:
    """Calculate enhanced metrics from response data."""
    
    @staticmethod
    def calculate_entropy(probability: float) -> float:
        """Calculate Shannon entropy H(p) = -p*log2(p) - (1-p)*log2(1-p)"""
        p = max(1e-6, min(1 - 1e-6, probability))  # Clamp to avoid log(0)
        return -(p * math.log2(p) + (1 - p) * math.log2(1 - p))
    
    @staticmethod
    def calculate_eig(current_p: float, accept_p: float = 0.95) -> float:
        """
        Calculate Expected Information Gain.
        EIG = H(p) - [p*H(accept_p) + (1-p)*H(p)]
        """
        h_p = MetricsCalculator.calculate_entropy(current_p)
        h_accept = MetricsCalculator.calculate_entropy(accept_p)
        return h_p - (current_p * h_accept + (1 - current_p) * h_p)
    
    @staticmethod
    def calculate_mutual_information(context_relevance: float, reasoning_depth: float) -> float:
        """
        Approximate I(G;M̃) from context relevance and reasoning depth.
        Higher values indicate better information flow.
        """
        return context_relevance * reasoning_depth
    
    @staticmethod
    def calculate_brier_score(predicted_confidence: float, actual_correct: bool) -> float:
        """
        Calculate Brier score: (predicted_probability - actual)^2
        Lower is better (0 = perfect calibration)
        """
        actual = 1.0 if actual_correct else 0.0
        return (predicted_confidence - actual) ** 2
    
    @staticmethod
    def calculate_geometric_success_rate(p: float, n: int) -> float:
        """
        Calculate success probability after N iterations.
        P(success in N) = 1 - (1-p)^N
        """
        if p <= 0 or p >= 1:
            return p
        return 1 - (1 - p) ** n
    
    @staticmethod
    def calculate_required_iterations(tau: float, p: float) -> int:
        """
        Calculate required iterations for target confidence.
        N ≥ ln(1-τ) / ln(1-p)
        """
        if p <= 0 or p >= 1 or tau <= 0 or tau >= 1:
            return 0
        return int(math.ceil(math.log(1 - tau) / math.log(1 - p)))
    
    @staticmethod
    def extract_generation_config(response_data: Dict[str, Any], provider_config: Dict[str, Any]) -> GenerationConfig:
        """Extract generation configuration from response metadata."""
        # Try model_name first, fallback to model
        model_name = provider_config.get('model_name', provider_config.get('model', 'unknown'))
        
        return GenerationConfig(
            temperature=provider_config.get('temperature', 0.7),
            max_tokens=provider_config.get('max_tokens', 32768),
            top_p=provider_config.get('top_p', 0.8),
            top_k=provider_config.get('top_k', 40),
            provider=provider_config.get('provider', 'unknown'),
            model_name=model_name,
            critic_provider=provider_config.get('critic_provider', ''),
            critic_model=provider_config.get('critic_model', ''),
            critic_threshold=provider_config.get('critic_threshold', 0.6)
        )
    
    @staticmethod
    def extract_plan_metrics(response_data: Dict[str, Any]) -> PlanMetrics:
        """Extract planning metrics from response with enhanced step-level data."""
        plan_steps = response_data.get('plan_steps', [])
        
        # Initialize
        metrics = PlanMetrics()
        metrics.total_steps_planned = len(plan_steps)
        
        # If we have detailed step information (NEW format with enhanced metrics)
        if response_data.get('steps'):
            steps = response_data['steps']
            for step in steps:
                if isinstance(step, dict):
                    # Status
                    status = step.get('status', 'unknown')
                    if status == 'completed':
                        metrics.total_steps_completed += 1
                    elif status == 'failed':
                        metrics.total_steps_failed += 1
                    
                    # Success probability (try enhanced field first, fallback to original)
                    prob = step.get('belief_after_critic', step.get('successProbability', 0.0))
                    if prob > 0:
                        metrics.step_success_probabilities.append(prob)
                    
                    # Step details
                    metrics.step_titles.append(step.get('title', ''))
                    
                    # Tools (prefer tools_actually_used over planned tools)
                    tools_used = step.get('tools_actually_used', step.get('tools', []))
                    metrics.step_tools_used.append(tools_used)
                    
                    # ENHANCED: Execution times
                    exec_time = step.get('execution_time', 0.0)
                    if exec_time > 0:
                        metrics.step_execution_times.append(exec_time)
                    
                    # ENHANCED: Critic scores
                    critic_score = step.get('critic_score', 0.0)
                    if critic_score > 0:
                        metrics.step_critic_scores.append(critic_score)
                    
                    # Dependencies
                    deps = step.get('dependencies', [])
                    metrics.total_dependencies += len(deps)
        
        # Calculate averages
        if metrics.step_success_probabilities:
            metrics.avg_step_success_probability = np.mean(metrics.step_success_probabilities)
        
        if metrics.total_steps_planned > 0:
            metrics.avg_dependencies_per_step = metrics.total_dependencies / metrics.total_steps_planned
        
        return metrics
    
    @staticmethod
    def extract_execution_metrics(response_data: Dict[str, Any]) -> ExecutionMetrics:
        """Extract execution metrics from response with enhanced data."""
        metrics = ExecutionMetrics()
        
        # Code metrics
        code = response_data.get('generated_code', '')
        if code:
            metrics.code_length_chars = len(code)
            metrics.code_length_lines = code.count('\n') + 1
        
        # Execution time (prefer phase timing if available)
        phase_timings = response_data.get('phase_timings', {})
        if phase_timings:
            # Use actual execution phase timing
            metrics.code_execution_time = phase_timings.get('iteration_time', 0.0)
        else:
            metrics.code_execution_time = response_data.get('execution_time', 0.0)
        
        # Events and rounds
        metrics.events_fired = response_data.get('events_fired', 0) or response_data.get('rounds', 0)
        
        # Estimate tokens (rough: 4 chars per token)
        metrics.tokens_used_estimate = metrics.code_length_chars // 4
        
        # Retry tracking (from router decisions)
        router_decisions = response_data.get('router_decisions', [])
        metrics.total_retries = len([d for d in router_decisions if d.startswith('fix_')])
        
        # Error tracking
        if response_data.get('execution_error'):
            metrics.total_execution_errors = 1
            error_msg = response_data['execution_error']
            # Extract error type
            if 'Error:' in error_msg:
                error_type = error_msg.split('Error:')[0].strip() + 'Error'
                metrics.error_types.append(error_type)
        
        # Parallel vs sequential (from steps if available)
        if response_data.get('steps'):
            # All steps are sequential in current SUPER-INFERENCE implementation
            metrics.sequential_steps_executed = len(response_data['steps'])
        
        return metrics
    
    @staticmethod
    def extract_critic_metrics(response_data: Dict[str, Any]) -> CriticMetrics:
        """Extract critic evaluation metrics with enhanced step-level data."""
        metrics = CriticMetrics()
        
        # SUPER-INFERENCE verifier calls
        metrics.total_evaluations = response_data.get('verifier_calls', 0)
        
        # Router decisions indicate rejections
        router_decisions = response_data.get('router_decisions', [])
        metrics.total_rejections = len([d for d in router_decisions if d.startswith('fix_')])
        metrics.total_approvals = metrics.total_evaluations - metrics.total_rejections
        
        if metrics.total_evaluations > 0:
            metrics.approval_rate = metrics.total_approvals / metrics.total_evaluations
        
        # ENHANCED: Extract critic scores from steps if available
        if response_data.get('steps'):
            for step in response_data['steps']:
                if isinstance(step, dict):
                    critic_score = step.get('critic_score', 0.0)
                    if critic_score > 0:
                        metrics.critic_scores.append(critic_score)
        
        # Calculate average critic score
        if metrics.critic_scores:
            metrics.avg_critic_score = np.mean(metrics.critic_scores)
        
        return metrics
    
    @staticmethod
    def calculate_information_theoretic_metrics(
        confidence: float,
        response_data: Dict[str, Any],
        context_relevance: float,
        reasoning_depth: float
    ) -> InformationTheoreticMetrics:
        """Calculate all information-theoretic metrics from MCP server data."""
        metrics = InformationTheoreticMetrics()
        
        # CRITICAL FIX: Use actual values from MCP server, not recalculate!
        # The server already computed these correctly
        info_theory = response_data.get('information_theory', {})
        
        if info_theory:
            # Use actual measured values from server
            metrics.initial_entropy = info_theory.get('initial_entropy_bits', 0.0)
            metrics.final_entropy = info_theory.get('final_entropy_bits', 0.0)
            metrics.entropy_reduction = info_theory.get('entropy_reduction_bits', 0.0)
            metrics.avg_eig_per_step = info_theory.get('avg_eig_per_event_bits', 0.0)
            metrics.total_eig = info_theory.get('total_eig_bits', 0.0)
            metrics.final_belief_probability = info_theory.get('final_belief', confidence)
        else:
            # Fallback: calculate from confidence if server data not available
            metrics.initial_entropy = MetricsCalculator.calculate_entropy(0.5)
            metrics.final_entropy = MetricsCalculator.calculate_entropy(confidence)
            metrics.entropy_reduction = metrics.initial_entropy - metrics.final_entropy
            
            # Calculate EIG from progression
            rounds = response_data.get('rounds', 1)
            if rounds > 0:
                steps = max(1, rounds)
                p_values = np.linspace(0.5, confidence, steps)
                eig_values = [MetricsCalculator.calculate_eig(p) for p in p_values]
                metrics.avg_eig_per_step = np.mean(eig_values) if eig_values else 0.0
                metrics.total_eig = sum(eig_values)
        
        # EIG efficiency (information gain per second)
        exec_time = response_data.get('execution_time', 1.0)
        if exec_time > 0:
            metrics.eig_efficiency = metrics.total_eig / exec_time
        
        # Mutual Information
        metrics.mutual_information = MetricsCalculator.calculate_mutual_information(
            context_relevance, reasoning_depth
        )
        metrics.context_relevance_score = context_relevance
        
        # Belief metrics
        metrics.final_belief_probability = confidence
        rounds = response_data.get('rounds', 1)
        if rounds > 1:
            # Convergence rate: how much probability increased per round
            metrics.belief_convergence_rate = (confidence - 0.5) / rounds
        
        # Stopping condition analysis
        metrics.tau_threshold_met = confidence >= 0.85  # κ threshold from paper
        
        # Check why we stopped (from response data)
        if confidence >= 0.85:
            metrics.stopped_by_confidence = True
        elif metrics.avg_eig_per_step < 0.015:  # ε threshold
            metrics.stopped_by_eig = True
        elif rounds >= response_data.get('max_rounds', 20):
            metrics.stopped_by_budget = True
        
        return metrics
    
    @staticmethod
    def calculate_calibration_metrics(
        confidence: float,
        is_correct: bool
    ) -> CalibrationMetrics:
        """Calculate calibration metrics."""
        metrics = CalibrationMetrics()
        
        metrics.confidence_score = confidence
        
        # Brier score
        metrics.brier_score = MetricsCalculator.calculate_brier_score(confidence, is_correct)
        
        # Calibration error
        actual = 1.0 if is_correct else 0.0
        metrics.calibration_error = abs(confidence - actual)
        
        # Over/under confidence
        if confidence > actual:
            metrics.overconfidence_amount = confidence - actual
        else:
            metrics.underconfidence_amount = actual - confidence
        
        return metrics
    
    @staticmethod
    def calculate_performance_metrics(
        response_time: float,
        rounds: int,
        confidence: float,
        is_correct: bool,
        total_eig: float,
        response_data: Dict[str, Any] = None
    ) -> PerformanceMetrics:
        """Calculate overall performance metrics with phase timing breakdown."""
        metrics = PerformanceMetrics()
        
        metrics.total_time = response_time
        metrics.actual_iterations_used = rounds
        
        # ENHANCED: Use phase timings if available
        if response_data and response_data.get('phase_timings'):
            phase_timings = response_data['phase_timings']
            metrics.planning_time = phase_timings.get('analysis_time', 0.0) + phase_timings.get('planning_time', 0.0)
            metrics.execution_time = phase_timings.get('iteration_time', 0.0)
            metrics.evaluation_time = phase_timings.get('finalization_time', 0.0)
        
        # Time per step
        if rounds > 0:
            metrics.time_per_step = response_time / rounds
        
        # Efficiency ratios
        if response_time > 0:
            metrics.accuracy_per_second = (1.0 if is_correct else 0.0) / response_time
            metrics.eig_per_second = total_eig / response_time
        
        # Geometric success rate
        if rounds > 0:
            # Estimate base probability from final confidence
            base_p = confidence / rounds if rounds > 1 else confidence
            metrics.geometric_success_rate = MetricsCalculator.calculate_geometric_success_rate(
                base_p, rounds
            )
        
        # Predicted iterations needed
        if confidence > 0 and confidence < 1:
            tau_target = 0.85  # Target confidence from paper
            base_p = max(0.1, min(0.9, confidence / max(rounds, 1)))
            metrics.predicted_iterations_needed = MetricsCalculator.calculate_required_iterations(
                tau_target, base_p
            )
        
        return metrics

def create_enhanced_result(
    basic_result: 'DABStepResult',  # Original result
    response_data: Dict[str, Any],  # Full response from MCP
    provider_config: Dict[str, Any]  # Generation config
) -> EnhancedDABStepResult:
    """
    Create enhanced result with all metrics from basic result and response data.
    
    Args:
        basic_result: Original DABStepResult
        response_data: Full MCP response with nested data
        provider_config: Provider and generation configuration
    
    Returns:
        EnhancedDABStepResult with comprehensive metrics
    """
    # Calculate all metric groups
    gen_config = MetricsCalculator.extract_generation_config(response_data, provider_config)
    
    # CRITICAL FIX: Use MCP server metrics if available (don't recalculate!)
    mcp_info_theory = response_data.get('information_theory', {})
    stopping_info = response_data.get('stopping_analysis', {})  # ✅ DEFINE stopping_info!
    
    if mcp_info_theory and mcp_info_theory.get('initial_entropy_bits', 0) > 0:
        # Calculate mutual information I(G;M̃) from paper
        # Approximation: I(G;M) ≈ context_relevance × reasoning_depth
        # This measures how much information the retrieved context M provides about goal G
        mutual_info = basic_result.context_relevance * basic_result.reasoning_depth
        
        # Calculate belief convergence rate from trajectory
        # Rate = (final_belief - initial_belief) / events_fired
        belief_trajectory = mcp_info_theory.get('belief_trajectory', [])
        if len(belief_trajectory) >= 2:
            initial_belief = belief_trajectory[0]
            final_belief = belief_trajectory[-1]
            events = mcp_info_theory.get('events_fired', 1)
            convergence_rate = (final_belief - initial_belief) / max(events, 1)
        else:
            convergence_rate = 0.0
        
        # Use MCP-calculated values directly
        info_theory = InformationTheoreticMetrics(
            initial_entropy=mcp_info_theory.get('initial_entropy_bits', 0.0),
            final_entropy=mcp_info_theory.get('final_entropy_bits', 0.0),
            entropy_reduction=mcp_info_theory.get('entropy_reduction_bits', 0.0),
            avg_eig_per_step=mcp_info_theory.get('avg_eig_per_event_bits', 0.0),
            total_eig=mcp_info_theory.get('total_eig_bits', 0.0),
            mutual_information=mutual_info,  # ✅ CALCULATED from context × reasoning
            context_relevance_score=basic_result.context_relevance,
            belief_convergence_rate=convergence_rate,  # ✅ CALCULATED from trajectory
            final_belief_probability=mcp_info_theory.get('final_belief', 0.5),
            tau_threshold_met=mcp_info_theory.get('final_belief', 0.5) >= 0.85,
            # Parse stopping reason from stopped_due_to string
            # "plan_sufficient_agreement" maps to stopped_by_confidence (high confidence in plan)
            stopped_by_confidence=(stopping_info.get('stopped_due_to', '') in [
                'belief_convergence', 'belief_threshold', 'plan_sufficient_agreement', 'plan_sufficient'
            ]),
            stopped_by_eig=(stopping_info.get('stopped_due_to', '') in [
                'eig_below_threshold', 'eig_threshold'
            ]),
            stopped_by_budget=(stopping_info.get('stopped_due_to', '') in [
                'max_events_reached', 'max_rounds', 'max_rounds_reached', 'repeated_errors', 'error_loop'
            ]),
            eig_efficiency=mcp_info_theory.get('total_eig_bits', 0.0) / max(basic_result.response_time, 1.0)
        )
    else:
        # Fallback: calculate from basic result fields
        info_theory = MetricsCalculator.calculate_information_theoretic_metrics(
            basic_result.confidence,
            response_data,
            basic_result.context_relevance,
            basic_result.reasoning_depth
        )
    
    calibration = MetricsCalculator.calculate_calibration_metrics(
        basic_result.confidence,
        basic_result.correct
    )
    
    plan_metrics = MetricsCalculator.extract_plan_metrics(response_data)
    execution = MetricsCalculator.extract_execution_metrics(response_data)
    critic = MetricsCalculator.extract_critic_metrics(response_data)
    
    # Memory metrics (would need to be passed from vector store)
    memory = MemoryMetrics()
    
    performance = MetricsCalculator.calculate_performance_metrics(
        basic_result.response_time,
        basic_result.supinf_rounds,
        basic_result.confidence,
        basic_result.correct,
        info_theory.total_eig,
        response_data  # Pass full response data for phase timings
    )
    
    # Create enhanced result
    return EnhancedDABStepResult(
        # Copy all original fields
        task_id=basic_result.task_id,
        question=basic_result.question,
        level=basic_result.level,
        correct_answer=basic_result.correct_answer,
        predicted_answer=basic_result.predicted_answer,
        correct=basic_result.correct,
        confidence=basic_result.confidence,
        response_time=basic_result.response_time,
        method=basic_result.method,
        reasoning_trace=basic_result.reasoning_trace,
        error_message=basic_result.error_message,
        solution_quality=basic_result.solution_quality,
        context_relevance=basic_result.context_relevance,
        solution_length=basic_result.solution_length,
        reasoning_depth=basic_result.reasoning_depth,
        code_quality=basic_result.code_quality,
        domain_expertise=basic_result.domain_expertise,
        supinf_rounds=basic_result.supinf_rounds,
        supinf_verifier_calls=basic_result.supinf_verifier_calls,
        supinf_backtracks=basic_result.supinf_backtracks,
        supinf_mode_enabled=basic_result.supinf_mode_enabled,
        # Add enhanced metrics
        generation_config=gen_config,
        information_theory=info_theory,
        calibration=calibration,
        plan_metrics=plan_metrics,
        execution=execution,
        critic=critic,
        memory=memory,
        performance=performance
    )

def calculate_aggregate_metrics(results: List[EnhancedDABStepResult]) -> Dict[str, Any]:
    """Calculate aggregate statistics across all results."""
    if not results:
        return {}
    
    return {
        "information_theory": {
            "avg_entropy_reduction": np.mean([r.information_theory.entropy_reduction for r in results]),
            "avg_eig_per_step": np.mean([r.information_theory.avg_eig_per_step for r in results]),
            "avg_mutual_information": np.mean([r.information_theory.mutual_information for r in results]),
            "avg_eig_efficiency": np.mean([r.information_theory.eig_efficiency for r in results]),
            "pct_stopped_by_confidence": np.mean([r.information_theory.stopped_by_confidence for r in results]) * 100,
            "pct_stopped_by_eig": np.mean([r.information_theory.stopped_by_eig for r in results]) * 100,
            "pct_stopped_by_budget": np.mean([r.information_theory.stopped_by_budget for r in results]) * 100,
        },
        "calibration": {
            "avg_brier_score": np.mean([r.calibration.brier_score for r in results]),
            "avg_calibration_error": np.mean([r.calibration.calibration_error for r in results]),
            "avg_overconfidence": np.mean([r.calibration.overconfidence_amount for r in results]),
            "avg_underconfidence": np.mean([r.calibration.underconfidence_amount for r in results]),
            "expected_calibration_error": _calculate_ece(results)
        },
        "planning": {
            "avg_steps_planned": np.mean([r.plan_metrics.total_steps_planned for r in results]),
            "avg_steps_completed": np.mean([r.plan_metrics.total_steps_completed for r in results]),
            "avg_step_success_prob": float(np.mean([r.plan_metrics.avg_step_success_probability for r in results if r.plan_metrics.avg_step_success_probability > 0])) if any(r.plan_metrics.avg_step_success_probability > 0 for r in results) else 0.0,
            "plan_completion_rate": np.mean([r.plan_metrics.total_steps_completed / max(r.plan_metrics.total_steps_planned, 1) for r in results])
        },
        "critic": {
            "avg_evaluations": np.mean([r.critic.total_evaluations for r in results]),
            "avg_approval_rate": np.mean([r.critic.approval_rate for r in results if r.critic.total_evaluations > 0]),
            "total_approvals": sum([r.critic.total_approvals for r in results]),
            "total_rejections": sum([r.critic.total_rejections for r in results])
        },
        "performance": {
            "avg_time_per_step": np.mean([r.performance.time_per_step for r in results if r.performance.time_per_step > 0]),
            "avg_accuracy_per_second": np.mean([r.performance.accuracy_per_second for r in results]),
            "avg_eig_per_second": np.mean([r.performance.eig_per_second for r in results]),
            "avg_geometric_success_rate": np.mean([r.performance.geometric_success_rate for r in results])
        },
        "execution": {
            "avg_code_length": np.mean([r.execution.code_length_chars for r in results if r.execution.code_length_chars > 0]),
            "avg_events_fired": np.mean([r.execution.events_fired for r in results]),
            "total_execution_errors": sum([r.execution.total_execution_errors for r in results]),
            "error_rate": np.mean([r.execution.total_execution_errors for r in results])
        },
        "token_usage": {
            "total_prompt_tokens": sum([getattr(r, 'token_usage', {}).get('total_prompt_tokens', 0) for r in results]),
            "total_output_tokens": sum([getattr(r, 'token_usage', {}).get('total_output_tokens', 0) for r in results]),
            "total_tokens": sum([getattr(r, 'token_usage', {}).get('total_tokens', 0) for r in results]),
            "avg_tokens_per_task": np.mean([getattr(r, 'token_usage', {}).get('total_tokens', 0) for r in results]),
            "avg_prompt_tokens_per_task": np.mean([getattr(r, 'token_usage', {}).get('total_prompt_tokens', 0) for r in results]),
            "avg_output_tokens_per_task": np.mean([getattr(r, 'token_usage', {}).get('total_output_tokens', 0) for r in results]),
            "estimated_cost_usd": sum([getattr(r, 'token_usage', {}).get('total_tokens', 0) for r in results]) / 1_000_000 * 3.5  # Gemini 2.5 Pro pricing
        }
    }

def _calculate_ece(results: List[EnhancedDABStepResult], num_bins: int = 10) -> float:
    """Calculate Expected Calibration Error (ECE)."""
    if not results:
        return 0.0
    
    # Bin by confidence
    bins = np.linspace(0, 1, num_bins + 1)
    ece = 0.0
    
    for i in range(num_bins):
        # Find results in this bin
        bin_results = [r for r in results if bins[i] <= r.confidence < bins[i + 1]]
        
        if not bin_results:
            continue
        
        # Calculate bin statistics
        bin_confidence = np.mean([r.confidence for r in bin_results])
        bin_accuracy = np.mean([1.0 if r.correct else 0.0 for r in bin_results])
        bin_weight = len(bin_results) / len(results)
        
        # Add to ECE
        ece += bin_weight * abs(bin_accuracy - bin_confidence)
    
    return ece

