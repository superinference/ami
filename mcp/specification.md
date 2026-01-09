# SuperInference Agent Spec

**Version:** 1.0.0  
**Status:** Draft Specification  
**Authors:** SuperInference Contributors  
**License:** GPL-3.0  
**Last Updated:** 2025-12-10

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Protocol Overview](#2-protocol-overview)
3. [Core Data Models](#3-core-data-models)
4. [Agent Architecture](#4-agent-architecture)
5. [Communication Protocol](#5-communication-protocol)
6. [Tool Metadata System](#6-tool-metadata-system)
7. [Belief & Information Theory](#7-belief--information-theory)
8. [Critic Protocol](#8-critic-protocol)
9. [Memory System](#9-memory-system)
10. [AI Provider Protocol](#10-ai-provider-protocol)
11. [MCP Transport Layer](#11-mcp-transport-layer)
12. [Result Schema](#12-result-schema)
13. [Error Handling](#13-error-handling)
14. [Sequence Diagrams](#14-sequence-diagrams)
15. [Configuration Reference](#15-configuration-reference)
16. [Security Considerations](#16-security-considerations)
17. [Appendix](#17-appendix)

---

## 1. Introduction

### 1.1 Purpose

This specification defines the **SuperInference Agent Interaction Protocol (SAIP)**, a comprehensive protocol for orchestrating multi-agent AI systems using event-driven planning with information-theoretic foundations.

### 1.2 Scope

SAIP covers:
- Agent-to-agent communication patterns
- Tool discovery and capability advertisement
- Belief tracking and information gain calculations
- Memory management with critic-gated updates
- Provider abstraction for multiple LLM backends
- Error handling and resilience patterns

### 1.3 Terminology

| Term | Definition |
|------|------------|
| **PRE Loop** | Planner-Retriever-Executor loop - the core reasoning cycle |
| **EIG** | Expected Information Gain - information-theoretic metric for step value |
| **Belief** | Probability estimate of successful task completion |
| **Critic** | Validation agent that approves/rejects outputs |
| **Memory M_t** | Vector store containing approved artifacts at time t |
| **Event** | A reasoning step that fires when EIG exceeds threshold |

### 1.4 Conformance

Implementations claiming SAIP conformance MUST implement all REQUIRED features and SHOULD implement OPTIONAL features as specified.

---

## 2. Protocol Overview

### 2.1 Architecture Summary

The SuperInference system implements a **PRE (Planner-Retriever-Executor) Loop** with:

- **Event-driven triggering**: Steps fire based on Expected Information Gain (EIG)
- **Critic-gated memory**: Only approved outputs enter shared memory
- **Multi-provider support**: Abstraction layer for different LLM backends
- **MCP transport**: Model Context Protocol for tool exposure

### 2.2 Design Principles

1. **Information-theoretic foundation**: All decisions grounded in entropy and EIG
2. **Compositional agents**: Specialized agents with clear responsibilities
3. **Resilient execution**: Circuit breakers, retries, and graceful degradation
4. **Observable**: Comprehensive metrics and logging throughout

### 2.3 System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SUPERINFERENCE MCP SERVER                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         MCP TRANSPORT LAYER                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │    STDIO    │  │    HTTP     │  │  Resources  │  │    Tools    │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         AGENT ORCHESTRATION                           │   │
│  │                                                                       │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │   │
│  │  │ Planner │ │Retriever│ │Executor │ │ Critic  │ │Finalizer│        │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘        │   │
│  │       │           │           │           │           │              │   │
│  │       └───────────┴─────┬─────┴───────────┴───────────┘              │   │
│  │                         │                                             │   │
│  │                    PRE LOOP                                           │   │
│  │               (Event-Driven)                                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐     │
│  │   VECTOR STORE     │  │   AI PROVIDERS     │  │   PERFORMANCE      │     │
│  │   (Memory M_t)     │  │   (LLM Backends)   │  │   MONITORING       │     │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Data Models

### 3.1 Position and Selection

Used for tracking cursor and selection state in source code.

```python
class Position:
    """A position in a text document."""
    line: int           # REQUIRED. Zero-indexed line number
    column: int = 0     # OPTIONAL. Zero-indexed column number
```

```python
class Selection:
    """A range in a text document."""
    startPosition: Position  # REQUIRED. Start of selection
    endPosition: Position    # REQUIRED. End of selection
```

### 3.2 File Context

```python
class CurrentFile:
    """Current file being edited."""
    contents: str                    # REQUIRED. Full file contents
    languageId: str                  # REQUIRED. Language identifier (e.g., "python")
    relativeWorkspacePath: str       # REQUIRED. Path relative to workspace root
    selection: Selection             # REQUIRED. Current selection
    cursorPosition: Position         # REQUIRED. Current cursor position
```

### 3.3 Conversation Messages

```python
class CodeChunk:
    """A chunk of code attached to a message."""
    relativeWorkspacePath: str  # REQUIRED. File path
    startLineNumber: int        # REQUIRED. Starting line (1-indexed)
    lines: List[str]            # REQUIRED. Lines of code

class ConversationMessage:
    """A message in a conversation."""
    type: str                              # REQUIRED. "MESSAGE_TYPE_HUMAN" | "MESSAGE_TYPE_AI"
    text: str                              # REQUIRED. Message text content
    attachedCodeChunks: List[CodeChunk]    # OPTIONAL. Code attachments
```

### 3.4 Planning Configuration

```python
class PlanningConfig:
    """Configuration for event-driven planning thresholds and budgets."""
    
    # Event triggering thresholds
    tau_event_threshold: float = 0.01    # REQUIRED. Minimum EIG to fire event
    kappa_confidence_stop: float = 0.90  # REQUIRED. Belief threshold for stopping
    epsilon_min_eig: float = 0.015       # REQUIRED. Minimum EIG for continuing
    
    # Resource limits
    max_events: int = 20                 # REQUIRED. Maximum reasoning events
    max_steps: int = 30                  # REQUIRED. Maximum plan steps
    
    # Critic configuration
    critic_accept_threshold: float = 0.85  # REQUIRED. Approval threshold [0,1]
    critic_provider: str = None            # OPTIONAL. Provider name for critic
    critic_model_override: str = None      # OPTIONAL. Specific model for critic
    
    # Parallel execution
    enable_parallel_execution: bool = True  # OPTIONAL. Enable parallel steps
    max_parallel_steps: int = 3             # OPTIONAL. Max concurrent steps
```

### 3.5 Plan Step

```python
class PlanStep:
    """Individual step in a reasoning plan."""
    
    # Identity
    id: str                      # REQUIRED. Unique step identifier
    title: str                   # REQUIRED. Short step title (max 300 chars)
    description: str             # REQUIRED. Step description (max 2000 chars)
    
    # State
    status: str = "pending"      # REQUIRED. "pending" | "completed" | "failed"
    successProbability: float = 0.5  # REQUIRED. Belief estimate [0,1]
    
    # Dependencies
    dependencies: List[str] = [] # OPTIONAL. IDs of prerequisite steps
    tools: List[str] = []        # OPTIONAL. MCP tools to use (max 10)
    
    # Results
    output: str = None           # OPTIONAL. Step output when completed
    error: str = None            # OPTIONAL. Error message if failed
    
    # Enhanced Metrics
    execution_time: float = 0.0           # Seconds to execute
    tools_actually_used: List[str] = []   # Tools actually invoked
    critic_score: float = 0.0             # Critic evaluation score
    critic_reasoning: str = ""            # Critic explanation
    belief_before_critic: float = 0.5     # Belief before validation
    belief_after_critic: float = 0.5      # Belief after validation
    eig_value: float = 0.0                # Expected Information Gain
```

### 3.6 Reasoning Plan

```python
class ReasoningPlan:
    """Complete reasoning plan with event tracking."""
    
    # Identity
    id: str                          # REQUIRED. Unique plan identifier
    instruction: str                 # REQUIRED. Original task instruction
    createdAt: float                 # REQUIRED. Unix timestamp
    
    # State
    status: str = "running"          # REQUIRED. "running" | "completed" | "failed"
    steps: List[PlanStep]            # REQUIRED. Plan steps
    eventsFired: int = 0             # REQUIRED. Number of events fired
    
    # Context
    context_files: List[Dict] = []   # OPTIONAL. Context file metadata
    current_file_content: str = ""   # OPTIONAL. Primary file content
    
    # Methods
    def pending_steps(self) -> List[PlanStep]:
        """Return steps with status='pending'."""
        
    def completed_steps(self) -> List[PlanStep]:
        """Return steps with status='completed'."""
        
    def has_unresolved(self) -> bool:
        """Return True if any step is pending."""
```

### 3.7 Embedding Entry

```python
class EnhancedEmbeddingEntry:
    """Vector store entry with semantic metadata."""
    
    # Core fields
    id: str                      # REQUIRED. Unique entry ID
    content: str                 # REQUIRED. Text content
    embedding: List[float]       # REQUIRED. Vector embedding
    metadata: Dict[str, Any]     # REQUIRED. Arbitrary metadata
    timestamp: float             # REQUIRED. Creation timestamp
    
    # Semantic classification
    chunk_type: str = "general"  # "function" | "class" | "docstring" | "file" | "general"
    
    # Code structure (optional)
    function_name: str = None    # Function name if chunk_type="function"
    class_name: str = None       # Class name if chunk_type="class"
    file_path: str = None        # Source file path
    start_line: int = None       # Starting line number
    end_line: int = None         # Ending line number
```

---

## 4. Agent Architecture

### 4.1 Agent Types

The protocol defines the following agent roles:

| Agent | Responsibility | Model Size | Input | Output |
|-------|---------------|------------|-------|--------|
| **Planner** | Task decomposition into steps | Large | instruction, context | List[PlanStep] |
| **Retriever** | Context retrieval from memory | Embedding | query | List[EmbeddingEntry] |
| **Executor** | Step execution using tools | Large | step, context | candidate_output |
| **Critic** | Output validation | Small/Fast | instruction, step, candidate | {approve, score, reason} |
| **Analyzer** | Data file analysis | Large | files | file_descriptions |
| **Verifier** | Plan sufficiency check | Large | plan, execution | "sufficient" \| "insufficient" |
| **Router** | Next action decision | Large | state, error | "continue" \| "backtrack" \| "add_step" |
| **Coder** | Code generation | Large | plan, context | executable_code |
| **Finalizer** | Answer extraction | Large | execution_result | final_answer |
| **Debugger** | Error recovery | Large | error, context | fix_suggestion |

### 4.2 Agent Interface

All agents MUST implement:

```python
class Agent(ABC):
    """Abstract base for all agents."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Agent identifier."""
        
    @property
    @abstractmethod
    def role(self) -> str:
        """Agent role description."""
    
    @abstractmethod
    async def process(self, request: AgentRequest) -> AgentResponse:
        """Process an agent request and return response."""
```

### 4.3 Agent State Machine

```
                         ┌──────────────────────────────────────┐
                         │             IDLE                      │
                         └──────────────────┬───────────────────┘
                                            │ receive(request)
                                            ▼
                         ┌──────────────────────────────────────┐
                         │           PROCESSING                  │
                         │  ┌────────────────────────────────┐  │
                         │  │ 1. Validate request            │  │
                         │  │ 2. Build context               │  │
                         │  │ 3. Generate prompt             │  │
                         │  │ 4. Call LLM                    │  │
                         │  │ 5. Parse response              │  │
                         │  │ 6. Build result                │  │
                         │  └────────────────────────────────┘  │
                         └──────────────────┬───────────────────┘
                                            │
                         ┌──────────────────┴───────────────────┐
                         │                                       │
                         ▼                                       ▼
          ┌──────────────────────────┐          ┌──────────────────────────┐
          │         SUCCESS           │          │          ERROR           │
          │  emit(AgentResponse)      │          │  emit(AgentError)        │
          └────────────┬─────────────┘          └────────────┬─────────────┘
                       │                                      │
                       └──────────────────┬───────────────────┘
                                          │
                                          ▼
                         ┌──────────────────────────────────────┐
                         │             IDLE                      │
                         └──────────────────────────────────────┘
```

### 4.4 PRE Loop Implementation

The PRE (Planner-Retriever-Executor) loop is the core reasoning cycle:

```python
class EventDrivenPlanner:
    """
    Implements PRE loop with event-based triggering and critic-gated memory.
    
    Algorithm:
    1. Generate initial plan from instruction
    2. While has_unresolved() AND should_fire_event():
       a. Select next step(s) by EIG
       b. Retrieve relevant context from M_t
       c. Execute step using specified tools
       d. Evaluate output with Critic
       e. If approved: add to M_t, update belief
       f. If rejected: retry or mark failed
    3. Return approved artifacts
    """
    
    def __init__(
        self,
        smart_ctx: SmartContextManager,
        vec_store: EnhancedVectorStore,
        provider: AIProvider,
        config: PlanningConfig
    ):
        self.smart_ctx = smart_ctx
        self.vec_store = vec_store
        self.provider = provider
        self.config = config
        self.critic = Critic(provider, config.critic_accept_threshold)
    
    async def generate_plan(
        self,
        instruction: str,
        current_file_content: str = None,
        max_steps: int = None,
        context_files: List[Dict] = None
    ) -> ReasoningPlan:
        """Generate a reasoning plan from instruction."""
        
    def select_next_step(
        self,
        plan: ReasoningPlan
    ) -> Tuple[PlanStep, float]:
        """Select highest-EIG pending step."""
        
    def should_fire_event(self, plan: ReasoningPlan) -> bool:
        """Check if next event should fire based on EIG threshold."""
        
    async def execute_step(
        self,
        plan: ReasoningPlan,
        step: PlanStep,
        language_id: str = "python",
        workspace_path: str = ""
    ) -> PlanStep:
        """Execute a plan step and update with results."""
```

---

## 5. Communication Protocol

### 5.1 Message Types

```python
class MessageType(Enum):
    """Types of inter-agent messages."""
    REQUEST = "request"      # Agent request
    RESPONSE = "response"    # Agent response
    EVENT = "event"          # Async event notification
    ERROR = "error"          # Error notification
```

### 5.2 Agent Request

```python
class AgentRequest:
    """Request from one agent to another."""
    
    # Routing
    message_id: str          # REQUIRED. Unique message ID
    sender: str              # REQUIRED. Sender agent name
    receiver: str            # REQUIRED. Target agent name
    message_type: str = "request"
    
    # Payload
    instruction: str         # REQUIRED. Task instruction
    context: Dict[str, Any]  # REQUIRED. Execution context
    
    # Metadata
    priority: int = 0        # OPTIONAL. 0=normal, higher=urgent
    timeout: float = 180.0   # OPTIONAL. Timeout in seconds
    correlation_id: str = None  # OPTIONAL. Links related messages
    timestamp: float         # REQUIRED. Unix timestamp
```

### 5.3 Agent Response

```python
class AgentResponse:
    """Response from agent."""
    
    # Routing
    message_id: str          # REQUIRED. Unique message ID
    sender: str              # REQUIRED. Responding agent
    receiver: str            # REQUIRED. Original requester
    message_type: str = "response"
    correlation_id: str      # REQUIRED. Original request ID
    
    # Result
    success: bool            # REQUIRED. Whether request succeeded
    result: Any              # REQUIRED. Result payload (type varies)
    error: str = None        # OPTIONAL. Error message if failed
    
    # Metrics
    metrics: Dict[str, Any]  # OPTIONAL. Execution metrics
    timestamp: float         # REQUIRED. Unix timestamp
```

### 5.4 Event Types

```python
class AgentEventType(Enum):
    """Types of events emitted during processing."""
    
    # Step lifecycle
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"
    
    # Critic events
    CRITIC_APPROVED = "critic_approved"
    CRITIC_REJECTED = "critic_rejected"
    
    # Belief events
    BELIEF_UPDATED = "belief_updated"
    EIG_CALCULATED = "eig_calculated"
    
    # Memory events
    MEMORY_UPDATED = "memory_updated"
    
    # Tool events
    TOOL_INVOKED = "tool_invoked"
    TOOL_COMPLETED = "tool_completed"
    
    # Control events
    STOPPING_CONDITION = "stopping_condition"
```

```python
class AgentEvent:
    """Event emitted during processing."""
    
    event_type: AgentEventType  # REQUIRED. Type of event
    source_agent: str           # REQUIRED. Emitting agent
    data: Dict[str, Any]        # REQUIRED. Event-specific data
    timestamp: float            # REQUIRED. Unix timestamp
```

---

## 6. Tool Metadata System

### 6.1 Tool Registration

Tools MUST be registered with metadata for dynamic discovery:

```python
class ToolMetadata:
    """Registry for MCP tools with capability advertisement."""
    
    registry: Dict[str, Dict] = {}  # Global registry
    
    @staticmethod
    def register(
        name: str,                # REQUIRED. Tool name
        category: str,            # REQUIRED. Category for grouping
        description: str,         # REQUIRED. Human-readable description
        capabilities: List[str],  # REQUIRED. Capability tags
        input_params: List[str],  # REQUIRED. Parameter names
        output_type: str,         # REQUIRED. Output description
        use_cases: List[str],     # REQUIRED. Example use cases
        requires: List[str] = []  # OPTIONAL. Tool dependencies
    ) -> Callable:
        """Decorator to register tool metadata."""
```

### 6.2 Tool Categories

| Category | Description | Example Tools |
|----------|-------------|---------------|
| `execution` | Code execution and analysis | `execute_data_analysis`, `superinference_unified` |
| `exploration` | Data file exploration | `grep_data`, `read_data_file`, `shell_analyze` |
| `generation` | Code/content generation | `stream_generate`, `stream_edit` |
| `analysis` | Code analysis | `analyze_language_features`, `analyze_code_structure` |
| `planning` | Plan generation/execution | `plan_execute`, `generate_plan_steps` |
| `monitoring` | System health/metrics | `health_check`, `get_performance_metrics` |

### 6.3 Capability Tags

Standard capability tags:

```
# Execution capabilities
code_generation, safe_execution, result_extraction, csv_analysis

# Search capabilities
pattern_search, semantic_search, similarity_ranking

# Analysis capabilities
schema_extraction, language_detection, syntax_analysis

# Planning capabilities
plan_generation, step_decomposition, dependency_analysis

# Memory capabilities
embedding_creation, vector_storage, context_retrieval
```

### 6.4 Tool Catalog Format

```json
{
  "tools": [
    {
      "name": "string",
      "category": "string",
      "description": "string",
      "capabilities": ["string"],
      "input": ["string"],
      "output": "string",
      "use_cases": ["string"],
      "requires": ["string"]
    }
  ],
  "tool_categories": {
    "category_name": ["tool_name1", "tool_name2"]
  },
  "tool_dependencies": {
    "tool_name": ["required_tool1", "required_tool2"]
  },
  "total_tools": "integer",
  "categories": ["string"]
}
```

### 6.5 Tool Selection Algorithm

```python
def _select_appropriate_tools(
    self,
    step: PlanStep,
    instruction: str,
    language_id: str
) -> List[str]:
    """
    Select tools when AI planner doesn't specify.
    
    Priority:
    1. Use step.tools if specified by planner
    2. Fallback to keyword-based selection
    
    Selection rules:
    - SA (subanswer) steps: tools=[] (pure LLM reasoning)
    - Data exploration: grep_data, read_data_file, shell_analyze
    - Code analysis: analyze_language_features, analyze_code_structure
    - Code editing: stream_edit with language analysis
    - Code generation: stream_generate
    """
```

---

## 7. Belief & Information Theory

### 7.1 Entropy Calculation

Shannon entropy for binary belief:

```python
def entropy(prob: float) -> float:
    """
    Calculate Shannon entropy H(p) for binary random variable.
    
    H(p) = -p*log2(p) - (1-p)*log2(1-p)
    
    Args:
        prob: Probability of success [0, 1]
        
    Returns:
        Entropy in bits [0, 1]
    """
    p = max(1e-6, min(1 - 1e-6, prob))  # Avoid log(0)
    return -(p * math.log2(p) + (1 - p) * math.log2(1 - p))
```

### 7.2 Expected Information Gain (EIG)

```python
def expected_info_gain(current_p: float, accept_p: float = 0.95) -> float:
    """
    Calculate Expected Information Gain from executing next step.
    
    EIG ≈ H(p) - [p*H(accept_p) + (1-p)*H(p)]
    
    Args:
        current_p: Current belief probability
        accept_p: Target acceptance probability
        
    Returns:
        Expected information gain in bits
    """
    h_p = entropy(current_p)
    return h_p - (current_p * entropy(accept_p) + (1 - current_p) * entropy(current_p))
```

### 7.3 Belief State Tracking

```python
class BeliefState:
    """Tracks belief evolution through reasoning."""
    
    # Belief tracking
    initial_belief: float = 0.5
    current_belief: float
    belief_trajectory: List[float] = []
    
    # Entropy metrics
    initial_entropy: float
    current_entropy: float
    entropy_reduction: float
    
    # EIG tracking
    eig_history: List[float] = []
    total_eig: float
    avg_eig_per_event: float
    
    # Event counting
    events_fired: int = 0
```

### 7.4 Belief Update Rules

After critic evaluation:

```python
def update_belief(
    current_belief: float,
    critic_score: float,
    approved: bool
) -> float:
    """
    Update belief based on critic evaluation.
    
    If approved:
        new_belief = current_belief + (1 - current_belief) * critic_score * 0.1
    If rejected:
        new_belief = current_belief * 0.95  # Small decrease
        
    Returns:
        Updated belief clamped to [0.01, 0.99]
    """
```

### 7.5 Stopping Conditions

The PRE loop terminates when ANY of these conditions are met:

| Condition | Description | Check |
|-----------|-------------|-------|
| `plan_sufficient` | Verifier approves plan | verification == "sufficient" |
| `max_rounds` | Maximum events reached | events_fired >= max_events |
| `eig_threshold` | EIG below threshold | EIG < tau_event_threshold |
| `belief_threshold` | High confidence reached | belief >= kappa_confidence_stop |
| `error_loop` | Consecutive errors | error_count >= error_threshold |

```python
class StoppingAnalysis:
    """Analysis of why PRE loop terminated."""
    
    stopped_due_to: str      # Stopping condition name
    final_eig: float         # EIG at termination
    final_belief: float      # Belief at termination
    tau_threshold: float     # EIG threshold used
    kappa_threshold: float   # Confidence threshold used
```

---

## 8. Critic Protocol

### 8.1 Critic Interface

```python
class Critic:
    """LLM-backed critic with heuristic fallback."""
    
    def __init__(
        self,
        provider: AIProvider,
        accept_threshold: float = 0.6
    ):
        self.provider = provider
        self.accept_threshold = accept_threshold
    
    def evaluate(
        self,
        instruction: str,
        step: PlanStep,
        candidate: str,
        language: str = "python",
        prior_outputs: List[str] = None
    ) -> Dict[str, Any]:
        """
        Evaluate candidate output.
        
        Returns:
            {
                "approve": bool,      # Whether to accept
                "score": float,       # Confidence [0, 1]
                "reason": str         # Explanation
            }
        """
```

### 8.2 Critic Prompt Template

```
You are a CRITIC validating outputs in a SuperInference PRE loop.

ORIGINAL INSTRUCTION:
{instruction}

CURRENT STEP:
Title: {step.title}
Description: {step.description}
{step_guidance}

CANDIDATE OUTPUT TO EVALUATE:
{candidate}

{lang_guidance}

{prior_context}

EVALUATION CRITERIA:
1. Correctness: Does the output correctly address the step?
2. Completeness: Is all required information present?
3. Consistency: Does it align with prior approved outputs?
4. Format: Is the output properly formatted?

Return ONLY valid JSON:
{
    "approve": true/false,
    "score": 0.0-1.0,
    "reason": "Brief explanation"
}
```

### 8.3 Step-Type Guidance

```python
# For Subquestion (SQ) steps:
step_guidance = """
**Step Type**: SUBQUESTION (SQ)
Evaluate if candidate correctly identifies relevant information from problem.
Focus on information extraction accuracy.
"""

# For Subanswer (SA) steps:
step_guidance = """
**Step Type**: SUBANSWER (SA)
Evaluate if candidate correctly computes the answer.
Focus ONLY on calculation logic, not on irrelevant context.
"""
```

### 8.4 Heuristic Fallback

When LLM evaluation fails:

```python
def heuristic_evaluate(candidate: str, instruction: str) -> Dict:
    """Fallback heuristic evaluation."""
    
    candidate_lines = candidate.strip().split('\n')
    has_substance = len([l for l in candidate_lines 
                         if l.strip() and not l.startswith('#')]) > 0
    has_reasonable_length = len(candidate.strip()) > 10
    
    instruction_words = set(instruction.lower().split()[:5])
    candidate_words = set(candidate.lower().split())
    word_overlap = len(instruction_words & candidate_words) > 0
    
    if has_substance and has_reasonable_length and word_overlap:
        return {"approve": True, "score": 0.75, "reason": "heuristic_substantial"}
    elif has_reasonable_length:
        return {"approve": True, "score": 0.65, "reason": "heuristic_reasonable"}
    else:
        return {"approve": False, "score": 0.3, "reason": "heuristic_insufficient"}
```

### 8.5 Critic Metrics

```python
class CriticMetrics:
    """Estimates critic accuracy for validation."""
    
    alpha_estimate: float   # False positive rate (approved bad outputs)
    beta_estimate: float    # False negative rate (rejected good outputs)
    approval_rate: float    # Overall approval percentage
    avg_score: float        # Average critic score
    
    def estimate_errors(decisions: List[Dict]) -> Tuple[float, float]:
        """
        Estimate α (false positive) and β (false negative) rates.
        
        Requires at least 5 decisions for meaningful estimates.
        """
```

---

## 9. Memory System

### 9.1 Vector Store

```python
class EnhancedVectorStore:
    """Vector store with function-level chunking and similarity search."""
    
    def __init__(self):
        self.entries: Dict[str, EnhancedEmbeddingEntry] = {}
        self.file_chunks: Dict[str, List[str]] = {}  # file_path -> chunk_ids
    
    def add_entry(self, entry: EnhancedEmbeddingEntry) -> None:
        """
        Add entry to vector store.
        
        - Stores entry by ID
        - Tracks file->chunks mapping
        """
    
    def search(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        min_similarity: float = 0.3
    ) -> List[Tuple[EnhancedEmbeddingEntry, float]]:
        """
        Search for similar entries.
        
        Algorithm:
        1. Compute cosine similarity with all entries
        2. Filter by min_similarity threshold
        3. Sort by similarity descending
        4. Diversify: max 3 chunks per file
        5. Return top_k results
        
        Returns:
            List of (entry, similarity_score) tuples
        """
```

### 9.2 Smart Context Manager

```python
class SmartContextManager:
    """Hybrid retrieval with dense + sparse scoring."""
    
    def __init__(
        self,
        vector_store: EnhancedVectorStore,
        provider: AIProvider = None
    ):
        self.vector_store = vector_store
        self.provider = provider
        self.context_cache: Dict[str, Any] = {}
    
    async def get_embedding(self, text: str) -> List[float]:
        """Get embedding from configured provider."""
    
    async def get_enhanced_relevant_context(
        self,
        query: str,
        max_context_length: int = 80000,
        top_k: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Retrieve context using hybrid re-ranking.
        
        Scoring:
        - Dense: Embedding cosine similarity (75% weight)
        - Sparse: Keyword overlap score (25% weight)
        
        combined_score = 0.75 * embedding_similarity + 0.25 * keyword_score
        
        De-duplication:
        - One chunk per file path
        - Respects max_context_length budget
        
        Returns:
            List of context items with:
            - content, similarity, original_similarity
            - metadata, chunk_type, file_path
            - function_name, class_name
        """
```

### 9.3 Memory Update Protocol

```
                    ┌─────────────────────┐
                    │  Execute Step       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Generate Candidate │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Critic Evaluation  │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                  │
              ▼                                  ▼
    ┌──────────────────┐               ┌──────────────────┐
    │    APPROVED      │               │    REJECTED      │
    │  score >= 0.6    │               │  score < 0.6     │
    └────────┬─────────┘               └────────┬─────────┘
             │                                   │
             ▼                                   ▼
    ┌──────────────────┐               ┌──────────────────┐
    │ 1. Embed output  │               │ 1. Log failure   │
    │ 2. Add to M_t    │               │ 2. Increment try │
    │ 3. Update belief │               │ 3. Increase temp │
    │ 4. Emit event    │               │ 4. Maybe retry   │
    └──────────────────┘               └──────────────────┘
```

---

## 10. AI Provider Protocol

### 10.1 Provider Interface

```python
class AIProvider(ABC):
    """Abstract base class for AI providers."""
    
    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: str = "",
        embedding_url: str = "",
        critic_url: str = ""
    ):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.embedding_url = embedding_url
        self.critic_url = critic_url or base_url
        
        # Generation config
        self.temperature = DEFAULT_TEMPERATURE
        self.max_tokens = DEFAULT_MAX_TOKENS
        self.top_p = DEFAULT_TOP_P
        self.top_k = DEFAULT_TOP_K
        
        # Thinking support
        self.last_thoughts = ""
        self.last_answer = ""
        self.supports_thinking = False
        
        # Token tracking
        self.last_usage_metadata = {}
    
    @abstractmethod
    def stream_response(
        self,
        prompt: str,
        context: str = "",
        include_thoughts: bool = False
    ) -> Generator[str, None, None]:
        """Stream response from LLM."""
    
    @abstractmethod
    def stream_critic_response(
        self,
        prompt: str,
        context: str = ""
    ) -> Generator[str, None, None]:
        """Stream from critic model."""
    
    @abstractmethod
    async def get_embedding(self, text: str) -> List[float]:
        """Get embedding vector."""
    
    def set_generation_config(
        self,
        temperature: float = None,
        max_tokens: int = None,
        top_p: float = None,
        top_k: int = None
    ) -> None:
        """Update generation parameters."""
    
    def get_last_thoughts(self) -> str:
        """Get thoughts from last generation (if supported)."""
    
    def get_last_usage_metadata(self) -> Dict[str, Any]:
        """Get token usage from last generation."""
    
    def get_safe_config(self) -> Dict[str, Any]:
        """Get config without sensitive data for logging."""
```

### 10.2 Supported Providers

```python
# Provider implementations
class GeminiProvider(AIProvider):
    """Google Gemini API provider."""
    supports_thinking = True
    # Uses SSE streaming
    # Supports thinkingConfig for reasoning

class OpenAIProvider(AIProvider):
    """OpenAI API provider."""
    supports_thinking = False
    # Uses SSE streaming
    # OpenAI-compatible API

class DeepSeekProvider(AIProvider):
    """DeepSeek API provider."""
    supports_thinking = False
    # Uses SSE streaming
    # OpenAI-compatible API

class VLLMProvider(AIProvider):
    """vLLM self-hosted provider."""
    supports_thinking = False
    # No authentication required
    # OpenAI-compatible API
```

### 10.3 Provider Configuration

```python
PROVIDER_CONFIG = {
    "gemini": {
        "api_key": GEMINI_API_KEY,
        "inference_model": "gemini-2.5-pro",
        "embedding_model": "gemini-embedding-001",
        "critic_model": "gemini-2.5-flash-lite",
        "base_url": "https://generativelanguage.googleapis.com/v1beta",
        "embedding_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent"
    },
    "openai": {
        "api_key": OPENAI_API_KEY,
        "inference_model": "gpt-4",
        "embedding_model": "text-embedding-ada-002",
        "critic_model": "gpt-3.5-turbo",
        "base_url": "https://api.openai.com/v1",
        "embedding_url": "https://api.openai.com/v1/embeddings"
    },
    "deepseek": {
        "api_key": DEEPSEEK_API_KEY,
        "inference_model": "deepseek-chat",
        "embedding_model": "deepseek-embedding",
        "critic_model": "deepseek-chat",
        "base_url": "https://api.deepseek.com/v1",
        "embedding_url": "https://api.deepseek.com/v1/embeddings"
    },
    "vllm": {
        "api_key": "none",  # No auth required
        "inference_model": "meta-llama/Llama-3.3-70B-Instruct",
        "embedding_model": "none",
        "critic_model": "meta-llama/Llama-3.3-70B-Instruct",
        "base_url": "https://your-vllm-endpoint/v1",
        "embedding_url": "https://your-vllm-endpoint/v1/embeddings"
    }
}
```

### 10.4 Provider Factory

```python
def create_provider(
    provider_name: str = None,
    api_key: str = None,
    model: str = None
) -> AIProvider:
    """
    Create an AI provider instance.
    
    Args:
        provider_name: "gemini" | "openai" | "deepseek" | "vllm"
        api_key: Override API key
        model: Override model name
        
    Returns:
        Configured AIProvider instance
    """
```

### 10.5 Connection Pooling

```python
CONNECTION_POOL_CONFIG = {
    'pool_connections': 10,   # Number of connection pools
    'pool_maxsize': 20,       # Max connections per pool
    'max_retries': 3,         # Retry count
    'pool_block': False       # Don't block when full
}

def get_session_for_provider(
    provider_type: str,
    verify_ssl: bool = True
) -> requests.Session:
    """Get shared session with connection pooling."""
```

---

## 11. MCP Transport Layer

### 11.1 Server Initialization

```python
from fastmcp import FastMCP, Context

mcp = FastMCP("SuperInference MCP Server")
```

### 11.2 Transport Modes

```python
# HTTP Transport (production)
mcp.run(
    transport="http",
    host="0.0.0.0",
    port=3000,
    path="/mcp"
)

# STDIO Transport (development/testing)
mcp.run(transport="stdio")
```

### 11.3 Tool Definition

```python
@ToolMetadata.register(
    name="tool_name",
    category="category",
    description="Tool description",
    capabilities=["cap1", "cap2"],
    input_params=["param1", "param2"],
    output_type="Return type description",
    use_cases=["use1", "use2"],
    requires=["dependency_tool"]
)
@mcp.tool
async def tool_name(
    param1: str,
    param2: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Tool implementation.
    
    Args:
        param1: Required parameter
        param2: Optional parameter
        ctx: MCP context (injected)
        
    Returns:
        Result dictionary
    """
    ...
```

### 11.4 Resource Definition

```python
@mcp.resource("tools://available")
async def get_available_tools() -> str:
    """
    Expose tool catalog as MCP resource.
    
    Returns:
        JSON string with tool catalog
    """
    tools_catalog = build_tools_catalog()
    return json.dumps(tools_catalog, indent=2)

@mcp.resource("config://server")
async def get_server_config() -> str:
    """
    Expose server configuration.
    
    Returns:
        JSON string with config
    """
    ...
```

### 11.5 Request Queue

```python
class RequestQueue:
    """Request queuing to prevent overload."""
    
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        self.active_requests = 0
        self.queued_requests = asyncio.Queue()
        self.stats = {
            'total_requests': 0,
            'queued_requests': 0,
            'rejected_requests': 0,
            'avg_queue_time': 0.0
        }
    
    async def acquire(self, request_id: str = None) -> bool:
        """
        Acquire processing slot.
        
        Returns:
            True if acquired, False if rejected
        """
    
    async def release(self) -> None:
        """Release processing slot."""
```

---

## 12. Result Schema

### 12.1 Standard Result Structure

```python
class AgentResult:
    """Comprehensive result from agent execution."""
    
    # ═══ PRIMARY OUTPUTS ═══
    final_answer: str           # REQUIRED. Extracted answer
    execution_result: str       # REQUIRED. Raw execution output
    generated_code: str         # REQUIRED. Generated code
    
    # ═══ PLAN TRACKING ═══
    plan_steps: List[Dict]      # REQUIRED. Step details
    rounds: int                 # REQUIRED. Number of rounds
    
    # ═══ PERFORMANCE ═══
    execution_time: float       # REQUIRED. Total seconds
    phase_timings: Dict[str, float]  # REQUIRED. Per-phase timing
    success: bool               # REQUIRED. Overall success
    
    # ═══ METHOD METADATA ═══
    method: str                 # "superinference_star_unified" | "superinference"
    supinf_mode: bool           # Whether SUPER-INFERENCE was used
    generation_config: Dict     # LLM configuration used
    
    # ═══ INFORMATION THEORY ═══
    information_theory: {
        "initial_belief": float,        # Starting belief
        "final_belief": float,          # Ending belief
        "belief_trajectory": List[float],  # Belief over time
        "initial_entropy_bits": float,  # Starting entropy
        "final_entropy_bits": float,    # Ending entropy
        "entropy_reduction_bits": float,  # Total reduction
        "eig_trajectory": List[float],  # EIG over time
        "total_eig_bits": float,        # Total EIG
        "avg_eig_per_event_bits": float,  # Average EIG
        "events_fired": int             # Event count
    }
    
    # ═══ STOPPING ANALYSIS ═══
    stopping_analysis: {
        "stopped_due_to": str,     # Stopping condition
        "final_eig": float,        # EIG at stop
        "final_belief": float,     # Belief at stop
        "tau_threshold": float,    # EIG threshold
        "kappa_threshold": float   # Confidence threshold
    }
    
    # ═══ CRITIC METRICS ═══
    critic_metrics: {
        "alpha_estimate": float,   # False positive rate
        "beta_estimate": float,    # False negative rate
        "approval_rate": float,    # Approval percentage
        "avg_score": float         # Average score
    }
    
    # ═══ TEMPERATURE ADAPTATION ═══
    temperature_adaptation: {
        "base_temperature": float,
        "final_temperature": float,
        "temperature_trajectory": List[float],
        "total_increases": int,
        "max_temperature_reached": float
    }
    
    # ═══ EXPLORATION TOOLS ═══
    exploration_tools: {
        "ground_truth_values": Dict[str, str],
        "tools_ran": List[str],
        "used_exploration": bool
    }
    
    # ═══ TOKEN USAGE ═══
    token_usage: {
        "total_prompt_tokens": int,
        "total_output_tokens": int,
        "total_tokens": int,
        "by_agent": Dict[str, {
            "calls": int,
            "prompt_tokens": int,
            "output_tokens": int,
            "total_tokens": int
        }]
    }
```

### 12.2 MCP Response Wrapper

```python
{
    "content": [{
        "type": "text",
        "text": "<JSON-encoded AgentResult>"
    }],
    "isError": false,
    "success": true,
    
    # Top-level fields for easy access
    "final_answer": "...",
    "execution_result": "...",
    "generated_code": "...",
    "plan_steps": [...],
    "rounds": 5,
    "execution_time": 45.2,
    # ... all other fields ...
}
```

---

## 13. Error Handling

### 13.1 Error Categories

```python
class ErrorCategory(Enum):
    """Error classification for handling."""
    
    RATE_LIMIT = "rate_limit"       # 429 Too Many Requests
    TIMEOUT = "timeout"             # Request timeout
    VALIDATION = "validation"       # Input validation failure
    EXECUTION = "execution"         # Code execution error
    CRITIC_FAIL = "critic_failure"  # Critic evaluation error
    MEMORY = "memory"               # Vector store error
    PROVIDER = "provider"           # LLM provider error
    CIRCUIT_OPEN = "circuit_open"   # Circuit breaker open
```

### 13.2 Retry Configuration

```python
class RetryConfig:
    """Retry behavior configuration."""
    
    max_retries: int = 5
    base_delay: float = 1.0          # Initial delay (seconds)
    exponential_backoff: bool = True
    max_delay: float = 60.0          # Cap on delay
    retry_codes: List[int] = [429, 500, 503]
```

### 13.3 Retry Algorithm

```python
async def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    *args,
    **kwargs
) -> Any:
    """
    Execute function with exponential backoff on failure.
    
    Algorithm:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except RetryableError as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay * (2 ** attempt), max_delay)
            await asyncio.sleep(delay)
    """
```

### 13.4 Circuit Breaker

```python
class ServerCircuitBreaker:
    """Circuit breaker for API resilience."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        monitoring_period: int = 10
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.monitoring_period = monitoring_period
        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED | OPEN | HALF_OPEN
        self.next_attempt_time = None
    
    def call(self, func: Callable, *args, **kwargs):
        """
        Execute function through circuit breaker.
        
        State transitions:
        - CLOSED -> OPEN: When failures >= threshold
        - OPEN -> HALF_OPEN: After recovery_timeout
        - HALF_OPEN -> CLOSED: On success
        - HALF_OPEN -> OPEN: On failure
        """
```

### 13.5 Error Response Format

```python
{
    "content": [{
        "type": "text",
        "text": "Error: <error message>"
    }],
    "isError": true,
    "success": false,
    "error": "<error message>",
    "error_category": "<ErrorCategory>",
    "traceback": "<optional stack trace>",
    "retry_after": <optional seconds>
}
```

---

## 14. Sequence Diagrams

### 14.1 Plan Execution Flow

```
┌────────┐  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌────────┐  ┌────────┐
│ Client │  │ Planner │  │ Retriever│  │ Executor│  │ Critic │  │ Memory │
└───┬────┘  └────┬────┘  └────┬─────┘  └────┬────┘  └───┬────┘  └───┬────┘
    │            │            │             │           │           │
    │ instruction│            │             │           │           │
    │───────────>│            │             │           │           │
    │            │            │             │           │           │
    │            │ generate_plan()          │           │           │
    │            │──────────────────────────────────────────────────>│
    │            │            │             │           │           │
    │            │<───────────────────────────────────plan_steps────│
    │            │            │             │           │           │
    │            │════════════════════════════════════════════════════
    │            │ ║          PRE LOOP                              ║
    │            │ ║                                                ║
    │            │ ║ calculate_EIG()                                ║
    │            │ ║──────────>│                                    ║
    │            │ ║           │                                    ║
    │            │ ║ get_context()          │                       ║
    │            │ ║───────────>│           │                       ║
    │            │ ║<──context──│           │                       ║
    │            │ ║            │           │                       ║
    │            │ ║ execute_step()         │                       ║
    │            │ ║────────────────────────>│                      ║
    │            │ ║<────────candidate───────│                      ║
    │            │ ║            │           │                       ║
    │            │ ║ evaluate()             │                       ║
    │            │ ║─────────────────────────────────>│             ║
    │            │ ║<─────────{approve, score}────────│             ║
    │            │ ║            │           │         │             ║
    │            │ ║            │           │         │ [if approved]
    │            │ ║ add_to_memory()        │         │             ║
    │            │ ║─────────────────────────────────────────────>│ ║
    │            │ ║            │           │         │           │ ║
    │            │ ║ update_belief()        │         │           │ ║
    │            │ ║            │           │         │           │ ║
    │            │ ║ [check stopping conditions]                   ║
    │            │ ════════════════════════════════════════════════
    │            │            │             │           │           │
    │<──result───│            │             │           │           │
    │            │            │             │           │           │
```

### 14.2 SUPER-INFERENCE Workflow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          SUPER-INFERENCE WORKFLOW                           │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                        ┌─────────────┴─────────────┐
                        │      PHASE 0: ANALYZE      │
                        │  ┌─────────────────────┐  │
                        │  │      ANALYZER       │  │
                        │  │  (file schemas)     │  │
                        │  └──────────┬──────────┘  │
                        └─────────────┴─────────────┘
                                      │ file_descriptions
                                      ▼
                        ┌─────────────────────────────┐
                        │   PHASE 0.5: EXPLORATION    │
                        │  ┌─────────────────────┐   │
                        │  │   LLM-DRIVEN        │   │
                        │  │   EXPLORATION       │   │
                        │  │   - grep_data       │   │
                        │  │   - read_data_file  │   │
                        │  │   - shell_analyze   │   │
                        │  └──────────┬──────────┘   │
                        └─────────────┴───────────────┘
                                      │ ground_truth_metrics
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                             PRE LOOP                                          │
│                                                                               │
│    ┌──────────────┐                    ┌──────────────┐                      │
│    │   PLANNER    │───────────────────>│    CODER     │                      │
│    │ (plan steps) │                    │  (generate)  │                      │
│    └──────┬───────┘                    └──────┬───────┘                      │
│           │                                   │                              │
│           │                                   ▼                              │
│           │                            ┌──────────────┐                      │
│           │                            │   EXECUTOR   │                      │
│           │                            │  (run code)  │                      │
│           │                            └──────┬───────┘                      │
│           │                                   │                              │
│           ▼                                   ▼                              │
│    ┌──────────────┐                    ┌──────────────┐                      │
│    │   VERIFIER   │<───────────────────│   CRITIC     │                      │
│    │ (sufficient?)│                    │  (validate)  │                      │
│    └──────┬───────┘                    └──────────────┘                      │
│           │                                                                  │
│     ┌─────┴─────┐                                                            │
│     ▼           ▼                                                            │
│ [sufficient] [insufficient]                                                  │
│     │           │                                                            │
│     │     ┌─────┴─────────────────────┐                                      │
│     │     ▼                           ▼                                      │
│     │  ┌──────────┐            ┌──────────┐                                  │
│     │  │  ROUTER  │            │ DEBUGGER │                                  │
│     │  │ (next?)  │            │ (if err) │                                  │
│     │  └────┬─────┘            └────┬─────┘                                  │
│     │       │                       │                                        │
│     │       │   ┌───────────────────┤                                        │
│     │       │   │                   │                                        │
│     │       ▼   ▼                   ▼                                        │
│     │  [continue]  [backtrack]  [add_step]                                   │
│     │       │         │             │                                        │
│     │       └─────────┴──────┬──────┘                                        │
│     │                        │                                               │
│     │                   ┌────┴────┐                                          │
│     │                   │  LOOP   │──────────────────────────┐               │
│     │                   └─────────┘                          │               │
│     │                                                        │               │
└─────┼────────────────────────────────────────────────────────┼───────────────┘
      │                                                        │
      │ [stopping condition met]                               │
      ▼                                                        │
┌──────────────┐                                               │
│  FINALIZER   │<──────────────────────────────────────────────┘
│  (extract)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ final_answer │
└──────────────┘
```

### 14.3 Critic Evaluation Flow

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  Executor  │     │   Critic   │     │  Provider  │     │   Memory   │
└─────┬──────┘     └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
      │                  │                  │                  │
      │ candidate_output │                  │                  │
      │─────────────────>│                  │                  │
      │                  │                  │                  │
      │                  │ build_prompt()   │                  │
      │                  │────────────────>│                  │
      │                  │                  │                  │
      │                  │ stream_critic_response()           │
      │                  │────────────────>│                  │
      │                  │                  │                  │
      │                  │<────chunks───────│                  │
      │                  │                  │                  │
      │                  │ parse_response() │                  │
      │                  │                  │                  │
      │                  │────────────────────────────────────┐│
      │                  │ {approve: true, score: 0.85}       ││
      │                  │                                    ││
      │                  │                  │      [if approve]│
      │                  │ embed_output()   │                  │
      │                  │────────────────────────────────────>│
      │                  │                  │                  │
      │<─────{approve, score, reason}──────│                  │
      │                  │                  │                  │
```

---

## 15. Configuration Reference

### 15.1 Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DEFAULT_PROVIDER` | string | `"vllm"` | AI provider name |
| `DEFAULT_TEMPERATURE` | float | `0.1` | LLM temperature |
| `DEFAULT_MAX_TOKENS` | int | `200000` | Max output tokens |
| `DEFAULT_TOP_P` | float | `0.8` | Nucleus sampling |
| `DEFAULT_TOP_K` | int | `40` | Top-k sampling |
| `BENCHMARK_MODE` | bool | `true` | Enable optimizations |
| `LOG_LEVEL` | string | `"INFO"` | Logging level |

### 15.2 Critic Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CRITIC_PROVIDER` | string | `DEFAULT_PROVIDER` | Critic provider |
| `CRITIC_ACCEPT_THRESHOLD` | float | `0.6` | Approval threshold |
| `CRITIC_ACCEPT_THRESHOLD_EASY` | float | `0.80` | Easy task threshold |
| `CRITIC_ACCEPT_THRESHOLD_HARD` | float | `0.70` | Hard task threshold |

### 15.3 Temperature Schedule

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `TEMP_BASE` | float | `0.1` | Initial temperature |
| `TEMP_ADD_STEP` | float | `0.05` | Increase when adding steps |
| `TEMP_BACKTRACK` | float | `0.10` | Increase on backtrack |
| `TEMP_CAP` | float | `0.90` | Maximum temperature |
| `TEMP_AFTER_AGREEMENT` | float | `0.10` | Temperature for finalization |

### 15.4 Planning Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `tau_event_threshold` | float | `0.01` | EIG threshold |
| `kappa_confidence_stop` | float | `0.90` | Belief stop threshold |
| `epsilon_min_eig` | float | `0.015` | Minimum EIG |
| `max_events` | int | `20` | Maximum events |
| `max_steps` | int | `30` | Maximum plan steps |

### 15.5 Performance Limits

| Variable | Type | Normal | Benchmark | Description |
|----------|------|--------|-----------|-------------|
| `DEFAULT_REQUEST_TIMEOUT` | int | 180 | 600 | Request timeout (s) |
| `MAX_CONCURRENT_REQUESTS` | int | 3 | 3 | Concurrent limit |
| `MAX_STREAMING_CHUNKS` | int | 500 | 5000 | Chunk limit |
| `MAX_RESPONSE_SIZE_MB` | int | 50 | 500 | Size limit (MB) |
| `CRITIC_RESPONSE_LIMIT` | int | 12000 | ∞ | Critic chars |
| `PLAN_GENERATION_LIMIT` | int | 8000 | ∞ | Plan chars |
| `STEP_EXECUTION_LIMIT` | int | 15000 | ∞ | Step chars |
| `CODE_GENERATION_LIMIT` | int | 25000 | ∞ | Code chars |

### 15.6 EIG Parameters

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `EIG_MIN_DELTA_EASY` | float | `0.03` | Min EIG delta (easy) |
| `EIG_MIN_DELTA_HARD` | float | `0.02` | Min EIG delta (hard) |
| `EIG_PLATEAU_ROUNDS_EASY` | int | `6` | Plateau rounds (easy) |
| `EIG_PLATEAU_ROUNDS_HARD` | int | `7` | Plateau rounds (hard) |

---

## 16. Security Considerations

### 16.1 Logging Sanitization

All logging MUST sanitize sensitive data:

```python
SENSITIVE_PATTERNS = [
    # API Keys
    (r'api[_-]?key.*?([A-Za-z0-9\-_]{15,})', '***REDACTED***'),
    (r'Bearer\s+([A-Za-z0-9\-_\.]{15,})', 'Bearer ***REDACTED***'),
    
    # Provider-specific patterns
    (r'AIza[A-Za-z0-9\-_]{35,}', '***REDACTED***'),  # Google
    (r'sk-[A-Za-z0-9]{20,}', '***REDACTED***'),       # OpenAI
    (r'ghp_[A-Za-z0-9]{36,}', '***REDACTED***'),      # GitHub
    
    # Generic secrets
    (r'token.*?([A-Za-z0-9\-_\.]{15,})', '***REDACTED***'),
    (r'secret.*?([A-Za-z0-9\-_\.]{15,})', '***REDACTED***'),
    (r'password.*?([A-Za-z0-9\-_\.]{8,})', '***REDACTED***'),
]
```

### 16.2 Code Execution Sandbox

Generated code MUST execute in a sandboxed environment:

```python
async def _safe_execute_code(code: str, data_directory: str) -> str:
    """
    Execute code with safety measures:
    
    1. Auto-correct relative paths to absolute
    2. Provide safe helper functions
    3. Capture stdout/stderr
    4. Handle exceptions gracefully
    5. Timeout long-running operations
    6. Limit resource consumption
    """
```

### 16.3 Input Validation

All external inputs MUST be validated:

- Maximum string lengths
- Valid file paths (no path traversal)
- Valid JSON structure
- Rate limiting per client

### 16.4 Authentication

HTTP transport SHOULD support authentication:

- API key validation
- Bearer token verification
- Client certificate (mTLS)

---

## 17. Appendix

### 17.1 Glossary

| Term | Definition |
|------|------------|
| **Belief** | Probability estimate P(success) for task completion |
| **Candidate** | Output generated by Executor for Critic evaluation |
| **Chunk** | Semantic unit of code (function, class, etc.) |
| **EIG** | Expected Information Gain from executing a step |
| **Entropy** | Uncertainty measure H(p) = -p*log(p) - (1-p)*log(1-p) |
| **Event** | A reasoning step that fires based on EIG threshold |
| **M_t** | Memory state at time t containing approved artifacts |
| **MCP** | Model Context Protocol for tool exposure |
| **PRE** | Planner-Retriever-Executor reasoning loop |

### 17.2 References

1. SuperInference Paper: Event-driven PRE loop with information-theoretic foundations
2. SUPER-INFERENCE: Multi-agent workflow for data analysis
3. Model Context Protocol (MCP) Specification
4. FastMCP Python Implementation

### 17.3 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-12-10 | Initial specification |

### 17.4 License

This specification is released under the GNU General Public License v3.0.

---

*End of Specification*

