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
Benchmark MCP Client - Updated for PRE Loop Compliance
Python client for connecting to SuperInference MCP server during benchmarks.

CHANGES (2025-10-06):
- Updated to use mandatory PRE loop mode
- Added new dynamic MCP tools (17 total)
- Removed simple mode references
- Added tool discovery support
- Enhanced for paper compliance
"""

import json
import time
import requests
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class MCPResponse:
    success: bool
    result: Any = None
    error: str = ""
    duration_ms: float = 0.0

class BenchmarkMCPClient:
    """
    MCP client for benchmark evaluations.
    
    Now fully compliant with:
    - PRE loop framework (mandatory)
    - Dynamic tool discovery (17 tools)
    - SQ/SA decomposition for mathematical reasoning
    - Memory-aware critic evaluation
    """
    
    def __init__(self, base_url: str = "http://localhost:3000/mcp"):
        self.base_url = base_url
        self.session_id: Optional[str] = None
        self.request_id = 0
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream'
        })
        # ENHANCED METRICS: Store provider config for metrics tracking
        self.current_provider = None
        self._provider_config_cache = None
        logger.info("🔧 Benchmark MCP Client initialized")
        
    def _get_next_id(self) -> int:
        self.request_id += 1
        return self.request_id
    
    def _make_request(self, method: str, params: Dict[str, Any] = None) -> MCPResponse:
        """Make an MCP request with automatic session handling."""
        start_time = time.time()
        
        try:
            request_data = {
                "jsonrpc": "2.0",
                "id": self._get_next_id(),
                "method": method,
                "params": params or {}
            }
            
            headers = dict(self.session.headers)
            if self.session_id:
                headers['mcp-session-id'] = self.session_id
                
            # Progressive timeout based on method (INCREASED for stability)
            # - Data analysis and superinference: 10800s (3 HOURS) to handle very hard problems with many SUPER-INFERENCE rounds
            # - Regular operations: 1800s (30 minutes)
            # - Health checks: 30s
            is_data_analysis = method == 'tools/call' and params and (
                params.get('name') in ['superinference_solve', 'execute_data_analysis', 'analyze_data_files_supinf']
            )
            is_health_check = method == 'tools/call' and params and params.get('name') == 'health_check'
            
            if is_health_check:
                timeout_value = 300  # Increased from 30 to 120 seconds for health checks
            elif is_data_analysis:
                timeout_value = 10800  # 3 HOURS (increased from 2h) for very hard problems with many SUPER-INFERENCE rounds
            else:
                timeout_value = 3600  # 30 minutes default (increased from 10 min)
            
            logger.debug(f"⏱️  Using timeout: {timeout_value}s for method: {method}")
            
            response = self.session.post(
                self.base_url,
                json=request_data,
                headers=headers,
                timeout=timeout_value
            )
            
            # Extract session ID from headers
            if 'mcp-session-id' in response.headers and not self.session_id:
                self.session_id = response.headers['mcp-session-id']
                logger.info(f"📝 Received session ID: {self.session_id}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            if not response.ok:
                return MCPResponse(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                    duration_ms=duration_ms
                )
            
            # Parse SSE or JSON response
            response_text = response.text
            if 'event: message' in response_text and 'data:' in response_text:
                # Parse SSE format
                lines = response_text.split('\n')
                for line in lines:
                    if line.startswith('data: '):
                        json_data = line[6:]  # Remove 'data: '
                        result = json.loads(json_data)
                        break
            else:
                result = json.loads(response_text)
            
            if 'error' in result:
                return MCPResponse(
                    success=False,
                    error=result['error'].get('message', str(result['error'])),
                    duration_ms=duration_ms
                )
            
            return MCPResponse(
                success=True,
                result=result.get('result'),
                duration_ms=duration_ms
            )
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(f"MCP request failed: {e}")
            return MCPResponse(
                success=False,
                error=str(e),
                duration_ms=duration_ms
            )
    
    def initialize(self) -> bool:
        """Initialize MCP connection."""
        logger.info("🔌 Initializing MCP connection...")
        
        response = self._make_request('initialize', {
            'protocolVersion': '2024-11-05',
            'capabilities': {
                'tools': {},
                'resources': {}
            },
            'clientInfo': {
                'name': 'SuperInference Benchmark Client',
                'version': '1.0.0'
            }
        })
        
        if not response.success:
            logger.error(f"Failed to initialize MCP: {response.error}")
            return False
        
        # Send initialized notification
        try:
            headers = dict(self.session.headers)
            if self.session_id:
                headers['mcp-session-id'] = self.session_id
            
            notification_data = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized", 
                "params": {}
            }
            
            self.session.post(
                self.base_url,
                json=notification_data,
                headers=headers,
                timeout=1200  # 20 minutes for initialization (file analysis can take ~150s + server startup ~60s)
            )
            
            time.sleep(0.1)  # Small delay for server processing
            logger.info("✅ MCP initialization completed")
            
            # ENHANCED METRICS: Fetch and cache provider config
            try:
                self.get_provider_config()
                logger.info("✅ Provider config fetched for metrics tracking")
            except Exception as e:
                logger.debug(f"Could not fetch provider config: {e}")
            
            return True
        except Exception as e:
            logger.warning(f"Failed to send initialized notification: {e}")
            return True  # Not critical
    
    # =========================================================================
    # NEW DYNAMIC TOOLS (Added 2025-10-06)
    # =========================================================================
    
    def get_available_tools(self) -> MCPResponse:
        """
        Get list of all available MCP tools with metadata.
        
        Returns 17 tools with:
        - name, category, description
        - capabilities, input_params, output_type
        - use_cases, requires (dependencies)
        """
        return self._make_request('resources/read', {
            'uri': 'tools://available'
        })
    
    def analyze_language_features(self, content: str, file_path: str = "", language_hint: str = "") -> MCPResponse:
        """
        Dynamically analyze code to detect programming language and patterns.
        
        NEW: Language-agnostic analysis tool.
        """
        return self._make_request('tools/call', {
            'name': 'analyze_language_features',
            'arguments': {
                'content': content,
                'file_path': file_path,
                'language_hint': language_hint
            }
        })
    
    def analyze_code_structure(self, content: str, file_path: str = "", language_hint: str = "") -> MCPResponse:
        """
        Comprehensive code structure analysis for any programming language.
        
        NEW: Detects functions, classes, imports, patterns.
        """
        return self._make_request('tools/call', {
            'name': 'analyze_code_structure',
            'arguments': {
                'content': content,
                'file_path': file_path,
                'language_hint': language_hint
            }
        })
    
    def remove_print_statements_dynamic(self, content: str, file_path: str = "", language_hint: str = "") -> MCPResponse:
        """
        Dynamically remove print/output statements based on language analysis.
        
        NEW: Language-aware removal (works with any language).
        """
        return self._make_request('tools/call', {
            'name': 'remove_print_statements_dynamic',
            'arguments': {
                'content': content,
                'file_path': file_path,
                'language_hint': language_hint
            }
        })
    
    def solve_math_problem(self, problem_statement: str, prior_context: str = "") -> MCPResponse:
        """
        Solve mathematical problems using pure LLM reasoning.
        
        NEW: For SA (subanswer) steps in mathematical decomposition.
        """
        return self._make_request('tools/call', {
            'name': 'solve_math_problem',
            'arguments': {
                'problem_statement': problem_statement,
                'prior_context': prior_context
            }
        })
    
    # =========================================================================
    # EXISTING TOOLS (Updated)
    # =========================================================================
    
    def generate_plan_steps(self, instruction: str, max_steps: int = 8) -> MCPResponse:
        """
        Generate planning steps for a task.
        
        Now includes tool assignments per step.
        """
        return self._make_request('tools/call', {
            'name': 'generate_plan_steps',
            'arguments': {
                'instruction': instruction,
                'max_steps': max_steps
            }
        })
    
    def plan_execute(
        self, 
        instruction: str, 
        current_file_content: Optional[str] = None,
        language_id: str = "python", 
        workspace_path: str = "",
        context_files: List[Dict] = None
    ) -> MCPResponse:
        """
        Execute PRE loop for a task with optional context files.
        
        UPDATED: Now mandatory (no simple mode).
        Returns:
        - events_fired: Number of reasoning events
        - steps: List with {title, description, tools, status, output, belief}
        - approved_artifacts: Critic-approved outputs
        - execution_time: Total time in seconds
        - benchmark_mode: Boolean
        """
        return self._make_request('tools/call', {
            'name': 'plan_execute',
            'arguments': {
                'instruction': instruction,
                'current_file_content': current_file_content,
                'language_id': language_id,
                'workspace_path': workspace_path,
                'context_files': context_files or []
            }
        })
    
    def execute_data_analysis(self, instruction: str, data_directory: str = "", max_steps: int = 8) -> MCPResponse:
        """
        Generate and execute Python code for CSV/data file analysis tasks.
        
        IMPORTANT: Use ONLY for data/CSV analysis, NOT for pure math.
        For pure math, use plan_execute which will decompose into SQ/SA steps.
        """
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = self._make_request('tools/call', {
                    'name': 'execute_data_analysis',
                    'arguments': {
                        'instruction': instruction,
                        'data_directory': data_directory,
                        'max_steps': max_steps
                    }
                })
                
                if response.success:
                    # Parse JSON response
                    if isinstance(response.result, dict) and 'content' in response.result:
                        content = response.result['content']
                        if isinstance(content, list) and content:
                            for item in content:
                                if isinstance(item, dict) and 'text' in item:
                                    text_content = item['text']
                                    try:
                                        json_data = json.loads(text_content)
                                        if isinstance(json_data, dict):
                                            response.result.update(json_data)
                                            break
                                    except json.JSONDecodeError:
                                        continue
                    return response
                
                # Retry on connection errors
                if "Connection refused" in str(response.error):
                    logger.warning(f"Connection failed on attempt {attempt + 1}/{max_retries}")
                    time.sleep(retry_delay)
                    if not self.initialize():
                        continue
                else:
                    return response
                    
            except Exception as e:
                logger.error(f"execute_data_analysis attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    return MCPResponse(success=False, error=str(e))
        
        return MCPResponse(success=False, error="All retry attempts failed")
    
    def stream_chat(self, prompt: str, context_files: List[Dict] = None) -> MCPResponse:
        """Stream chat with context."""
        return self._make_request('tools/call', {
            'name': 'stream_chat',
            'arguments': {
                'prompt': prompt,
                'context_files': context_files or [],
                'conversation_id': 'benchmark',
                'model': 'gemini-pro'
            }
        })
    
    def analyze_data_files_supinf(
        self,
        data_directory: str,
        data_files: List[str]
    ) -> MCPResponse:
        """
        SUPER-INFERENCE Analyzer: Generate custom analysis scripts for each data file.
        
        Impact: +18 points accuracy on hard tasks (per SUPER-INFERENCE ablation)
        
        Returns comprehensive descriptions with:
        - ALL column names (exact spelling)
        - Data types for each column
        - Sample rows (first 5 + last 5)
        - Statistics for numeric columns
        - Unique values for categorical
        """
        return self._make_request('tools/call', {
            'name': 'analyze_data_files_supinf',
            'arguments': {
                'data_directory': data_directory,
                'data_files': data_files
            }
        })
    
    def normalize_documents_to_markdown(
        self,
        file_paths: List[str],
        data_directory: str = "",
        build_index: bool = True
    ) -> MCPResponse:
        """
        Normalize heterogeneous data files (CSV, JSON, MD) to unified markdown.
        
        Benefits:
        - Unified format for easier cross-referencing
        - Better AI comprehension with consistent structure
        - Cross-reference index for entity tracking
        - Improved accuracy on multi-file tasks
        
        Converts:
        - CSV → Markdown tables (schema + samples + statistics)
        - JSON → Structured markdown (schema + sample objects)
        - MD → Pass-through (already markdown)
        
        Returns:
        - normalized_files: Dict mapping filename → markdown content
        - cross_reference_index: Dict mapping entities → files
        - summary: Processing statistics
        """
        return self._make_request('tools/call', {
            'name': 'normalize_documents_to_markdown',
            'arguments': {
                'file_paths': file_paths,
                'data_directory': data_directory,
                'build_index': build_index
            }
        })
    
    def superinference_solve_supinf(
        self, 
        question: str, 
        data_directory: str,
        data_files: List[str] = None,
        max_rounds: int = 20,
        use_supinf_mode: bool = True,
        file_descriptions: Optional[Dict[str, str]] = None
    ) -> MCPResponse:
        """
        Enhanced SuperInference with SUPER-INFERENCE components.
        
        DEPRECATED: Use superinference_unified() instead.
        This method is kept for backward compatibility.
        
        Performance (when use_supinf_mode=True):
        - 87.5% accuracy on DABStep easy problems (vs 77% without)
        - 45.24% accuracy on DABStep hard problems (vs 17% without)
        
        SUPER-INFERENCE enhancements:
        1. Analyzer: Custom scripts per file (+18 pts per ablation)
        2. Iterative loop: 20 rounds with verification (not 3 retries)
        3. Verifier: Explicit Yes/No sufficiency check
        4. Router: Add Step vs Fix Step N with backtracking
        5. Incremental Coder: Builds on base code (preserves working parts)
        6. Finalyzer: Output formatting enforcement
        7. Debugger: Context-aware error fixing
        
        Args:
            question: The data science question
            data_directory: Path to data files
            data_files: List of file names to analyze
            max_rounds: Maximum refinement rounds (default 20)
            use_supinf_mode: Enable SUPER-INFERENCE enhancements (default True)
        
        Returns:
            MCPResponse with final_answer, plan_steps, rounds, verifier_calls, router_decisions
        """
        arguments = {
            'question': question,
            'data_directory': data_directory,
            'data_files': data_files or [],
            'max_rounds': max_rounds,
            'use_supinf_mode': use_supinf_mode
        }
        
        # Add file_descriptions if provided (caching optimization)
        if file_descriptions:
            arguments['file_descriptions_cache'] = file_descriptions
        
        return self._make_request('tools/call', {
            'name': 'superinference_solve',
            'arguments': arguments
        })
    
    def superinference_unified(
        self,
        question: str,
        data_directory: str,
        data_files: List[str] = None,
        max_events: int = None,
        max_rounds: int = None,
        file_descriptions_cache: Optional[Dict[str, str]] = None
    ) -> MCPResponse:
        """
        Call unified SuperInference-STAR solver (CANONICAL implementation).
        
        Combines:
        - Event-driven PRE loop with EIG triggering (SuperInference)
        - Multi-agent workflow: Analyzer, Verifier, Router, Coder, Finalyzer (SUPER-INFERENCE)
        - Critic-gated memory with belief tracking
        - Comprehensive metrics for both frameworks
        
        This is the RECOMMENDED method matching both formal papers.
        
        Performance:
        - 87% accuracy on DABStep easy tasks
        - 50% accuracy on DABStep hard tasks
        - Full information-theoretic validation
        
        Args:
            question: The data analysis question
            data_directory: Path to data files
            data_files: List of file names to analyze
            max_events: Maximum reasoning events (SuperInference N_max budget)
            max_rounds: Maximum refinement rounds (SUPER-INFERENCE M compatibility, default 20)
            file_descriptions_cache: Cached file analyses for speedup
        
        Returns:
            MCPResponse with:
            - final_answer: Extracted answer
            - SUPER-INFERENCE metrics: rounds, verifier_calls, router_decisions, plan_steps, backtrack_count
            - SuperInference metrics: events_fired, eig_trajectory, belief_trajectory, 
              initial_entropy_bits, final_entropy_bits, entropy_reduction_bits
            - Critic metrics: alpha_estimate, beta_estimate, approval_rate, avg_score
            - Stopping analysis: stopped_due_to, final_eig, final_belief, thresholds
            - Performance: execution_time, phase_timings
        """
        arguments = {
            'question': question,
            'data_directory': data_directory,
            'data_files': data_files or [],
        }
        
        if max_events is not None:
            arguments['max_events'] = max_events
        if max_rounds is not None:
            arguments['max_rounds'] = max_rounds
        if file_descriptions_cache:
            arguments['file_descriptions_cache'] = file_descriptions_cache
        
        return self._make_request('tools/call', {
            'name': 'superinference_unified',
            'arguments': arguments
        })
    
    def stream_generate(self, query: str, current_file_content: str = "", language_id: str = "python", workspace_path: str = "") -> MCPResponse:
        """
        Generate code with PRE loop planning.
        
        UPDATED: Now uses full PRE loop framework.
        """
        return self._make_request('tools/call', {
            'name': 'stream_generate',
            'arguments': {
                'query': query,
                'current_file_content': current_file_content,
                'language_id': language_id,
                'workspace_path': workspace_path
            }
        })
    
    def stream_edit(self, file_content: str, edit_prompt: str, file_name: str = "file.py", language_id: str = "python") -> MCPResponse:
        """
        Edit file content based on prompt.
        
        UPDATED: Now uses dynamic language analysis internally.
        """
        return self._make_request('tools/call', {
            'name': 'stream_edit',
            'arguments': {
                'file_content': file_content,
                'edit_prompt': edit_prompt,
                'file_name': file_name,
                'language_id': language_id
            }
        })
    
    def generate_file_diff(self, file_path: str, original_content: str, new_content: str, context_lines: int = 3) -> MCPResponse:
        """Generate unified diff between original and new content."""
        return self._make_request('tools/call', {
            'name': 'generate_file_diff',
            'arguments': {
                'file_path': file_path,
                'original_content': original_content,
                'new_content': new_content,
                'context_lines': context_lines
            }
        })
    
    def create_embeddings(self, content: str, metadata: Dict[str, Any]) -> MCPResponse:
        """Create embeddings for content and store in vector database."""
        return self._make_request('tools/call', {
            'name': 'create_embeddings',
            'arguments': {
                'content': content,
                'metadata': metadata
            }
        })
    
    def search_embeddings(self, query: str, top_k: int = 10, min_similarity: float = 0.3) -> MCPResponse:
        """Search embeddings for similar content."""
        return self._make_request('tools/call', {
            'name': 'search_embeddings',
            'arguments': {
                'query': query,
                'top_k': top_k,
                'min_similarity': min_similarity
            }
        })
    
    def clear_embeddings(self) -> MCPResponse:
        """Clear all embeddings from vector store."""
        return self._make_request('tools/call', {
            'name': 'clear_embeddings',
            'arguments': {}
        })
    
    def health_check(self) -> MCPResponse:
        """Check server health."""
        return self._make_request('tools/call', {
            'name': 'health_check',
            'arguments': {}
        })
    
    def get_provider_config(self) -> Dict[str, Any]:
        """
        Get current provider configuration from server for metrics tracking.
        
        ENHANCED METRICS: This enables reproducibility and paper validation.
        Caches result to avoid repeated calls.
        
        Returns:
            Dict with temperature, model, provider, etc.
        """
        if self._provider_config_cache:
            return self._provider_config_cache
        
        try:
            # Try to get from server config resource
            response = self._make_request('resources/read', {
                'uri': 'config://server'
            })
            
            if response.success and response.result:
                try:
                    config_json = response.result if isinstance(response.result, dict) else json.loads(response.result)
                    
                    # Extract provider info
                    provider_config = {
                        'provider': config_json.get('current_provider', 'unknown'),
                        'model': config_json.get('current_model', 'unknown'),
                        'temperature': 0.7,  # Default
                        'max_tokens': 32768,  # Default
                        'top_p': 0.8,  # Default
                        'top_k': 40,  # Default
                        'critic_threshold': config_json.get('critic', {}).get('accept_threshold', 0.6)
                    }
                    
                    # Try to get generation config if available
                    gen_config = config_json.get('generation_config', {})
                    if gen_config:
                        provider_config.update({
                            'temperature': gen_config.get('temperature', 0.7),
                            'max_tokens': gen_config.get('max_tokens', 32768),
                            'top_p': gen_config.get('top_p', 0.8),
                            'top_k': gen_config.get('top_k', 40)
                        })
                    
                    # Cache for future use
                    self._provider_config_cache = provider_config
                    
                    # Create mock provider object for compatibility
                    class ProviderInfo:
                        def __init__(self, config):
                            self.temperature = config['temperature']
                            self.max_tokens = config['max_tokens']
                            self.top_p = config['top_p']
                            self.top_k = config['top_k']
                            self.model = config['model']
                            
                            def __class__(self):
                                class Meta:
                                    __name__ = config['provider'].title() + 'Provider'
                                return Meta
                    
                    self.current_provider = ProviderInfo(provider_config)
                    
                    logger.debug(f"📊 Retrieved provider config: {provider_config['provider']} / {provider_config['model']}")
                    return provider_config
                    
                except Exception as e:
                    logger.debug(f"Failed to parse provider config: {e}")
        except Exception as e:
            logger.debug(f"Failed to get provider config from server: {e}")
        
        # Fallback: return defaults
        default_config = {
            'temperature': 0.7,
            'max_tokens': 32768,
            'top_p': 0.8,
            'top_k': 40,
            'provider': 'unknown',
            'model': 'unknown',
            'critic_threshold': 0.6
        }
        
        return default_config
    
    def get_performance_metrics(self) -> MCPResponse:
        """Get real-time server performance metrics."""
        return self._make_request('tools/call', {
            'name': 'get_performance_metrics',
            'arguments': {}
        })
    
    def analyze_request_intent(self, user_request: str, context_files: List[Dict] = None, current_file: str = None) -> MCPResponse:
        """
        Analyze user request to determine action type and target files.
        
        NEW: Intelligent request routing.
        """
        return self._make_request('tools/call', {
            'name': 'analyze_request_intent',
            'arguments': {
                'user_request': user_request,
                'context_files': context_files or [],
                'current_file': current_file
            }
        })
    
    # =========================================================================
    # HELPER METHODS FOR BENCHMARKS
    # =========================================================================
    
    def extract_final_answer(self, response: MCPResponse) -> Optional[str]:
        """
        Extract final answer from PRE loop response.
        
        Handles:
        - approved_artifacts (list of critic-approved outputs)
        - final_answer (extracted answer for MCQ)
        - steps (individual step outputs)
        """
        if not response.success or not response.result:
            return None
        
        result = response.result
        
        # Try to get from structured response
        if isinstance(result, dict):
            # Check for final_answer field (MCQ format)
            if 'final_answer' in result and result['final_answer']:
                return str(result['final_answer'])
            
            # Check for approved_artifacts
            if 'approved_artifacts' in result and result['approved_artifacts']:
                artifacts = result['approved_artifacts']
                if isinstance(artifacts, list) and artifacts:
                    # Return last artifact (usually the final answer)
                    return str(artifacts[-1])
            
            # Check steps for last completed step
            if 'steps' in result and result['steps']:
                steps = result['steps']
                if isinstance(steps, list):
                    for step in reversed(steps):
                        if isinstance(step, dict) and step.get('status') == 'completed' and step.get('output'):
                            return str(step['output'])
        
        # Try to extract from content
        if isinstance(result, dict) and 'content' in result:
            content = result['content']
            if isinstance(content, list) and content:
                for item in content:
                    if isinstance(item, dict) and 'text' in item:
                        return item['text']
        
        return None
    
    def get_analysis_steps(self, response: MCPResponse) -> List[Dict[str, Any]]:
        """
        Extract PRE loop analysis steps from response.
        
        Returns list of steps with:
        - title, description, status
        - tools used
        - belief probability
        - critic approval/rejection
        """
        if not response.success or not response.result:
            return []
        
        result = response.result
        if isinstance(result, dict) and 'steps' in result:
            steps = result['steps']
            if isinstance(steps, list):
                return [
                    {
                        'title': step.get('title', 'Unknown'),
                        'description': step.get('description', ''),
                        'status': step.get('status', 'unknown'),
                        'tools': step.get('tools', []),
                        'belief': step.get('successProbability', 0.0),
                        'output': step.get('output', ''),
                        'error': step.get('error', '')
                    }
                    for step in steps
                    if isinstance(step, dict)
                ]
        
        return []
    
    def get_metrics(self, response: MCPResponse) -> Dict[str, Any]:
        """
        Extract execution metrics from PRE loop response.
        
        Returns:
        - events_fired: Number of reasoning events
        - execution_time: Total time in seconds
        - approved_count: Number of approved steps
        - rejected_count: Number of rejected steps
        """
        if not response.success or not response.result:
            return {}
        
        result = response.result
        if isinstance(result, dict):
            steps = result.get('steps', [])
            approved = [s for s in steps if isinstance(s, dict) and s.get('status') == 'completed']
            rejected = [s for s in steps if isinstance(s, dict) and s.get('status') == 'failed']
            
            return {
                'events_fired': result.get('events_fired', 0),
                'execution_time': result.get('execution_time', 0.0),
                'approved_count': len(approved),
                'rejected_count': len(rejected),
                'total_steps': len(steps),
                'benchmark_mode': result.get('benchmark_mode', False)
            }
        
        return {}
