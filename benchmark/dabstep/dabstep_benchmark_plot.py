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
DABStep Benchmark Plotting - Publication-Quality Figures
Creates comprehensive visualizations for paper results section
"""
import json
import argparse
from pathlib import Path
from collections import defaultdict, Counter
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')

# Set style for publication-quality plots
plt.style.use('seaborn-v0_8-paper')
sns.set_palette("husl")
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['legend.fontsize'] = 9
plt.rcParams['figure.titlesize'] = 14


class DABStepPlotter:
    """Generate publication-quality plots from DABStep benchmark results"""

    def __init__(self, results_path: str):
        """
        Initialize plotter with results directory
        
        Args:
            results_path: Path to superinference_results_YYYYMMDD_HHMMSS directory
        """
        self.results_dir = Path(results_path)
        if not self.results_dir.exists():
            raise ValueError(f"Results directory not found: {results_path}")
        
        print(f"📂 Loading results from: {self.results_dir}")
        
        # Load all checkpoint data
        self.chunks_data = self._load_all_chunks()
        
        # Load ground truth BEFORE creating dataframe (needed for gt comparison)
        self.ground_truth = self._load_ground_truth()
        
        # Now create dataframe with ground truth available
        self.df = self._create_dataframe()
        
        # Augment with debug artifacts metrics if available
        dbg_df = self._load_debug_artifacts_metrics()
        if not dbg_df.empty:
            print(f"  📊 Loaded debug artifacts: {len(dbg_df)} tasks with metrics")
            if 'rounds' in dbg_df.columns:
                dbg_rounds_nonzero = (dbg_df['rounds'] > 0).sum()
                print(f"     Debug artifacts rounds: {dbg_rounds_nonzero}/{len(dbg_df)} tasks have rounds > 0")
            try:
                # Merge carefully - include ALL columns from debug artifacts
                # Use suffixes to handle conflicts, especially for rounds
                # CRITICAL: Always merge rounds from debug artifacts (they have the real data)
                self.df = self.df.merge(dbg_df, on='task_id', how='left', suffixes=('', '_dbg'))
                
                # CRITICAL: Prefer debug artifacts rounds over checkpoint rounds
                # Debug artifacts have the actual rounds data, checkpoint may have 0
                if 'rounds_dbg' in self.df.columns:
                    if 'rounds' in self.df.columns:
                        # Use dbg rounds where available (even if original is 0)
                        # Debug artifacts are the source of truth for rounds
                        mask = self.df['rounds_dbg'].notna()
                        self.df.loc[mask, 'rounds'] = self.df.loc[mask, 'rounds_dbg']
                    else:
                        # If rounds doesn't exist, create it from rounds_dbg
                        self.df['rounds'] = self.df['rounds_dbg']
                    self.df = self.df.drop(columns=['rounds_dbg'])
                
                # Also handle avg_eig from debug artifacts (needed for plot_53)
                if 'avg_eig_per_event_bits_dbg' in self.df.columns:
                    if 'avg_eig' in self.df.columns:
                        # Prefer debug artifacts avg_eig
                        mask = self.df['avg_eig_per_event_bits_dbg'].notna() & (self.df['avg_eig_per_event_bits_dbg'] > 0)
                        self.df.loc[mask, 'avg_eig'] = self.df.loc[mask, 'avg_eig_per_event_bits_dbg']
                    self.df = self.df.drop(columns=['avg_eig_per_event_bits_dbg'])
                
                # Clean up other _dbg columns that were duplicates (keep only if they add value)
                dbg_cols_to_drop = [col for col in self.df.columns if col.endswith('_dbg')]
                if dbg_cols_to_drop:
                    self.df = self.df.drop(columns=dbg_cols_to_drop)
            except Exception as e:
                print(f"  ⚠️  Failed to merge debug metrics: {e}")
        
        # Ensure critical columns exist with defaults
        if 'rounds' not in self.df.columns:
            print("  ⚠️  'rounds' column missing, creating from supinf_rounds or defaulting to 0")
            if 'supinf_rounds' in self.df.columns:
                self.df['rounds'] = self.df['supinf_rounds'].fillna(0).astype(int)
            else:
                self.df['rounds'] = 0
        
        if 'verifier_calls' not in self.df.columns:
            if 'supinf_verifier_calls' in self.df.columns:
                self.df['verifier_calls'] = self.df['supinf_verifier_calls'].fillna(0).astype(int)
            else:
                self.df['verifier_calls'] = 0
        
        if 'backtracks' not in self.df.columns:
            if 'supinf_backtracks' in self.df.columns:
                self.df['backtracks'] = self.df['supinf_backtracks'].fillna(0).astype(int)
            else:
                self.df['backtracks'] = 0
        
        # CRITICAL: Create filtered dataframe excluding false positives and false negatives
        # This shows clean system behavior patterns
        if self.df['correct_gt'].notna().any():
            self.df_filtered = self.df[
                ((self.df['correct_internal'] == True) & (self.df['correct_gt'] == True)) |  # True Positives
                ((self.df['correct_internal'] == False) & (self.df['correct_gt'] == False))   # True Negatives
            ].copy()
            
            fp_count = ((self.df['correct_internal'] == True) & (self.df['correct_gt'] == False)).sum()
            fn_count = ((self.df['correct_internal'] == False) & (self.df['correct_gt'] == True)).sum()
            
            print(f"🔍 Filtering: {len(self.df)} total → {len(self.df_filtered)} clean (TP+TN)")
            print(f"   ✅ True Positives (TP): {((self.df['correct_internal'] == True) & (self.df['correct_gt'] == True)).sum()}")
            print(f"   ✅ True Negatives (TN): {((self.df['correct_internal'] == False) & (self.df['correct_gt'] == False)).sum()}")
            print(f"   ❌ Filtered False Positives (FP): {fp_count}")
            print(f"   ❌ Filtered False Negatives (FN): {fn_count}")
        else:
            # No ground truth - use all data
            self.df_filtered = self.df.copy()
            print(f"⚠️  No ground truth - using all {len(self.df)} tasks (unfiltered)")
        
        # Create output directory for plots
        self.plot_dir = self.results_dir / 'publication_plots'
        self.plot_dir.mkdir(exist_ok=True)
        
        # Load submission metadata for model information
        self.model_family, self.model_name, self.inference_model = self._load_submission_metadata()
        
        print(f"✅ Loaded {len(self.df)} tasks from {len(self.chunks_data)} chunks")
        print(f"📊 Plots will be saved to: {self.plot_dir}")
    
    def _load_submission_metadata(self) -> tuple[str, str, str]:
        """Load model family, model name, and inference_model from submission_metadata.json.
        
        Returns:
            tuple: (model_family, model_name, inference_model) - defaults if not found
        """
        metadata_path = self.results_dir / 'huggingface_submission' / 'submission_metadata.json'
        
        if not metadata_path.exists():
            print(f"  ⚠️  submission_metadata.json not found at {metadata_path}, using defaults")
            return ('Gemini', 'Gemini 2.5 Pro', 'gemini-2.5-pro')
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Extract model information
            model_name_raw = metadata.get('model_name', '')
            inference_model = metadata.get('technical_details', {}).get('inference_model', 'gemini-2.5-pro')
            
            # Parse model name (e.g., "SuperInference-Gemini-2.5-Pro" -> "Gemini 2.5 Pro")
            # or use inference_model (e.g., "gemini-2.5-pro" -> "Gemini 2.5 Pro")
            model_family = 'Gemini'  # default
            model_name = 'Gemini 2.5 Pro'  # default
            
            # Try to extract from model_name first
            if model_name_raw:
                # Remove "SuperInference-" prefix if present
                name_parts = model_name_raw.replace('SuperInference-', '').split('-')
                if len(name_parts) >= 1:
                    # First part is usually the family
                    family_raw = name_parts[0]
                    model_family = family_raw.capitalize()
                    
                    # Reconstruct model name (e.g., ["Gemini", "2.5", "Pro"] -> "Gemini 2.5 Pro")
                    if len(name_parts) > 1:
                        model_name = ' '.join(name_parts).title()
                    else:
                        model_name = model_family
            
            # Fallback to inference_model if model_name didn't work
            if inference_model and model_name == 'Gemini 2.5 Pro':
                # Parse "gemini-2.5-pro" -> "Gemini 2.5 Pro"
                parts = inference_model.split('-')
                if len(parts) >= 1:
                    model_family = parts[0].capitalize()
                    if len(parts) > 1:
                        model_name = ' '.join(parts).title()
                    else:
                        model_name = model_family
            
            print(f"  📋 Model info from metadata: {model_family} / {model_name} / {inference_model}")
            return (model_family, model_name, inference_model)
            
        except Exception as e:
            print(f"  ⚠️  Failed to load submission_metadata.json: {e}, using defaults")
            return ('Gemini', 'Gemini 2.5 Pro', 'gemini-2.5-pro')
    
    def _load_all_chunks(self) -> List[Dict]:
        """Load data from all chunk checkpoints with robust error handling"""
        chunks = []
        for chunk_num in range(1, 10):  # Support up to 10 chunks
            checkpoint = self.results_dir / f'chunk{chunk_num}' / 'checkpoint_superinference.json'
            if checkpoint.exists():
                try:
                    with open(checkpoint, 'r', encoding='utf-8', errors='replace') as f:
                        # Try to load JSON
                        content = f.read()
                        
                        # Check for common JSON issues
                        if not content.strip():
                            print(f"  ⚠️  Chunk {chunk_num}: Empty file, skipping")
                            continue
                        
                        # Try to parse
                        try:
                            data = json.loads(content)
                        except json.JSONDecodeError as e:
                            print(f"  ⚠️  Chunk {chunk_num}: JSON parse error at position {e.pos}")
                            print(f"      Error: {str(e)[:100]}")
                            print(f"      Attempting repair...")
                            
                            # Try to salvage: find last valid JSON object
                            # Look for the last complete task entry
                            lines = content.split('\n')
                            
                            # Find where the array starts
                            if content.strip().startswith('['):
                                # Try to find last complete object
                                last_complete = content.rfind('},')
                                if last_complete > 0:
                                    # Truncate to last complete object and close array
                                    repaired = content[:last_complete+1] + '\n]'
                                    try:
                                        data = json.loads(repaired)
                                        print(f"      ✅ Repaired: Recovered {len(data)} tasks (some may be lost)")
                                    except:
                                        print(f"      ❌ Repair failed, skipping chunk {chunk_num}")
                                        continue
                                else:
                                    print(f"      ❌ Cannot repair, skipping chunk {chunk_num}")
                                    continue
                            else:
                                print(f"      ❌ Not a JSON array, skipping chunk {chunk_num}")
                                continue
                        
                        if not isinstance(data, list):
                            print(f"  ⚠️  Chunk {chunk_num}: Not a list, skipping")
                            continue
                        
                        chunks.append({
                            'chunk_num': chunk_num,
                            'tasks': data,
                            'count': len(data)
                        })
                        print(f"  ✅ Loaded Chunk {chunk_num}: {len(data)} tasks")
                        
                except Exception as e:
                    print(f"  ❌ Chunk {chunk_num}: Failed to load: {e}")
                    continue
        
        if not chunks:
            raise ValueError("No valid chunks loaded! Check your results directory.")
        
        return chunks
    
    def _load_ground_truth(self) -> Dict:
        """Load ground truth from official scores.jsonl if available"""
        # PRIORITY 1: Use scores.jsonl (official DABStep scoring)
        scores_path = self.results_dir / 'scores.jsonl'
        if scores_path.exists():
            print(f"  📊 Loading OFFICIAL scores from: {scores_path}")
            ground_truth = {}
            with open(scores_path) as f:
                for line in f:
                    entry = json.loads(line)
                    task_id = str(entry['task_id'])
                    ground_truth[task_id] = {
                        'score': bool(entry.get('score', False)),  # Official score
                        'level': entry.get('level', 'unknown'),
                        'agent_answer': entry.get('agent_answer', ''),
                        'submission_id': entry.get('submission_id', '')
                    }
            print(f"  ✅ Loaded OFFICIAL scores for {len(ground_truth)} tasks")
            print(f"     Accuracy: {sum(1 for gt in ground_truth.values() if gt['score'])/len(ground_truth)*100:.1f}%")
            return ground_truth
        
        # FALLBACK: Use all_scored_true_sorted.jsonl (for comparison)
        gt_path = self.results_dir.parent.parent / 'results' / 'all_scored_true_sorted.jsonl'
        if not gt_path.exists():
            print("  ⚠️  Ground truth file not found, accuracy metrics will be approximate")
            return {}
        
        ground_truth = {}
        with open(gt_path) as f:
            for line in f:
                entry = json.loads(line)
                task_id = str(entry['task_id'])
                if task_id not in ground_truth:
                    ground_truth[task_id] = {
                        'answers': [],
                        'level': entry.get('level', 'unknown')
                    }
                ground_truth[task_id]['answers'].append(entry['agent_answer'])
        
        print(f"  ✅ Loaded ground truth for {len(ground_truth)} tasks")
        return ground_truth
    
    def _create_dataframe(self) -> pd.DataFrame:
        """Create unified DataFrame from all chunks with robust error handling"""
        all_tasks = []
        
        for chunk in self.chunks_data:
            for task in chunk['tasks']:
                try:
                    # Calculate ground truth correctness if available
                    task_id = str(task.get('task_id', 'unknown'))
                    if task_id == 'unknown':
                        continue  # Skip tasks without ID
                    
                    gt_correct = None
                    
                    if task_id in self.ground_truth:
                        # PRIORITY: Use official score if available (from scores.jsonl)
                        if 'score' in self.ground_truth[task_id]:
                            gt_correct = self.ground_truth[task_id]['score']
                        # FALLBACK: Compare with acceptable answers
                        elif 'answers' in self.ground_truth[task_id]:
                            pred = self._normalize_answer(task.get('predicted_answer', ''))
                            acceptable = [self._normalize_answer(ans) for ans in self.ground_truth[task_id]['answers']]
                            gt_correct = any(pred == acc for acc in acceptable)
                
                    # Extract all available metrics with safe defaults
                    task_data = {
                        'task_id': task_id,
                        'chunk': chunk['chunk_num'],
                        'difficulty': task.get('difficulty', task.get('level', 'unknown')),
                        'correct_internal': bool(task.get('correct', False)),
                        'correct_gt': gt_correct,
                        'response_time': float(task.get('response_time', 0)),
                        'confidence': float(task.get('confidence', 0.5)),
                        
                        # Quality metrics
                        'solution_quality': float(task.get('solution_quality', 0)),
                        'context_relevance': float(task.get('context_relevance', 0)),
                        'reasoning_depth': float(task.get('reasoning_depth', 0)),
                        'code_quality': float(task.get('code_quality', 0)),
                        'domain_expertise': float(task.get('domain_expertise', 0)),
                        
                        # SUPER-INFERENCE metrics
                        'rounds': int(task.get('supinf_rounds', 0)),
                        'verifier_calls': int(task.get('supinf_verifier_calls', 0)),
                        'backtracks': int(task.get('supinf_backtracks', 0)),
                        
                        # Information theory metrics
                        'initial_entropy': float(task.get('initial_entropy', 0)),
                        'final_entropy': float(task.get('final_entropy', 0)),
                        'entropy_reduction': float(task.get('entropy_reduction', 0)),
                        'total_eig': float(task.get('total_eig', 0)),
                        'avg_eig': float(task.get('avg_eig_per_event', 0)),
                        'events_fired': int(task.get('events_fired', 0)),
                        'final_belief': float(task.get('final_belief', 0.5)),
                        
                        # Temperature adaptation
                        'base_temperature': float(task.get('base_temperature', 0.1)),
                        'final_temperature': float(task.get('final_temperature', 0.1)),
                        'temperature_increases': int(task.get('temperature_increases', 0)),
                        
                        # Error tracking
                        'has_error': bool(task.get('error_message', '')),
                        'stopped_due_to': str(task.get('stopped_due_to', 'unknown')),
                        
                        # Store predicted answer for later analysis
                        'predicted_answer': str(task.get('predicted_answer', ''))
                    }
                    
                    # Phase timings if present
                    pt = task.get('phase_timings') or {}
                    if isinstance(pt, dict):
                        for k in ['analysis_time','planning_time','iteration_time','finalization_time']:
                            if k in pt:
                                task_data[k] = float(pt.get(k, 0))
                    if 'total_time' in task:
                        try:
                            task_data['total_time'] = float(task.get('total_time'))
                        except Exception:
                            pass
                    
                    # Temperature history if present
                    th = task.get('temperature_history') or []
                    if isinstance(th, list) and len(th) > 0:
                        try:
                            thf = [float(x) for x in th]
                            task_data['temperature_history_len'] = len(thf)
                            task_data['max_temperature'] = max(thf)
                        except Exception:
                            task_data['temperature_history_len'] = len(th)
                    
                    all_tasks.append(task_data)
                    
                except Exception as e:
                    print(f"  ⚠️  Skipping malformed task in chunk {chunk['chunk_num']}: {e}")
                    continue
        
        return pd.DataFrame(all_tasks)
    
    def _load_debug_artifacts_metrics(self) -> pd.DataFrame:
        """Scan debug_artifacts for per-task metrics and return a DataFrame keyed by task_id.
        Looks for files: chunk*/debug_artifacts/task_*/metrics.json
        Extracts ALL available metrics comprehensively.
        
        ALSO extracts full trajectories and per-round evolution data for plotting.
        """
        rows = []
        trajectories = {}  # Store per-task trajectories separately
        task_evolution = {}  # Store per-round evolution data
        
        for chunk in range(1, 11):
            base = self.results_dir / f'chunk{chunk}' / 'debug_artifacts'
            if not base.exists():
                continue
            for task_dir in base.glob('task_*'):
                try:
                    task_id = str(task_dir.name.split('_')[-1])
                    mfile = task_dir / 'metrics.json'
                    if not mfile.exists():
                        continue
                    with open(mfile, 'r', encoding='utf-8', errors='replace') as f:
                        m = json.load(f)
                    row = {'task_id': task_id}
                    
                    # Basic metrics
                    row['rounds'] = int(m.get('rounds', 0))
                    row['verifier_calls'] = int(m.get('verifier_calls', 0))
                    
                    # Information Theory - comprehensive extraction
                    info_theory = m.get('information_theory') or {}
                    if isinstance(info_theory, dict):
                        row['initial_belief'] = float(info_theory.get('initial_belief', 0.5))
                        row['final_belief'] = float(info_theory.get('final_belief', 0.5))
                        row['initial_entropy_bits'] = float(info_theory.get('initial_entropy_bits', 0))
                        row['final_entropy_bits'] = float(info_theory.get('final_entropy_bits', 0))
                        row['entropy_reduction_bits'] = float(info_theory.get('entropy_reduction_bits', 0))
                        row['total_eig_bits'] = float(info_theory.get('total_eig_bits', 0))
                        row['avg_eig_per_event_bits'] = float(info_theory.get('avg_eig_per_event_bits', 0))
                        row['events_fired'] = int(info_theory.get('events_fired', 0))
                        
                        # Trajectories
                        belief_traj = info_theory.get('belief_trajectory') or []
                        eig_traj = info_theory.get('eig_trajectory') or []
                        if isinstance(belief_traj, list) and belief_traj:
                            trajectories[task_id] = {
                                'belief_trajectory': belief_traj,
                                'eig_trajectory': eig_traj if isinstance(eig_traj, list) else [],
                                'temperature_trajectory': [],
                                'rounds': row['rounds']
                            }
                            
                            # Create per-round evolution data
                            evolution_data = []
                            for round_idx in range(len(belief_traj)):
                                evolution_data.append({
                                    'task_id': task_id,
                                    'round': round_idx,
                                    'belief': belief_traj[round_idx] if round_idx < len(belief_traj) else None,
                                    'eig': eig_traj[round_idx] if round_idx < len(eig_traj) else None,
                                    'temperature': None  # Will be filled from temperature_adaptation
                                })
                            task_evolution[task_id] = evolution_data
                    
                    # Temperature adaptation - comprehensive extraction
                    temp_adapt = m.get('temperature_adaptation') or {}
                    th = temp_adapt.get('temperature_trajectory') or []
                    if isinstance(th, list) and th:
                        try:
                            thf = [float(x) for x in th]
                            row['temperature_history_len'] = len(thf)
                            row['max_temperature'] = max(thf)
                            row['min_temperature'] = min(thf)
                            row['base_temperature'] = float(temp_adapt.get('base_temperature', 0.1))
                            row['final_temperature'] = float(temp_adapt.get('final_temperature', 0.1))
                            row['total_increases'] = int(temp_adapt.get('total_increases', 0))
                            row['max_temperature_reached'] = float(temp_adapt.get('max_temperature_reached', 0))
                            
                            # Update trajectories with temperature
                            if task_id in trajectories:
                                trajectories[task_id]['temperature_trajectory'] = thf
                            
                            # Update evolution data with temperature
                            if task_id in task_evolution:
                                for round_idx, evo in enumerate(task_evolution[task_id]):
                                    if round_idx < len(thf):
                                        evo['temperature'] = thf[round_idx]
                        except Exception:
                            row['temperature_history_len'] = len(th)
                            row['max_temperature'] = None
                    else:
                        row['temperature_history_len'] = 0
                        row['max_temperature'] = None
                        row['base_temperature'] = float(temp_adapt.get('base_temperature', 0.1))
                        row['final_temperature'] = float(temp_adapt.get('final_temperature', 0.1))
                    
                    # Load full_response.json to get code evolution data
                    full_response_file = task_dir / 'full_response.json'
                    if full_response_file.exists():
                        try:
                            with open(full_response_file, 'r', encoding='utf-8', errors='replace') as f:
                                full_resp = json.load(f)
                            
                            # Extract code_evolution data
                            code_evolution = full_resp.get('code_evolution') or []
                            if isinstance(code_evolution, list) and code_evolution:
                                # Update evolution data with code metrics
                                if task_id in task_evolution:
                                    for round_idx, evo in enumerate(task_evolution[task_id]):
                                        # Find matching round in code_evolution
                                        code_evo_round = None
                                        for ce in code_evolution:
                                            if isinstance(ce, dict) and ce.get('round') == round_idx + 1:  # code_evolution uses 1-indexed
                                                code_evo_round = ce
                                                break
                                        
                                        if code_evo_round:
                                            evo['code_length'] = int(code_evo_round.get('code_length', 0))
                                            evo['plan_steps'] = int(code_evo_round.get('plan_steps', 0))
                                            evo['verification'] = str(code_evo_round.get('verification', 'unknown'))
                                            evo['debugger_used'] = bool(code_evo_round.get('debugger_used', False))
                                            evo['has_error'] = code_evo_round.get('original_error') is not None
                                            evo['execution_output'] = code_evo_round.get('execution_output', '')
                                            if 'temperature' in code_evo_round:
                                                temp_val = code_evo_round['temperature']
                                                if evo.get('temperature') is None:
                                                    evo['temperature'] = float(temp_val) if temp_val is not None else None
                                else:
                                    # Create evolution data from code_evolution if metrics.json didn't have trajectories
                                    evolution_data = []
                                    for ce in code_evolution:
                                        if isinstance(ce, dict):
                                            round_num = int(ce.get('round', 0)) - 1  # Convert to 0-indexed
                                            evolution_data.append({
                                                'task_id': task_id,
                                                'round': round_num,
                                                'belief': None,
                                                'eig': None,
                                                'temperature': float(ce.get('temperature', 0)) if ce.get('temperature') is not None else None,
                                                'code_length': int(ce.get('code_length', 0)),
                                                'plan_steps': int(ce.get('plan_steps', 0)),
                                                'verification': str(ce.get('verification', 'unknown')),
                                                'debugger_used': bool(ce.get('debugger_used', False)),
                                                'has_error': ce.get('original_error') is not None
                                            })
                                    if evolution_data:
                                        task_evolution[task_id] = evolution_data
                        except Exception as e:
                            # Silently continue if full_response.json can't be loaded
                            pass
                    
                    rows.append(row)
                except Exception as e:
                    print(f"  ⚠️  Error loading metrics for {task_dir}: {e}")
                    continue
        
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['task_id'])
        # Store trajectories and evolution as class attributes
        self.trajectories = trajectories
        self.task_evolution = task_evolution  # Per-round evolution data
        return df
    
    def _normalize_answer(self, ans):
        """Normalize answer for comparison"""
        if ans is None:
            return ""
        return str(ans).strip().lower().replace('\n', ' ').replace('  ', ' ')
    
    def _get_clean_df(self):
        """Get clean dataframe (TP+TN only) for behavior analysis plots.
        Returns: (df_filtered, correct_column_name)
        """
        return self.df_filtered, 'correct_gt' if self.df_filtered['correct_gt'].notna().any() else 'correct_internal'
    
    def _get_all_tasks_df(self):
        """Get ALL tasks with ground truth (TP+TN+FP+FN) grouped by correct_gt.
        Use this for plots that should show complete workflow including wrong answers.
        
        Classification:
        - Success/Green: correct_gt == True (TP + FN) - all tasks with correct answers
        - Failure/Red: correct_gt == False (TN + FP) - all tasks with wrong answers
        
        Returns: (df_all, correct_column_name)
        """
        if self.ground_truth and self.df['correct_gt'].notna().any():
            df_all = self.df[self.df['correct_gt'].notna()].copy()
            return df_all, 'correct_gt'
        else:
            # Fallback to filtered if no ground truth
            return self._get_clean_df()
    
    def generate_all_plots(self):
        """Generate the 3 required publication-quality plots"""
        print("\n" + "="*80)
        print("📊 GENERATING PUBLICATION-QUALITY PLOTS")
        print("="*80)
        print()
        
        # Generate the 3 required plots
        if self.ground_truth:
            self.plot_11_sota_leaderboard_comparison()
        
        self.plot_53_time_eig_efficiency_comparison()
        self.plot_54_task_completion_code_efficiency()
        
        print(f"\n✅ All plots saved to: {self.plot_dir}")
        print(f"📈 Total plots generated: 3")
    def plot_11_sota_leaderboard_comparison(self):
        """Figure 11: SOTA Leaderboard Comparison (Paper Results vs State-of-the-Art)"""
        # Load SOTA results - adjusted path
        sota_path = self.results_dir.parent.parent.parent / 'results_sota' / 'dabstep_sota.csv'
        if not sota_path.exists():
            print(f"  ⚠️  SOTA CSV not found at {sota_path}, skipping leaderboard plot")
            return
        
        try:
            # Try reading with new column structure
            sota_df = pd.read_csv(sota_path, encoding='utf-8', skipinitialspace=True, skip_blank_lines=True)
            
            # Remove empty rows
            sota_df = sota_df.dropna(how='all')
            
            # Clean column names (remove extra spaces)
            sota_df.columns = sota_df.columns.str.strip()
            
            # Normalize column names for backward compatibility
            if 'Easy Accuracy (%)' in sota_df.columns:
                sota_df.rename(columns={
                    'Easy Accuracy (%)': 'Easy Level Accuracy (%)',
                    'Hard Accuracy (%)': 'Hard Level Accuracy (%)'
                }, inplace=True)
            
            print(f"  📊 Loaded {len(sota_df)} SOTA results from leaderboard")
            print(f"     Columns: {list(sota_df.columns)}")
        except Exception as e:
            print(f"  ❌ Failed to load SOTA CSV: {e}")
            print(f"     Trying alternative parsing...")
            
            # Try reading as plain text and parsing manually
            try:
                with open(sota_path, 'r', encoding='utf-8') as f:
                    lines = [line.strip() for line in f if line.strip()]
                
                if len(lines) < 2:
                    print(f"  ❌ CSV file too short ({len(lines)} lines), skipping")
                    return
                
                # Parse header
                header = lines[0].split(',')
                print(f"  📊 Manual parse: {len(lines)-1} data rows, columns: {header[:3]}")
                
                # Parse data rows
                data_rows = []
                for line in lines[1:]:
                    # Skip empty lines
                    if not line or line == ',,,,,':
                        continue
                    row = line.split(',')
                    if len(row) >= 3:  # At least Agent, Easy%, Hard%
                        data_rows.append(row)
                
                if not data_rows:
                    print(f"  ❌ No valid data rows found, skipping")
                    return
                
                # Create dataframe manually
                sota_df = pd.DataFrame(data_rows, columns=header[:len(data_rows[0])])
                
                # Convert numeric columns
                sota_df['Easy Level Accuracy (%)'] = pd.to_numeric(sota_df['Easy Level Accuracy (%)'], errors='coerce')
                sota_df['Hard Level Accuracy (%)'] = pd.to_numeric(sota_df['Hard Level Accuracy (%)'], errors='coerce')
                
                print(f"  ✅ Manual parse successful: {len(sota_df)} agents")
                
            except Exception as e2:
                print(f"  ❌ Manual parse also failed: {e2}")
                return
        
        # Calculate OUR accuracy from scores.jsonl (OFFICIAL)
        # For SOTA comparison, use ALL tasks with ground truth (not filtered to TP+TN)
        if not self.ground_truth or 'score' not in list(self.ground_truth.values())[0]:
            print("  ⚠️  scores.jsonl not loaded, skipping SOTA comparison")
            return
        
        # Use ALL tasks with ground truth for fair SOTA comparison
        df_with_gt = self.df[self.df['correct_gt'].notna()].copy()
        
        # Calculate accuracy from ALL ground truth data
        easy_tasks = df_with_gt[df_with_gt['difficulty'] == 'easy']
        hard_tasks = df_with_gt[df_with_gt['difficulty'] == 'hard']
        
        our_easy_acc = (easy_tasks['correct_gt'].sum() / len(easy_tasks) * 100) if len(easy_tasks) > 0 else 0
        our_hard_acc = (hard_tasks['correct_gt'].sum() / len(hard_tasks) * 100) if len(hard_tasks) > 0 else 0
        
        # Calculate confusion matrix for debugging
        tp_easy = ((easy_tasks['correct_internal'] == True) & (easy_tasks['correct_gt'] == True)).sum()
        tn_easy = ((easy_tasks['correct_internal'] == False) & (easy_tasks['correct_gt'] == False)).sum()
        fp_easy = ((easy_tasks['correct_internal'] == True) & (easy_tasks['correct_gt'] == False)).sum()
        fn_easy = ((easy_tasks['correct_internal'] == False) & (easy_tasks['correct_gt'] == True)).sum()
        
        tp_hard = ((hard_tasks['correct_internal'] == True) & (hard_tasks['correct_gt'] == True)).sum()
        tn_hard = ((hard_tasks['correct_internal'] == False) & (hard_tasks['correct_gt'] == False)).sum()
        fp_hard = ((hard_tasks['correct_internal'] == True) & (hard_tasks['correct_gt'] == False)).sum()
        fn_hard = ((hard_tasks['correct_internal'] == False) & (hard_tasks['correct_gt'] == True)).sum()
        
        print(f"  📊 Our Results (from scores.jsonl, ALL ground truth tasks):")
        print(f"     Easy: {our_easy_acc:.2f}% (TP={tp_easy}, TN={tn_easy}, FP={fp_easy}, FN={fn_easy}, Total={len(easy_tasks)})")
        print(f"     Hard: {our_hard_acc:.2f}% (TP={tp_hard}, TN={tn_hard}, FP={fp_hard}, FN={fn_hard}, Total={len(hard_tasks)})")
        
        # Identify providers and categorize agents
        print(f"  🎨 Categorizing agents by provider (first word in Model Family):")
        provider_agents = {}
        for idx, row in sota_df.iterrows():
            model_family = str(row.get('Model Family', '')).strip() if 'Model Family' in row else ''
            provider = model_family.split()[0] if model_family else 'Other'
            agent_name = row['Agent'].replace('**', '').strip()
            hard_acc_val = row['Hard Level Accuracy (%)']
            
            if provider not in provider_agents:
                provider_agents[provider] = []
            provider_agents[provider].append((agent_name, hard_acc_val))
        
        # Print by provider
        for provider in sorted(provider_agents.keys(), key=lambda p: max(acc for _, acc in provider_agents[p]), reverse=True):
            agents = provider_agents[provider]
            color_name = provider.lower()
            print(f"     {provider}: {len(agents)} agent(s)")
            for name, acc in sorted(agents, key=lambda x: x[1], reverse=True)[:3]:  # Top 3 per provider
                print(f"       - {name}: {acc:.2f}%")
        
        # Simple clean figure with just the bar chart - wider to accommodate more spacing
        fig, ax = plt.subplots(1, 1, figsize=(22, 9))
        fig.suptitle('SuperInference vs State-of-the-Art (DABStep Hard Tasks Dataset)',
                     fontweight='bold', fontsize=20)
        
        # Sort by hard accuracy (descending for vertical bars)
        sota_df_sorted = sota_df.sort_values('Hard Level Accuracy (%)', ascending=False)
        
        # Prepare data for plotting (top 15 + ours)
        top_n = 15
        if len(sota_df_sorted) > top_n:
            sota_df_plot = sota_df_sorted.head(top_n)
        else:
            sota_df_plot = sota_df_sorted
        
        # Format inference_model for display (e.g., "gemini-2.5-pro" -> "Gemini 2.5-pro")
        # Handle case where inference_model might already contain \n (preserve it)
        if '\n' in self.inference_model:
            # Already has newline, use as-is but capitalize appropriately
            inference_model_display = self.inference_model
        else:
            # Capitalize first letter of each word but keep hyphens
            parts = self.inference_model.split('-')
            inference_model_display = '-'.join([p.capitalize() for p in parts])
        
        # Build Agent name with inference model - keep it simpler (2 lines max)
        # Format: "★ SuperInference\n[Model Name] (This Work)"
        agent_name = '★ SuperInference'
        if inference_model_display:
            # Remove any newlines from inference_model_display and put it on same line as "(This Work)"
            model_clean = inference_model_display.replace('\n', ' ')
            agent_name += '\n' + model_clean + ' (This Work)'
        else:
            agent_name += '\n(This Work)'
        
        # Add our results
        our_entry = pd.DataFrame([{
            'Agent': agent_name,
            'Easy Level Accuracy (%)': our_easy_acc,
            'Hard Level Accuracy (%)': our_hard_acc,
            'Organization': 'Research',
            'Model Family': self.model_family,
            'Model': inference_model_display  # Use inference_model from technical_details
        }])
        
        sota_df_plot = pd.concat([sota_df_plot, our_entry], ignore_index=True)
        sota_df_plot = sota_df_plot.sort_values('Hard Level Accuracy (%)', ascending=False)
        
        # Create vertical bars for Hard tasks
        # Color scheme by provider (first word in Model Family)
        # Extract provider from model family
        provider_colors = {
            'gemini': '#27ae60',    # Green - our family
            'gpt': '#3498db',       # Blue
            'openai': '#3498db',    # Blue (same as GPT)
            'claude': '#9b59b6',    # Purple
            'llama': '#e67e22',     # Orange
            'deepseek': '#e91e63', # Pink (distinct, non-overlapping)
            'qwen': '#f39c12',      # Yellow
            'other': '#95a5a6'      # Gray
        }
        
        # Create x positions with more spacing between bars (1.5x spacing)
        num_bars = len(sota_df_plot)
        x_pos = np.arange(num_bars) * 1.5
        colors = []
        providers = []
        
        for idx, row in sota_df_plot.iterrows():
            agent_name = row['Agent']
            model_family = str(row.get('Model Family', '')).strip() if 'Model Family' in row else ''
            
            # Extract provider (first word) and normalize
            if model_family:
                provider_raw = model_family.split()[0].lower()
                # Normalize variations
                if provider_raw in ['gpt', 'openai']:
                    provider = 'openai'
                elif provider_raw == 'gemini':
                    provider = 'gemini'
                elif 'claude' in provider_raw:
                    provider = 'claude'
                elif 'llama' in provider_raw:
                    provider = 'llama'
                elif 'deepseek' in provider_raw:
                    provider = 'deepseek'
                elif 'qwen' in provider_raw:
                    provider = 'qwen'
                else:
                    provider = 'other'
            else:
                provider = 'other'
            
            providers.append(provider)
            
            if '★ SuperInference' in agent_name:
                # Red for our work (highlighted)
                colors.append('#e74c3c')
            else:
                # Use provider color
                colors.append(provider_colors[provider])
        
        # Reduce bar width to create more visual separation between bars
        bars = ax.bar(x_pos, sota_df_plot['Hard Level Accuracy (%)'],
                      color=colors, alpha=0.8, edgecolor='black', linewidth=0.5, width=0.6)
        
        # Customize x-axis - handle multi-line labels (with \n)
        ax.set_xticks(x_pos)
        # Process agent names: replace **, strip, and ensure \n is preserved for line breaks
        agent_labels = []
        for agent in sota_df_plot['Agent']:
            agent_clean = agent.replace('**', '').strip()
            # Replace literal \n string with actual newline character if needed
            if '\\n' in agent_clean:
                agent_clean = agent_clean.replace('\\n', '\n')
            agent_labels.append(agent_clean)
        
        ax.set_xticklabels(agent_labels, rotation=45, ha='right', fontsize=14)
        ax.set_ylabel('Hard Accuracy (%)', fontweight='bold', fontsize=16)
        ax.tick_params(axis='y', labelsize=14)
        ax.tick_params(axis='x', labelsize=14)
        ax.grid(axis='y', alpha=0.3)
        # Increase y-axis limit to 100% to accommodate model labels and percentages without overlap
        ax.set_ylim(0, 100)
        
        # Add model labels as arrows/annotations above each bar
        for i, (bar, idx_row) in enumerate(zip(bars, sota_df_plot.iterrows())):
            idx, row = idx_row
            height = bar.get_height()
            
            # Get model name and handle NaN
            model_name = row.get('Model', 'N/A') if 'Model' in row else 'N/A'
            if pd.isna(model_name) or not isinstance(model_name, str):
                model_name = 'N/A'
            
            # Handle newlines in model name (replace with space for display)
            model_name = model_name.replace('\n', ' ')
            
            # Shorten model name for display
            if len(model_name) > 18:
                model_short = model_name[:15] + '...'
            else:
                model_short = model_name
            
            # Add model name with arrow FIRST (closer to bar)
            # Position model name closer to bar, with more spacing
            model_y = height + 4
            
            ax.annotate(model_short,
                       xy=(bar.get_x() + bar.get_width()/2., height),
                       xytext=(bar.get_x() + bar.get_width()/2., model_y),
                       ha='center', va='bottom',
                       fontsize=11, style='italic', color='#2c3e50',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                edgecolor=colors[i], alpha=0.85, linewidth=1.5),
                       arrowprops=dict(arrowstyle='->', lw=1.2, color=colors[i], alpha=0.6))
            
            # Add accuracy value ABOVE the model labels (so it's on top)
            # Position it well above the model name with sufficient spacing
            accuracy_y = height + 10
            ax.text(bar.get_x() + bar.get_width()/2., accuracy_y,
                   f'{row["Hard Level Accuracy (%)"]:.1f}%', 
                   ha='center', va='bottom', fontweight='bold', fontsize=14)
        
        # Highlight our result (find position in x_pos, not dataframe index)
        for i, (idx, row) in enumerate(sota_df_plot.iterrows()):
            if '★ SuperInference' in row['Agent']:
                ax.get_xticklabels()[i].set_color('red')
                ax.get_xticklabels()[i].set_fontweight('bold')
                ax.get_xticklabels()[i].set_fontsize(15)
                break
        
        # Add baseline reference line and provider color legend
        baseline_hard = 3.7  # Llama 3.3 70B baseline
        baseline_line = ax.axhline(y=baseline_hard, color='darkred', linestyle='--', linewidth=2, 
                                   alpha=0.7)
        
        # Build legend dynamically based on providers present
        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        
        unique_providers = list(dict.fromkeys(providers))  # Preserve order
        legend_elements = [
            Patch(facecolor='#e74c3c', edgecolor='black', label='★ SuperInference (This Work)', linewidth=2)
        ]
        
        # Add provider colors
        provider_labels = {
            'gemini': 'Gemini (Google)',
            'openai': 'OpenAI (GPT/o-series)',
            'claude': 'Claude (Anthropic)',
            'llama': 'Llama (Meta)',
            'deepseek': 'DeepSeek',
            'qwen': 'Qwen (Alibaba)',
            'other': 'Other/Unknown'
        }
        
        # Only add providers that are present (exclude gemini since we handle it separately)
        for prov in unique_providers:
            if prov in provider_colors and prov != 'gemini':
                legend_elements.append(
                    Patch(facecolor=provider_colors[prov], edgecolor='black', 
                         label=provider_labels.get(prov, prov.title()))
                )
        
        # Add Gemini separately (lighter green for other Gemini agents)
        if 'gemini' in unique_providers:
            legend_elements.insert(1, Patch(facecolor='#27ae60', edgecolor='black', 
                                           label='Gemini (Google)'))
        
        # Add baseline line
        legend_elements.append(
            Line2D([0], [0], color='darkred', linestyle='--', linewidth=2, 
                  label=f'Llama 3.3 70B Baseline: {baseline_hard}%')
        )
        
        ax.legend(handles=legend_elements, fontsize=14, loc='upper right', 
                 title='Provider / Model Family', title_fontsize=16)
        
        # Calculate our rank - find SuperInference entry flexibly (name may include model)
        sorted_agents = list(sota_df_plot.sort_values('Hard Level Accuracy (%)', ascending=False)['Agent'])
        our_rank_hard = next((i+1 for i, agent in enumerate(sorted_agents) if 'SuperInference' in agent), None)
        if our_rank_hard is None:
            our_rank_hard = len(sorted_agents)  # Fallback if not found
        
        # Calculate improvement vs baseline
        baseline_hard = 3.7
        absolute_improvement = our_hard_acc - baseline_hard
        relative_improvement = (absolute_improvement / baseline_hard * 100) if baseline_hard > 0 else 0
        
        # Count Gemini agents and calculate stats
        gemini_agents_list = []
        for i, (idx, row) in enumerate(sota_df_plot.iterrows()):
            if providers[i] == 'gemini':
                agent_name = row['Agent'].replace('**','').strip()
                hard_acc_val = row['Hard Level Accuracy (%)']
                gemini_agents_list.append((i, agent_name, hard_acc_val))
        
        gemini_agents_sorted = sorted(gemini_agents_list, key=lambda x: x[2], reverse=True)
        our_rank_gemini = next((i+1 for i, (_, name, _) in enumerate(gemini_agents_sorted) if 'SuperInference' in name), None)
        
        # Count how many Gemini agents we beat
        gemini_beaten = sum(1 for _, name, acc in gemini_agents_list if acc < our_hard_acc and 'SuperInference' not in name)
        gemini_total = len([a for a in gemini_agents_list if 'SuperInference' not in a[1]])
        
        # Adjust layout to accommodate multi-line x-axis labels
        plt.tight_layout(rect=[0, 0.15, 1, 0.98])  # More bottom margin for multi-line labels
        output_file = self.plot_dir / 'fig11_sota_leaderboard.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"  ✅ Figure 11 saved: {output_file.name} (Provider-categorized with model details)")
        print(f"     Our Hard Accuracy: {our_hard_acc:.2f}% | Overall Rank: {our_rank_hard}/{len(sota_df_plot)}")
        print(f"     Gemini Rank: {our_rank_gemini}/{len(gemini_agents_list)} | Beat {gemini_beaten}/{gemini_total} Gemini agents")
    
    def plot_53_time_eig_efficiency_comparison(self):
        """Figure 53: Time and EIG Efficiency Comparison
        Combines time efficiency (fig19b) and EIG efficiency (fig27b) for comprehensive efficiency analysis.
        """
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))
        fig.suptitle('Efficiency Analysis: Time and Information Gain per Round', 
                     fontweight='bold', fontsize=20, y=0.98)
        
        # Use all tasks with ground truth (TP+TN+FP+FN) to include all failed tasks
        df, correct_col = self._get_all_tasks_df()
        
        # Determine common rounds range for both panels
        # Debug: Check what rounds data we have
        rounds_stats = df['rounds'].describe()
        print(f"  📊 Rounds data stats: min={rounds_stats['min']:.0f}, max={rounds_stats['max']:.0f}, mean={rounds_stats['mean']:.2f}, non-zero={df[df['rounds'] > 0].shape[0]}/{len(df)}")
        
        df_time = df[df['rounds'] > 0].copy()
        df_eig = df[(df['avg_eig'] > 0) & (df['response_time'] > 0) & (df['rounds'] > 0)].copy()
        
        # Get union of rounds from both datasets
        rounds_time = set(df_time['rounds'].unique())
        rounds_eig = set(df_eig['rounds'].unique()) if len(df_eig) > 0 else set()
        all_rounds = sorted(list(rounds_time.union(rounds_eig)))
        
        if len(all_rounds) == 0:
            print(f"  ⚠️  No rounds data available (all rounds are 0 or missing)")
            print(f"     Total tasks: {len(df)}, Tasks with rounds>0: {len(df_time)}, Tasks with EIG>0: {len(df_eig)}")
            print(f"     This plot requires rounds data from debug_artifacts/metrics.json files")
            print(f"     Skipping efficiency comparison")
            return
        
        min_round = min(all_rounds)
        max_round = max(all_rounds)
        
        # Initialize variables for shared colorbar
        scatter_a = None
        scatter_b = None
        
        # Panel (a): Time Efficiency vs Success Rate (from fig19b)
        df['time_per_round'] = df['response_time'] / df['rounds'].replace(0, 1)
        
        efficiency_by_rounds = df.groupby('rounds').agg({
            'time_per_round': ['mean', 'std'],
            correct_col: 'mean',
            'task_id': 'count'
        }).reset_index()
        efficiency_by_rounds.columns = ['rounds', 'mean_time_per_round', 'std_time_per_round', 
                                        'success_rate', 'count']
        efficiency_by_rounds = efficiency_by_rounds[efficiency_by_rounds['rounds'] > 0]
        
        if len(efficiency_by_rounds) > 0:
            x = efficiency_by_rounds['rounds']
            y = efficiency_by_rounds['mean_time_per_round']
            yerr = efficiency_by_rounds['std_time_per_round']
            colors_scatter = efficiency_by_rounds['success_rate']
            sizes = 100 + (efficiency_by_rounds['count'] / efficiency_by_rounds['count'].max() * 300)
            
            scatter = axes[0].scatter(x, y, c=colors_scatter, s=sizes, cmap='RdYlGn',
                                    edgecolor='black', linewidth=2.5, alpha=0.85, vmin=0, vmax=1,
                                    zorder=3)
            axes[0].errorbar(x, y, yerr=yerr, fmt='none', color='gray', alpha=0.6,
                           capsize=6, capthick=2, linewidth=2, zorder=2)
            
            # Add trend line
            if len(x) > 2:
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)
                x_trend = np.linspace(x.min(), x.max(), 100)
                axes[0].plot(x_trend, p(x_trend), '--', linewidth=3, color='#3498db',
                           alpha=0.7, label='Trend Line', zorder=1)
            
            # Store scatter for shared colorbar (will be created after both panels)
            scatter_a = scatter
            
            axes[0].set_xlabel('Number of Rounds', fontweight='bold', fontsize=16)
            axes[0].set_ylabel('Mean Time per Round (seconds)', fontweight='bold', fontsize=16)
            axes[0].set_title('(a) Time Efficiency vs Success Rate', 
                             fontweight='bold', fontsize=18, pad=15)
            axes[0].tick_params(axis='both', labelsize=14)
            axes[0].grid(True, alpha=0.3, linestyle=':', linewidth=1)
            # Set consistent x-axis limits and ticks
            axes[0].set_xlim(min_round - 0.5, max_round + 0.5)
            axes[0].set_xticks(all_rounds)
            axes[0].xaxis.set_major_locator(plt.MaxNLocator(integer=True))
        
        # Panel (b): EIG Efficiency vs Rounds (from fig27b)
        rounds_efficiency = pd.DataFrame()  # Initialize empty dataframe
        if len(df_eig) > 0:
            df_eig['eig_efficiency'] = df_eig['avg_eig'] / df_eig['response_time']
            
            rounds_efficiency = df_eig.groupby('rounds').agg({
                'eig_efficiency': ['mean', 'std', 'count'],
                correct_col: 'mean'
            }).reset_index()
            rounds_efficiency.columns = ['rounds', 'mean_efficiency', 'std_efficiency', 'count', 'success_rate']
            # Show all rounds with at least 1 task (lowered from 3 to show all available data)
            rounds_efficiency = rounds_efficiency[rounds_efficiency['count'] >= 1]
            
            if len(rounds_efficiency) > 0:
                # Handle NaN std values (for rounds with only 1 task)
                rounds_efficiency['std_efficiency'] = rounds_efficiency['std_efficiency'].fillna(0)
                
                scatter_b = axes[1].scatter(rounds_efficiency['rounds'], rounds_efficiency['mean_efficiency'],
                                        s=rounds_efficiency['count'] * 20, c=rounds_efficiency['success_rate'],
                                        cmap='RdYlGn', vmin=0, vmax=1, alpha=0.7, edgecolor='black',
                                        linewidth=2, zorder=3)
                
                # Add error bars (only for rounds with count >= 2, since std needs at least 2 values)
                rounds_with_error = rounds_efficiency[rounds_efficiency['count'] >= 2]
                if len(rounds_with_error) > 0:
                    axes[1].errorbar(rounds_with_error['rounds'], rounds_with_error['mean_efficiency'],
                                   yerr=rounds_with_error['std_efficiency'], fmt='none', color='gray',
                                   alpha=0.5, capsize=5, capthick=2, linewidth=2, zorder=2)
                
                # Add colorbar to panel (b) - this will be the shared colorbar for both panels
                cbar = plt.colorbar(scatter_b, ax=axes[1], label='Success Rate', shrink=0.8)
                cbar.set_ticks([0, 0.25, 0.5, 0.75, 1.0])
                cbar.set_ticklabels(['0%', '25%', '50%', '75%', '100%'])
                cbar.set_label('Success Rate', fontweight='bold', fontsize=16)
                cbar.ax.tick_params(labelsize=14)
                
                axes[1].set_xlabel('Number of Rounds', fontweight='bold', fontsize=16)
                axes[1].set_ylabel('Mean EIG Efficiency (bits/second)', fontweight='bold', fontsize=16)
                axes[1].set_title('(b) EIG Efficiency vs Rounds', 
                                 fontweight='bold', fontsize=18, pad=15)
                axes[1].tick_params(axis='both', labelsize=14)
                axes[1].grid(True, alpha=0.3, linestyle=':', linewidth=1)
                # Set consistent x-axis limits and ticks (same as panel a)
                axes[1].set_xlim(min_round - 0.5, max_round + 0.5)
                axes[1].set_xticks(all_rounds)
                axes[1].xaxis.set_major_locator(plt.MaxNLocator(integer=True))
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.97])
        out = self.plot_dir / 'fig53_time_eig_efficiency_comparison.png'
        plt.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"  ✅ Figure 53 saved: {out.name} (Time and EIG efficiency comparison)")
    
    def plot_54_task_completion_code_efficiency(self):
        """Figure 54: Task Completion and Code Efficiency Analysis
        Combines task completion tracking (fig39c) and code refinement efficiency (fig40d).
        """
        fig, axes = plt.subplots(1, 2, figsize=(18, 7))
        fig.suptitle('Task Completion Dynamics and Code Refinement Efficiency', 
                     fontweight='bold', fontsize=20, y=0.98)
        
        # Panel (a): Tasks Ended per Round and Cumulative Success Rate (from fig39c)
        if not hasattr(self, 'task_evolution') or not self.task_evolution:
            print("  ⚠️  No task evolution data available, skipping task completion panel")
            axes[0].text(0.5, 0.5, 'No task evolution data available', 
                       ha='center', va='center', transform=axes[0].transAxes, fontsize=12)
            axes[0].set_title('(a) Tasks Ended per Round and Cumulative Success Rate', fontweight='bold', fontsize=14)
        else:
            # Use all tasks with ground truth (TP+TN+FP+FN) to include all failed tasks
            df, correct_col = self._get_all_tasks_df()
            
            # Convert evolution data with outcomes
            evolution_rows = []
            for task_id, evo_list in self.task_evolution.items():
                task_row = df[df['task_id'] == task_id]
                outcome = bool(task_row.iloc[0][correct_col]) if len(task_row) > 0 else None
                for evo in evo_list:
                    evo_copy = evo.copy()
                    evo_copy['outcome'] = outcome
                    evolution_rows.append(evo_copy)
            
            if evolution_rows:
                evo_df = pd.DataFrame(evolution_rows)
                total_unique_tasks = len(evo_df['task_id'].unique())
                task_success_round = {}
                task_final_round = {}
                
                for task_id in evo_df['task_id'].unique():
                    task_evo = evo_df[evo_df['task_id'] == task_id].sort_values('round')
                    final_round = int(task_evo['round'].max())
                    task_final_round[task_id] = final_round
                    
                    outcome = task_evo['outcome'].iloc[0] if len(task_evo) > 0 else None
                    if outcome == True:
                        success_rounds = task_evo[task_evo['belief'].notna() & (task_evo['belief'] >= 0.92)]
                        if len(success_rounds) > 0:
                            task_success_round[task_id] = int(success_rounds.iloc[0]['round'])
                        else:
                            task_success_round[task_id] = final_round
                    else:
                        task_success_round[task_id] = None
                
                max_round = int(evo_df['round'].max())
                cumulative_stats = []
                
                for round_num in range(0, max_round + 1):
                    tasks_ended_this_round = 0
                    success_ended_this_round = 0
                    failed_ended_this_round = 0
                    
                    for task_id in evo_df['task_id'].unique():
                        success_r = task_success_round.get(task_id)
                        final_r = task_final_round.get(task_id)
                        
                        if success_r is not None:
                            if success_r == round_num:
                                tasks_ended_this_round += 1
                                success_ended_this_round += 1
                        else:
                            if final_r == round_num:
                                tasks_ended_this_round += 1
                                failed_ended_this_round += 1
                    
                    # Verify: success_ended + failed_ended should equal tasks_ended
                    if success_ended_this_round + failed_ended_this_round != tasks_ended_this_round:
                        print(f"  ⚠️  Round {round_num}: Mismatch - Success: {success_ended_this_round}, Failed: {failed_ended_this_round}, Total: {tasks_ended_this_round}")
                    
                    succeeded_by_round = sum(1 for success_r in task_success_round.values()
                                           if success_r is not None and success_r <= round_num)
                    cumulative_success_rate = (succeeded_by_round / total_unique_tasks * 100) if total_unique_tasks > 0 else 0
                    
                    # Calculate cumulative failure rate (tasks that failed and ended by this round)
                    failed_by_round = sum(1 for task_id, success_r in task_success_round.items()
                                        if success_r is None and task_final_round.get(task_id, -1) <= round_num)
                    cumulative_failure_rate = (failed_by_round / total_unique_tasks * 100) if total_unique_tasks > 0 else 0
                    
                    cumulative_stats.append({
                        'round': round_num,
                        'tasks_ended': tasks_ended_this_round,
                        'success_ended': success_ended_this_round,
                        'failed_ended': failed_ended_this_round,
                        'cumulative_success_rate': cumulative_success_rate,
                        'cumulative_failure_rate': cumulative_failure_rate
                    })
                
                round_stats = pd.DataFrame(cumulative_stats)
                max_tasks = round_stats['tasks_ended'].max()
                max_success_rate = round_stats['cumulative_success_rate'].max()
                max_failure_rate = round_stats['cumulative_failure_rate'].max()
                max_rate = max(max_success_rate, max_failure_rate)
                
                ax2_twin = axes[0].twinx()
                bars = axes[0].bar(round_stats['round'], round_stats['tasks_ended'],
                                 color='#3498db', alpha=0.6, edgecolor='black', linewidth=2, width=0.6,
                                 label='Tasks Ended This Round')
                # Success rate line (green)
                line_success = ax2_twin.plot(round_stats['round'], round_stats['cumulative_success_rate'],
                                    'o-', linewidth=1.5, markersize=8, label='Cumulative Success Rate', color='#2ecc71')
                # Failure rate line (red)
                line_failure = ax2_twin.plot(round_stats['round'], round_stats['cumulative_failure_rate'],
                                    'o-', linewidth=1.5, markersize=8, label='Cumulative Failure Rate', color='#e74c3c')
                
                axes[0].set_ylim(0, max(total_unique_tasks, max_tasks * 1.1))
                ax2_twin.set_ylim(0, max(100, max_rate * 1.1))
                
                axes[0].set_xlabel('Round', fontweight='bold', fontsize=16)
                axes[0].set_ylabel('Tasks Ended This Round', fontweight='bold', fontsize=16, color='black')
                ax2_twin.set_ylabel('Cumulative Rate (%)', fontweight='bold', fontsize=16, color='black')
                axes[0].set_title('(a) Tasks Ended per Round and Cumulative Success/Failure Rates', 
                                 fontweight='bold', fontsize=18, pad=15)
                axes[0].tick_params(axis='both', labelsize=14, labelcolor='black')
                ax2_twin.tick_params(axis='y', labelsize=14, labelcolor='black')
                axes[0].grid(axis='y', alpha=0.3)
                axes[0].set_xticks(round_stats['round'].unique())
                axes[0].xaxis.set_major_locator(plt.MaxNLocator(integer=True))
                
                # Add legend combining both axes
                lines1, labels1 = axes[0].get_legend_handles_labels()
                lines2, labels2 = ax2_twin.get_legend_handles_labels()
                axes[0].legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=14)
                
                for bar, count in zip(bars, round_stats['tasks_ended']):
                    height = bar.get_height()
                    if height > 0:
                        axes[0].text(bar.get_x() + bar.get_width()/2., height + max(1, height*0.01),
                                   f'{int(count)}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # Panel (b): Code Refinement Efficiency (from fig40d)
        if not hasattr(self, 'task_evolution') or not self.task_evolution:
            axes[1].text(0.5, 0.5, 'No task evolution data available', 
                       ha='center', va='center', transform=axes[1].transAxes, fontsize=14)
            axes[1].set_title('(b) Code Refinement Efficiency', fontweight='bold', fontsize=18, pad=15)
        else:
            # Use all tasks with ground truth (TP+TN+FP+FN) to include all failed tasks
            df, correct_col = self._get_all_tasks_df()
            
            evolution_rows = []
            for task_id, evo_list in self.task_evolution.items():
                task_row = df[df['task_id'] == task_id]
                outcome = bool(task_row.iloc[0][correct_col]) if len(task_row) > 0 else None
                for evo in evo_list:
                    evo_copy = evo.copy()
                    evo_copy['outcome'] = outcome
                    evolution_rows.append(evo_copy)
            
            if evolution_rows:
                evo_df = pd.DataFrame(evolution_rows)
                evo_df_code = evo_df[evo_df['code_length'].notna() & (evo_df['code_length'] > 0)]
                
                if len(evo_df_code) > 0:
                    evo_df_code['code_length_bin'] = pd.cut(evo_df_code['code_length'], bins=10)
                    code_success = evo_df_code.groupby('code_length_bin')['outcome'].agg(['mean', 'count']).reset_index()
                    code_success = code_success[code_success['count'] >= 5]
                    code_success['code_length_mid'] = code_success['code_length_bin'].apply(lambda x: float(x.mid) if pd.notna(x) else np.nan)
                    code_success = code_success[code_success['code_length_mid'].notna()]
                    
                    if len(code_success) > 0:
                        x_vals = code_success['code_length_mid'].values.astype(float)
                        y_vals = (code_success['mean'] * 100).values.astype(float)
                        
                        axes[1].scatter(x_vals, y_vals,
                                      s=code_success['count'].values * 10, alpha=0.6, color='#3498db',
                                      edgecolor='black', linewidth=2, label='Success Rate')
                        
                        # Add trend line
                        if len(x_vals) > 1:
                            z = np.polyfit(x_vals, y_vals, 1)
                            p = np.poly1d(z)
                            x_trend = np.linspace(x_vals.min(), x_vals.max(), 100)
                            axes[1].plot(x_trend, p(x_trend),
                                       "r--", alpha=0.7, linewidth=2, label='Trend')
                        
                        axes[1].set_xlabel('Code Length (characters)', fontweight='bold', fontsize=16)
                        axes[1].set_ylabel('Success Rate (%)', fontweight='bold', fontsize=16)
                        axes[1].set_title('(b) Code Refinement Efficiency (Size vs Success Rate)', 
                                        fontweight='bold', fontsize=18, pad=15)
                        axes[1].tick_params(axis='both', labelsize=14)
                        # Fix legend: reduce marker size to prevent overlap
                        legend = axes[1].legend(fontsize=14, scatterpoints=1, markerscale=0.5)
                        axes[1].grid(True, alpha=0.3)
                else:
                    axes[1].text(0.5, 0.5, 'No code length data available', 
                               ha='center', va='center', transform=axes[1].transAxes, fontsize=14)
                    axes[1].set_title('(b) Code Refinement Efficiency', fontweight='bold', fontsize=18, pad=15)
            else:
                axes[1].text(0.5, 0.5, 'No evolution data available', 
                           ha='center', va='center', transform=axes[1].transAxes, fontsize=14)
                axes[1].set_title('(b) Code Refinement Efficiency', fontweight='bold', fontsize=18, pad=15)
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.97])
        out = self.plot_dir / 'fig54_task_completion_code_efficiency.png'
        plt.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"  ✅ Figure 54 saved: {out.name} (Task completion and code efficiency)")


def main():
    parser = argparse.ArgumentParser(description='Generate publication-quality plots from DABStep results')
    parser.add_argument('results_path', type=str, 
                       help='Path to superinference_results_YYYYMMDD_HHMMSS directory')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Override output directory for plots (default: results_path/publication_plots)')
    
    args = parser.parse_args()
    
    try:
        # Create plotter
        plotter = DABStepPlotter(args.results_path)
        
        # Override output directory if specified
        if args.output_dir:
            plotter.plot_dir = Path(args.output_dir)
            plotter.plot_dir.mkdir(exist_ok=True, parents=True)
        
        # Generate all plots
        plotter.generate_all_plots()
        
        print("\n" + "="*80)
        print("✅ ALL PUBLICATION PLOTS GENERATED SUCCESSFULLY")
        print("="*80)
        print(f"\n📊 Plots saved to: {plotter.plot_dir}")
        print(f"📈 Total figures: 3")
        print("\n🎉 Ready for paper submission!")
        
    except Exception as e:
        print(f"\n❌ Error generating plots: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())

