"""
Base classes for DAG visualization.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path


class BaseDAGVisualizer(ABC):
    """Abstract base class for DAG visualizers."""
    
    @abstractmethod
    def visualize(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate visualization from DAG structure.
        
        Args:
            dag_structure: Dictionary containing nodes, edges, and execution order
            
        Returns:
            String representation of the DAG
        """
        pass
    
    def save(self, content: str, filepath: str) -> None:
        """
        Save visualization to file.
        
        Args:
            content: Visualization content to save
            filepath: Path to save file
        """
        output_path = Path(filepath)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def get_task_symbol(self, task_type: str) -> str:
        """
        Get symbol for task type.
        
        Args:
            task_type: Type of task (extractor, transformer, loader)
            
        Returns:
            Symbol representing the task type
        """
        symbols = {
            'extractor': '[E]',
            'transformer': '[T]',
            'loader': '[L]'
        }
        return symbols.get(task_type, '[?]')
    
    def get_task_color(self, task_type: str) -> str:
        """
        Get color for task type (for colored output).
        
        Args:
            task_type: Type of task
            
        Returns:
            Color code or name
        """
        colors = {
            'extractor': 'green',
            'transformer': 'yellow',
            'loader': 'blue'
        }
        return colors.get(task_type, 'white')