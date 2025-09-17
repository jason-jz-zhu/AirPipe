"""
Mermaid diagram generator for DAG visualization.
"""

from typing import Dict, Any, List
from airpipe.core.base_visualizer import BaseDAGVisualizer


class MermaidDAGVisualizer(BaseDAGVisualizer):
    """Generate Mermaid diagram syntax for DAG visualization."""
    
    def visualize(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate Mermaid diagram syntax.
        
        Args:
            dag_structure: Dictionary with nodes, edges, and execution order
            
        Returns:
            Mermaid diagram syntax string
        """
        nodes = dag_structure.get('nodes', [])
        edges = dag_structure.get('edges', [])
        
        if not nodes:
            return "graph LR\n    Empty[No tasks defined]"
        
        lines = []
        lines.append("```mermaid")
        lines.append("graph TD")
        
        # Define nodes with their types
        for node in nodes:
            node_id = node['id']
            node_type = node.get('type', 'unknown')
            
            # Create safe node ID for Mermaid (replace spaces and special chars)
            safe_id = node_id.replace(' ', '_').replace('-', '_')
            
            # Choose node shape based on type
            if node_type == 'extractor':
                lines.append(f"    {safe_id}([{node_id}]):::extractor")
            elif node_type == 'transformer':
                lines.append(f"    {safe_id}[{node_id}]:::transformer")
            elif node_type == 'loader':
                lines.append(f"    {safe_id}[/{node_id}/]:::loader")
            else:
                lines.append(f"    {safe_id}[{node_id}]")
        
        lines.append("")
        
        # Add edges
        for edge in edges:
            from_id = edge['from'].replace(' ', '_').replace('-', '_')
            to_id = edge['to'].replace(' ', '_').replace('-', '_')
            lines.append(f"    {from_id} --> {to_id}")
        
        lines.append("")
        
        # Add styling
        lines.append("    classDef extractor fill:#e1f5fe,stroke:#01579b,stroke-width:2px")
        lines.append("    classDef transformer fill:#fff3e0,stroke:#e65100,stroke-width:2px")
        lines.append("    classDef loader fill:#f3e5f5,stroke:#4a148c,stroke-width:2px")
        
        lines.append("```")
        
        # Add artifact information as a separate section
        lines.append("")
        lines.append("### Artifact Flow")
        lines.append("")
        
        for node in nodes:
            if node.get('produces') or node.get('consumes'):
                lines.append(f"- **{node['id']}**")
                if node.get('consumes'):
                    consumes = node['consumes']
                    if isinstance(consumes, list):
                        consumes = ', '.join(consumes)
                    lines.append(f"  - Consumes: `{consumes}`")
                if node.get('produces'):
                    lines.append(f"  - Produces: `{node['produces']}`")
        
        return '\n'.join(lines)
    
    def generate_flowchart(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate a flowchart-style Mermaid diagram.
        
        Args:
            dag_structure: Dictionary with nodes and edges
            
        Returns:
            Flowchart-style Mermaid diagram
        """
        nodes = dag_structure.get('nodes', [])
        edges = dag_structure.get('edges', [])
        
        lines = []
        lines.append("```mermaid")
        lines.append("flowchart LR")
        
        # Group nodes by type
        extractors = []
        transformers = []
        loaders = []
        
        for node in nodes:
            node_id = node['id']
            safe_id = node_id.replace(' ', '_').replace('-', '_')
            node_type = node.get('type', 'unknown')
            
            if node_type == 'extractor':
                extractors.append(safe_id)
                lines.append(f"    {safe_id}[📥 {node_id}]")
            elif node_type == 'transformer':
                transformers.append(safe_id)
                lines.append(f"    {safe_id}[🔄 {node_id}]")
            elif node_type == 'loader':
                loaders.append(safe_id)
                lines.append(f"    {safe_id}[📤 {node_id}]")
            else:
                lines.append(f"    {safe_id}[{node_id}]")
        
        lines.append("")
        
        # Add edges with labels if artifacts are involved
        for edge in edges:
            from_id = edge['from'].replace(' ', '_').replace('-', '_')
            to_id = edge['to'].replace(' ', '_').replace('-', '_')
            
            # Find if there's an artifact flowing between these nodes
            from_node = next((n for n in nodes if n['id'] == edge['from']), None)
            to_node = next((n for n in nodes if n['id'] == edge['to']), None)
            
            if from_node and to_node:
                if from_node.get('produces') and to_node.get('consumes'):
                    # Check if the artifact matches
                    produces = from_node.get('produces')
                    consumes = to_node.get('consumes')
                    if isinstance(consumes, list) and produces in consumes:
                        lines.append(f"    {from_id} -->|{produces}| {to_id}")
                    elif produces == consumes:
                        lines.append(f"    {from_id} -->|{produces}| {to_id}")
                    else:
                        lines.append(f"    {from_id} --> {to_id}")
                else:
                    lines.append(f"    {from_id} --> {to_id}")
            else:
                lines.append(f"    {from_id} --> {to_id}")
        
        # Add subgraphs for organization
        if extractors or transformers or loaders:
            lines.append("")
            
            if extractors:
                lines.append("    subgraph Extractors")
                for e in extractors:
                    lines.append(f"        {e}")
                lines.append("    end")
            
            if transformers:
                lines.append("    subgraph Transformers")
                for t in transformers:
                    lines.append(f"        {t}")
                lines.append("    end")
            
            if loaders:
                lines.append("    subgraph Loaders")
                for l in loaders:
                    lines.append(f"        {l}")
                lines.append("    end")
        
        lines.append("```")
        
        return '\n'.join(lines)
    
    def generate_gantt(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate a Gantt-style chart showing execution stages.
        
        Args:
            dag_structure: Dictionary with execution order
            
        Returns:
            Gantt-style Mermaid diagram
        """
        execution_order = dag_structure.get('execution_order', [])
        
        if not execution_order:
            return "No execution order available"
        
        lines = []
        lines.append("```mermaid")
        lines.append("gantt")
        lines.append("    title Pipeline Execution Stages")
        lines.append("    dateFormat HH:mm:ss")
        lines.append("    axisFormat %H:%M:%S")
        lines.append("")
        
        for i, stage in enumerate(execution_order, 1):
            if isinstance(stage, list):
                lines.append(f"    section Stage {i}")
                for task in stage:
                    lines.append(f"    {task} :active, {i}s, 1s")
            else:
                lines.append(f"    section Stage {i}")
                lines.append(f"    {stage} :active, {i}s, 1s")
        
        lines.append("```")
        
        return '\n'.join(lines)