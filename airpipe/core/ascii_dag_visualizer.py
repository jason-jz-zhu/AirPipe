"""
ASCII-based DAG visualization for workflows.
"""

from typing import Dict, Any, List, Set, Tuple
from collections import defaultdict, deque
from airpipe.core.base_visualizer import BaseDAGVisualizer


class ASCIIDAGVisualizer(BaseDAGVisualizer):
    """Generate ASCII art representation of task DAG."""
    
    def visualize(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate ASCII art representation of the DAG.
        
        Args:
            dag_structure: Dictionary with nodes, edges, and execution order
            
        Returns:
            ASCII art string representation
        """
        nodes = dag_structure.get('nodes', [])
        edges = dag_structure.get('edges', [])
        execution_order = dag_structure.get('execution_order', [])
        
        if not nodes:
            return "Empty pipeline - no tasks defined"
        
        # Build adjacency lists
        children = defaultdict(list)
        parents = defaultdict(list)
        
        for edge in edges:
            children[edge['from']].append(edge['to'])
            parents[edge['to']].append(edge['from'])
        
        # Find root nodes (no parents)
        roots = [node['id'] for node in nodes if not parents[node['id']]]
        
        # Build the ASCII representation
        lines = []
        lines.append("Task Dependency Graph")
        lines.append("=" * 50)
        lines.append("")
        
        # Add legend
        lines.append("Legend: [E] Extractor  [T] Transformer  [L] Loader")
        lines.append("")
        
        # Create tree representation
        visited = set()
        for root in roots:
            tree_lines = self._build_tree(root, nodes, children, visited)
            lines.extend(tree_lines)
        
        # Add execution order information
        lines.append("")
        lines.append("Execution Order (parallel groups):")
        lines.append("-" * 50)
        for i, group in enumerate(execution_order, 1):
            if isinstance(group, list):
                lines.append(f"Stage {i}: {' | '.join(group)} (parallel)")
            else:
                lines.append(f"Stage {i}: {group}")
        
        # Add artifact flow information if available
        lines.append("")
        lines.append("Artifact Flow:")
        lines.append("-" * 50)
        
        for node in nodes:
            if node.get('produces'):
                lines.append(f"  {node['id']} -> produces: {node['produces']}")
            if node.get('consumes'):
                consumes = node['consumes']
                if isinstance(consumes, list):
                    consumes = ', '.join(consumes)
                lines.append(f"  {node['id']} <- consumes: {consumes}")
        
        return '\n'.join(lines)
    
    def _build_tree(self, node_id: str, nodes: List[Dict], 
                    children: Dict[str, List[str]], visited: Set[str],
                    prefix: str = "", is_last: bool = True, 
                    level: int = 0) -> List[str]:
        """
        Build tree representation of a node and its children.
        
        Args:
            node_id: Current node ID
            nodes: List of all nodes
            children: Adjacency list of children
            visited: Set of visited nodes
            prefix: Prefix for indentation
            is_last: Whether this is the last child
            level: Current depth level
            
        Returns:
            List of lines representing the tree
        """
        if node_id in visited:
            return [f"{prefix}{'└── ' if is_last else '├── '}(→ {node_id})"]
        
        visited.add(node_id)
        
        # Find node details
        node_info = next((n for n in nodes if n['id'] == node_id), None)
        if not node_info:
            return []
        
        # Build current node line
        connector = "└── " if is_last else "├── "
        if level == 0:
            connector = ""
        
        task_type = node_info.get('type', 'unknown')
        symbol = self.get_task_symbol(task_type)
        
        lines = [f"{prefix}{connector}{symbol} {node_id}"]
        
        # Add children
        node_children = children.get(node_id, [])
        if node_children:
            # Update prefix for children
            if level == 0:
                child_prefix = "    "
            else:
                child_prefix = prefix + ("    " if is_last else "│   ")
            
            for i, child in enumerate(node_children):
                is_last_child = (i == len(node_children) - 1)
                child_lines = self._build_tree(
                    child, nodes, children, visited,
                    child_prefix, is_last_child, level + 1
                )
                lines.extend(child_lines)
        
        return lines
    
    def generate_simple_flow(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate a simplified linear flow representation.
        
        Args:
            dag_structure: Dictionary with nodes and edges
            
        Returns:
            Simplified flow string
        """
        nodes = dag_structure.get('nodes', [])
        edges = dag_structure.get('edges', [])
        
        # Build adjacency lists
        children = defaultdict(list)
        parents = defaultdict(list)
        
        for edge in edges:
            children[edge['from']].append(edge['to'])
            parents[edge['to']].append(edge['from'])
        
        # Find root nodes
        roots = [node['id'] for node in nodes if not parents[node['id']]]
        
        # Build flow
        lines = []
        for root in roots:
            flow = self._build_flow_chain(root, children, set())
            lines.append(' → '.join(flow))
        
        return '\n'.join(lines)
    
    def _build_flow_chain(self, node_id: str, 
                         children: Dict[str, List[str]], 
                         visited: Set[str]) -> List[str]:
        """
        Build a flow chain from a node.
        
        Args:
            node_id: Starting node
            children: Adjacency list
            visited: Set of visited nodes
            
        Returns:
            List of nodes in flow order
        """
        if node_id in visited:
            return [f"({node_id})"]
        
        visited.add(node_id)
        chain = [node_id]
        
        node_children = children.get(node_id, [])
        if len(node_children) == 1:
            # Linear flow
            chain.extend(self._build_flow_chain(node_children[0], children, visited))
        elif len(node_children) > 1:
            # Branching
            branches = []
            for child in node_children:
                child_chain = self._build_flow_chain(child, children, visited)
                branches.append(' → '.join(child_chain))
            
            if branches:
                chain.append(f"[{' | '.join(branches)}]")
        
        return chain
    
    def generate_compact(self, dag_structure: Dict[str, Any]) -> str:
        """
        Generate a compact representation of the DAG.
        
        Args:
            dag_structure: Dictionary with nodes and edges
            
        Returns:
            Compact string representation
        """
        nodes = dag_structure.get('nodes', [])
        edges = dag_structure.get('edges', [])
        
        lines = []
        lines.append("Pipeline Tasks:")
        
        # Group by task type
        extractors = [n for n in nodes if n.get('type') == 'extractor']
        transformers = [n for n in nodes if n.get('type') == 'transformer']
        loaders = [n for n in nodes if n.get('type') == 'loader']
        
        if extractors:
            lines.append(f"  Extractors: {', '.join(n['id'] for n in extractors)}")
        if transformers:
            lines.append(f"  Transformers: {', '.join(n['id'] for n in transformers)}")
        if loaders:
            lines.append(f"  Loaders: {', '.join(n['id'] for n in loaders)}")
        
        lines.append("")
        lines.append("Dependencies:")
        
        for edge in edges:
            lines.append(f"  {edge['from']} → {edge['to']}")
        
        return '\n'.join(lines)