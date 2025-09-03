"""
Task-based execution system for imperative workflows.
"""

from typing import Callable, Any, Dict, List, Optional, Union
from functools import wraps
from dataclasses import dataclass
from enum import Enum
import inspect
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from airpipe.artifacts.data_artifact import DataArtifact
from airpipe.core.ascii_dag_visualizer import ASCIIDAGVisualizer
from airpipe.core.mermaid_dag_visualizer import MermaidDAGVisualizer


class TaskType(Enum):
    """Type of task based on function signature."""
    EXTRACTOR = "extractor"  # No artifact input
    TRANSFORMER = "transformer"  # Takes artifact(s) as input
    LOADER = "loader"  # Takes artifact, returns None


@dataclass
class Task:
    """Represents a single task in the pipeline."""
    name: str
    func: Callable
    task_type: TaskType
    dependencies: List[str] = None
    produces: Optional[str] = None  # Name of artifact this task produces
    consumes: Optional[Union[str, List[str]]] = None  # Name(s) of artifacts this task consumes
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        # Normalize consumes to list
        if self.consumes is not None and isinstance(self.consumes, str):
            self.consumes = [self.consumes]


class TaskPipeline:
    """
    Task-based pipeline for imperative workflow execution.
    
    This allows developers to write ETL logic as decorated Python functions
    rather than using configuration objects.
    """
    
    def __init__(self, name: str = "task_pipeline", lineage_tracker=None):
        """
        Initialize task pipeline.
        
        Args:
            name: Pipeline name
            lineage_tracker: Optional SplineLineageTracker instance for lineage tracking
        """
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.artifacts: Dict[str, DataArtifact] = {}
        self.named_artifacts: Dict[str, DataArtifact] = {}  # Store artifacts by custom names
        self.logger = logging.getLogger(f"TaskPipeline.{name}")
        self._execution_order: List[str] = []
        self.lineage_tracker = lineage_tracker
        
        # Initialize visualizers
        self._visualizers = {
            'ascii': ASCIIDAGVisualizer(),
            'mermaid': MermaidDAGVisualizer()
        }
        
    def task(self, 
             depends_on: Optional[List[str]] = None,
             produces: Optional[str] = None,
             consumes: Optional[Union[str, List[str]]] = None,
             task_type: Optional[TaskType] = None):
        """
        Enhanced decorator for defining pipeline tasks with explicit dependencies.
        
        Args:
            depends_on: List of task names this task depends on
            produces: Name of the artifact this task produces
            consumes: Name(s) of artifacts this task consumes
            task_type: Optional explicit task type
            
        Returns:
            Decorated function
        """
        def decorator(func: Callable) -> Callable:
            # Infer task type from function signature if not provided
            if task_type is None:
                inferred_type = self._infer_task_type(func)
            else:
                inferred_type = task_type
            
            # Use explicit dependencies if provided, otherwise infer from function signature
            if depends_on is not None:
                task_dependencies = depends_on
            else:
                task_dependencies = self._get_dependencies(func)
            
            # Create task
            task = Task(
                name=func.__name__,
                func=func,
                task_type=inferred_type,
                dependencies=task_dependencies,
                produces=produces,
                consumes=consumes
            )
            
            # Register task
            self.tasks[task.name] = task
            
            @wraps(func)
            def wrapper(*args, **kwargs):
                # This wrapper will be called during workflow execution
                return self._execute_task(task, *args, **kwargs)
            
            # Add task reference to wrapper
            wrapper._task = task
            wrapper._pipeline = self
            
            return wrapper
            
        return decorator
    
    def _infer_task_type(self, func: Callable) -> TaskType:
        """Infer task type from function signature."""
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        
        # Skip 'self' if present
        if params and params[0].name == 'self':
            params = params[1:]
        
        # No parameters or only optional parameters -> Extractor
        if not params or all(p.default != inspect.Parameter.empty for p in params):
            return TaskType.EXTRACTOR
        
        # Check return type annotation
        return_type = sig.return_annotation
        if return_type == type(None) or return_type is None:
            return TaskType.LOADER
        
        # Has parameters and returns something -> Transformer
        return TaskType.TRANSFORMER
    
    def _get_dependencies(self, func: Callable) -> List[str]:
        """Extract task dependencies from function parameters."""
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        
        # Skip 'self' if present
        if params and params[0].name == 'self':
            params = params[1:]
        
        # Parameter names become dependencies
        dependencies = []
        for param in params:
            if param.default == inspect.Parameter.empty:
                # Required parameter is a dependency
                dependencies.append(param.name)
        
        return dependencies
    
    def _execute_task(self, task: Task, *args, **kwargs):
        """Execute a single task."""
        self.logger.info(f"Executing task: {task.name}")
        
        # Track task start with lineage tracker
        if self.lineage_tracker:
            input_artifacts = []
            if task.consumes:
                input_artifacts = task.consumes if isinstance(task.consumes, list) else [task.consumes]
            
            self.lineage_tracker.track_task_start(
                task_name=task.name,
                task_type=task.task_type.value if task.task_type else 'unknown',
                dependencies=task.dependencies or [],
                metadata={'produces': task.produces, 'consumes': task.consumes}
            )
        
        try:
            # Check if function expects no arguments (uses get_artifact internally)
            import inspect
            sig = inspect.signature(task.func)
            params = list(sig.parameters.values())
            
            # If function has no parameters and uses consumes, don't pass arguments
            # (it will use get_artifact internally)
            if not params and task.consumes:
                # Function uses internal get_artifact, don't pass args
                result = task.func()
            elif task.consumes and not args:
                # Function expects arguments, prepare them
                args = []
                for artifact_name in task.consumes:
                    args.append(self.get_artifact(artifact_name))
                result = task.func(*args, **kwargs)
            else:
                # Call with provided arguments
                result = task.func(*args, **kwargs)
            
            # Store result if it's an artifact
            if isinstance(result, DataArtifact):
                self.artifacts[task.name] = result
                
                # If task has a 'produces' name, store with that name too
                if task.produces:
                    self.set_artifact(task.produces, result)
                    self.logger.info(f"Task {task.name} produced artifact: {task.produces}")
                else:
                    self.logger.info(f"Task {task.name} produced artifact: {result.name}")
            
            # Track task completion with lineage tracker
            if self.lineage_tracker:
                input_artifacts = []
                if task.consumes:
                    input_artifacts = task.consumes if isinstance(task.consumes, list) else [task.consumes]
                
                output_artifact = None
                if isinstance(result, DataArtifact):
                    output_artifact = task.produces or result.name
                
                self.lineage_tracker.track_task_complete(
                    task_name=task.name,
                    result=result,
                    input_artifacts=input_artifacts,
                    output_artifact=output_artifact
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Task {task.name} failed: {str(e)}")
            
            # Track task failure with lineage tracker
            if self.lineage_tracker:
                self.lineage_tracker.track_task_complete(
                    task_name=task.name,
                    result=None,
                    input_artifacts=[],
                    output_artifact=None
                )
            
            raise
    
    def create_artifact(self, data: Any, name: str) -> DataArtifact:
        """
        Helper method to create artifacts within tasks.
        
        Args:
            data: The data to wrap
            name: Artifact name
            
        Returns:
            DataArtifact instance
        """
        artifact = DataArtifact(data=data, name=name)
        # Store in named artifacts for easy retrieval
        self.set_artifact(name, artifact)
        
        # Track artifact creation with lineage tracker
        if self.lineage_tracker:
            metadata = {
                'format': str(artifact.metadata.format.value) if artifact.metadata else 'unknown',
                'row_count': artifact.metadata.row_count if artifact.metadata else None,
                'column_count': artifact.metadata.column_count if artifact.metadata else None
            }
            self.lineage_tracker.track_artifact_created(name, data, metadata)
        
        return artifact
    
    def get_artifact(self, name: str) -> DataArtifact:
        """
        Retrieve an artifact by its name.
        
        Args:
            name: The name of the artifact to retrieve
            
        Returns:
            DataArtifact instance
            
        Raises:
            KeyError: If artifact with given name doesn't exist
        """
        if name in self.named_artifacts:
            return self.named_artifacts[name]
        elif name in self.artifacts:
            return self.artifacts[name]
        else:
            raise KeyError(f"Artifact '{name}' not found in pipeline")
    
    def set_artifact(self, name: str, artifact: DataArtifact) -> None:
        """
        Store an artifact with a custom name.
        
        Args:
            name: The name to store the artifact under
            artifact: The DataArtifact to store
        """
        self.named_artifacts[name] = artifact
        self.logger.debug(f"Stored artifact '{name}' in pipeline")
    
    def execute(self, parallel: bool = True, max_workers: int = 4) -> Dict[str, Any]:
        """
        Execute all registered tasks in dependency order.
        
        Args:
            parallel: Whether to run independent tasks in parallel
            max_workers: Maximum number of parallel workers
            
        Returns:
            Execution results
        """
        self.logger.info(f"Executing pipeline: {self.name}")
        
        # Start lineage tracking
        if self.lineage_tracker:
            pipeline_metadata = {
                'total_tasks': len(self.tasks),
                'parallel_execution': parallel,
                'max_workers': max_workers
            }
            self.lineage_tracker.start_pipeline(self.name, pipeline_metadata)
        
        try:
            # Build execution order based on dependencies
            execution_order = self._build_execution_order()
            
            # Execute tasks
            if parallel:
                self._execute_parallel(execution_order, max_workers)
            else:
                self._execute_sequential(execution_order)
            
            # Return results
            results = {
                "pipeline": self.name,
                "tasks_executed": len(execution_order),
                "artifacts_created": len(self.artifacts),
                "artifacts": list(self.artifacts.keys()),
                "status": "completed"
            }
            
            self.logger.info(f"Pipeline complete: {results['tasks_executed']} tasks executed")
            
            # End lineage tracking on success
            if self.lineage_tracker:
                self.lineage_tracker.end_pipeline(success=True)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            
            # End lineage tracking on failure
            if self.lineage_tracker:
                self.lineage_tracker.end_pipeline(success=False, error=str(e))
            
            raise
    
    def _build_execution_order(self) -> List[List[str]]:
        """
        Build execution order based on task dependencies.
        
        Returns:
            List of task groups that can be executed in parallel
        """
        # Simple topological sort
        visited = set()
        order = []
        
        def visit(task_name: str):
            if task_name in visited:
                return
            
            task = self.tasks.get(task_name)
            if task:
                # Visit dependencies first
                for dep in task.dependencies:
                    visit(dep)
                
                visited.add(task_name)
                order.append(task_name)
        
        # Visit all tasks
        for task_name in self.tasks:
            visit(task_name)
        
        # Group tasks that can run in parallel
        groups = []
        remaining = set(order)
        completed = set()
        
        while remaining:
            # Find tasks that can run now
            ready = []
            for task_name in remaining:
                task = self.tasks[task_name]
                if all(dep in completed for dep in task.dependencies):
                    ready.append(task_name)
            
            if ready:
                groups.append(ready)
                completed.update(ready)
                remaining.difference_update(ready)
            else:
                # Circular dependency or error
                raise RuntimeError(f"Cannot resolve dependencies for tasks: {remaining}")
        
        return groups
    
    def _execute_sequential(self, execution_order: List[List[str]]) -> None:
        """Execute tasks sequentially."""
        for group in execution_order:
            for task_name in group:
                task = self.tasks[task_name]
                
                # Prepare arguments from artifacts
                args = []
                for dep in task.dependencies:
                    if dep in self.artifacts:
                        args.append(self.artifacts[dep])
                
                # Execute task
                self._execute_task(task, *args)
    
    def _execute_parallel(self, execution_order: List[List[str]], max_workers: int) -> None:
        """Execute tasks in parallel where possible."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for group in execution_order:
                if len(group) == 1:
                    # Single task, run directly
                    task_name = group[0]
                    task = self.tasks[task_name]
                    
                    args = []
                    for dep in task.dependencies:
                        if dep in self.artifacts:
                            args.append(self.artifacts[dep])
                    
                    self._execute_task(task, *args)
                else:
                    # Multiple tasks, run in parallel
                    futures = {}
                    
                    for task_name in group:
                        task = self.tasks[task_name]
                        
                        args = []
                        for dep in task.dependencies:
                            if dep in self.artifacts:
                                args.append(self.artifacts[dep])
                        
                        future = executor.submit(self._execute_task, task, *args)
                        futures[future] = task_name
                    
                    # Wait for all to complete
                    for future in as_completed(futures):
                        task_name = futures[future]
                        try:
                            future.result()
                        except Exception as e:
                            self.logger.error(f"Task {task_name} failed: {str(e)}")
                            raise
    
    def get_dag_structure(self) -> Dict[str, Any]:
        """
        Get DAG structure for visualization.
        
        Returns:
            Dictionary containing nodes, edges, and execution order
        """
        nodes = []
        edges = []
        
        for task_name, task in self.tasks.items():
            # Node information
            node = {
                'id': task_name,
                'type': task.task_type.value if task.task_type else 'unknown',
                'produces': task.produces,
                'consumes': task.consumes,
                'dependencies': task.dependencies if task.dependencies else []
            }
            nodes.append(node)
            
            # Edge information
            for dep in task.dependencies:
                edges.append({
                    'from': dep,
                    'to': task_name
                })
        
        # Get execution order
        try:
            execution_order = self._build_execution_order()
        except Exception:
            execution_order = []
        
        return {
            'nodes': nodes,
            'edges': edges,
            'execution_order': execution_order,
            'pipeline_name': self.name
        }
    
    def visualize_dag(self, format: str = 'ascii', output_file: Optional[str] = None) -> str:
        """
        Visualize the task DAG.
        
        Args:
            format: Visualization format ('ascii', 'mermaid')
            output_file: Optional file path to save visualization
            
        Returns:
            String representation of the DAG
            
        Raises:
            ValueError: If format is not supported
        """
        if format not in self._visualizers:
            available = ', '.join(self._visualizers.keys())
            raise ValueError(f"Unsupported format '{format}'. Available: {available}")
        
        # Get DAG structure
        dag_structure = self.get_dag_structure()
        
        # Generate visualization
        visualizer = self._visualizers[format]
        visualization = visualizer.visualize(dag_structure)
        
        # Add pipeline name header
        header = f"\n{'=' * 60}\n"
        header += f"Pipeline: {self.name}\n"
        header += f"{'=' * 60}\n\n"
        full_visualization = header + visualization
        
        # Save if output file specified
        if output_file:
            visualizer.save(full_visualization, output_file)
            self.logger.info(f"DAG visualization saved to {output_file}")
        
        return full_visualization
    
    def validate_dag(self) -> bool:
        """
        Validate that the task graph is a valid DAG (no cycles).
        
        Returns:
            True if valid DAG, False if cycles detected
            
        Raises:
            RuntimeError: If cycles are detected (with details)
        """
        # Build adjacency list
        adj_list = {}
        for task_name, task in self.tasks.items():
            adj_list[task_name] = task.dependencies if task.dependencies else []
        
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in adj_list.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        for task_name in self.tasks:
            if task_name not in visited:
                if has_cycle(task_name):
                    raise RuntimeError(f"Cycle detected in task graph involving task: {task_name}")
        
        return True
    
    def get_task_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the pipeline tasks.
        
        Returns:
            Dictionary with task statistics
        """
        dag_structure = self.get_dag_structure()
        nodes = dag_structure['nodes']
        edges = dag_structure['edges']
        
        # Count by type
        type_counts = {}
        for node in nodes:
            task_type = node.get('type', 'unknown')
            type_counts[task_type] = type_counts.get(task_type, 0) + 1
        
        # Calculate complexity metrics
        total_tasks = len(nodes)
        total_dependencies = len(edges)
        
        # Find critical path (longest dependency chain)
        def find_longest_path(node_id: str, visited: set) -> int:
            if node_id in visited:
                return 0
            visited.add(node_id)
            
            children = [e['to'] for e in edges if e['from'] == node_id]
            if not children:
                return 1
            
            max_path = 0
            for child in children:
                path_length = find_longest_path(child, visited.copy())
                max_path = max(max_path, path_length)
            
            return max_path + 1
        
        # Find root tasks
        all_targets = {e['to'] for e in edges}
        roots = [n['id'] for n in nodes if n['id'] not in all_targets]
        
        critical_path_length = 0
        for root in roots:
            path_length = find_longest_path(root, set())
            critical_path_length = max(critical_path_length, path_length)
        
        return {
            'total_tasks': total_tasks,
            'total_dependencies': total_dependencies,
            'task_types': type_counts,
            'root_tasks': roots,
            'critical_path_length': critical_path_length,
            'average_dependencies': total_dependencies / total_tasks if total_tasks > 0 else 0
        }


# Global pipeline instance for convenience
pipeline = TaskPipeline("default")

# Export task decorator
task = pipeline.task