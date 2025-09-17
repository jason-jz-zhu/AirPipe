"""
State management for stateful streaming operations.
"""

import json
import pickle
from typing import Any, Dict, Optional, List
from pathlib import Path
from datetime import datetime
import logging
from threading import Lock

logger = logging.getLogger(__name__)


class StateManager:
    """Manage state for streaming operations."""
    
    def __init__(self, backend: str = "memory", config: Optional[Dict] = None):
        """
        Initialize state manager.
        
        Args:
            backend: Storage backend ("memory", "file", "redis")
            config: Backend-specific configuration
        """
        self.backend = backend
        self.config = config or {}
        self.state_lock = Lock()
        
        if backend == "memory":
            self.store = InMemoryStateStore()
        elif backend == "file":
            checkpoint_dir = self.config.get('checkpoint_dir', './checkpoints')
            self.store = FileStateStore(checkpoint_dir)
        elif backend == "redis":
            # Redis support can be added if redis-py is installed
            raise NotImplementedError("Redis backend requires redis-py installation")
        else:
            raise ValueError(f"Unknown backend: {backend}")
            
        logger.info(f"Initialized StateManager with {backend} backend")
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """
        Retrieve state for key.
        
        Args:
            key: State key
            default: Default value if key not found
            
        Returns:
            State value or default
        """
        with self.state_lock:
            return self.store.get(key, default)
    
    def update_state(self, key: str, value: Any) -> None:
        """
        Update state value.
        
        Args:
            key: State key
            value: New value
        """
        with self.state_lock:
            self.store.set(key, value)
            logger.debug(f"Updated state for key: {key}")
    
    def delete_state(self, key: str) -> None:
        """Delete state for key."""
        with self.state_lock:
            self.store.delete(key)
            logger.debug(f"Deleted state for key: {key}")
    
    def checkpoint(self, checkpoint_id: Optional[str] = None) -> str:
        """
        Save state checkpoint.
        
        Args:
            checkpoint_id: Optional checkpoint identifier
            
        Returns:
            Checkpoint ID
        """
        if not checkpoint_id:
            checkpoint_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        with self.state_lock:
            self.store.checkpoint(checkpoint_id)
            logger.info(f"Created checkpoint: {checkpoint_id}")
            
        return checkpoint_id
    
    def restore(self, checkpoint_id: str) -> None:
        """
        Restore from checkpoint.
        
        Args:
            checkpoint_id: Checkpoint identifier
        """
        with self.state_lock:
            self.store.restore(checkpoint_id)
            logger.info(f"Restored from checkpoint: {checkpoint_id}")
    
    def clear(self) -> None:
        """Clear all state."""
        with self.state_lock:
            self.store.clear()
            logger.info("Cleared all state")
    
    def get_all_state(self) -> Dict[str, Any]:
        """Get all state as dictionary."""
        with self.state_lock:
            return self.store.get_all()


class StateStore:
    """Abstract base class for state storage backends."""
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value for key."""
        raise NotImplementedError
    
    def set(self, key: str, value: Any) -> None:
        """Set value for key."""
        raise NotImplementedError
    
    def delete(self, key: str) -> None:
        """Delete key."""
        raise NotImplementedError
    
    def clear(self) -> None:
        """Clear all state."""
        raise NotImplementedError
    
    def checkpoint(self, checkpoint_id: str) -> None:
        """Save checkpoint."""
        raise NotImplementedError
    
    def restore(self, checkpoint_id: str) -> None:
        """Restore from checkpoint."""
        raise NotImplementedError
    
    def get_all(self) -> Dict[str, Any]:
        """Get all state."""
        raise NotImplementedError


class InMemoryStateStore(StateStore):
    """In-memory state storage."""
    
    def __init__(self):
        self.state = {}
        self.checkpoints = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.state[key] = value
    
    def delete(self, key: str) -> None:
        if key in self.state:
            del self.state[key]
    
    def clear(self) -> None:
        self.state.clear()
    
    def checkpoint(self, checkpoint_id: str) -> None:
        # Deep copy state for checkpoint
        import copy
        self.checkpoints[checkpoint_id] = copy.deepcopy(self.state)
    
    def restore(self, checkpoint_id: str) -> None:
        if checkpoint_id not in self.checkpoints:
            raise ValueError(f"Checkpoint not found: {checkpoint_id}")
        
        import copy
        self.state = copy.deepcopy(self.checkpoints[checkpoint_id])
    
    def get_all(self) -> Dict[str, Any]:
        return self.state.copy()


class FileStateStore(StateStore):
    """File-based state storage."""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.checkpoint_dir / "current_state.pkl"
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"Error loading state: {e}")
                return {}
        return {}
    
    def _save_state(self) -> None:
        """Save state to file."""
        try:
            with open(self.state_file, 'wb') as f:
                pickle.dump(self.state, f)
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.state[key] = value
        self._save_state()
    
    def delete(self, key: str) -> None:
        if key in self.state:
            del self.state[key]
            self._save_state()
    
    def clear(self) -> None:
        self.state.clear()
        self._save_state()
    
    def checkpoint(self, checkpoint_id: str) -> None:
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{checkpoint_id}.pkl"
        
        try:
            with open(checkpoint_file, 'wb') as f:
                pickle.dump(self.state, f)
            
            # Also save metadata
            metadata_file = self.checkpoint_dir / f"checkpoint_{checkpoint_id}.json"
            metadata = {
                'checkpoint_id': checkpoint_id,
                'timestamp': datetime.now().isoformat(),
                'num_keys': len(self.state)
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error creating checkpoint: {e}")
            raise
    
    def restore(self, checkpoint_id: str) -> None:
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{checkpoint_id}.pkl"
        
        if not checkpoint_file.exists():
            raise ValueError(f"Checkpoint file not found: {checkpoint_file}")
        
        try:
            with open(checkpoint_file, 'rb') as f:
                self.state = pickle.load(f)
            self._save_state()
        except Exception as e:
            logger.error(f"Error restoring checkpoint: {e}")
            raise
    
    def get_all(self) -> Dict[str, Any]:
        return self.state.copy()


class StreamingContext:
    """Context for stateful streaming operations."""
    
    def __init__(self):
        """Initialize streaming context."""
        self.watermark = None  # Event time watermark
        self.processing_time = None  # Current processing time
        self.event_time = None  # Current event time
        self.batch_id = None  # Current batch identifier
        self.window = None  # Current window if windowing is active
        self.metadata = {}  # Additional metadata
        
    def update_watermark(self, timestamp: datetime) -> None:
        """Update event time watermark."""
        if self.watermark is None or timestamp > self.watermark:
            self.watermark = timestamp
            logger.debug(f"Updated watermark to: {timestamp}")
    
    def set_batch_context(self, batch_id: str, processing_time: datetime) -> None:
        """Set context for current batch."""
        self.batch_id = batch_id
        self.processing_time = processing_time
    
    def set_window_context(self, window_start: datetime, window_end: datetime) -> None:
        """Set context for current window."""
        self.window = {
            'start': window_start,
            'end': window_end,
            'duration': (window_end - window_start).total_seconds()
        }
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to context."""
        self.metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata from context."""
        return self.metadata.get(key, default)
    
    def clear(self) -> None:
        """Clear context."""
        self.watermark = None
        self.processing_time = None
        self.event_time = None
        self.batch_id = None
        self.window = None
        self.metadata.clear()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary."""
        return {
            'watermark': self.watermark.isoformat() if self.watermark else None,
            'processing_time': self.processing_time.isoformat() if self.processing_time else None,
            'event_time': self.event_time.isoformat() if self.event_time else None,
            'batch_id': self.batch_id,
            'window': self.window,
            'metadata': self.metadata
        }


class StatefulProcessor:
    """Helper for stateful processing in streaming pipelines."""
    
    def __init__(self, state_manager: StateManager):
        """
        Initialize stateful processor.
        
        Args:
            state_manager: State manager instance
        """
        self.state_manager = state_manager
        self.context = StreamingContext()
    
    def update_and_get(self, key: str, update_func: 'Callable', default: Any = None) -> Any:
        """
        Update state and return new value atomically.
        
        Args:
            key: State key
            update_func: Function to update state (takes current value, returns new value)
            default: Default value if key doesn't exist
            
        Returns:
            Updated value
        """
        current = self.state_manager.get_state(key, default)
        new_value = update_func(current)
        self.state_manager.update_state(key, new_value)
        return new_value
    
    def increment_counter(self, key: str, increment: int = 1) -> int:
        """
        Increment a counter.
        
        Args:
            key: Counter key
            increment: Amount to increment
            
        Returns:
            New counter value
        """
        return self.update_and_get(key, lambda x: (x or 0) + increment, 0)
    
    def append_to_list(self, key: str, value: Any, max_size: Optional[int] = None) -> List[Any]:
        """
        Append to a list in state.
        
        Args:
            key: List key
            value: Value to append
            max_size: Maximum list size (removes oldest if exceeded)
            
        Returns:
            Updated list
        """
        def update_list(current):
            lst = current or []
            lst.append(value)
            if max_size and len(lst) > max_size:
                lst = lst[-max_size:]  # Keep most recent
            return lst
        
        return self.update_and_get(key, update_list, [])