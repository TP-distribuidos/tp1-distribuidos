import json
from common.StateInterpreterInterface import StateInterpreterInterface
from typing import Any, Dict

class ConsumerStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for consumer data, handling batch information.
    Implements the StateInterpreterInterface.
    """
    
    def __init__(self, status_completed="COMPLETED_"):
        """
        Initialize the ConsumerStateInterpreter.
        
        Args:
            status_completed (str): The status string that indicates a completed operation
        """
        self.STATUS_COMPLETED = status_completed
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        Args:
            data: Data to format (consumer message data)
            
        Returns:
            str: Formatted data ready for storage
        """
        # Special handling for checkpoint data
        if isinstance(data, dict) and "data" in data and "processed_logs" in data:
            # This is a checkpoint manifest, serialize it as a single JSON object
            return json.dumps(data)
        
        # For regular log entries, format as JSON strings, one per line
        formatted_lines = []
        
        # Handle dictionaries (most common case)
        if isinstance(data, dict):
            for key, value in data.items():
                formatted_lines.append(json.dumps({key: value}))
        # Handle lists of items
        elif isinstance(data, list):
            for item in data:
                formatted_lines.append(json.dumps(item))
        # Handle single items
        else:
            formatted_lines.append(json.dumps(data))
            
        return "\n".join(formatted_lines)

    def parse_data(self, content: str) -> Any:
        """
        Parse stored data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed data as a dictionary
        """
        # First try to parse as a single JSON object (checkpoint case)
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "data" in data and "processed_logs" in data:
                # This is a checkpoint, return the entire checkpoint data
                return data
        except json.JSONDecodeError:
            # Not a single JSON object, continue with line-by-line parsing
            pass
        
        # Normal line-by-line parsing for log entries
        parsed_data = {}
        batch_id = None
        
        # Split content into lines and process each one
        lines = content.strip().split("\n")
        for line in lines:
            if not line.strip():
                continue
                
            try:
                item = json.loads(line)
                
                if isinstance(item, dict):
                    # Extract batch ID if present
                    if "batch" in item:
                        batch_id = item["batch"]
                    
                    # Add all items to parsed data
                    for key, value in item.items():
                        parsed_data[key] = value
                    
            except json.JSONDecodeError:
                # Skip invalid lines
                continue
                
        # Add batch ID as a special field if found
        if batch_id is not None:
            parsed_data["_batch_id"] = batch_id
            
        return parsed_data
    
    def merge_data(self, data_entries: Dict[str, Any]) -> Any:
        """
        Merge multiple data entries into a single state.
        Used for checkpointing or combining multiple logs.
        
        Args:
            data_entries: Dictionary mapping entry IDs to their data
            
        Returns:
            Any: Merged data representing combined state
        """
        merged_data = {}
        seen_batches = set()
        
        # Handle special case for checkpoint data
        if "checkpoint" in data_entries:
            checkpoint_data = data_entries.pop("checkpoint")
            merged_data.update(checkpoint_data)
            
            # Collect batch IDs from checkpoint
            if "_batch_id" in checkpoint_data:
                seen_batches.add(checkpoint_data["_batch_id"])
        
        # Process all other entries
        for op_id, entry_data in data_entries.items():
            if not entry_data:
                continue
                
            # Skip batches we've already processed
            if "_batch_id" in entry_data and entry_data["_batch_id"] in seen_batches:
                continue
                
            # Add this batch to seen batches
            if "_batch_id" in entry_data:
                seen_batches.add(entry_data["_batch_id"])
                
            # Update merged data with this entry's data
            for key, value in entry_data.items():
                if key != "_batch_id":  # Don't duplicate batch IDs in the output
                    merged_data[key] = value
        
        # Store all seen batches as a list
        if seen_batches:
            merged_data["_batch_ids"] = list(seen_batches)
            
        return merged_data
