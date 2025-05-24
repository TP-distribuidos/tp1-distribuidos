import json
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface
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
            # Make a copy to avoid modifying the original
            data_copy = data.copy()
            
            # Ensure batch is consistently stored as a string
            if 'batch' in data_copy and data_copy['batch'] is not None:
                data_copy['batch'] = str(data_copy['batch'])
                
            # Store the full dictionary as one JSON object
            formatted_lines.append(json.dumps(data_copy))
        # Handle lists of items
        elif isinstance(data, list):
            # Store each item as a separate line
            for item in data:
                if isinstance(item, dict) and 'batch' in item and item['batch'] is not None:
                    item_copy = item.copy()
                    item_copy['batch'] = str(item_copy['batch'])
                    formatted_lines.append(json.dumps(item_copy))
                else:
                    formatted_lines.append(json.dumps(item))
        # Handle single items
        else:
            # For any other type, just JSON serialize it
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
            if isinstance(data, dict):
                # Handle new checkpoint format
                if "data" in data and isinstance(data["data"], str) and "processed_batch_ids" in data:
                    return data
                # Handle old checkpoint format
                elif "data" in data and "processed_logs" in data:
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
                        # Ensure batch is stored as a string for consistency
                        batch_id = str(item["batch"])
                        item = item.copy()  # Make a copy to avoid modifying the original
                        item["batch"] = batch_id
                    
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
        Used for checkpointing or combining logs.
        
        Args:
            data_entries: Dictionary mapping entry IDs to their data
            
        Returns:
            Any: Merged data representing combined state - focused on complete lorem ipsum
        """
        # New format: We'll store the complete sequences of lorem text as one unit
        # and track the processed batch IDs separately
        processed_batch_ids = set()
        batch_contents = {}
        full_content = ""
        
        # Check if we already have a full content entry from a previous checkpoint
        if "_full_content_entry" in data_entries:
            entry = data_entries.get("_full_content_entry")
            if isinstance(entry, dict) and "content" in entry:
                full_content = entry.get("content", "")
                
        # Check for processed batch IDs from a previous checkpoint
        if "_processed_batch_ids" in data_entries:
            batch_id_list = data_entries.get("_processed_batch_ids")
            if isinstance(batch_id_list, list):
                for batch_id in batch_id_list:
                    processed_batch_ids.add(str(batch_id))
        
        # Process all regular entries
        for op_id, entry_data in data_entries.items():
            # Skip special fields
            if op_id in ("_full_content_entry", "_processed_batch_ids"):
                continue
                
            if not isinstance(entry_data, dict) or not entry_data:
                continue
                
            # Track batch information
            batch = entry_data.get("batch")
            if batch:
                # Convert batch to int if it's a digit string
                batch_int = batch
                if isinstance(batch, str) and batch.isdigit():
                    batch_int = int(batch)
                
                # Save the processed batch ID
                processed_batch_ids.add(str(batch))
                
                # Save content by batch number for ordering
                content = entry_data.get("content", "")
                if content and content != "Part of combined content":
                    batch_contents[batch_int] = content
        
        # Combine content in correct batch order
        if batch_contents:
            sorted_batches = sorted(batch_contents.keys())
            combined_content = ""
            for batch in sorted_batches:
                combined_content += batch_contents[batch] + " "
            full_content = combined_content.strip()
        
        # Construct the new merged data format
        merged_data = {}
        
        # Store each entry by operation ID as before (needed for WAL operations)
        for op_id, entry_data in data_entries.items():
            merged_data[op_id] = entry_data
            
        # Add our new summary fields
        merged_data["_processed_batch_ids"] = list(processed_batch_ids)
        merged_data["_full_content"] = full_content
        
        # Add legacy fields for backward compatibility
        batch_ids = []
        for id in processed_batch_ids:
            try:
                if isinstance(id, str) and id.isdigit():
                    batch_ids.append(int(id))
                elif isinstance(id, int):
                    batch_ids.append(id)
            except (ValueError, TypeError):
                pass
        
        if batch_ids:
            merged_data["_last_batch"] = max(batch_ids)
            
        return merged_data
