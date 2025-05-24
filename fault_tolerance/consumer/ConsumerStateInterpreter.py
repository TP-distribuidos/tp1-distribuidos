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
        # For regular log entries, format as JSON strings
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
        Used for combining logs.
        
        Args:
            data_entries: Dictionary mapping entry IDs to their data
            
        Returns:
            Any: Merged data representing combined state - focused on complete lorem ipsum
        """
        batch_contents = {}
        processed_batch_ids = set()
        
        # Process all entries
        for op_id, entry_data in data_entries.items():
            if not isinstance(entry_data, dict) or not entry_data:
                continue
                
            # Track batch information
            batch = entry_data.get("batch")
            if batch:
                # Convert batch to int if it's a digit string for ordering
                batch_int = batch
                if isinstance(batch, str) and batch.isdigit():
                    batch_int = int(batch)
                
                # Save the processed batch ID
                processed_batch_ids.add(str(batch))
                
                # Save content by batch number for ordering
                content = entry_data.get("content", "")
                if content:
                    batch_contents[batch_int] = content
        
        # Combine content in correct batch order
        combined_content = ""
        if batch_contents:
            sorted_batches = sorted(batch_contents.keys())
            for batch in sorted_batches:
                combined_content += batch_contents[batch] + " "
            combined_content = combined_content.strip()
        
        # Construct the merged data
        merged_data = {}
        
        # Store each entry by operation ID
        for op_id, entry_data in data_entries.items():
            merged_data[op_id] = entry_data
            
        # Add our summary fields
        merged_data["_processed_batch_ids"] = list(processed_batch_ids)
        merged_data["_combined_content"] = combined_content
        
        # Track last batch number
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
