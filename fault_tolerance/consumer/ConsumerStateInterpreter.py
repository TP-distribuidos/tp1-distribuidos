import json
import logging
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface
from typing import Any, Dict, List

class ConsumerStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for consumer data, handling message information.
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
        if isinstance(data, dict) and "data" in data:
            # This is a checkpoint manifest, serialize it as a single JSON object
            return json.dumps(data)
        
        # For regular log entries, format as JSON strings
        formatted_lines = []
        
        # Handle dictionaries (most common case)
        if isinstance(data, dict):
            # Make a copy to avoid modifying the original
            data_copy = data.copy()
            
            # Ensure message_id is consistently stored as a string
            if 'message_id' in data_copy and data_copy['message_id'] is not None:
                data_copy['message_id'] = str(data_copy['message_id'])
            # Handle legacy batch field if it still exists
            elif 'batch' in data_copy and data_copy['batch'] is not None:
                data_copy['message_id'] = str(data_copy['batch'])
                data_copy.pop('batch', None)
                
            # Store the full dictionary as one JSON object
            formatted_lines.append(json.dumps(data_copy))
        # Handle lists of items
        elif isinstance(data, list):
            # Store each item as a separate line
            for item in data:
                if isinstance(item, dict):
                    item_copy = item.copy()
                    # Handle message_id
                    if 'message_id' in item_copy and item_copy['message_id'] is not None:
                        item_copy['message_id'] = str(item_copy['message_id'])
                    # Handle legacy batch field
                    elif 'batch' in item_copy and item_copy['batch'] is not None:
                        item_copy['message_id'] = str(item_copy['batch'])
                        item_copy.pop('batch', None)
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
                # Handle checkpoint format
                if "data" in data:
                    return data
        except json.JSONDecodeError:
            # Not a single JSON object, continue with line-by-line parsing
            pass
        
        # Normal line-by-line parsing for log entries
        parsed_data = {}
        message_id = None
        
        # Split content into lines and process each one
        lines = content.strip().split("\n")
        for line in lines:
            if not line.strip():
                continue
                
            try:
                item = json.loads(line)
                
                if isinstance(item, dict):
                    # Extract message ID if present
                    if "message_id" in item:
                        message_id = str(item["message_id"])
                        item = item.copy()  # Make a copy to avoid modifying the original
                        item["message_id"] = message_id
                    # Handle legacy batch field
                    elif "batch" in item:
                        message_id = str(item["batch"])
                        item = item.copy()  # Make a copy to avoid modifying the original
                        item["message_id"] = message_id
                        item.pop("batch", None)
                    
                    # Add all items to parsed data
                    for key, value in item.items():
                        parsed_data[key] = value
                    
            except json.JSONDecodeError:
                # Skip invalid lines
                continue
                
        # Add message ID as a special field if found
        if message_id is not None:
            parsed_data["_message_id"] = message_id
            
        return parsed_data
    
    def merge_data(self, data_entries: Any) -> Any:
        """
        Merge multiple data entries into a single state.
        This method handles both direct list inputs (checkpoint creation) 
        and dictionary inputs (retrieval/combining).
        
        Args:
            data_entries: Either a list of log entries or a dictionary mapping IDs to entries
            
        Returns:
            Dict[str, Any]: Merged data suitable for storage
        """
        # Handle dictionary input by converting to list of entries
        if isinstance(data_entries, dict):
            # For dictionary data, extract relevant entries
            log_entries = []
            for op_id, entry in data_entries.items():
                if isinstance(entry, dict):
                    # Check if this is a checkpoint with content and messages_id
                    if op_id == "_checkpoint_data":
                        if "content" in entry and "messages_id" in entry:
                            return entry  # Just return the checkpoint data directly
                    else:
                        log_entries.append(entry)
            
            # Use the extracted log entries for merging
            data_entries = log_entries
        
        # At this point, data_entries should be a list of entries
        if not isinstance(data_entries, list):
            logging.warning(f"Expected list of entries, got {type(data_entries)}")
            return {"messages_id": [], "content": ""}
        
        # Track message IDs and content
        message_ids = []
        content_by_id = {}
        
        for entry in data_entries:
            # Extract message_id
            msg_id = None
            if "message_id" in entry:
                msg_id = entry["message_id"]
            elif "_message_id" in entry:
                msg_id = entry["_message_id"]
            elif "batch" in entry:  # Legacy support
                msg_id = entry["batch"]
            
            if msg_id:
                try:
                    # Try to convert to int for sorting if it's a digit string
                    if isinstance(msg_id, str) and msg_id.isdigit():
                        int_id = int(msg_id)
                    else:
                        int_id = msg_id
                    
                    # Store message ID and content
                    message_ids.append(str(msg_id))
                    
                    # Get content if available
                    content = entry.get("content", "")
                    if content:
                        content_by_id[int_id] = content
                except (ValueError, TypeError):
                    # Handle non-numeric IDs
                    message_ids.append(str(msg_id))
                    content_by_id[msg_id] = entry.get("content", "")
        
        # Combine content in correct order (sorted by message_id)
        sorted_ids = sorted(content_by_id.keys())
        combined_content = ""
        for id in sorted_ids:
            combined_content += content_by_id[id] + " "
        combined_content = combined_content.strip()
        
        # Create checkpoint data
        checkpoint_data = {
            "messages_id": message_ids,
            "content": combined_content
        }
        
        return checkpoint_data
