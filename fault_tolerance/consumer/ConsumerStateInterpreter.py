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
        # Special handling for checkpoint data (both formats)
        if isinstance(data, dict):
            # Check if this is checkpoint data with messages_id and content
            if "messages_id" in data and "content" in data:
                # This is the new checkpoint format - serialize directly
                logging.debug(f"Formatting checkpoint with {len(data.get('messages_id', []))} messages (new format)")
                return json.dumps(data)
            # Legacy format with data wrapper
            elif "data" in data and isinstance(data["data"], dict):
                # Check if inner data has the correct structure
                inner_data = data["data"]
                if "messages_id" in inner_data and "content" in inner_data:
                    logging.debug(f"Formatting checkpoint with {len(inner_data.get('messages_id', []))} messages (legacy format)")
                    # Preserve legacy format for backward compatibility
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
                # Check for both checkpoint formats
                
                # Legacy format with data wrapper
                if "data" in data and isinstance(data["data"], dict):
                    inner_data = data["data"]
                    if "messages_id" in inner_data and "content" in inner_data:
                        logging.debug(f"Parsed checkpoint data with {len(inner_data.get('messages_id', []))} messages (legacy format)")
                        return data
                
                # New format with direct messages_id and content
                if "messages_id" in data and "content" in data:
                    logging.debug(f"Parsed checkpoint data with {len(data.get('messages_id', []))} messages (new format)")
                    return data
                
                # Might be a regular log entry as a single object
                # Continue to process it as we would other log entries
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
            existing_checkpoint = None
            
            # First, check if we have an existing checkpoint to build upon
            for op_id, entry in data_entries.items():
                if isinstance(entry, dict):
                    # Check if this is a checkpoint with content and messages_id - prioritize this
                    if op_id == "_checkpoint_data":
                        if "content" in entry and "messages_id" in entry:
                            existing_checkpoint = entry
                            logging.debug(f"Found existing checkpoint data with {len(entry.get('messages_id', []))} messages")
                    # Also check checkpoint entries by filename
                    elif op_id.startswith("checkpoint_") and "data" in entry:
                        # Legacy format with data wrapper
                        inner_data = entry["data"]
                        if isinstance(inner_data, dict) and "messages_id" in inner_data and "content" in inner_data:
                            if existing_checkpoint is None:
                                existing_checkpoint = inner_data
                                logging.debug(f"Found legacy checkpoint with {len(inner_data.get('messages_id', []))} messages")
                    elif op_id.startswith("checkpoint_"):
                        # New format without wrapper
                        if isinstance(entry, dict) and "messages_id" in entry and "content" in entry:
                            if existing_checkpoint is None:
                                existing_checkpoint = entry
                                logging.debug(f"Found checkpoint with {len(entry.get('messages_id', []))} messages")
                    else:
                        # Regular log entry
                        log_entries.append(entry)
            
            # If we have an existing checkpoint, start with that
            if existing_checkpoint:
                # If we have no logs to merge with the checkpoint, just return the checkpoint
                if not log_entries:
                    return existing_checkpoint
                
                # Otherwise add checkpoint first in the list so it's processed first
                log_entries.insert(0, existing_checkpoint)
            
            # Use the extracted log entries for merging
            data_entries = log_entries
        
        # At this point, data_entries should be a list of entries
        if not isinstance(data_entries, list):
            logging.warning(f"Expected list of entries, got {type(data_entries)}")
            return {"messages_id": [], "content": ""}
        
        # Check if first entry is a checkpoint - if so, extract its data to build upon
        existing_messages = []
        existing_content = {}
        if data_entries and isinstance(data_entries[0], dict):
            first_entry = data_entries[0]
            
            # Check if this is a checkpoint format
            if "messages_id" in first_entry and "content" in first_entry:
                # Get existing messages and content
                existing_messages = first_entry.get("messages_id", []).copy()
                
                # If we have existing content and message IDs, build a mapping
                content_str = first_entry.get("content", "")
                # Just preserve the content as is - don't try to split it up
                if content_str and existing_messages:
                    # Store the content with the last message ID as key
                    # This ensures it will be preserved when merging with new messages
                    try:
                        key = existing_messages[-1]
                        # Try to use an integer key for better sorting
                        if isinstance(key, str) and key.isdigit():
                            key = int(key)
                        existing_content[key] = content_str
                    except (IndexError, ValueError):
                        # If no messages or conversion fails, use string key
                        existing_content["checkpoint_content"] = content_str
                
                logging.debug(f"Building on existing checkpoint with {len(existing_messages)} messages")
                
                # Remove the checkpoint entry as we've extracted its data
                data_entries = data_entries[1:]
        
        # Track message IDs and content - start with existing data
        message_ids = existing_messages
        content_by_id = existing_content.copy()
        
        # Process new entries
        for entry in data_entries:
            # Skip entries that don't have needed fields
            if not isinstance(entry, dict):
                continue
                
            # Extract message_id with various fallbacks
            msg_id = None
            if "message_id" in entry:
                msg_id = entry["message_id"]
            elif "_message_id" in entry:
                msg_id = entry["_message_id"]
            elif "batch" in entry:  # Legacy support
                msg_id = entry["batch"]
            elif "messages_id" in entry and isinstance(entry["messages_id"], list) and entry["messages_id"]:
                # If this is another checkpoint format, use its last message ID
                msg_id = entry["messages_id"][-1]
            
            if msg_id:
                # Convert ID to string for consistent storage
                str_msg_id = str(msg_id)
                
                # Only add if it's not already in the list
                if str_msg_id not in message_ids:
                    message_ids.append(str_msg_id)
                
                # Get content with special handling for checkpoints
                content = ""
                if "content" in entry:
                    content = entry["content"]
                
                # Only add content if we have it
                if content:
                    try:
                        # Try to use an integer key for better sorting if it's a number
                        int_key = int(msg_id) if isinstance(msg_id, str) and msg_id.isdigit() else msg_id
                        content_by_id[int_key] = content
                    except (ValueError, TypeError):
                        # Fallback for non-numeric IDs
                        content_by_id[str_msg_id] = content
        
        # Remove duplicates from message_ids while preserving order
        unique_messages = []
        seen = set()
        for msg_id in message_ids:
            if msg_id not in seen:
                seen.add(msg_id)
                unique_messages.append(msg_id)
        message_ids = unique_messages
        
        # Combine content in correct order (sorted by message_id)
        sorted_ids = sorted(content_by_id.keys())
        combined_content = ""
        for id in sorted_ids:
            content_piece = content_by_id[id]
            if combined_content and not combined_content.endswith(" "):
                combined_content += " "
            combined_content += content_piece
        combined_content = combined_content.strip()
        
        # Create checkpoint data
        checkpoint_data = {
            "messages_id": message_ids,
            "content": combined_content
        }
        
        logging.debug(f"Created checkpoint with {len(message_ids)} messages")
        return checkpoint_data
