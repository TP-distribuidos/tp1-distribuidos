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
        Format business data for storage.
        
        This method only handles business data formatting, not WAL structure.
        The WAL's responsibility is to add any needed WAL-specific metadata.
        
        Args:
            data: Business data to format
            
        Returns:
            str: Formatted business data ready for storage
        """
        # Create standardized structure for business data
        wal_structure = {
            "data": data,  # Store original business data unchanged
            "_wal_metadata": {}  # Reserved for WAL to add its metadata
        }
            
        # Serialize the structure
        return json.dumps(wal_structure)

    def parse_data(self, content: str) -> Any:
        """
        Parse stored data, separating business data from WAL implementation details.
        
        This method:
        1. Parses the string content into structured data
        2. Identifies and extracts business data from WAL structure
        3. Maintains backward compatibility with legacy formats
        4. Provides clean business data to consumers without WAL implementation details
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed business data as a dictionary
        """
        # First try to parse as a single JSON object
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                # Handle WAL format with proper structure
                if "data" in data and "_wal_metadata" in data:
                    # Extract pure business data
                    business_data = data["data"]
                    
                    # If the business data contains a message_id from WAL metadata, add it
                    if "_wal_metadata" in data and "message_id" in data["_wal_metadata"]:
                        wal_message_id = data["_wal_metadata"]["message_id"]
                        # Only add if business data is a dict and doesn't already have message_id
                        if isinstance(business_data, dict) and "message_id" not in business_data:
                            business_data = business_data.copy()  # Create copy to avoid modifying original
                            business_data["message_id"] = wal_message_id
                    
                    logging.debug(f"Extracted business data from WAL structure")
                    return business_data
                
                # Handle checkpoint formats (both new TCP-style and old format for backward compatibility)
                if "max_message_id" in data and "content" in data:
                    # New TCP-style format with just max_message_id
                    logging.debug(f"Parsed TCP-style checkpoint with max message ID {data.get('max_message_id')}")
                    return data
                # For direct business data without WAL structure, return as-is
                return data
                
        except json.JSONDecodeError:
            # Not a single JSON object, continue with line-by-line parsing for legacy support
            logging.debug(f"Content is not a single JSON object, trying line-by-line parsing")
            pass
        
        # Line-by-line parsing for legacy log entries
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
                    # Check if this is a WAL structure and extract business data
                    if "data" in item and "_wal_metadata" in item:
                        business_data = item["data"]
                        if isinstance(business_data, dict):
                            # Add message_id from WAL metadata if available
                            if "message_id" in item["_wal_metadata"]:
                                message_id = str(item["_wal_metadata"]["message_id"])
                                business_data["message_id"] = message_id
                            
                            # Add all business data fields to parsed_data
                            for key, value in business_data.items():
                                parsed_data[key] = value
                            continue
                    
                    # Direct extraction for non-WAL format
                    # Extract message ID if present
                    if "message_id" in item:
                        message_id = str(item["message_id"])
                        item = item.copy()  # Make a copy to avoid modifying the original
                        item["message_id"] = message_id
                    
                    # Add all items to parsed data
                    for key, value in item.items():
                        parsed_data[key] = value
                    
            except json.JSONDecodeError:
                # Skip invalid lines
                continue
        
        # Add message ID as a field if found but not already included
        if message_id is not None and "message_id" not in parsed_data:
            parsed_data["message_id"] = message_id
            
        return parsed_data
    
    def merge_data(self, data_entries: Any) -> Any:
        """
        Merge multiple data entries into a single state.
        
        This method focuses solely on merging business data from multiple entries.
        It doesn't need to understand WAL implementation details or checkpoint formats.
        
        Args:
            data_entries: List of business data entries to merge
            
        Returns:
            Dict[str, Any]: Merged business data
        """
        
        # We're dealing with a list of dictionary entries
        entries_to_merge = data_entries if isinstance(data_entries, list) else []
            
        # If nothing to merge, return empty result
        if not entries_to_merge:
            logging.warning("No entries to merge")
            return {"content": ""}
        
        # Collect all batch IDs and content
        batch_ids = []
        content_by_batch = {}
        
        # Start with an existing content from previous checkpoint
        existing_content = ""
        
        # Process each entry
        for entry in entries_to_merge:
            # Handle different entry types
            if "content" in entry:
                # Case 1: Entry with only content (likely from a checkpoint)
                if "batch" not in entry:
                    # This is probably checkpoint content, store it as existing content
                    existing_content = entry.get("content", "")
                    continue
                
                # Case 2: Entry with batch ID and content
                batch_id = entry.get("batch")
                if batch_id:
                    # Convert to string for consistent handling
                    batch_id_str = str(batch_id)
                    
                    # Only add if not already processed
                    if batch_id_str not in batch_ids:
                        batch_ids.append(batch_id_str)
                        
                        # Store content if available
                        content = entry.get("content", "")
                        if content:
                            # Use numeric batch ID for better sorting
                            try:
                                batch_id_int = int(batch_id)
                                content_by_batch[batch_id_int] = content
                            except (ValueError, TypeError):
                                content_by_batch[batch_id_str] = content
        
        # Combine content in sorted order by batch ID
        sorted_batch_ids = sorted(content_by_batch.keys())
        combined_content = existing_content  # Start with existing checkpoint content
        
        # Add new content from logs
        for batch_id in sorted_batch_ids:
            content_piece = content_by_batch[batch_id]
            if combined_content and not combined_content.endswith(" "):
                combined_content += " "
            combined_content += content_piece
        combined_content = combined_content.strip()
        
        
        # Create final business data structure - use the latest batch ID as an identifier
        # If we have batch IDs from new messages, use the latest one
        # Otherwise keep using the previous batch ID if it exists in the checkpoint content
        latest_batch = None
        if batch_ids:
            try:
                latest_batch = int(batch_ids[-1])
            except (ValueError, TypeError):
                latest_batch = batch_ids[-1]
        
        result = {
            "content": combined_content,
            "batch": latest_batch
        }

        return result
