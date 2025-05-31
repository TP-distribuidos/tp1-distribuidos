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
            "_metadata": {}  # Reserved for WAL to add its metadata
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
                if "data" in data:
                    # Extract pure business data
                    business_data = data["data"]
                    
                    # Check for message_id in metadata
                    message_id = None
                    if "_metadata" in data and "message_id" in data["_metadata"]:
                        message_id = data["_metadata"]["message_id"]
                    
                    # If we found a message_id in metadata, add it to business data
                    if message_id is not None:
                        # Only add if business data is a dict and doesn't already have message_id
                        if isinstance(business_data, dict) and "message_id" not in business_data:
                            business_data = business_data.copy()  # Create copy to avoid modifying original
                            business_data["message_id"] = message_id
                    
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
                    if "data" in item:
                        business_data = item["data"]
                        if isinstance(business_data, dict):
                            # Check for message_id in metadata
                            msg_id = None
                            if "_metadata" in item and "message_id" in item["_metadata"]:
                                msg_id = str(item["_metadata"]["message_id"])
                                
                            # Add message_id from metadata if available
                            if msg_id:
                                message_id = msg_id
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
        Merge multiple data entries by adding up their values.
        
        Args:
            data_entries: List of business data entries to merge
            
        Returns:
            Dict[str, Any]: Merged business data with sum of all values
        """
        
        # We're dealing with a list of dictionary entries
        entries_to_merge = data_entries if isinstance(data_entries, list) else []
            
        # If nothing to merge, return zero
        if not entries_to_merge:
            logging.warning("No entries to merge")
            return {"total": 0, "count": 0}
        
        # Start with zero totals
        current_total = 0
        current_count = 0
        
        # Process each entry
        for entry in entries_to_merge:
            # Handle checkpoint entries (from different nodes)
            if "total" in entry and "count" in entry:
                # This is a checkpoint entry with accumulated total - ADD to running total
                node_total = entry.get("total", 0)
                node_count = entry.get("count", 0)
                current_total += node_total
                current_count += node_count
                logging.debug(f"Added checkpoint entry: total={node_total}, count={node_count}, running total={current_total}")
                continue
            
            # Handle individual value entries (from logs)
            if "value" in entry:
                value = entry.get("value", 0)
                current_total += value
                current_count += 1
                logging.debug(f"Added value {value}, new total: {current_total}")
        
        # Create final result
        result = {
            "total": current_total,
            "count": current_count
        }
        
        logging.info(f"Merged data: total={current_total}, count={current_count}")
        return result
