import json
import logging
from typing import Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Average Sentiment Collector Worker.
    Handles formatting, parsing and merging the sentiment data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        Args:
            data: Business data to format 
            
        Returns:
            str: JSON string with the required structure
        """
        formatted = {
            "data": data,  
            "_metadata": {}
        }
        return json.dumps(formatted)
    
    def parse_data(self, content: str) -> Any:
        """
        Parse stored data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed data
        """
        try:
            parsed_data = json.loads(content)
            if "data" in parsed_data:
                return parsed_data["data"]
            return parsed_data
        except json.JSONDecodeError:
            return None
    
    def merge_data(self, data_entries: List[Any]) -> Any:
        """
        Merge multiple data entries into a single state.
        For sentiment data, we need to combine the sums and counts.
        
        Args:
            data_entries: List of data entries to merge
            
        Returns:
            Any: Merged sentiment data
        """
        if not data_entries:
            return {
                "POSITIVE": {"sum": 0, "count": 0},
                "NEGATIVE": {"sum": 0, "count": 0}
            }
        
        # Initialize merged data structure
        merged_data = {
            "POSITIVE": {"sum": 0, "count": 0},
            "NEGATIVE": {"sum": 0, "count": 0}
        }
        
        # Process all entries
        for entry in data_entries:
            for sentiment_item in entry:
                sentiment = sentiment_item.get("sentiment")
                merged_data[sentiment]["sum"] += sentiment_item.get("sum")
                merged_data[sentiment]["count"] += sentiment_item.get("count")
        
        return merged_data