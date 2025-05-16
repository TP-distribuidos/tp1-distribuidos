import json
from common.IParser import IParser

class ConsumerParser(IParser):
    """
    Parser for consumer log files to extract batch information and associated data.
    Implements the IParser interface.
    """
    
    def __init__(self, status_completed="COMPLETED_"):
        """
        Initialize the ConsumerParser.
        
        Args:
            status_completed (str): The status string that indicates a completed operation
        """
        self.STATUS_COMPLETED = status_completed
    
    def parse(self, data):
        """
        Parse log file data to extract batch information.
        
        Args:
            data: List of strings representing lines from a log file
            
        Returns:
            tuple: (batch_id, batch_data) where batch_id is the identified batch 
                  and batch_data is a dictionary of all other properties
        """
        # Handle empty data case
        if not data:
            return None, {}
            
        # Check if the first line indicates a completed operation
        first_line = data[0].strip() if isinstance(data[0], str) else str(data[0]).strip()
        if first_line != self.STATUS_COMPLETED:
            return None, {}
            
        # Process data lines (all lines except the status line)
        data_lines = data[1:] if len(data) > 1 else []
        
        current_batch = None
        batch_data = {}

        for line in data_lines:
            if not isinstance(line, str) or not line.strip():  # Skip empty lines
                continue
                
            try:
                item = json.loads(line)
                
                if "batch" in item:
                    current_batch = item["batch"]
                else:
                    # Add other properties to the batch data
                    for key, value in item.items():
                        batch_data[key] = value
            except json.JSONDecodeError:
                # Skip invalid JSON lines
                continue
        
        return current_batch, batch_data
    
    @staticmethod
    def format_output(batch_id, batch_data):
        """
        Format the parsed result in a pretty way.
        
        Args:
            batch_id: The batch identifier
            batch_data: Dictionary of batch properties
            
        Returns:
            str: A formatted string representation of the batch data
        """
        if batch_id is None:
            return "No valid batch data found."
            
        output = [f"Batch ID: {batch_id}", "-" * 40]
        
        for key, value in sorted(batch_data.items()):
            output.append(f"{key}: {value}")
            
        return "\n".join(output)
