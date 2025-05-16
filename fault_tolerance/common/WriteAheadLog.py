import os
import json
import logging
from pathlib import Path
import shutil
import time

class WriteAheadLog:
    """
    Persistent logging system implementing Write-Ahead Log pattern.
    Ensures data is durable and survives system crashes.
    """
    
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED_"

    LIST_CHUNK_SIZE = 1000

    READ_MODE = 'r'
    WRITE_MODE = 'w'
    LOG_FILE_EXTENSION = '.log'

    def __init__(self, base_dir="/app/wal", service_name=None, parser=None):
        """
        Initialize the persistent log with a directory for storing log files.
        
        Args:
            base_dir (str): Base directory for storing log files
            service_name (str, optional): Name of the service using the log
            parser (IParser, optional): Parser implementation for handling log data
        """
        if service_name:
            self.base_dir = Path(base_dir) / service_name
        else:
            self.base_dir = Path(base_dir)

        self.base_dir.mkdir(exist_ok=True, parents=True)

        if parser:
            self.parser = parser
        else:
            raise ValueError("A parser implementing IParser interface must be provided")

        logging.info(f"WriteAheadLog initialized at {self.base_dir}")
        
    def _get_client_dir(self, client_id):
        """Get the directory for a specific client's logs"""
        client_dir = self.base_dir / str(client_id)
        client_dir.mkdir(exist_ok=True, parents=True)
        return client_dir
    
    def save_data(self, client_id, data, operation_id=None):
        """
        Save data with write-ahead logging to ensure durability.
        
        Args:
            client_id (str): Client identifier
            data (any): Data to be persisted (must be JSON serializable)
            operation_id (str, optional): Unique identifier for this operation
            
        Returns:
            bool: True if data was successfully persisted or already exists
        """
        client_dir = self._get_client_dir(client_id)
        
        # Generate operation ID if not provided
        if operation_id is None:
            raise ValueError("operation_id must be provided")
            
        log_path = client_dir / f"{operation_id}{self.LOG_FILE_EXTENSION}"
        
        # Check if already processed
        if log_path.exists():
            try:
                with open(log_path, self.READ_MODE) as f:
                    first_line = f.readline().strip()
                    if first_line == self.STATUS_COMPLETED:
                        logging.info(f"Data for client {client_id}, operation {operation_id} already persisted")
                        return True
            except Exception as e:
                logging.warning(f"Failed to read existing log file: {e}")
                # File might be corrupted, continue with rewriting
        
        try:
            # Write everything in a single file operation
            with open(log_path, self.WRITE_MODE) as f:
                # First line is status - write PROCESSING with padding to match COMPLETED length

                status_line = f"{self.STATUS_PROCESSING}\n"
                f.write(status_line)
                
                # Write data line by line if iterable, otherwise as single JSON
                if isinstance(data, list) or isinstance(data, dict) and hasattr(data, '__iter__'):
                    if isinstance(data, dict):
                        # Handle dictionary by writing each key-value pair
                        for key, value in data.items():
                            f.write(json.dumps({key: value}) + "\n")
                    else:
                        # Process in chunks for large lists
                        chunk_size = self.LIST_CHUNK_SIZE  # Adjust based on expected data size
                        for i in range(0, len(data), chunk_size):
                            chunk = data[i:i+chunk_size]
                            for item in chunk:
                                f.write(json.dumps(item) + "\n")
                else:
                    # Write as single JSON for non-iterable data
                    f.write(json.dumps(data) + "\n")

                # Remember current position at end of data
                end_pos = f.tell()

                # Go back to beginning to update status
                f.seek(0)

                # Write COMPLETED status - same length as PROCESSING
                f.write(f"{self.STATUS_COMPLETED}\n")
                # logging.info(f"SLEEPING 5 seconds before writing to WAL for client {client_id}, operation {operation_id}")
                # # Sleep for 5 seconds before writing to simulate slow I/O
                # time.sleep(5) 
                # Go back to end of file
                f.seek(end_pos)
                
                # Force flush to disk - only one sync operation
                f.flush()
                os.fsync(f.fileno())
            
            return True
            
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}: {e}")
            
            # If the file is corrupted or incomplete, remove it
            try:
                if log_path.exists():
                    os.remove(log_path)
            except:
                pass
                
            return False
    
    def get_data(self, client_id):
        """
        Retrieve persisted data for a client.
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            dict/list or None: Retrieved data or None if no data found or not completed
        """
        client_dir = self._get_client_dir(client_id)
        
        all_data = {}
        for log_file in client_dir.glob(f"*{self.LOG_FILE_EXTENSION}"):
            try:
                with open(log_file, self.READ_MODE) as f:
                    lines = f.readlines()
                    result = self.parser.parse(lines)
                    batch_id, batch_data = result
                    if batch_id is not None:
                        all_data[batch_id] = batch_data
            except Exception as e:
                logging.warning(f"Skipping invalid log {log_file}: {e}")
                continue
                
        return all_data if all_data else None
    
    def clear_client_data(self, client_id):
        """
        Remove all persisted data for a client.
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            bool: True if data was successfully cleared
        """
        client_dir = self._get_client_dir(client_id)
        if not client_dir.exists():
            return True
            
        try:
            shutil.rmtree(client_dir)
            return True
        except Exception as e:
            logging.error(f"Error clearing data for client {client_id}: {e}")
            return False
    
    def has_completed_operation(self, client_id, operation_id):
        """
        Check if an operation has been completed for a client.
        
        Args:
            client_id (str): Client identifier
            operation_id (str): Operation identifier
            
        Returns:
            bool: True if operation exists and is marked as COMPLETED
        """
        client_dir = self._get_client_dir(client_id)
        log_path = client_dir / f"{operation_id}{self.LOG_FILE_EXTENSION}"
        
        if not log_path.exists():
            return False
            
        try:
            with open(log_path, self.READ_MODE) as f:
                first_line = f.readline().strip()
                return first_line == self.STATUS_COMPLETED
        except FileNotFoundError:
            return False
        except IOError as e:
            logging.warning(f"IO error checking operation {operation_id}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error checking operation {operation_id}: {e}")
            return False
    
    def recover_state(self):
        """
        Recover all completed operations from log files.
        
        Returns:
            dict: Dictionary mapping client_ids to their recovered data
        """
        recovered_data = {}
        
        # Process client directory by client directory to limit memory usage
        for client_dir in self.base_dir.glob("*"):
            if not client_dir.is_dir():
                continue
                
            client_id = client_dir.name
            client_data = self._recover_client_data(client_id, client_dir)
            if client_data:
                recovered_data[client_id] = client_data
                    
        return recovered_data

    def _recover_client_data(self, client_id, client_dir):
        """Helper method to recover data for a single client"""
        client_data = []
        
        # Process log files one by one
        for log_file in client_dir.glob(f"*{self.LOG_FILE_EXTENSION}"):
            try:
                with open(log_file, self.READ_MODE) as f:
                    lines = f.readlines()
                    
                    if not lines or lines[0].strip() != self.STATUS_COMPLETED:
                        self._safely_remove_file(log_file)
                        logging.warning(f"Removed incomplete log file: {log_file}")
                        continue
                    
                    # Process data lines
                    for line in lines[1:]:
                        if line.strip():  # Skip empty lines
                            client_data.append(json.loads(line))
            except Exception as e:
                logging.warning(f"Error processing log {log_file}, removing: {e}")
                self._safely_remove_file(log_file)
        
        return client_data

    def _safely_remove_file(self, file_path):
        """Safely remove a file with appropriate error handling"""
        try:
            if file_path.exists():
                os.remove(file_path)
                return True
        except Exception as e:
            logging.warning(f"Failed to remove file {file_path}: {e}")
            return False

    def list_operations(self, client_id):
        """
        List all operations stored for a client.
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            dict: Dictionary mapping operation IDs to their completion status
        """
        client_dir = self._get_client_dir(client_id)
        if not client_dir.exists():
            return {}
            
        operations = {}
        for log_file in client_dir.glob(f"*{self.LOG_FILE_EXTENSION}"):
            operation_id = log_file.stem  # Get filename without extension
            is_completed = self.has_completed_operation(client_id, operation_id)
            operations[operation_id] = is_completed
            
        return operations
