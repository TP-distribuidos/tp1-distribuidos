import os
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional

from common.data_persistance.DataPersistenceInterface import DataPersistenceInterface
from common.data_persistance.StorageInterface import StorageInterface
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class WriteAheadLog(DataPersistenceInterface):
    """
    Simplified Write-Ahead Log implementation of the DataPersistenceInterface.
    
    This class implements a durable logging mechanism that:
    1. Writes data to log files before processing
    2. Keeps all logs for transparency and debugging
    3. Provides recovery capabilities
    
    It uses:
    - StateInterpreterInterface for formatting/parsing data
    - StorageInterface for file system operations
    """
    
    # File extensions and prefixes
    LOG_PREFIX = "log_"
    LOG_EXTENSION = ".log"
    
    # Status indicators
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED"

    def __init__(self, 
                 state_interpreter: StateInterpreterInterface,
                 storage: StorageInterface,
                 service_name: str = "default_service",
                 base_dir: str = "/app/wal"):
        """
        Initialize the WriteAheadLog.
        
        Args:
            state_interpreter: StateInterpreterInterface implementation for formatting/parsing data
            storage: StorageInterface implementation for file operations
            service_name: Name of the service using this WAL (used for directory naming)
            base_dir: Base directory for storing log files
        """
        self.state_interpreter = state_interpreter
        self.storage = storage
        self.service_name = service_name
        
        # Set up the base directory for this service
        self.base_dir = Path(base_dir)
        self.storage.create_directory(self.base_dir)
        
        # Set up logging
        logging.info(f"WriteAheadLog initialized for service {service_name} at {base_dir}")
        
    def _get_client_dir(self, client_id: str) -> Path:
        """Get the directory path for a specific client's logs"""
        client_dir = self.base_dir / client_id
        self.storage.create_directory(client_dir)
        return client_dir
    
    def _get_log_file_path(self, client_dir: Path, operation_id: str) -> Path:
        """Get the path for a log file based on operation ID"""
        return client_dir / f"{self.LOG_PREFIX}{operation_id}{self.LOG_EXTENSION}"
    
    def _get_all_logs(self, client_dir: Path) -> List[Path]:
        """Get all log files for a client"""
        pattern = f"{self.LOG_PREFIX}*{self.LOG_EXTENSION}"
        return self.storage.list_files(client_dir, pattern)
    
    def persist(self, client_id: str, data: Any, operation_id: str) -> bool:
        """
        Persist data for a client with write-ahead logging.
        
        Args:
            client_id: Client identifier
            data: Data to persist
            operation_id: External ID used for reference (e.g., batch ID)
            
        Returns:
            bool: True if successfully persisted
        """
        # Generate internal timestamp-based ID for storage
        timestamp = str(int(time.time() * 1000))  # millisecond precision
        internal_id = f"{timestamp}_{operation_id}"
        
        client_dir = self._get_client_dir(client_id)
        log_file_path = self._get_log_file_path(client_dir, internal_id)
        
        # Add the operation_id to data for reference
        if isinstance(data, dict):
            data = data.copy()  # Make a copy to avoid modifying the original
            data['_external_operation_id'] = operation_id
        
        # Check if already processed
        if self.storage.file_exists(log_file_path):
            try:
                content = self.storage.read_file(log_file_path)
                if content.startswith(self.STATUS_COMPLETED):
                    logging.info(f"Data for client {client_id}, operation {operation_id} already persisted")
                    return True
            except Exception as e:
                logging.warning(f"Failed to read existing log file: {e}")
                # Continue with rewriting
        
        try:
            # Format the data using the state interpreter
            formatted_data = self.state_interpreter.format_data(data)
            
            # Create log content with status header
            log_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            
            # Write the log file
            success = self.storage.write_file(log_file_path, log_content)
            if not success:
                return False
                
            # Update the status to COMPLETED
            log_content = f"{self.STATUS_COMPLETED}\n{formatted_data}"
            success = self.storage.write_file(log_file_path, log_content)
            if not success:
                return False
            
            logging.info(f"Successfully persisted log for operation {operation_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}: {e}")
            # Try to remove the file if it exists and is corrupted
            if self.storage.file_exists(log_file_path):
                self.storage.delete_file(log_file_path)
            return False
    
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client by processing all log files.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Retrieved data or None if not found
        """
        client_dir = self._get_client_dir(client_id)
        
        # Data storage
        all_data = {}
        
        # Get all log files
        log_files = self._get_all_logs(client_dir)
        logging.info(f"Found {len(log_files)} log files for client {client_id}")
        
        # Process each log file
        for log_file in log_files:
            try:
                # Extract operation ID from filename
                file_name = log_file.name
                operation_id = file_name[len(self.LOG_PREFIX):-len(self.LOG_EXTENSION)]
                
                # Read the log content
                content = self.storage.read_file(log_file)
                
                # Skip processing logs that are not completed
                content_lines = content.splitlines()
                if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                    logging.info(f"Skipping incomplete log: {log_file}")
                    continue
                
                # Parse data from content (skip status line)
                if len(content_lines) > 1:
                    data_content = "\n".join(content_lines[1:])
                    parsed_data = self.state_interpreter.parse_data(data_content)
                    all_data[operation_id] = parsed_data
                    logging.debug(f"Processed log for operation {operation_id}")
            except Exception as e:
                logging.warning(f"Error processing log file {log_file}: {e}")
        
        # If all_data is empty, return None
        return all_data if all_data else None
    
    def clear(self, client_id: str) -> bool:
        """
        Clear all data for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if successfully cleared
        """
        client_dir = self._get_client_dir(client_id)
        
        # List all files in the client directory
        try:
            files = self.storage.list_files(client_dir)
            for file_path in files:
                self.storage.delete_file(file_path)
                
            return True
        except Exception as e:
            logging.error(f"Error clearing data for client {client_id}: {e}")
            return False
    
    def recover_state(self) -> Dict[str, Any]:
        """
        Recover all completed operations from log files.
        
        Returns:
            Dict[str, Any]: Dictionary mapping client_ids to their recovered data
        """
        recovered_data = {}
        
        # Find all client directories
        for item in self.storage.list_files(self.base_dir):
            client_dir = Path(item)
            
            if not client_dir.is_dir():
                continue
                
            client_id = client_dir.name
            client_data = self.retrieve(client_id)
            
            if client_data:
                recovered_data[client_id] = client_data
                
        return recovered_data