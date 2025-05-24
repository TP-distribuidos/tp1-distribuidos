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
    Write-Ahead Log implementation of the DataPersistenceInterface.
    
    This class implements a durable logging mechanism that:
    1. Writes data to log files before processing
    2. Keeps all logs for transparency and debugging
    3. Creates checkpoints after every 3 logs
    4. Provides recovery capabilities
    
    It uses:
    - StateInterpreterInterface for formatting/parsing data
    - StorageInterface for file system operations
    """
    
    # File extensions and prefixes
    LOG_PREFIX = "log_"
    LOG_EXTENSION = ".log"
    CHECKPOINT_PREFIX = "checkpoint_"
    CHECKPOINT_EXTENSION = ".log"
    
    # Status indicators
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED"
    
    # Checkpoint threshold
    CHECKPOINT_THRESHOLD = 3  # Create checkpoint after every 3 logs

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
        self.log_count = {}  # Dictionary to track log count per client_id
        
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
    
    def _get_checkpoint_file_path(self, client_dir: Path, timestamp: str) -> Path:
        """Get the path for a checkpoint file"""
        return client_dir / f"{self.CHECKPOINT_PREFIX}{timestamp}{self.CHECKPOINT_EXTENSION}"
    
    def _get_all_logs(self, client_dir: Path) -> List[Path]:
        """Get all log files for a client"""
        pattern = f"{self.LOG_PREFIX}*{self.LOG_EXTENSION}"
        return self.storage.list_files(client_dir, pattern)
    
    def _get_all_checkpoints(self, client_dir: Path) -> List[Path]:
        """Get all checkpoint files for a client"""
        pattern = f"{self.CHECKPOINT_PREFIX}*{self.CHECKPOINT_EXTENSION}"
        return self.storage.list_files(client_dir, pattern)
    
    def _get_latest_checkpoint(self, client_dir: Path) -> Optional[Path]:
        """Get the most recent checkpoint file"""
        checkpoint_files = self._get_all_checkpoints(client_dir)
        
        if not checkpoint_files:
            return None
            
        # Sort by timestamp in filename (descending)
        checkpoint_files.sort(reverse=True)
        return checkpoint_files[0]
    
    def _count_logs(self, client_dir: Path) -> int:
        """Count the number of log files for a client"""
        logs = self._get_all_logs(client_dir)
        return len(logs)
    
    def _create_checkpoint(self, client_id: str) -> bool:
        """
        Create a checkpoint for a client by merging all existing logs.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if checkpoint created successfully
        """
        client_dir = self._get_client_dir(client_id)
        
        try:
            # Get all log files
            log_files = self._get_all_logs(client_dir)
            if not log_files:
                logging.warning(f"No logs found for client {client_id}, skipping checkpoint creation")
                return False
                
            # Load all log entries
            log_entries = []
            
            for log_file in log_files:
                try:
                    # Read the log content
                    content = self.storage.read_file(log_file)
                    
                    # Skip processing logs
                    content_lines = content.splitlines()
                    if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                        continue
                    
                    # Parse data from content (skip status line)
                    if len(content_lines) > 1:
                        data_content = "\n".join(content_lines[1:])
                        parsed_data = self.state_interpreter.parse_data(data_content)
                        # Add to log entries
                        log_entries.append(parsed_data)
                except Exception as e:
                    logging.warning(f"Error processing log file {log_file} for checkpoint: {e}")
                    
            # If no valid logs, skip checkpoint
            if not log_entries:
                return False
                
            # Create checkpoint
            timestamp = str(int(time.time() * 1000))  # millisecond precision
            checkpoint_path = self._get_checkpoint_file_path(client_dir, timestamp)
            
            # Use state interpreter to create merged representation
            if hasattr(self.state_interpreter, 'merge_checkpoint_data'):
                merged_data = self.state_interpreter.merge_checkpoint_data(log_entries)
            else:
                # Fall back to merge_data if merge_checkpoint_data doesn't exist
                merged_data = self.state_interpreter.merge_data(log_entries)
            
            # Format the checkpoint data - only include merged data
            formatted_data = self.state_interpreter.format_data({
                "data": merged_data
            })
            
            # Write checkpoint file
            checkpoint_content = f"{self.STATUS_COMPLETED}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if success:
                logging.info(f"Created checkpoint for client {client_id} with {len(log_entries)} log entries")
                
                # Do not delete logs to keep all history for debugging
                logging.info(f"Keeping all logs for client {client_id} for debugging purposes")
                
                return True
            else:
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error creating checkpoint for client {client_id}: {e}")
            return False
    
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
        
        # Convert batch to message_id in data
        if isinstance(data, dict) and 'batch' in data and 'message_id' not in data:
            data = data.copy()  # Make a copy to avoid modifying the original
            data['message_id'] = data.pop('batch')  # Replace batch with message_id
            
        # Add the operation_id to data for reference
        if isinstance(data, dict):
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
            
            # Initialize client log count if not exists
            if client_id not in self.log_count:
                self.log_count[client_id] = 0
                
            # Increment log count for this client
            self.log_count[client_id] += 1
            
            # Check if we need to create a checkpoint
            if self.log_count[client_id] >= self.CHECKPOINT_THRESHOLD:
                logging.info(f"Log count {self.log_count[client_id]} reached threshold {self.CHECKPOINT_THRESHOLD}, creating checkpoint")
                self._create_checkpoint(client_id)
                # Reset log count after checkpoint creation
                self.log_count[client_id] = 0
            
            return True
            
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}: {e}")
            # Try to remove the file if it exists and is corrupted
            if self.storage.file_exists(log_file_path):
                self.storage.delete_file(log_file_path)
            return False
    
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client.
        
        This method combines data from the latest checkpoint (if any)
        and all subsequent log files to provide the most up-to-date view.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Retrieved data or None if not found
        """
        client_dir = self._get_client_dir(client_id)
        
        # Data storage
        all_data = {}
        
        # Try to get the latest checkpoint
        latest_checkpoint = self._get_latest_checkpoint(client_dir)
        if latest_checkpoint:
            try:
                # Read checkpoint content
                content = self.storage.read_file(latest_checkpoint)
                
                # Skip the status line
                content_lines = content.splitlines()
                if len(content_lines) > 1 and content_lines[0] == self.STATUS_COMPLETED:
                    # Parse the checkpoint data
                    checkpoint_content = "\n".join(content_lines[1:])
                    parsed_data = self.state_interpreter.parse_data(checkpoint_content)
                    
                    # Extract data from checkpoint
                    if isinstance(parsed_data, dict):
                        if "data" in parsed_data:
                            checkpoint_data = parsed_data["data"]
                            if isinstance(checkpoint_data, dict):
                                # Store checkpoint data with special key
                                all_data["_checkpoint_data"] = checkpoint_data
                                
                                # Add checkpoint filename key to help identify it in logs
                                checkpoint_name = latest_checkpoint.name
                                all_data[checkpoint_name] = {"data": checkpoint_data}
            except Exception as e:
                logging.error(f"Error reading checkpoint {latest_checkpoint} for client {client_id}: {e}")
        
        # Get all log files
        log_files = self._get_all_logs(client_dir)
        
        # Process each log file
        for log_file in log_files:
            try:
                # Extract operation ID from filename
                file_name = log_file.name
                operation_id = file_name[len(self.LOG_PREFIX):-len(self.LOG_EXTENSION)]
                
                # Read the log content
                content = self.storage.read_file(log_file)
                
                # Skip processing logs
                content_lines = content.splitlines()
                if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                    continue
                
                # Parse data from content (skip status line)
                if len(content_lines) > 1:
                    data_content = "\n".join(content_lines[1:])
                    parsed_data = self.state_interpreter.parse_data(data_content)
                    all_data[operation_id] = parsed_data
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
        Recover all completed operations from log files and checkpoints.
        
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