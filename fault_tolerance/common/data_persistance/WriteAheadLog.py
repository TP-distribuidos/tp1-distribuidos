import os
import json
import logging
from pathlib import Path
import shutil
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
    2. Creates checkpoints after a configured number of logs
    3. Provides recovery capabilities
    
    It uses:
    - StateInterpreterInterface for formatting/parsing data
    - StorageInterface for file system operations
    """
    
    # File extensions and prefixes
    LOG_PREFIX = "log_"
    CHECKPOINT_PREFIX = "checkpoint_"
    LOG_EXTENSION = ".log"
    
    # Status indicators
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED"
    
    # Default checkpoint threshold (create checkpoint after 3 logs)
    CHECKPOINT_THRESHOLD = 3

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
    
    def _get_checkpoint_file_path(self, client_dir: Path, timestamp: str) -> Path:
        """Get the path for a checkpoint file based on timestamp"""
        return client_dir / f"{self.CHECKPOINT_PREFIX}{timestamp}{self.LOG_EXTENSION}"
    
    def _count_logs(self, client_dir: Path) -> int:
        """Count the number of log files for a client"""
        pattern = f"{self.LOG_PREFIX}*{self.LOG_EXTENSION}"
        logs = self.storage.list_files(client_dir, pattern)
        return len(logs)
    
    def _get_all_logs(self, client_dir: Path) -> List[Path]:
        """Get all log files for a client"""
        pattern = f"{self.LOG_PREFIX}*{self.LOG_EXTENSION}"
        return self.storage.list_files(client_dir, pattern)
    
    def _get_latest_checkpoint(self, client_dir: Path) -> Optional[Path]:
        """Get the most recent checkpoint file"""
        pattern = f"{self.CHECKPOINT_PREFIX}*{self.LOG_EXTENSION}"
        checkpoints = self.storage.list_files(client_dir, pattern)
        
        if not checkpoints:
            return None
            
        # Sort by timestamp (newest first)
        checkpoints.sort(key=lambda p: str(p), reverse=True)
        return checkpoints[0]
        
    def _delete_previous_checkpoints(self, client_dir: Path, current_checkpoint_path: Path) -> None:
        """
        Delete all previous checkpoints for a client after a new checkpoint is created.
        
        Args:
            client_dir: The client's directory containing the checkpoints
            current_checkpoint_path: Path to the newly created checkpoint (which should be kept)
        """
        pattern = f"{self.CHECKPOINT_PREFIX}*{self.LOG_EXTENSION}"
        checkpoints = self.storage.list_files(client_dir, pattern)
        
        # Extract the current checkpoint filename for comparison
        current_checkpoint_filename = current_checkpoint_path.name
        
        # Delete all checkpoints except the current one
        deleted_count = 0
        for checkpoint in checkpoints:
            if checkpoint.name != current_checkpoint_filename:
                try:
                    self.storage.delete_file(checkpoint)
                    deleted_count += 1
                except Exception as e:
                    logging.warning(f"Failed to delete previous checkpoint {checkpoint}: {e}")
        
        if deleted_count > 0:
            logging.info(f"Deleted {deleted_count} previous checkpoint(s) for client directory {client_dir.name}")
    
    def _create_checkpoint(self, client_id: str) -> bool:
        """
        Create a checkpoint from existing logs for a client.
        
        This consolidates all log data into a single checkpoint file
        and then removes the individual log files.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if checkpoint was created successfully
        """
        client_dir = self._get_client_dir(client_id)
        log_files = self._get_all_logs(client_dir)
        
        if not log_files:
            return True  # Nothing to checkpoint
        
        # First, get data from the previous checkpoint if it exists
        previous_batch_ids = []
        previous_content = ""
        latest_checkpoint = self._get_latest_checkpoint(client_dir)
        if latest_checkpoint:
            try:
                content = self.storage.read_file(latest_checkpoint)
                content_lines = content.splitlines()
                if len(content_lines) > 1 and content_lines[0] == self.STATUS_COMPLETED:
                    checkpoint_content = "\n".join(content_lines[1:])
                    parsed_checkpoint = self.state_interpreter.parse_data(checkpoint_content)
                    
                    if isinstance(parsed_checkpoint, dict):
                        previous_batch_ids = parsed_checkpoint.get("processed_batch_ids", [])
                        previous_content = parsed_checkpoint.get("data", "")
                        logging.info(f"Found previous checkpoint with {len(previous_batch_ids)} batches")
            except Exception as e:
                logging.error(f"Error reading previous checkpoint {latest_checkpoint}: {e}")
        
        # Collect data from all logs
        log_data = {}
        for log_file in log_files:
            try:
                # Read the log content
                content = self.storage.read_file(log_file)
                
                # Skip processing logs
                if content.startswith(self.STATUS_PROCESSING):
                    continue
                
                # Parse data from content (skip status line)
                content_lines = content.splitlines()
                if len(content_lines) > 1:
                    data_content = "\n".join(content_lines[1:])
                    parsed_data = self.state_interpreter.parse_data(data_content)
                    
                    # Extract operation ID from filename
                    file_name = log_file.name
                    operation_id = file_name[len(self.LOG_PREFIX):-len(self.LOG_EXTENSION)]
                    
                    # Store with operation ID as key
                    log_data[operation_id] = parsed_data
                
            except Exception as e:
                logging.error(f"Error processing log file {log_file} during checkpoint: {e}")
        
        # If we have data to checkpoint
        if log_data:
            timestamp = str(int(time.time()))
            checkpoint_path = self._get_checkpoint_file_path(client_dir, timestamp)
            
            # Merge all log data using the state interpreter
            merged_data = self.state_interpreter.merge_data(log_data)
            
            # Extract batch IDs and full content for simplified checkpoint format
            new_batch_ids = merged_data.get("_processed_batch_ids", [])
            new_content = merged_data.get("_full_content", "")
            
            # Combine with previous checkpoint data
            all_batch_ids = list(set(previous_batch_ids + new_batch_ids))
            
            # Combine content - if both exist, we need to be smart about not duplicating
            full_content = new_content
            if previous_content and not new_content:
                full_content = previous_content
            elif previous_content and new_content:
                # Simple concatenation for now - in a real system, we'd need logic to avoid duplications
                full_content = previous_content + " " + new_content
            
            # Create a checkpoint with the simplified format as requested
            checkpoint_data = {
                "processed_batch_ids": all_batch_ids,
                "data": full_content,
                "processed_logs": list(log_data.keys()),
                "timestamp": timestamp
            }
            
            # Format the data
            formatted_data = self.state_interpreter.format_data(checkpoint_data)
            
            # Write to checkpoint file
            checkpoint_content = f"{self.STATUS_COMPLETED}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if success:
                # Remove processed log files
                for log_file in log_files:
                    try:
                        self.storage.delete_file(log_file)
                    except Exception as e:
                        logging.warning(f"Failed to delete log file {log_file} after checkpoint: {e}")
                
                # Find and delete previous checkpoints
                self._delete_previous_checkpoints(client_dir, checkpoint_path)
                
                logging.info(f"Created checkpoint for client {client_id} with {len(log_data)} logs")
                return True
            else:
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False
        
        return True
    
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
            
            # Check if we need to create a checkpoint
            log_count = self._count_logs(client_dir)
            if log_count >= self.CHECKPOINT_THRESHOLD:
                self._create_checkpoint(client_id)
            
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
        processed_logs = set()
        
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
                    
                    # Extract data and processed logs from the new checkpoint format
                    if isinstance(parsed_data, dict):
                        # Handle the new checkpoint format with direct string data
                        if "data" in parsed_data and isinstance(parsed_data["data"], str):
                            # For the full content string format, create a synthetic entry
                            all_data["_full_content_entry"] = {
                                "batch": "all",
                                "content": parsed_data["data"],
                                "is_combined_content": True
                            }
                            
                            # Also store the list of processed batch IDs if available
                            if "processed_batch_ids" in parsed_data:
                                batch_ids = parsed_data["processed_batch_ids"]
                                all_data["_processed_batch_ids"] = batch_ids
                                
                                # Create individual synthetic entries for each batch ID
                                # This helps with compatibility with code expecting individual entries
                                for batch_id in batch_ids:
                                    if isinstance(batch_id, (str, int)):
                                        all_data[f"batch_{batch_id}"] = {
                                            "batch": str(batch_id),
                                            "content": "Part of combined content",
                                            "_from_checkpoint": True
                                        }
                            
                        # Handle legacy format where data is a dictionary of entries
                        elif "data" in parsed_data and isinstance(parsed_data["data"], dict):
                            all_data.update(parsed_data["data"])
                            
                        if "processed_logs" in parsed_data:
                            processed_logs.update(parsed_data["processed_logs"])
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
                
                # Skip if already processed in the checkpoint
                if operation_id in processed_logs:
                    continue
                
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
    
    def has_completed_operation(self, client_id: str, operation_id: str) -> bool:
        """
        Check if an operation has been completed for a client.
        
        Args:
            client_id: Client identifier
            operation_id: Operation identifier
            
        Returns:
            bool: True if operation exists and is marked as COMPLETED
        """
        client_dir = self._get_client_dir(client_id)
        log_path = self._get_log_file_path(client_dir, operation_id)
        
        if not self.storage.file_exists(log_path):
            return False
            
        try:
            content = self.storage.read_file(log_path)
            first_line = content.splitlines()[0] if content else ""
            return first_line == self.STATUS_COMPLETED
        except Exception as e:
            logging.error(f"Error checking operation {operation_id}: {e}")
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
        
    def list_operations(self, client_id: str) -> Dict[str, bool]:
        """
        List all operations stored for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Dict[str, bool]: Dictionary mapping operation IDs to their completion status
        """
        client_dir = self._get_client_dir(client_id)
        operations = {}
        
        # List all log files
        log_files = self._get_all_logs(client_dir)
        for log_file in log_files:
            # Extract operation ID from filename
            operation_id = log_file.name[len(self.LOG_PREFIX):-len(self.LOG_EXTENSION)]
            is_completed = self.has_completed_operation(client_id, operation_id)
            operations[operation_id] = is_completed
            
        # Also check checkpoint files
        pattern = f"{self.CHECKPOINT_PREFIX}*{self.LOG_EXTENSION}"
        checkpoint_files = self.storage.list_files(client_dir, pattern)
        for checkpoint_file in checkpoint_files:
            try:
                content = self.storage.read_file(checkpoint_file)
                if content.startswith(self.STATUS_COMPLETED):
                    # Extract timestamp from filename
                    timestamp = checkpoint_file.name[len(self.CHECKPOINT_PREFIX):-len(self.LOG_EXTENSION)]
                    operations[f"checkpoint_{timestamp}"] = True
            except Exception as e:
                logging.warning(f"Error checking checkpoint {checkpoint_file}: {e}")
                
        return operations