import time
import logging
import time
import os
from pathlib import Path
from typing import Any, Dict, Optional, List

from common.DataPersistenceInterface import DataPersistenceInterface
from common.StateInterpreterInterface import StateInterpreterInterface
from common.StorageInterface import StorageInterface
from common.FileSystemStorage import FileSystemStorage

class WriteAheadLog(DataPersistenceInterface):
    """
    Write-Ahead Log implementation of the DataPersistence interface.
    Provides durability through logging with checkpoint optimization.
    """
    
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED_"
    LOG_FILE_EXTENSION = '.log'
    CHECKPOINT_EXTENSION = '.checkpoint'
    DEFAULT_LOG_COUNT_THRESHOLD = 3
    DEFAULT_TIME_THRESHOLD = 3600  # 1 hour
    
    
    def __init__(self, 
                 state_interpreter: StateInterpreterInterface,
                 storage: Optional[StorageInterface] = None,
                 base_dir: str = "/app/persistence", 
                 service_name: Optional[str] = None,
                 log_count_threshold: int = DEFAULT_LOG_COUNT_THRESHOLD,
                 time_threshold: int = DEFAULT_TIME_THRESHOLD):
        """
        Initialize the Write-Ahead Log algorithm.
        
        Args:
            state_interpreter: Interpreter for data formatting/parsing
            storage: Storage implementation (defaults to FileSystemStorage)
            base_dir: Base directory for storing logs and checkpoints
            service_name: Name of the service (for subdirectory)
            log_count_threshold: Number of logs before checkpoint
            time_threshold: Seconds between checkpoints
        """
        # Initialize components
        self.state_interpreter = state_interpreter
        self.storage = storage if storage else FileSystemStorage()
        
        # Set up base directory
        if service_name:
            self.base_dir = Path(base_dir) / service_name
        else:
            self.base_dir = Path(base_dir)
            
        self.storage.create_directory(self.base_dir)
        
        # Checkpoint configuration
        self.log_count_threshold = log_count_threshold
        self.time_threshold = time_threshold
        
        # Checkpoint tracking
        self.client_log_counts = {}  # Maps client_id -> log count since last checkpoint
        self.last_checkpoint_time = {}  # Maps client_id -> timestamp of last checkpoint
        
        # Initialize tracking data
        self._recover_tracking_info()
        
        logging.info(f"WriteAheadLog initialized at {self.base_dir}")
    
    def _get_client_dir(self, client_id: str) -> Path:
        """Get the directory for a specific client's logs"""
        client_dir = self.base_dir / str(client_id)
        self.storage.create_directory(client_dir)
        return client_dir
    
    def _get_latest_checkpoint_path(self, client_id: str) -> Optional[Path]:
        """Get the path to the client's most recent checkpoint file"""
        client_dir = self._get_client_dir(client_id)
        
        # Find all checkpoint files
        checkpoints = list(self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"))
        
        if not checkpoints:
            return None
            
        # Sort by timestamp (embedded in filename)
        # Use the newest valid checkpoint
        valid_checkpoints = []
        for checkpoint in checkpoints:
            try:
                # Extract timestamp from filename (state_TIMESTAMP.checkpoint)
                filename = checkpoint.name
                timestamp_str = filename.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, "")
                timestamp = int(timestamp_str)
                
                # Verify the checkpoint isn't empty or corrupted
                if self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0:
                    valid_checkpoints.append((timestamp, checkpoint))
            except (ValueError, TypeError):
                logging.warning(f"Invalid checkpoint filename format: {checkpoint}")
                
        if not valid_checkpoints:
            return None
            
        # Get the most recent valid checkpoint
        valid_checkpoints.sort(reverse=True)  # Sort by timestamp, newest first
        return valid_checkpoints[0][1]  # Return the path
    
    def persist(self, client_id: str, data: Any, operation_id: str) -> bool:
        """
        Persist data using write-ahead logging.
        
        Args:
            client_id: Client identifier
            data: Data to persist
            operation_id: Unique operation identifier
            
        Returns:
            bool: True if successfully persisted
        """
        client_dir = self._get_client_dir(client_id)

        log_path = client_dir / f"{operation_id}{self.LOG_FILE_EXTENSION}"
        
        # Check if already processed
        if self.storage.file_exists(log_path):
            try:
                lines = self.storage.read_file_lines(log_path)
                if lines and lines[0].strip() == self.STATUS_COMPLETED:
                    logging.info(f"Data for client {client_id}, operation {operation_id} already persisted")
                    return True
            except Exception as e:
                logging.warning(f"Failed to read existing log file: {e}")
                # Continue with rewriting
        
        try:
            # Check if we should create a checkpoint
            self._check_checkpoint_needed(client_id)

            # Format data using the interpreter
            formatted_data = self.state_interpreter.format_data(data)

            # First, write with PROCESSING status
            content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            success = self.storage.write_file(log_path, content)
            if not success:
                return False

            logging.info("TEST POINT 1: Kill process now to test recovery of PROCESSING state logs")
            time.sleep(5)  # Wait for manual testing

            # Then, update to COMPLETED status
            # We can use open_file directly to just update the first line
            try:
                with self.storage.open_file(log_path, 'r+') as f:
                    f.write(self.STATUS_COMPLETED)
                    f.flush()
                    os.fsync(f.fileno())
                
                logging.info("TEST POINT 2: Kill process now to test recovery after log completion")
                time.sleep(5)  # Wait for manual testing
                
                # Update checkpoint tracking
                self.client_log_counts[client_id] = self.client_log_counts.get(client_id, 0) + 1
                return True
            except Exception as e:
                logging.error(f"Error updating status for {log_path}: {e}")
                return False
                
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}: {e}")
            self.storage.delete_file(log_path)
            return False
    
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client by combining checkpoint and logs.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Combined data or None if not found
        """
        client_dir = self._get_client_dir(client_id)
        checkpoint_path = self._get_latest_checkpoint_path(client_id)
        
        # Start with checkpoint data if available
        checkpoint_data = None
        if checkpoint_path and self.storage.file_exists(checkpoint_path):
            try:
                checkpoint_content = self.storage.read_file(checkpoint_path)
                parsed_checkpoint = self.state_interpreter.parse_data(checkpoint_content)
                
                # Handle different checkpoint formats
                if isinstance(parsed_checkpoint, dict) and "data" in parsed_checkpoint:
                    checkpoint_data = parsed_checkpoint["data"]
                else:
                    checkpoint_data = parsed_checkpoint
                    
            except Exception as e:
                logging.warning(f"Error loading checkpoint {checkpoint_path} for client {client_id}: {e}")

        # Process any log files
        log_data = {}
        for log_path in self.storage.list_files(client_dir, f"*{self.LOG_FILE_EXTENSION}"):
            try:
                lines = self.storage.read_file_lines(log_path)
                
                if not lines or lines[0].strip() != self.STATUS_COMPLETED:
                    continue
                
                # Get log content (skip status line)
                log_content = "".join(lines[1:])
                
                # Parse the content
                parsed_data = self.state_interpreter.parse_data(log_content)
                
                # Store with operation ID
                operation_id = log_path.stem
                log_data[operation_id] = parsed_data
                
            except Exception as e:
                logging.warning(f"Error processing log {log_path}: {e}")
                continue
        
        # If no data found
        if not log_data and checkpoint_data is None:
            return None
            
        # If we only have a checkpoint
        if not log_data:
            return checkpoint_data
            
        # If we only have logs
        if checkpoint_data is None:
            return self.state_interpreter.merge_data(log_data)
            
        # If we have both checkpoint and logs
        # First put checkpoint in the merged data
        merged_data = {'checkpoint': checkpoint_data}
        merged_data.update(log_data)
        
        # Let the interpreter merge everything
        return self.state_interpreter.merge_data(merged_data)
    
    def clear(self, client_id: str) -> bool:
        """
        Clear all data for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if successfully cleared
        """
        client_dir = self._get_client_dir(client_id)
        
        # Reset tracking info
        self.client_log_counts.pop(client_id, None)
        self.last_checkpoint_time.pop(client_id, None)
                
        try:
            # Delete all files in the client directory
            for file_path in self.storage.list_files(client_dir):
                self.storage.delete_file(file_path)
            return True
        except Exception as e:
            logging.error(f"Error clearing data for client {client_id}: {e}")
            return False
    
    def _check_checkpoint_needed(self, client_id: str) -> None:
        """
        Check if a checkpoint is needed and create one if necessary.
        
        Args:
            client_id: Client identifier
        """
        current_time = time.time()
        log_count = self.client_log_counts.get(client_id, 0)
        last_checkpoint = self.last_checkpoint_time.get(client_id, 0)
        
        time_elapsed = current_time - last_checkpoint
        
        if (log_count >= self.log_count_threshold) or \
           (time_elapsed >= self.time_threshold and log_count > 0):
            logging.info(f"Creating checkpoint for client {client_id}: {log_count} logs, {time_elapsed:.1f}s elapsed")
            self._create_checkpoint(client_id)
    
    def _create_checkpoint(self, client_id: str) -> bool:
        """
        Create a checkpoint by merging existing checkpoint and new logs.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if checkpoint created successfully
        """
        # Get current checkpoint data if it exists
        checkpoint_path = self._get_latest_checkpoint_path(client_id)
        checkpoint_data = None
        
        if checkpoint_path and self.storage.file_exists(checkpoint_path):
            try:
                checkpoint_content = self.storage.read_file(checkpoint_path)
                parsed_checkpoint = self.state_interpreter.parse_data(checkpoint_content)
                if isinstance(parsed_checkpoint, dict) and "data" in parsed_checkpoint:
                    checkpoint_data = parsed_checkpoint["data"]
                else:
                    checkpoint_data = parsed_checkpoint
            except Exception as e:
                logging.warning(f"Error reading existing checkpoint for {client_id}: {e}")
        
        # Get all new logs
        log_data = self._read_all_logs(client_id)
        
        if not log_data and checkpoint_data is None:
            logging.info(f"No data found for client {client_id}, skipping checkpoint")
            return False
        
        # Merge checkpoint with logs
        if checkpoint_data is None:
            merged_data = self.state_interpreter.merge_data(log_data)
        else:
            # Include checkpoint in the merge
            data_to_merge = {'checkpoint': checkpoint_data}
            data_to_merge.update(log_data)
            merged_data = self.state_interpreter.merge_data(data_to_merge)
        
        log_files_to_delete = list(log_data.keys())

        # Include metadata in checkpoint about which logs were processed
        checkpoint_with_manifest = {
            "data": merged_data,
            "processed_logs": log_files_to_delete
        }
        formatted_checkpoint = self.state_interpreter.format_data(checkpoint_with_manifest)
        
        # Save checkpoint
        return self._save_checkpoint(client_id, formatted_checkpoint, log_files_to_delete)
    
    def _read_all_logs(self, client_id: str) -> Dict[str, Any]:
        """
        Read all logs for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Dict[str, Any]: Dictionary mapping operation IDs to parsed data
        """
        client_dir = self._get_client_dir(client_id)
        checkpoint_path = self._get_latest_checkpoint_path(client_id)
        already_processed_logs = []
        
        # Get list of logs already processed in checkpoint
        if checkpoint_path and self.storage.file_exists(checkpoint_path):
            try:
                checkpoint_content = self.storage.read_file(checkpoint_path)
                checkpoint_data = self.state_interpreter.parse_data(checkpoint_content)
                if isinstance(checkpoint_data, dict) and "processed_logs" in checkpoint_data:
                    already_processed_logs = checkpoint_data["processed_logs"]
            except Exception:
                pass
        
        # Only process logs that weren't in the manifest
        log_data = {}
        for log_path in self.storage.list_files(client_dir, f"*{self.LOG_FILE_EXTENSION}"):
            operation_id = log_path.stem
            if operation_id in already_processed_logs:
                continue
            try:
                lines = self.storage.read_file_lines(log_path)
                
                if not lines or lines[0].strip() != self.STATUS_COMPLETED:
                    continue
                
                # Get log content (skip status line)
                log_content = "".join(lines[1:])
                
                # Parse the content
                parsed_data = self.state_interpreter.parse_data(log_content)
                
                # Store with operation ID
                operation_id = log_path.stem
                log_data[operation_id] = parsed_data
                
            except Exception as e:
                logging.warning(f"Error processing log {log_path}: {e}")
                
        return log_data
    
    def _save_checkpoint(self, client_id: str, checkpoint_data: str, log_files_to_delete: List[str]) -> bool:
        """
        Save a checkpoint and clean up logs using the timestamp-based approach.
        
        Args:
            client_id: Client identifier
            checkpoint_data: Formatted checkpoint data
            log_files_to_delete: List of log file names to delete
            
        Returns:
            bool: True if checkpoint saved successfully
        """
        client_dir = self._get_client_dir(client_id)
        
        # Generate a timestamped checkpoint filename
        timestamp = int(time.time())
        new_checkpoint_path = client_dir / f"state_{timestamp}{self.CHECKPOINT_EXTENSION}"
        
        try:
            # Write new checkpoint file
            if not self.storage.write_file(new_checkpoint_path, checkpoint_data):
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False
            
            logging.info("TEST POINT 3: Kill process now to test checkpoint creation before old checkpoint deletion")
            time.sleep(5)  # Wait for manual testing
        
            
            # Find and delete any older checkpoint files
            for old_checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
                if old_checkpoint != new_checkpoint_path:
                    logging.info(f"Removing old checkpoint: {old_checkpoint}")
                    self.storage.delete_file(old_checkpoint)
            
            # Update tracking info
            self.last_checkpoint_time[client_id] = timestamp
            self.client_log_counts[client_id] = 0
            
            logging.info("TEST POINT 4: Kill process now to test recovery with new checkpoint but undeleted logs")
            time.sleep(5)  # Wait for manual testing

            # Delete log files
            for log_name in log_files_to_delete:
                log_path = client_dir / f"{log_name}{self.LOG_FILE_EXTENSION}"
                self.storage.delete_file(log_path)
                
            logging.info(f"Checkpoint created for client {client_id}, {len(log_files_to_delete)} logs processed")
            return True
            
        except Exception as e:
            logging.error(f"Error saving checkpoint for client {client_id}: {e}")
            # Try to delete the new checkpoint if it was created but there was a subsequent error
            if self.storage.file_exists(new_checkpoint_path):
                self.storage.delete_file(new_checkpoint_path)
            return False
    
    def _clean_checkpoint_logs(self, client_dir: Path, checkpoint_path: Path) -> None:
        """
        Clean up logs that are associated with a specific checkpoint.
        
        Args:
            client_dir: Directory containing the logs
            checkpoint_path: Path to the checkpoint file
        """
        try:
            # Read the checkpoint to get its processed logs
            checkpoint_content = self.storage.read_file(checkpoint_path)
            checkpoint_data = self.state_interpreter.parse_data(checkpoint_content)
            if isinstance(checkpoint_data, dict) and "processed_logs" in checkpoint_data:
                # Delete any logs that were processed in this checkpoint
                for log_name in checkpoint_data["processed_logs"]:
                    log_path = client_dir / f"{log_name}{self.LOG_FILE_EXTENSION}"
                    if self.storage.file_exists(log_path):
                        logging.info(f"Removing processed log during recovery: {log_path}")
                        self.storage.delete_file(log_path)
        except Exception as e:
            logging.error(f"Error cleaning logs for checkpoint {checkpoint_path}: {e}")

    def _recover_tracking_info(self) -> None:
        """Initialize checkpoint tracking information and clean up stale checkpoints/logs"""
        for client_dir in self.storage.list_files(self.base_dir):
            if not client_dir.is_dir():
                continue
                
            client_id = client_dir.name
            logging.info(f"Starting recovery for client {client_id}")
            
            # Get all checkpoints sorted by timestamp (newest first)
            checkpoints = []
            for checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
                try:
                    # Extract timestamp from filename
                    filename = checkpoint.name
                    timestamp_str = filename.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, "")
                    timestamp = int(timestamp_str)
                    if self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0:
                        checkpoints.append((timestamp, checkpoint))
                except (ValueError, TypeError):
                    logging.warning(f"Invalid checkpoint filename format: {checkpoint}")
            
            # Sort checkpoints by timestamp (newest first)
            checkpoints.sort(reverse=True)
            
            if not checkpoints:
                # No checkpoints found, just count logs
                logging.info(f"No checkpoints found for client {client_id}")
                log_count = len(list(self.storage.list_files(client_dir, f"*{self.LOG_FILE_EXTENSION}")))
                self.client_log_counts[client_id] = log_count
                self.last_checkpoint_time[client_id] = 0
                continue
            
            # Process the newest checkpoint
            newest_timestamp, newest_checkpoint = checkpoints[0]
            logging.info(f"Found latest checkpoint for client {client_id}: {newest_checkpoint}")
            
            # Clean up logs associated with the newest checkpoint
            self._clean_checkpoint_logs(client_dir, newest_checkpoint)
            
            # Delete any older checkpoints and their associated logs
            for _, old_checkpoint in checkpoints[1:]:
                logging.info(f"Processing old checkpoint for removal: {old_checkpoint}")
                # First clean up its logs
                self._clean_checkpoint_logs(client_dir, old_checkpoint)
                # Then delete the checkpoint itself
                logging.info(f"Removing old checkpoint: {old_checkpoint}")
                self.storage.delete_file(old_checkpoint)
            
            # Update tracking info
            self.last_checkpoint_time[client_id] = newest_timestamp
            
            # Count remaining logs (these are logs created after the newest checkpoint)
            log_count = len(list(self.storage.list_files(client_dir, f"*{self.LOG_FILE_EXTENSION}")))
            self.client_log_counts[client_id] = log_count
            
            logging.info(f"Recovery completed for client {client_id}. Found {log_count} new logs after checkpoint")
