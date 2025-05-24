import time
import logging
import time
import os
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

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
    
    def _get_checkpoint_info(self, client_id: str) -> Tuple[Optional[Path], bool]:
        """
        Get checkpoint information for recovery.
        
        Returns:
            Tuple of (latest_valid_checkpoint_path, needs_recovery)
        """
        client_dir = self._get_client_dir(client_id)
        valid_checkpoints = []
        incomplete_checkpoints = []
        
        for checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
            try:
                # First check if file exists and has content
                if not (self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0):
                    continue
                    
                # Extract timestamp from filename
                filename = checkpoint.name
                timestamp_str = filename.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, "")
                timestamp = int(timestamp_str)
                
                # Check checkpoint status
                lines = self.storage.read_file_lines(checkpoint)
                if not lines or lines[0].strip() == self.STATUS_PROCESSING:
                    incomplete_checkpoints.append((timestamp, checkpoint))
                    logging.warning(f"Found incomplete checkpoint {checkpoint}, needs recovery")
                    continue
                
                if lines[0].strip() == self.STATUS_COMPLETED:
                    valid_checkpoints.append((timestamp, checkpoint))
                    
            except (ValueError, TypeError, Exception) as e:
                logging.warning(f"Invalid checkpoint {checkpoint}: {e}")
                continue
        
        # Sort checkpoints by timestamp (newest first)
        valid_checkpoints.sort(reverse=True)
        incomplete_checkpoints.sort(reverse=True)
        
        # Determine if recovery is needed
        needs_recovery = False
        if incomplete_checkpoints:
            newest_incomplete_ts = incomplete_checkpoints[0][0]
            if not valid_checkpoints or newest_incomplete_ts > valid_checkpoints[0][0]:
                needs_recovery = True
        
        # Return the latest valid checkpoint (may be None if no valid checkpoints exist)
        latest_valid = valid_checkpoints[0][1] if valid_checkpoints else None
        
        return latest_valid, needs_recovery
    
    def _get_latest_checkpoint_path(self, client_id: str) -> Optional[Path]:
        """Get path to latest valid checkpoint file (for normal operations)"""
        latest_valid, needs_recovery = self._get_checkpoint_info(client_id)
        
        # For normal operations, return None if recovery is needed to trigger rebuild
        if needs_recovery:
            return None
            
        return latest_valid
    
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

            # logging.info("TEST POINT 1: Kill process now to test recovery of PROCESSING state logs")
            # time.sleep(5)  # Wait for manual testing

            # Then, update to COMPLETED status
            # We can use open_file directly to just update the first line
            try:
                with self.storage.open_file(log_path, 'r+') as f:
                    f.write(self.STATUS_COMPLETED)
                    f.flush()
                    os.fsync(f.fileno())
                
                # logging.info("TEST POINT 2: Kill process now to test recovery after log completion")
                # time.sleep(5)  # Wait for manual testing
                
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
        Only processes logs that haven't been incorporated into existing checkpoints.
        
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

        # CRITICAL FIX: Only process logs that haven't been incorporated into ANY valid checkpoint
        unprocessed_log_data = self._read_truly_unprocessed_logs(client_id)
        
        # If no data found
        if not unprocessed_log_data and checkpoint_data is None:
            return None
            
        # If we only have a checkpoint
        if not unprocessed_log_data:
            return checkpoint_data
            
        # If we only have unprocessed logs
        if checkpoint_data is None:
            return self.state_interpreter.merge_data(unprocessed_log_data)
            
        # If we have both checkpoint and unprocessed logs
        # First put checkpoint in the merged data
        merged_data = {'checkpoint': checkpoint_data}
        merged_data.update(unprocessed_log_data)
        
        # Let the interpreter merge everything
        return self.state_interpreter.merge_data(merged_data)

    def _get_latest_checkpoint_path_for_retrieve(self, client_id: str) -> Optional[Path]:
        """
        Get path to latest valid checkpoint file specifically for retrieve operations.
        Unlike the normal _get_latest_checkpoint_path, this doesn't return None for recovery cases,
        as retrieve should work even when there are incomplete checkpoints.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Optional[Path]: Path to latest valid checkpoint or None if no valid checkpoints exist
        """
        latest_valid, _ = self._get_checkpoint_info(client_id)
        return latest_valid
    
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
        # Get current checkpoint data if it exists - use the recovery-aware method
        checkpoint_data = self._get_checkpoint_data_for_recovery(client_id)
        
        # Get all new logs that haven't been processed by ANY valid checkpoint
        log_data = self._read_truly_unprocessed_logs(client_id)
        
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
    
    def _get_checkpoint_data_for_recovery(self, client_id: str) -> Any:
        """
        Get checkpoint data for recovery operations, always using the latest valid checkpoint.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Checkpoint data or None if no valid checkpoint exists
        """
        latest_valid_checkpoint, _ = self._get_checkpoint_info(client_id)
        
        if not latest_valid_checkpoint or not self.storage.file_exists(latest_valid_checkpoint):
            return None
            
        try:
            checkpoint_content = self.storage.read_file(latest_valid_checkpoint)
            parsed_checkpoint = self.state_interpreter.parse_data(checkpoint_content)
            
            # Handle different checkpoint formats
            if isinstance(parsed_checkpoint, dict) and "data" in parsed_checkpoint:
                return parsed_checkpoint["data"]
            else:
                return parsed_checkpoint
                
        except Exception as e:
            logging.warning(f"Error loading checkpoint {latest_valid_checkpoint} for client {client_id}: {e}")
            return None
    
    def _read_all_logs(self, client_id: str) -> Dict[str, Any]:
        """
        Read all unprocessed logs for a client.
        Only reads logs that haven't been processed by ANY valid checkpoint.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Dict[str, Any]: Dictionary mapping operation IDs to parsed data
        """
        return self._read_truly_unprocessed_logs(client_id)
    
    def _read_truly_unprocessed_logs(self, client_id: str) -> Dict[str, Any]:
        """
        Read logs that are truly unprocessed - haven't been incorporated into ANY valid checkpoint.
        Improved orphan detection that's more conservative and accounts for restarts.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Dict[str, Any]: Dictionary mapping operation IDs to parsed data
        """
        client_dir = self._get_client_dir(client_id)
        all_processed_logs = set()
        latest_checkpoint_timestamp = 0
        
        # Get processed logs from ALL valid checkpoints and find the latest checkpoint timestamp
        for checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
            try:
                if not (self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0):
                    continue
                    
                # Extract checkpoint timestamp
                filename = checkpoint.name
                timestamp_str = filename.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, "")
                checkpoint_timestamp = int(timestamp_str)
                
                # Check if this is a valid checkpoint
                lines = self.storage.read_file_lines(checkpoint)
                if not lines or lines[0].strip() != self.STATUS_COMPLETED:
                    continue
                
                # Track the latest valid checkpoint timestamp
                latest_checkpoint_timestamp = max(latest_checkpoint_timestamp, checkpoint_timestamp)
                
                # Get processed logs from this checkpoint
                checkpoint_content = self.storage.read_file(checkpoint)
                checkpoint_data = self.state_interpreter.parse_data(checkpoint_content)
                if isinstance(checkpoint_data, dict) and "processed_logs" in checkpoint_data:
                    all_processed_logs.update(checkpoint_data["processed_logs"])
                    logging.debug(f"Checkpoint {checkpoint.name} processed logs: {checkpoint_data['processed_logs']}")
                    
            except Exception as e:
                logging.warning(f"Error reading checkpoint {checkpoint}: {e}")
                continue
        
        logging.info(f"Client {client_id}: Found {len(all_processed_logs)} logs processed by existing checkpoints: {all_processed_logs}")
        logging.info(f"Client {client_id}: Latest valid checkpoint timestamp: {latest_checkpoint_timestamp}")
        
        # Process logs with improved orphan detection
        log_data = {}
        orphan_logs_deleted = []
        
        for log_path in self.storage.list_files(client_dir, f"*{self.LOG_FILE_EXTENSION}"):
            operation_id = log_path.stem
            
            # Skip logs that were already processed by ANY valid checkpoint
            if operation_id in all_processed_logs:
                logging.debug(f"Skipping explicitly processed log: {operation_id}")
                continue
            
            try:
                # IMPROVED ORPHAN DETECTION LOGIC
                should_delete_as_orphan = False
                
                # Only try orphan detection if we have a valid checkpoint to compare against
                if latest_checkpoint_timestamp > 0:
                    # Try to extract timestamp from operation ID (most reliable)
                    operation_timestamp = self._extract_timestamp_from_operation_id(operation_id)
                    
                    if operation_timestamp is not None:
                        # We have a reliable operation timestamp
                        # Only consider it an orphan if it's significantly older than the checkpoint
                        # Add a safety buffer of 300 seconds (5 minutes) to account for processing delays
                        safety_buffer = 300
                        if operation_timestamp < (latest_checkpoint_timestamp - safety_buffer):
                            logging.info(f"Detected orphan log {operation_id} (operation timestamp: {operation_timestamp} < checkpoint: {latest_checkpoint_timestamp} - {safety_buffer})")
                            should_delete_as_orphan = True
                    else:
                        # No reliable operation timestamp available
                        # Be much more conservative with file timestamps after restarts
                        log_file_timestamp = int(os.path.getmtime(log_path))
                        
                        # Only consider it an orphan if:
                        # 1. The file timestamp is significantly older than checkpoint (more than 1 hour)
                        # 2. AND the log file is in PROCESSING state (incomplete)
                        time_diff = latest_checkpoint_timestamp - log_file_timestamp
                        if time_diff > 3600:  # 1 hour buffer for file timestamp unreliability
                            # Additional check: only delete if the log is incomplete
                            try:
                                lines = self.storage.read_file_lines(log_path)
                                if lines and lines[0].strip() == self.STATUS_PROCESSING:
                                    logging.info(f"Detected incomplete orphan log {operation_id} (file timestamp: {log_file_timestamp}, incomplete, {time_diff}s older than checkpoint)")
                                    should_delete_as_orphan = True
                                else:
                                    logging.info(f"Log {operation_id} is old but complete - keeping for safety (file timestamp: {log_file_timestamp})")
                            except Exception as e:
                                logging.warning(f"Error checking log status for {operation_id}: {e}")
                                # If we can't read it, don't delete it for safety
                        else:
                            logging.debug(f"Log {operation_id} within safety window (file timestamp: {log_file_timestamp}, diff: {time_diff}s)")
                
                # Delete orphan if detected
                if should_delete_as_orphan:
                    try:
                        self.storage.delete_file(log_path)
                        orphan_logs_deleted.append(operation_id)
                        logging.info(f"Deleted orphan log: {operation_id}")
                        continue
                    except Exception as e:
                        logging.warning(f"Failed to delete orphan log {operation_id}: {e}")
                        # Continue processing it as a safety measure
                
                # Process the log if it's not an orphan
                lines = self.storage.read_file_lines(log_path)
                
                if not lines or lines[0].strip() != self.STATUS_COMPLETED:
                    logging.debug(f"Skipping incomplete log: {operation_id}")
                    continue
                
                # Get log content (skip status line)
                log_content = "".join(lines[1:])
                
                # Parse the content
                parsed_data = self.state_interpreter.parse_data(log_content)
                
                # Store with operation ID
                log_data[operation_id] = parsed_data
                logging.debug(f"Added unprocessed log to merge: {operation_id}")
                
            except Exception as e:
                logging.warning(f"Error processing log {log_path}: {e}")
                continue
                
        if orphan_logs_deleted:
            logging.info(f"Client {client_id}: Deleted {len(orphan_logs_deleted)} orphan logs: {orphan_logs_deleted}")
        
        logging.info(f"Client {client_id}: Found {len(log_data)} truly unprocessed logs: {list(log_data.keys())}")
        return log_data

    def _extract_timestamp_from_operation_id(self, operation_id: str) -> Optional[int]:
        """
        Extract timestamp from operation ID if it follows a timestamp-based naming pattern.
        This provides a more reliable way to detect orphan logs.
        
        Args:
            operation_id: The operation ID to parse
            
        Returns:
            Optional[int]: Timestamp if extractable, None otherwise
        """
        try:
            # Pattern 1: operation_id is a float timestamp (e.g., "1748052657.4553096")
            if "." in operation_id:
                float_timestamp = float(operation_id)
                return int(float_timestamp)
            
            # Pattern 2: operation_id contains timestamp (e.g., "op_1234567890")
            if "_" in operation_id:
                parts = operation_id.split("_")
                for part in parts:
                    if part.isdigit() and len(part) >= 10:  # Unix timestamp length
                        return int(part)
                    # Also check for float timestamps in parts
                    if "." in part:
                        try:
                            return int(float(part))
                        except ValueError:
                            continue
            
            # Pattern 3: operation_id is pure integer timestamp
            if operation_id.isdigit() and len(operation_id) >= 10:
                return int(operation_id)
                
            # Pattern 4: operation_id starts with timestamp
            if len(operation_id) >= 10 and operation_id[:10].isdigit():
                return int(operation_id[:10])
                
        except (ValueError, IndexError):
            pass
            
        return None
    
    def _save_checkpoint(self, client_id: str, checkpoint_data: str, log_files_to_delete: List[str]) -> bool:
        """Save checkpoint with two-phase write protocol"""
        client_dir = self._get_client_dir(client_id)
        timestamp = int(time.time())
        new_checkpoint_path = client_dir / f"state_{timestamp}{self.CHECKPOINT_EXTENSION}"
        
        try:
            logging.info("TEST POINT 3: Kill process now to test right before checkpoint file creation")
            time.sleep(5)  # Wait for manual testing

            # First write checkpoint with PROCESSING status
            content = f"{self.STATUS_PROCESSING}\n{checkpoint_data}"
            if not self.storage.write_file(new_checkpoint_path, content):
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False

            logging.info("TEST POINT 4: Kill process now to test checkpoint creation in PROCESSING state")
            time.sleep(5)

            # Update to COMPLETED status
            try:
                with self.storage.open_file(new_checkpoint_path, 'r+') as f:
                    f.write(self.STATUS_COMPLETED)
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                logging.error(f"Error updating checkpoint status: {e}")
                self.storage.delete_file(new_checkpoint_path)
                return False

            logging.info("TEST POINT 5: Kill process now to test checkpoint with COMPLETED status and old logs and checkpoints")
            time.sleep(5)
            
            # CRITICAL FIX: Delete processed logs BEFORE removing old checkpoints
            # This prevents orphan detection from incorrectly identifying processed logs
            for log_name in log_files_to_delete:
                log_path = client_dir / f"{log_name}{self.LOG_FILE_EXTENSION}"
                if self.storage.file_exists(log_path):
                    self.storage.delete_file(log_path)
                    logging.debug(f"Deleted processed log: {log_name}")
            
            # Update tracking info BEFORE cleaning old checkpoints
            self.last_checkpoint_time[client_id] = timestamp
            self.client_log_counts[client_id] = 0

            logging.info("TEST POINT 6: Kill process now to test checkpoint creation with old chekpoints but no old logs")
            time.sleep(5)

            # Now handle old checkpoints (their logs were already cleaned by the processed_logs cleanup above)
            for old_checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
                if old_checkpoint != new_checkpoint_path:
                    logging.info(f"Removing old checkpoint: {old_checkpoint}")
                    self.storage.delete_file(old_checkpoint)
                
            return True
                
        except Exception as e:
            logging.error(f"Error saving checkpoint for client {client_id}: {e}")
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
        """Initialize tracking info and rebuild corrupted checkpoints"""
        for client_dir in self.storage.list_files(self.base_dir):
            if not client_dir.is_dir():
                continue
                
            client_id = client_dir.name
            logging.info(f"Starting recovery for client {client_id}")
            
            # Get checkpoint information
            latest_valid_checkpoint, needs_recovery = self._get_checkpoint_info(client_id)
            
            # Clean up incomplete checkpoints first
            if needs_recovery:
                logging.info(f"Recovery needed for client {client_id}, cleaning incomplete checkpoints")
                self._clean_incomplete_checkpoints(client_id)
            
            # Clean up old checkpoints and their logs, keeping only the latest valid one
            if latest_valid_checkpoint:
                timestamp = int(latest_valid_checkpoint.name.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, ""))
                self._clean_old_checkpoints_and_logs(client_id, timestamp)
                self.last_checkpoint_time[client_id] = timestamp
            
            # CRITICAL FIX: Check for truly unprocessed logs (this will also clean orphan logs)
            unprocessed_logs = self._read_truly_unprocessed_logs(client_id)
            
            # Only create a new checkpoint if we have truly unprocessed logs OR if recovery is needed
            should_create_checkpoint = False
            
            if needs_recovery and not latest_valid_checkpoint:
                # No valid checkpoint but we had incomplete ones - rebuild from all logs
                logging.info(f"No valid checkpoint after recovery cleanup for client {client_id}, rebuilding from logs")
                should_create_checkpoint = True
            elif unprocessed_logs:
                # We have logs that aren't covered by any existing valid checkpoint
                logging.info(f"Found {len(unprocessed_logs)} truly unprocessed logs for client {client_id}")
                should_create_checkpoint = True
            
            if should_create_checkpoint:
                logging.info(f"Creating checkpoint for client {client_id} with {len(unprocessed_logs)} unprocessed logs")
                if self._create_checkpoint(client_id):
                    # Get the newly created checkpoint info
                    latest_valid_checkpoint, _ = self._get_checkpoint_info(client_id)
                    if latest_valid_checkpoint:
                        timestamp = int(latest_valid_checkpoint.name.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, ""))
                        self.last_checkpoint_time[client_id] = timestamp
            
            # Count remaining logs after recovery (should only be truly new logs)
            remaining_logs = self._read_truly_unprocessed_logs(client_id)
            remaining_log_count = len(remaining_logs)
            self.client_log_counts[client_id] = remaining_log_count
            
            if not latest_valid_checkpoint and remaining_log_count == 0:
                # No checkpoint and no logs
                self.last_checkpoint_time[client_id] = 0
            
            logging.info(f"Recovery completed for client {client_id}. "
                        f"Latest checkpoint: {latest_valid_checkpoint.name if latest_valid_checkpoint else 'None'}, "
                        f"Remaining unprocessed logs: {remaining_log_count}")
    
    def cleanup_orphan_logs(self, client_id: str) -> int:
        """
        Manually clean up orphan logs for a specific client.
        This can be called periodically or on-demand to ensure data consistency.
        
        Args:
            client_id: Client identifier
            
        Returns:
            int: Number of orphan logs deleted
        """
        logging.info(f"Starting orphan log cleanup for client {client_id}")
        
        # This will automatically detect and clean orphan logs
        before_count = len(list(self.storage.list_files(self._get_client_dir(client_id), f"*{self.LOG_FILE_EXTENSION}")))
        
        # Read truly unprocessed logs (this triggers orphan cleanup)
        unprocessed_logs = self._read_truly_unprocessed_logs(client_id)
        
        after_count = len(list(self.storage.list_files(self._get_client_dir(client_id), f"*{self.LOG_FILE_EXTENSION}")))
        
        orphans_deleted = before_count - after_count - len(unprocessed_logs)
        
        if orphans_deleted > 0:
            logging.info(f"Cleaned up {orphans_deleted} orphan logs for client {client_id}")
        
        return orphans_deleted
    
    def _clean_incomplete_checkpoints(self, client_id: str) -> None:
        """Remove incomplete checkpoints for a client"""
        client_dir = self._get_client_dir(client_id)
        
        for checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
            try:
                if not (self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0):
                    logging.info(f"Removing empty checkpoint: {checkpoint}")
                    self.storage.delete_file(checkpoint)
                    continue
                    
                # Check checkpoint status
                lines = self.storage.read_file_lines(checkpoint)
                if not lines or lines[0].strip() == self.STATUS_PROCESSING:
                    logging.info(f"Removing incomplete checkpoint: {checkpoint}")
                    self.storage.delete_file(checkpoint)
                    
            except Exception as e:
                logging.warning(f"Error checking checkpoint {checkpoint}: {e}")
                # Remove problematic checkpoint
                try:
                    self.storage.delete_file(checkpoint)
                except:
                    pass
    
    def _clean_old_checkpoints_and_logs(self, client_id: str, keep_timestamp: int) -> None:
        """
        Remove checkpoints older than the specified timestamp and their associated logs.
        This ensures we don't have duplicate data from multiple checkpoints.
        """
        client_dir = self._get_client_dir(client_id)
        
        for checkpoint in self.storage.list_files(client_dir, f"state_*{self.CHECKPOINT_EXTENSION}"):
            try:
                # Extract timestamp from filename
                filename = checkpoint.name
                timestamp_str = filename.replace("state_", "").replace(self.CHECKPOINT_EXTENSION, "")
                timestamp = int(timestamp_str)
                
                if timestamp < keep_timestamp:
                    logging.info(f"Removing old checkpoint: {checkpoint}")
                    
                    # First, clean up logs that belong to this checkpoint
                    try:
                        if self.storage.file_exists(checkpoint) and os.path.getsize(checkpoint) > 0:
                            lines = self.storage.read_file_lines(checkpoint)
                            if lines and lines[0].strip() == self.STATUS_COMPLETED:
                                # This is a valid old checkpoint, clean its logs
                                self._clean_checkpoint_logs(client_dir, checkpoint)
                    except Exception as e:
                        logging.warning(f"Error cleaning logs for old checkpoint {checkpoint}: {e}")
                    
                    # Then delete the checkpoint itself
                    self.storage.delete_file(checkpoint)
                    
            except (ValueError, TypeError) as e:
                logging.warning(f"Invalid checkpoint filename format: {checkpoint}")
                # Remove invalid checkpoint
                try:
                    self.storage.delete_file(checkpoint)
                except:
                    pass
