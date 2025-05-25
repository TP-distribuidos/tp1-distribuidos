import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set

from common.data_persistance.DataPersistenceInterface import DataPersistenceInterface
from common.data_persistance.StorageInterface import StorageInterface
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class WriteAheadLog(DataPersistenceInterface):
    """
    Write-Ahead Log implementation of the DataPersistenceInterface.
    
    This class implements a durable logging mechanism that:
    1. Writes data to log files before processing
    2. Creates checkpoints after every 3 logs
    3. Combines checkpoint with new logs for better efficiency
    4. Deletes old logs and checkpoints after incorporating them into a new checkpoint
    5. Provides recovery capabilities
    
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
        self.processed_ids = {}  # Dictionary to track processed message IDs per client_id
        
        # Set up the base directory for this service
        self.base_dir = Path(base_dir)
        self.storage.create_directory(self.base_dir)
        
        # Set up logging
        logging.info(f"WriteAheadLog initialized for service {service_name} at {base_dir}")
        
        # Load the processed message IDs and log counts from checkpoints
        self._load_processed_ids_and_log_counts()
        
    def _load_processed_ids_and_log_counts(self):
        """
        Load processed message IDs and log counts from existing checkpoints.
        This helps prevent reprocessing of messages after system restarts and
        ensures log count continuity.
        """
        try:
            # Find all client directories
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            logging.info(f"Found {len(client_dirs)} client directories for loading state")
            
            # Process each client directory
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Initialize client data structures if not exists
                if client_id not in self.processed_ids:
                    self.processed_ids[client_id] = set()
                if client_id not in self.log_count:
                    self.log_count[client_id] = 0
                
                # Get the latest checkpoint for this client
                # Get all checkpoints and filter for valid ones
                checkpoint_files = self._get_all_checkpoints(Path(client_dir))
                valid_checkpoints = []
                
                # First scan and identify any incomplete checkpoints (with PROCESSING status)
                for ckpt in checkpoint_files:
                    try:
                        # Quick check of the first line to see if it's a valid checkpoint
                        content = self.storage.read_file(ckpt)
                        content_lines = content.splitlines()
                        
                        # Skip incomplete checkpoints
                        if not content_lines or content_lines[0] == self.STATUS_PROCESSING:
                            logging.warning(f"Found incomplete checkpoint {ckpt.name} for client {client_id}, skipping")
                            # Optionally, could delete these incomplete checkpoints here
                            continue
                        
                        if content_lines[0] == self.STATUS_COMPLETED:
                            valid_checkpoints.append(ckpt)
                    except Exception as e:
                        logging.warning(f"Error checking checkpoint {ckpt}: {e}")
                
                # Sort valid checkpoints by timestamp (most recent first)
                valid_checkpoints.sort(reverse=True)
                
                # Use the latest valid checkpoint
                latest_checkpoint = valid_checkpoints[0] if valid_checkpoints else None
                
                # Process the checkpoint if it exists
                if latest_checkpoint:
                    try:
                        content = self.storage.read_file(latest_checkpoint)
                        
                        # Skip the status line
                        content_lines = content.splitlines()
                        if len(content_lines) > 1 and content_lines[0] == self.STATUS_COMPLETED:
                            # Parse the checkpoint data
                            checkpoint_content = "\n".join(content_lines[1:])
                            checkpoint_data = self.state_interpreter.parse_data(checkpoint_content)
                            
                            # Extract message IDs from checkpoint (supports both formats)
                            if isinstance(checkpoint_data, dict):
                                message_ids = []
                                
                                # New format with direct messages_id and content
                                if "messages_id" in checkpoint_data:
                                    message_ids = checkpoint_data.get("messages_id", [])
                                # Legacy format with data wrapper
                                elif "data" in checkpoint_data and isinstance(checkpoint_data["data"], dict):
                                    inner_data = checkpoint_data["data"]
                                    if "messages_id" in inner_data:
                                        message_ids = inner_data.get("messages_id", [])
                                
                                # Add all message IDs to the processed set
                                for msg_id in message_ids:
                                    self.processed_ids[client_id].add(str(msg_id))
                                
                                # Extract log_count from checkpoint metadata
                                if "_metadata" in checkpoint_data and isinstance(checkpoint_data["_metadata"], dict):
                                    if "log_count" in checkpoint_data["_metadata"]:
                                        log_count = checkpoint_data["_metadata"]["log_count"]
                                        self.log_count[client_id] = log_count
                                        logging.info(f"Restored log_count {log_count} from checkpoint metadata for client {client_id}")
                                # Legacy format with data wrapper
                                elif "data" in checkpoint_data and isinstance(checkpoint_data["data"], dict):
                                    inner_data = checkpoint_data["data"]
                                    if "_metadata" in inner_data and isinstance(inner_data["_metadata"], dict):
                                        if "log_count" in inner_data["_metadata"]:
                                            log_count = inner_data["_metadata"]["log_count"]
                                            self.log_count[client_id] = log_count
                                            logging.info(f"Restored log_count {log_count} from checkpoint metadata for client {client_id}")
                                
                                logging.info(f"Loaded {len(message_ids)} processed message IDs from checkpoint for client {client_id}")
                    except Exception as e:
                        logging.error(f"Error loading processed IDs from checkpoint for client {client_id}: {e}")
                        
                # Now process any completed log files to get message IDs that might
                # not be in the checkpoint yet
                log_files = self._get_all_logs(Path(client_dir))
                for log_file in log_files:
                    try:
                        # Read the log content
                        content = self.storage.read_file(log_file)
                        
                        # Skip processing logs that aren't completed
                        content_lines = content.splitlines()
                        if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                            continue
                        
                        # Parse data from content (skip status line)
                        if len(content_lines) > 1:
                            data_content = "\n".join(content_lines[1:])
                            parsed_data = self.state_interpreter.parse_data(data_content)
                            
                            # Extract message ID from parsed data
                            msg_id = None
                            if isinstance(parsed_data, dict):
                                # Try different field names for message ID
                                for field in ["message_id", "_message_id", "batch"]:
                                    if field in parsed_data:
                                        msg_id = str(parsed_data[field])
                                        break
                            
                            # If found, add to processed IDs
                            if msg_id:
                                self.processed_ids[client_id].add(msg_id)
                    except Exception as e:
                        logging.warning(f"Error processing log file {log_file} for processed IDs: {e}")
                
                # Only update log count based on current log files if it wasn't 
                # already loaded from checkpoint metadata
                if client_id not in self.log_count or self.log_count[client_id] == 0:
                    log_file_count = len(log_files)
                    self.log_count[client_id] = log_file_count
                    logging.info(f"Set log_count to {log_file_count} based on existing log files for client {client_id}")
                
                logging.info(f"Client {client_id} has {len(self.processed_ids[client_id])} " +
                             f"processed message IDs and log count {self.log_count[client_id]}")
        except Exception as e:
            logging.error(f"Error loading processed message IDs: {e}")
    
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
        """
        Get the most recent completed checkpoint file.
        Skips over any incomplete checkpoints (those with PROCESSING status).
        """
        checkpoint_files = self._get_all_checkpoints(client_dir)
        
        if not checkpoint_files:
            return None
            
        # Sort by timestamp in filename (descending)
        checkpoint_files.sort(reverse=True)
        
        # Find the most recent COMPLETED checkpoint
        for checkpoint in checkpoint_files:
            try:
                content = self.storage.read_file(checkpoint)
                first_line = content.splitlines()[0] if content else ""
                
                if first_line == self.STATUS_COMPLETED:
                    return checkpoint
                elif first_line == self.STATUS_PROCESSING:
                    logging.debug(f"Skipping incomplete checkpoint {checkpoint.name}")
            except Exception as e:
                logging.warning(f"Error checking checkpoint status {checkpoint.name}: {e}")
        
        # No valid completed checkpoints found
        return None
    
    def _count_logs(self, client_dir: Path) -> int:
        """Count the number of log files for a client"""
        logs = self._get_all_logs(client_dir)
        return len(logs)
    
    def _create_checkpoint(self, client_id: str) -> bool:
        """
        Create a checkpoint for a client by merging the latest checkpoint (if any) with recent logs.
        The old checkpoint is deleted after successful creation of the new one.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if checkpoint created successfully
        """
        client_dir = self._get_client_dir(client_id)
        
        try:
            # Get latest checkpoint (if any)
            latest_checkpoint = self._get_latest_checkpoint(client_dir)
            
            # Get all log files
            log_files = self._get_all_logs(client_dir)
            if not log_files:
                logging.warning(f"No logs found for client {client_id}, skipping checkpoint creation")
                return False
            
            # Items to merge - will include checkpoint data and logs
            merge_items = []
            
            # First, try to get the latest checkpoint data if it exists
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
                        
                        # Handle both checkpoint formats
                        if isinstance(parsed_data, dict):
                            # New format with direct messages_id and content
                            if "messages_id" in parsed_data and "content" in parsed_data:
                                merge_items.append(parsed_data)
                                logging.info(f"Found existing checkpoint {latest_checkpoint.name} with {len(parsed_data.get('messages_id', []))} messages")
                            # Legacy format with data wrapper
                            elif "data" in parsed_data:
                                checkpoint_data = parsed_data["data"]
                                if isinstance(checkpoint_data, dict):
                                    merge_items.append(checkpoint_data)
                                    logging.info(f"Found existing checkpoint {latest_checkpoint.name} with {len(checkpoint_data.get('messages_id', []))} messages (legacy format)")
                except Exception as e:
                    logging.error(f"Error reading checkpoint {latest_checkpoint} for client {client_id}: {e}")
            
            # Process the new log entries
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
                        # Add to merge items
                        merge_items.append(parsed_data)
                        logging.debug(f"Added log {log_file.name} to checkpoint for client {client_id}")
                except Exception as e:
                    logging.warning(f"Error processing log file {log_file} for checkpoint: {e}")
                    
            # If no valid items to merge, skip checkpoint
            if not merge_items:
                logging.warning(f"No valid items to merge for checkpoint, client {client_id}")
                return False
                
            # Create checkpoint using a two-phase approach
            timestamp = str(int(time.time() * 1000))  # millisecond precision
            
            # Phase 1: Create a temporary checkpoint file
            temp_checkpoint_path = self._get_checkpoint_file_path(client_dir, f"temp_{timestamp}")
            checkpoint_path = self._get_checkpoint_file_path(client_dir, timestamp)
            
            # Use state interpreter to create merged representation
            merged_data = self.state_interpreter.merge_data(merge_items)
            
            # Add metadata to the merged data - add log_count for recovery after restart
            if isinstance(merged_data, dict):
                # Add metadata dictionary if it doesn't exist
                if "_metadata" not in merged_data:
                    merged_data["_metadata"] = {}
                
                # Store current log count
                merged_data["_metadata"]["log_count"] = self.log_count.get(client_id, 0)
                logging.debug(f"Added log_count {self.log_count.get(client_id, 0)} to checkpoint metadata for client {client_id}")
            
            # Format the checkpoint data - directly use the merged data without any wrapper
            formatted_data = self.state_interpreter.format_data(merged_data)
            
            # Phase 1: Write to temporary checkpoint file first
            # Use PROCESSING status to indicate incomplete checkpoint
            temp_checkpoint_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            success = self.storage.write_file(temp_checkpoint_path, temp_checkpoint_content)
            
            if not success:
                logging.error(f"Failed to write temporary checkpoint file for client {client_id}")
                return False
                
            # Phase 2: Now that temp file is written, create the final checkpoint file
            checkpoint_content = f"{self.STATUS_COMPLETED}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if success:
                logging.info(f"Created checkpoint {checkpoint_path.name} for client {client_id} with {len(merge_items)} items")
                
                # Phase 3: Clean up - delete temporary file now that final checkpoint is ready
                try:
                    self.storage.delete_file(temp_checkpoint_path)
                    logging.debug(f"Deleted temporary checkpoint file {temp_checkpoint_path.name}")
                except Exception as e:
                    logging.warning(f"Error deleting temporary checkpoint {temp_checkpoint_path}: {e}")
                    # Continue with cleanup even if temp file deletion fails
                
                # Delete old checkpoint since we've incorporated its data into the new one
                if latest_checkpoint:
                    try:
                        self.storage.delete_file(latest_checkpoint)
                        logging.info(f"Deleted old checkpoint {latest_checkpoint.name}")
                    except Exception as e:
                        logging.warning(f"Error deleting old checkpoint {latest_checkpoint}: {e}")
                
                # Now clean up log files that are included in the checkpoint
                # This is safe because their data is now in the checkpoint
                for log_file in log_files:
                    try:
                        self.storage.delete_file(log_file)
                        logging.info(f"Deleted log file {log_file.name} after checkpoint creation")
                    except Exception as e:
                        logging.warning(f"Error deleting log file {log_file}: {e}")
                
                return True
            else:
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                
                # Clean up the temporary file if the final write failed
                try:
                    if self.storage.file_exists(temp_checkpoint_path):
                        self.storage.delete_file(temp_checkpoint_path)
                        logging.debug(f"Deleted temporary checkpoint file after final checkpoint creation failed")
                except Exception as e:
                    logging.warning(f"Error cleaning up temporary checkpoint file: {e}")
                    
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
        message_id = operation_id  # Default to operation_id if message_id is not found
        if isinstance(data, dict):
            # Make a copy to avoid modifying the original
            data = data.copy()
            
            if 'batch' in data and 'message_id' not in data:
                # Replace batch with message_id
                data['message_id'] = data.pop('batch')
            
            # Extract message_id for checking if already processed
            if 'message_id' in data:
                message_id = str(data['message_id'])
            
            # Add the operation_id to data for reference
            data['_external_operation_id'] = operation_id
        
        # Initialize client data structures if they don't exist
        if client_id not in self.processed_ids:
            self.processed_ids[client_id] = set()
            
        # Check if message has already been processed
        if message_id in self.processed_ids[client_id]:
            logging.info(f"Message ID {message_id} for client {client_id} has already been processed, skipping")
            return True
            
        # Check if there's an existing log file for this operation
        if self.storage.file_exists(log_file_path):
            try:
                content = self.storage.read_file(log_file_path)
                if content.startswith(self.STATUS_COMPLETED):
                    logging.info(f"Data for client {client_id}, operation {operation_id} already persisted")
                    
                    # Add to processed IDs
                    self.processed_ids[client_id].add(message_id)
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
            
            # Add the message ID to the set of processed IDs
            self.processed_ids[client_id].add(message_id)
            logging.debug(f"Added message ID {message_id} to processed IDs for client {client_id}")
            
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
        
        This method consolidates data from the latest checkpoint (if any)
        and all subsequent log files to provide a single coherent view of all data.
        It performs a just-in-time checkpoint creation when retrieving data.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Consolidated data or None if not found
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
                    checkpoint_data = self.state_interpreter.parse_data(checkpoint_content)
                    
                    # Extract data from checkpoint (may or may not have data wrapper)
                    if isinstance(checkpoint_data, dict):
                        # Handle both checkpoint formats - with or without data wrapper
                        if "data" in checkpoint_data:
                            # Legacy format with data wrapper
                            inner_data = checkpoint_data["data"]
                            if isinstance(inner_data, dict):
                                # Store checkpoint data with special key
                                all_data["_checkpoint_data"] = inner_data
                                
                                # Add checkpoint filename key to help identify it in logs
                                checkpoint_name = latest_checkpoint.name
                                all_data[checkpoint_name] = {"data": inner_data}
                                logging.debug(f"Retrieved legacy format checkpoint {checkpoint_name}")
                        elif "messages_id" in checkpoint_data and "content" in checkpoint_data:
                            # New format without data wrapper
                            # Store checkpoint data with special key
                            all_data["_checkpoint_data"] = checkpoint_data
                            
                            # Add checkpoint filename key to help identify it in logs
                            checkpoint_name = latest_checkpoint.name
                            all_data[checkpoint_name] = checkpoint_data
                            logging.debug(f"Retrieved new format checkpoint {checkpoint_name}")
            except Exception as e:
                logging.error(f"Error reading checkpoint {latest_checkpoint} for client {client_id}: {e}")
        
        # Get all log files - since we delete logs after checkpoint creation,
        # all logs in the directory are newer than the latest checkpoint
        log_files = self._get_all_logs(client_dir)
        logging.debug(f"Found {len(log_files)} log files for client {client_id}")
        
        # Process each log file
        for log_file in log_files:
            try:
                # Extract operation ID from filename
                file_name = log_file.name
                operation_id = file_name[len(self.LOG_PREFIX):-len(self.LOG_EXTENSION)]
                
                # Read the log content
                content = self.storage.read_file(log_file)
                
                # Skip processing logs that aren't completed
                content_lines = content.splitlines()
                if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                    continue
                
                # Parse data from content (skip status line)
                if len(content_lines) > 1:
                    data_content = "\n".join(content_lines[1:])
                    parsed_data = self.state_interpreter.parse_data(data_content)
                    all_data[operation_id] = parsed_data
                    logging.debug(f"Retrieved log {file_name}")
            except Exception as e:
                logging.warning(f"Error processing log file {log_file}: {e}")
        
        # If all_data is empty, return None
        if not all_data:
            logging.info(f"No data found for client {client_id}")
            return None
        
        # IMPORTANT: Create a consolidated view before returning
        # This performs a just-in-time consolidation of all data
        try:
            # Use state interpreter to merge all data
            logging.info(f"Consolidating data from checkpoint and {len(log_files)} logs for client {client_id}")
            consolidated_data = self.state_interpreter.merge_data(all_data)
            
            # Create a structure optimized for client consumption
            consolidated_view = {}
            
            # Include messages in the consolidated view with message_id as the key
            if isinstance(consolidated_data, dict) and "messages_id" in consolidated_data:
                message_ids = consolidated_data.get("messages_id", [])
                content = consolidated_data.get("content", "")
                
                # Include the consolidated data directly for reference
                consolidated_view["_consolidated_data"] = consolidated_data
                
                # For easier client consumption, extract each message separately
                # This way the client code doesn't need to know about the internal structure
                for i, msg_id in enumerate(message_ids):
                    consolidated_view[str(msg_id)] = {
                        "message_id": str(msg_id),
                        "content": content  # Each message contains the full content for now
                    }
                
                logging.info(f"Created consolidated view with {len(message_ids)} messages")
                return consolidated_view
            
            # Fallback - if the consolidated structure isn't as expected, return all data
            return all_data
            
        except Exception as e:
            logging.error(f"Error creating consolidated view for client {client_id}: {e}")
            # Fall back to returning the raw data
            return all_data
    
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
            
            # Reset the processed IDs for this client
            if client_id in self.processed_ids:
                processed_count = len(self.processed_ids[client_id])
                self.processed_ids[client_id] = set()
                logging.info(f"Cleared {processed_count} processed message IDs for client {client_id}")
            
            # Reset log count for this client
            if client_id in self.log_count:
                self.log_count[client_id] = 0
                logging.info(f"Reset log count for client {client_id}")
                
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