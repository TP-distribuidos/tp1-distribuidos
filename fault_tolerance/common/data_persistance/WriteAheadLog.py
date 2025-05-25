import logging
import json
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
    
    def _create_checkpoint(self, client_id: str) -> bool:
        """
        Create a checkpoint for a client by merging the latest checkpoint (if any) with recent logs.
        The old checkpoint is deleted after successful creation of the new one.
        
        This method now handles WAL-specific concerns internally:
        1. Tracking message IDs from logs
        2. Managing checkpoint metadata (log_count, timestamps)
        3. Storing and reconstructing WAL-specific structures
        
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
            
            # Collect business data from logs and checkpoint
            business_data_items = []
            all_message_ids = set()  # For WAL internal tracking
            
            # First, extract data from the existing checkpoint if it exists
            checkpoint_business_data = None
            if latest_checkpoint:
                try:
                    # Read checkpoint content
                    content = self.storage.read_file(latest_checkpoint)
                    
                    # Skip the status line
                    content_lines = content.splitlines()
                    if len(content_lines) > 1 and content_lines[0] == self.STATUS_COMPLETED:
                        # Parse the checkpoint data
                        checkpoint_content = "\n".join(content_lines[1:])
                        checkpoint_data = json.loads(checkpoint_content)

                        
                        if isinstance(checkpoint_data, dict):
                            # Extract message IDs for WAL tracking
                            if "messages_id" in checkpoint_data and isinstance(checkpoint_data["messages_id"], list):
                                for msg_id in checkpoint_data["messages_id"]:
                                    all_message_ids.add(str(msg_id))
                                logging.info(f"Loaded {len(checkpoint_data['messages_id'])} message IDs from checkpoint")
                            
                            # Extract just the business data (content field)
                            checkpoint_business_data = {"content": checkpoint_data.get("content", "")}
                            if checkpoint_business_data["content"]:
                                business_data_items.append(checkpoint_business_data)
                except Exception as e:
                    logging.error(f"Error reading checkpoint {latest_checkpoint} for client {client_id}: {e}")
            
            # Process the log entries to extract business data and message IDs
            for log_file in log_files:
                try:
                    # Read the log content
                    content = self.storage.read_file(log_file)
                    
                    # Skip processing logs that aren't complete
                    content_lines = content.splitlines()
                    if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                        continue
                    
                    # Parse data from content (skip status line)
                    if len(content_lines) > 1:
                        data_content = "\n".join(content_lines[1:])
                        parsed_log = json.loads(data_content)
                        
                        # Extract message ID for WAL tracking
                        msg_id = None
                        if isinstance(parsed_log, dict):
                            if "_wal_metadata" in parsed_log and "message_id" in parsed_log["_wal_metadata"]:
                                msg_id = parsed_log["_wal_metadata"]["message_id"]
                            elif "message_id" in parsed_log:
                                msg_id = parsed_log["message_id"]
                        
                        if msg_id:
                            all_message_ids.add(str(msg_id))
                            
                        # Extract business data
                        business_data = None
                        if isinstance(parsed_log, dict):
                            if "data" in parsed_log:  # WAL wrapper format
                                business_data = parsed_log["data"]
                            else:  # Direct business data
                                business_data = parsed_log
                                
                        # Add business data to items for merging
                        if business_data is not None:
                            business_data_items.append(business_data)
                            logging.debug(f"Added business data from log {log_file.name}")
                except Exception as e:
                    logging.warning(f"Error processing log file {log_file} for checkpoint: {e}")
                    
            # If no valid business data items to merge, skip checkpoint
            if not business_data_items:
                logging.warning(f"No business data found for checkpoint, client {client_id}")
                return False
            
            # Create checkpoint using a two-phase approach
            timestamp = str(int(time.time() * 1000))  # millisecond precision
            
            # Create the checkpoint file path
            checkpoint_path = self._get_checkpoint_file_path(client_dir, timestamp)

            # Use state interpreter to merge only the business data
            merged_business_data = self.state_interpreter.merge_data(business_data_items)

            # Create a WAL-specific checkpoint structure with:
            # 1. Business data from the merge
            # 2. WAL-tracked message IDs
            # 3. WAL metadata (log_count, timestamp)
            checkpoint_structure = {
                "content": merged_business_data.get("content", ""),
                "messages_id": sorted(list(all_message_ids)),  # WAL's responsibility to track these
                "_metadata": {
                    "log_count": self.log_count.get(client_id, 0),
                    "timestamp": timestamp
                }
            }
            
            # Format the complete checkpoint structure
            formatted_data = json.dumps(checkpoint_structure)
            
            # Write the checkpoint file with PROCESSING status initially
            checkpoint_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if not success:
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False
                
            # Update just the status line to COMPLETED - more efficient than rewriting the whole file
            success = self.storage.update_first_line(checkpoint_path, self.STATUS_COMPLETED)
            
            if success:
                logging.info(f"Created checkpoint {checkpoint_path.name} for client {client_id} with {len(business_data_items)} business data items and {len(all_message_ids)} tracked message IDs")
                
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
                    except Exception as e:
                        logging.warning(f"Error deleting log file {log_file}: {e}")
                
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
        
        This method handles the WAL implementation details, including:
        1. Tracking message IDs for deduplication
        2. Managing checkpoint creation thresholds
        3. Storing WAL metadata appropriately
        4. Ensuring durability with two-phase writes
        
        The business data is formatted by the StateInterpreter, but all WAL metadata handling
        is contained within this method.
        
        Args:
            client_id: Client identifier
            data: Business data to persist (domain-specific)
            operation_id: External ID used for reference (used as message_id for deduplication)
            
        Returns:
            bool: True if successfully persisted
        """
        # Generate internal timestamp-based ID for storage
        timestamp = str(int(time.time() * 1000))  # millisecond precision
        internal_id = f"{timestamp}_{operation_id}"
        
        client_dir = self._get_client_dir(client_id)
        log_file_path = self._get_log_file_path(client_dir, internal_id)
        
        # Use operation_id as the message_id for tracking
        message_id = str(operation_id)
        
        # Initialize client data structures if they don't exist
        if client_id not in self.processed_ids:
            self.processed_ids[client_id] = set()
            
        # Check if message has already been processed (deduplication)
        if message_id in self.processed_ids[client_id]:
            logging.info(f"Message ID {message_id} for client {client_id} has already been processed, skipping")
            return True
        
        try:
            # Let the state interpreter format the data as a string representation
            # The format_data method MUST return a JSON string representing a dictionary with data and _wal_metadata fields
            intermediate_data = self.state_interpreter.format_data(data)
            
            # Parse the intermediate data and add WAL-specific metadata
            parsed_data = json.loads(intermediate_data)
            
            # Validate the contract with StateInterpreter
            if not isinstance(parsed_data, dict) or "data" not in parsed_data or "_wal_metadata" not in parsed_data:
                raise ValueError(f"StateInterpreter.format_data must return a JSON string with 'data' and '_wal_metadata' fields")
                
            # Add WAL metadata (timestamp and message_id for deduplication)
            parsed_data["_wal_metadata"]["timestamp"] = timestamp
            parsed_data["_wal_metadata"]["message_id"] = message_id
            formatted_data = json.dumps(parsed_data)
            
            # Two-phase commit approach:
            # 1. Write with PROCESSING status
            log_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            if not self.storage.write_file(log_file_path, log_content):
                return False
                
            # 2. Update status to COMPLETED
            if not self.storage.update_first_line(log_file_path, self.STATUS_COMPLETED):
                return False
            
            # Successfully persisted - update tracking information
            self.processed_ids[client_id].add(message_id)
            
            # Manage log count and checkpoint creation
            if client_id not in self.log_count:
                self.log_count[client_id] = 0
            
            self.log_count[client_id] += 1
            logging.info(f"Successfully persisted log for operation {operation_id}, log count: {self.log_count[client_id]}")
            
            # Create a checkpoint if we've reached the threshold
            if self.log_count[client_id] >= self.CHECKPOINT_THRESHOLD:
                logging.info(f"Creating checkpoint after reaching threshold of {self.CHECKPOINT_THRESHOLD} logs")
                self._create_checkpoint(client_id)
                self.log_count[client_id] = 0
            
            return True
        
        except json.JSONDecodeError as e:
            logging.error(f"StateInterpreter.format_data returned invalid JSON for client {client_id}: {e}")
            raise ValueError("StateInterpreter must return valid JSON") from e
        
        except ValueError as e:
            # Re-raise contract violations from the StateInterpreter
            logging.error(f"Contract violation with StateInterpreter: {e}")
            raise
            
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}: {e}")
            # Clean up any partial writes
            if self.storage.file_exists(log_file_path):
                self.storage.delete_file(log_file_path)
            return False
    
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client.
        
        This method:
        1. Consolidates data from the latest checkpoint and all subsequent logs
        2. Provides a single coherent view of business data without WAL implementation details
        3. Abstracts away internal WAL storage details from the consumer
        4. Keeps WAL-specific metadata (like message IDs) internal to the WAL
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Consolidated business data or None if not found
        """
        client_dir = self._get_client_dir(client_id)
        
        # Business data collection - focus on extracting only business data
        business_data_items = []
        
        # Try to get the latest checkpoint
        latest_checkpoint = self._get_latest_checkpoint(client_dir)
        checkpoint_content = None
        
        if latest_checkpoint:
            try:
                # Read checkpoint content
                content = self.storage.read_file(latest_checkpoint)
                
                # Skip the status line which is WAL-specific
                content_lines = content.splitlines()
                if len(content_lines) > 1 and content_lines[0] == self.STATUS_COMPLETED:
                    # Get the checkpoint content without parsing yet
                    checkpoint_content = "\n".join(content_lines[1:])
                    
                    # Parse the checkpoint data
                    checkpoint_data = json.loads(checkpoint_content)
                    
                    # Extract just business data, not WAL metadata
                    if isinstance(checkpoint_data, dict):
                        # Create business data structure with content
                        business_data = {"content": checkpoint_data.get("content", "")}
                        
                        # Add the business data to our collection
                        if business_data["content"]:
                            business_data_items.append(business_data)
                            logging.debug(f"Extracted business content from checkpoint {latest_checkpoint.name}")
            except Exception as e:
                logging.error(f"Error reading checkpoint {latest_checkpoint} for client {client_id}: {e}")
        
        # Get all log files - since we delete logs after checkpoint creation,
        # all logs in the directory are newer than the latest checkpoint
        log_files = self._get_all_logs(client_dir)
        logging.debug(f"Found {len(log_files)} log files for client {client_id}")
        
        # Process each log file to extract business data
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
                    try:
                        # Parse the log entry
                        log_data = json.loads(data_content)
                        
                        # Extract just business data
                        business_data = None
                        if isinstance(log_data, dict):
                            if "data" in log_data:  # WAL wrapper format
                                business_data = log_data["data"]
                            else:  # Direct business data format or legacy format
                                # Filter out WAL-specific fields
                                if "_wal_metadata" in log_data:
                                    # Has WAL metadata but not in expected format
                                    business_data = {k: v for k, v in log_data.items() if k != "_wal_metadata"}
                                else:
                                    # Assume everything is business data
                                    business_data = log_data
                        
                        # Add business data to our collection
                        if business_data is not None:
                            business_data_items.append(business_data)
                            logging.debug(f"Extracted business data from log {log_file.name}")
                    except json.JSONDecodeError:
                        logging.warning(f"Invalid JSON in log file {log_file.name}")
            except Exception as e:
                logging.warning(f"Error processing log file {log_file}: {e}")
        
        # If no business data items found, return None
        if not business_data_items:
            logging.info(f"No business data found for client {client_id}")
            return None
            
        # Use state interpreter to merge business data items
        try:
            if len(business_data_items) == 1:
                # Just one item, no need to merge
                return business_data_items[0]
            
            # Merge all business data items
            logging.info(f"Merging {len(business_data_items)} business data items")
            consolidated_data = self.state_interpreter.merge_data(business_data_items)
            
            return consolidated_data
            
        except Exception as e:
            logging.error(f"Error merging business data: {e}")
            
            # Fallback: return the most recent business data
            if business_data_items:
                return business_data_items[-1]
            
            return None
    
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