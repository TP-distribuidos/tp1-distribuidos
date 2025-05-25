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
        
        # Clean up any redundant logs (logs whose message IDs are already in checkpoints)
        self._cleanup_redundant_logs()

        # Clean up incomplete checkpoints and old checkpoints
        self._cleanup_checkpoints()

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
            
            logging.info(f"Found {len(client_dirs)} client directories for loading state")                # Process each client directory
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Initialize client data structures if not exists
                if client_id not in self.processed_ids:
                    self.processed_ids[client_id] = set()
                
                # Get the latest checkpoint for this client directly using our helper method
                latest_checkpoint = self._get_latest_checkpoint(Path(client_dir))
                
                # Process the checkpoint if it exists
                if latest_checkpoint:
                    checkpoint_content = self._read_completed_file(latest_checkpoint)
                    if checkpoint_content:
                        checkpoint_data = self._parse_file_content(checkpoint_content, use_json=False)
                        
                        # Extract message IDs from checkpoint (supports both formats)
                        if isinstance(checkpoint_data, dict):
                            # Handle the new TCP-style max_message_id approach
                            if "max_message_id" in checkpoint_data:
                                max_id = checkpoint_data.get("max_message_id")
                                if max_id:
                                    # Store the max_message_id for future comparisons
                                    self.processed_ids[client_id].add(str(max_id))
                                    logging.info(f"Loaded max message ID {max_id} from checkpoint for client {client_id}")
                
                # Now process any completed log files to get message IDs that might
                # not be in the checkpoint yet
                log_files = self._get_all_logs(Path(client_dir))
                for log_file in log_files:
                    log_content = self._read_completed_file(log_file)
                    if log_content:
                        parsed_data = self._parse_file_content(log_content, use_json=False)
                        
                        if isinstance(parsed_data, dict):
                            # Extract message ID from parsed data
                            msg_id = None
                            # Try different field names for message ID
                            for field in ["message_id", "_message_id", "batch"]:
                                if field in parsed_data:
                                    msg_id = str(parsed_data[field])
                                    break
                            
                            # If found, add to processed IDs
                            if msg_id:
                                self.processed_ids[client_id].add(msg_id)
                
                # Update the log count based on the actual number of log files
                log_file_count = len(log_files)
                if client_id not in self.log_count:
                    self.log_count[client_id] = log_file_count
                else:
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
                # Just read the first line to check status
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
                checkpoint_content = self._read_completed_file(latest_checkpoint)
                if checkpoint_content:
                    checkpoint_data = self._parse_file_content(checkpoint_content)
                    
                    if isinstance(checkpoint_data, dict):
                        # Extract message IDs for WAL tracking (support both TCP-style and legacy formats)
                        if "max_message_id" in checkpoint_data:
                            # TCP-style approach with max_message_id
                            max_id = checkpoint_data.get("max_message_id")
                            if max_id:
                                all_message_ids.add(str(max_id))
                                logging.info(f"Loaded max message ID {max_id} from checkpoint")
                        
                        # Extract just the business data (content field)
                        checkpoint_business_data = {"content": checkpoint_data.get("content", "")}
                        if checkpoint_business_data["content"]:
                            business_data_items.append(checkpoint_business_data)
            
            # Process the log entries to extract business data and message IDs
            for log_file in log_files:
                log_content = self._read_completed_file(log_file)
                if log_content:
                    parsed_log = self._parse_file_content(log_content)
                    
                    if isinstance(parsed_log, dict):
                        # Extract message ID for WAL tracking
                        msg_id = None
                        if "_wal_metadata" in parsed_log and "message_id" in parsed_log["_wal_metadata"]:
                            msg_id = parsed_log["_wal_metadata"]["message_id"]
                        elif "message_id" in parsed_log:
                            msg_id = parsed_log["message_id"]
                    
                        if msg_id:
                            all_message_ids.add(str(msg_id))
                        
                        # Extract business data
                        business_data = None
                        if "data" in parsed_log:  # WAL wrapper format
                            business_data = parsed_log["data"]
                        else:  # Direct business data
                            business_data = parsed_log
                            
                        # Add business data to items for merging
                        if business_data is not None:
                            business_data_items.append(business_data)
                            logging.debug(f"Added business data from log {log_file.name}")
                    
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

            # Find the maximum message ID (using TCP approach)
            max_message_id = None
            if all_message_ids:
                # Convert all IDs to integers if possible for proper comparison
                numeric_ids = []
                for msg_id in all_message_ids:
                    try:
                        numeric_ids.append(int(msg_id))
                    except (ValueError, TypeError):
                        # If any ID can't be converted, keep using string comparison
                        numeric_ids = None
                        break
                
                if numeric_ids:
                    # If all IDs could be converted to int, find the max numeric ID
                    max_message_id = str(max(numeric_ids))
                else:
                    # Otherwise use string comparison (lexicographical order)
                    max_message_id = max(all_message_ids)
                    
                logging.info(f"Maximum message ID found: {max_message_id} (TCP-style approach)")
            
            # Create a WAL-specific checkpoint structure with:
            # 1. Business data from the merge
            # 2. Maximum message ID (TCP style) instead of all message IDs
            # 3. WAL metadata (timestamp only - no log_count as we count files directly)
            checkpoint_structure = {
                "content": merged_business_data.get("content", ""),
                "max_message_id": max_message_id,  # Store only the maximum ID
                "_metadata": {
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
                for log_file in log_files:
                    try:
                        self.storage.delete_file(log_file)
                    except Exception as e:
                        logging.warning(f"Error deleting log file {log_file}: {e}")

                if latest_checkpoint:
                    try:
                        self.storage.delete_file(latest_checkpoint)
                        logging.info(f"Deleted old checkpoint {latest_checkpoint.name}")
                    except Exception as e:
                        logging.warning(f"Error deleting old checkpoint {latest_checkpoint}: {e}")
                
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
            
        # Check if message has already been processed (using TCP-style approach)
        if self.processed_ids[client_id]:
            # Get the maximum processed ID for this client
            max_processed_id = None
            try:
                # First try to convert all IDs to integers for numeric comparison
                numeric_ids = []
                for msg_id in self.processed_ids[client_id]:
                    try:
                        numeric_ids.append(int(msg_id))
                    except (ValueError, TypeError):
                        numeric_ids = None
                        break
                        
                if numeric_ids:
                    max_processed_id = str(max(numeric_ids))
                else:
                    # If numeric conversion failed, use string comparison
                    max_processed_id = max(self.processed_ids[client_id])
                    
                # Compare the current message_id with the max_processed_id
                # In TCP approach, if message_id <= max_processed_id, it's already processed
                try:
                    # Try numeric comparison first
                    if str(message_id).isdigit() and str(max_processed_id).isdigit():
                        if int(message_id) <= int(max_processed_id):
                            logging.info(f"Message ID {message_id} <= max processed ID {max_processed_id} for client {client_id}, skipping (numeric comparison)")
                            return True
                    # Otherwise use string comparison
                    elif str(message_id) <= str(max_processed_id):
                        logging.info(f"Message ID {message_id} <= max processed ID {max_processed_id} for client {client_id}, skipping (string comparison)")
                        return True
                except Exception as e:
                    logging.warning(f"Error comparing message IDs: {e}, falling back to exact match check")
                    # Fall back to exact match if comparison fails
                    if message_id in self.processed_ids[client_id]:
                        logging.info(f"Message ID {message_id} for client {client_id} has already been processed, skipping (exact match)")
                        return True
            except Exception as e:
                logging.warning(f"Error finding max processed ID: {e}, falling back to exact match check")
                # Fall back to exact match if max finding fails
                if message_id in self.processed_ids[client_id]:
                    logging.info(f"Message ID {message_id} for client {client_id} has already been processed, skipping (exact match)")
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
            
            # Get the current log count based on actual files
            log_files = self._get_all_logs(client_dir)
            self.log_count[client_id] = len(log_files)
            logging.info(f"Successfully persisted log for operation {operation_id}, log count: {self.log_count[client_id]}")
            
            # Create a checkpoint if we've reached the threshold
            if self.log_count[client_id] >= self.CHECKPOINT_THRESHOLD:
                logging.info(f"Creating checkpoint after reaching threshold of {self.CHECKPOINT_THRESHOLD} logs")
                self._create_checkpoint(client_id)
            
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
        
        if latest_checkpoint:
            checkpoint_content = self._read_completed_file(latest_checkpoint)
            if checkpoint_content:
                checkpoint_data = self._parse_file_content(checkpoint_content)
                
                # Extract just business data, not WAL metadata
                if isinstance(checkpoint_data, dict):
                    # Create business data structure with content
                    business_data = {"content": checkpoint_data.get("content", "")}
                    
                    # Add the business data to our collection
                    if business_data["content"]:
                        business_data_items.append(business_data)
                        logging.debug(f"Extracted business content from checkpoint {latest_checkpoint.name}")
        
        # Get all log files - since we delete logs after checkpoint creation,
        # all logs in the directory are newer than the latest checkpoint
        log_files = self._get_all_logs(client_dir)
        logging.debug(f"Found {len(log_files)} log files for client {client_id}")
        
        # Process each log file to extract business data
        for log_file in log_files:
            log_content = self._read_completed_file(log_file)
            if log_content:
                log_data = self._parse_file_content(log_content)
                
                if isinstance(log_data, dict):
                    # Extract just business data
                    business_data = None
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
            
            # The log_count will be automatically 0 on next access since we're counting actual files
                
            return True
        except Exception as e:
            logging.error(f"Error clearing data for client {client_id}: {e}")
            return False
        
    def _cleanup_redundant_logs(self):
        """
        Clean up any log files whose message IDs are already included in checkpoints.
        This helps maintain consistency and prevent redundant storage.
        Called during initialization to ensure a clean state.
        """
        try:
            # Find all client directories
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            logging.info(f"Checking {len(client_dirs)} client directories for redundant logs")
            
            # Process each client directory
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Get the latest checkpoint for this client
                latest_checkpoint = self._get_latest_checkpoint(Path(client_dir))
                
                # If there's no checkpoint, nothing to clean up
                if not latest_checkpoint:
                    continue
                    
                try:
                    # Read checkpoint content to extract message IDs
                    checkpoint_content = self._read_completed_file(latest_checkpoint)
                    if not checkpoint_content:
                        continue
                        
                    checkpoint_data = self._parse_file_content(checkpoint_content)
                    
                    # Extract max message ID from checkpoint (TCP-style approach)
                    max_message_id = None
                    # Handle the new TCP-style max_message_id format
                    if isinstance(checkpoint_data, dict) and "max_message_id" in checkpoint_data:
                        max_message_id = checkpoint_data.get("max_message_id")
                        if max_message_id:
                            max_message_id = str(max_message_id)
                            logging.info(f"Found max message ID {max_message_id} in checkpoint")
                    
                    if not max_message_id:
                        continue
                        
                    # Now check all logs to see if any message IDs are in the checkpoint
                    logs_to_delete = []
                    log_files = self._get_all_logs(Path(client_dir))
                    
                    for log_file in log_files:
                        log_content = self._read_completed_file(log_file)
                        if log_content:
                            parsed_log = self._parse_file_content(log_content)
                            
                            # Extract message ID
                            msg_id = None
                            if isinstance(parsed_log, dict):
                                # Check multiple possible locations for message ID
                                if "_wal_metadata" in parsed_log and "message_id" in parsed_log["_wal_metadata"]:
                                    msg_id = parsed_log["_wal_metadata"]["message_id"]
                                elif "message_id" in parsed_log:
                                    msg_id = parsed_log["message_id"]
                            
                            # In the TCP approach, compare with max_message_id instead of checking membership
                            # If the message ID is less than or equal to max_message_id, it's already covered by the checkpoint
                            if msg_id is not None:
                                # Convert to appropriate type for comparison
                                try:
                                    # Try numeric comparison if both are numbers
                                    if str(msg_id).isdigit() and str(max_message_id).isdigit():
                                        if int(msg_id) <= int(max_message_id):
                                            logs_to_delete.append(log_file)
                                    # Otherwise, fall back to string comparison
                                    elif str(msg_id) <= str(max_message_id):
                                        logs_to_delete.append(log_file)
                                except Exception as e:
                                    logging.warning(f"Error comparing message IDs during cleanup: {e}")
                    
                    # Delete redundant logs
                    if logs_to_delete:
                        for log_file in logs_to_delete:
                            try:
                                self.storage.delete_file(log_file)
                                logging.info(f"Deleted redundant log {log_file.name} for client {client_id} (message ID already in checkpoint)")
                            except Exception as e:
                                logging.warning(f"Error deleting redundant log {log_file}: {e}")
                        
                        logging.info(f"Cleaned up {len(logs_to_delete)} redundant logs for client {client_id}")
                except Exception as e:
                    logging.error(f"Error processing checkpoint {latest_checkpoint} for cleanup: {e}")
                    
        except Exception as e:
            logging.error(f"Error cleaning up redundant logs: {e}")
    
    def _cleanup_checkpoints(self):
        """
        Clean up checkpoints that:
        1. Have PROCESSING status (incomplete/abandoned checkpoints)
        2. Are older than the most recent COMPLETED checkpoint
        
        This helps maintain a clean state by removing stale or corrupted checkpoint files.
        Called during initialization to ensure a clean state.
        """
        try:
            # Find all client directories
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            logging.info(f"Checking {len(client_dirs)} client directories for checkpoint cleanup")
            
            # Process each client directory
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                checkpoint_files = self._get_all_checkpoints(Path(client_dir))
                
                if not checkpoint_files:
                    continue
                
                # Sort checkpoints by name (timestamp) in descending order (newest first)
                checkpoint_files.sort(reverse=True)
                
                # Find the most recent COMPLETED checkpoint
                latest_completed = None
                processing_checkpoints = []
                
                # First pass: identify latest completed checkpoint and any processing checkpoints
                for checkpoint in checkpoint_files:
                    try:
                        content = self.storage.read_file(checkpoint)
                        first_line = content.splitlines()[0] if content else ""
                        
                        if first_line == self.STATUS_COMPLETED:
                            if latest_completed is None:
                                latest_completed = checkpoint
                        elif first_line == self.STATUS_PROCESSING:
                            processing_checkpoints.append(checkpoint)
                    except Exception as e:
                        logging.warning(f"Error checking checkpoint status {checkpoint.name}: {e}")
                
                # Delete all checkpoints with PROCESSING status (incomplete/abandoned)
                for checkpoint in processing_checkpoints:
                    try:
                        self.storage.delete_file(checkpoint)
                        logging.info(f"Deleted incomplete checkpoint {checkpoint.name} for client {client_id}")
                    except Exception as e:
                        logging.warning(f"Error deleting incomplete checkpoint {checkpoint}: {e}")
                
                # Delete old checkpoints (all but the latest completed one)
                if latest_completed:
                    old_checkpoints = [cp for cp in checkpoint_files if cp != latest_completed]
                    for checkpoint in old_checkpoints:
                        try:
                            # Skip if we already deleted it as a processing checkpoint
                            if checkpoint in processing_checkpoints:
                                continue
                                
                            self.storage.delete_file(checkpoint)
                            logging.info(f"Deleted old checkpoint {checkpoint.name} for client {client_id}")
                        except Exception as e:
                            logging.warning(f"Error deleting old checkpoint {checkpoint}: {e}")
                    
                    if old_checkpoints:
                        logging.info(f"Cleaned up {len(old_checkpoints)} old checkpoints for client {client_id}, keeping {latest_completed.name}")
        except Exception as e:
            logging.error(f"Error cleaning up checkpoints: {e}")
    
    def _read_completed_file(self, file_path: Path) -> Optional[str]:
        """
        Read a file and verify if it has the COMPLETED status.
        
        Args:
            file_path: Path to the file to read
            
        Returns:
            str: Content of the file without status line if completed, None otherwise
        """
        try:
            # Read file content
            content = self.storage.read_file(file_path)
            
            # Check status in first line
            content_lines = content.splitlines()
            if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                return None
                
            # Return content without status line
            if len(content_lines) > 1:
                return "\n".join(content_lines[1:])
            else:
                return ""
        except Exception as e:
            logging.warning(f"Error reading file {file_path}: {e}")
            return None
    
    def _parse_file_content(self, content: str, use_json: bool = True) -> Optional[Dict]:
        """
        Parse file content as JSON or using state interpreter.
        
        Args:
            content: Content string to parse
            use_json: If True, use json.loads, otherwise use state_interpreter.parse_data
            
        Returns:
            Dict: Parsed content as dictionary or None if parsing fails
        """
        if not content:
            return None
            
        try:
            if use_json:
                return json.loads(content)
            else:
                return self.state_interpreter.parse_data(content)
        except Exception as e:
            logging.warning(f"Error parsing content: {e}")
            return None