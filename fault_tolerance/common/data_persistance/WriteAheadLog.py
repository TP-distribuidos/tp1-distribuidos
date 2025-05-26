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
    
    LOG_PREFIX = "log_"
    LOG_EXTENSION = ".log"
    CHECKPOINT_PREFIX = "checkpoint_"
    CHECKPOINT_EXTENSION = ".log"
    
    STATUS_PROCESSING = "PROCESSING"
    STATUS_COMPLETED = "COMPLETED"
    
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
        self.log_count = {}  
        self.processed_ids = {}  
        self.base_dir = Path(base_dir)
        self.storage.create_directory(self.base_dir)
        
        logging.info(f"WriteAheadLog initialized for service {service_name} at {base_dir}")
        
        self._cleanup_redundant_logs()

        self._cleanup_checkpoints()

        self._load_processed_ids_and_log_counts()
        
        
    def _load_processed_ids_and_log_counts(self):
        """
        Load processed message IDs and log counts from existing checkpoints.
        This helps prevent reprocessing of messages after system restarts and
        ensures log count continuity.
        """
        try:
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                self.processed_ids[client_id] = None
                checkpoint_max_id = None
                
                latest_checkpoint = self._get_latest_checkpoint(Path(client_dir))
                if latest_checkpoint:
                    checkpoint_content = self._read_completed_file(latest_checkpoint)
                    if checkpoint_content:
                        checkpoint_data = self._parse_file_content(checkpoint_content, use_json=False)
                        
                        if isinstance(checkpoint_data, dict) and "max_message_id" in checkpoint_data:
                            max_id = checkpoint_data.get("max_message_id")
                            if max_id:
                                checkpoint_max_id = str(max_id)
                
                log_files = self._get_all_logs(Path(client_dir))
                all_msg_ids = []  
                
                if checkpoint_max_id is not None:
                    all_msg_ids.append(checkpoint_max_id)
                
                for log_file in log_files:
                    log_content = self._read_completed_file(log_file)
                    if log_content:
                        parsed_data = self._parse_file_content(log_content, use_json=False)
                        
                        if isinstance(parsed_data, dict):
                            msg_id = None
                            # Check in _wal_metadata first (standard location)
                            if isinstance(parsed_data.get("_wal_metadata"), dict) and "message_id" in parsed_data["_wal_metadata"]:
                                msg_id = parsed_data["_wal_metadata"]["message_id"]
                            # Then check direct message_id field
                            elif "message_id" in parsed_data:
                                msg_id = parsed_data["message_id"]
                            
                            # If found a valid message ID, add to our list for max calculation
                            if msg_id:
                                all_msg_ids.append(str(msg_id))
                
                # Calculate the true max message ID considering both checkpoint and logs
                if all_msg_ids:
                    try:
                        # Convert all message IDs to integers once
                        numeric_ids = [int(msg_id) for msg_id in all_msg_ids]
                        max_id = str(max(numeric_ids))
                        self.processed_ids[client_id] = max_id
                        
                    except Exception as e:
                        logging.warning(f"Error calculating max message ID: {e}, using checkpoint value")
                        if checkpoint_max_id is not None:
                            self.processed_ids[client_id] = checkpoint_max_id
                
                # Update the log count based on the actual number of log files
                log_file_count = len(log_files)
                if client_id not in self.log_count:
                    self.log_count[client_id] = log_file_count
                else:
                    self.log_count[client_id] = log_file_count
                
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
            latest_checkpoint = self._get_latest_checkpoint(client_dir)
            
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
                        if "max_message_id" in checkpoint_data:
                            max_id = checkpoint_data.get("max_message_id")
                            if max_id:
                                all_message_ids.add(str(max_id))
                                logging.info(f"Loaded max message ID {max_id} from checkpoint")
                        
                        checkpoint_business_data = {"content": checkpoint_data.get("content", "")}
                        if checkpoint_business_data["content"]:
                            business_data_items.append(checkpoint_business_data)
            
            for log_file in log_files:
                log_content = self._read_completed_file(log_file)
                if log_content:
                    parsed_log = self._parse_file_content(log_content)
                    
                    if isinstance(parsed_log, dict):
                        msg_id = None
                        if "_wal_metadata" in parsed_log and "message_id" in parsed_log["_wal_metadata"]:
                            msg_id = parsed_log["_wal_metadata"]["message_id"]
                        elif "message_id" in parsed_log:
                            msg_id = parsed_log["message_id"]
                    
                        if msg_id:
                            all_message_ids.add(str(msg_id))
                        
                        business_data = None
                        if "data" in parsed_log:
                            business_data = parsed_log["data"]
                        else:
                            business_data = parsed_log
                            
                        if business_data is not None:
                            business_data_items.append(business_data)
                    
            # If no valid business data items to merge, skip checkpoint
            if not business_data_items:
                logging.warning(f"No business data found for checkpoint, client {client_id}")
                return False
            
            timestamp = str(int(time.time() * 1000))  # millisecond precision
            
            checkpoint_path = self._get_checkpoint_file_path(client_dir, timestamp)

            merged_business_data = self.state_interpreter.merge_data(business_data_items)

            max_message_id = None
            if all_message_ids:
                try:
                    numeric_ids = [int(msg_id) for msg_id in all_message_ids]
                    max_message_id = str(max(numeric_ids))
                except (ValueError, TypeError) as e:
                    # Fallback in case there are non-numeric IDs (shouldn't happen with our changes)
                    logging.warning(f"Found non-numeric message IDs: {e}")
                    max_message_id = max(all_message_ids)
                    
            # Create a WAL-specific checkpoint structure with:
            # 1. Business data from the merge
            # 2. Maximum message ID
            # 3. WAL metadata
            checkpoint_structure = {
                "content": merged_business_data.get("content", ""),
                "max_message_id": max_message_id,
                "_metadata": {
                    "timestamp": timestamp
                }
            }
            
            formatted_data = json.dumps(checkpoint_structure)

            checkpoint_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if not success:
                logging.error(f"Failed to write checkpoint file for client {client_id}")
                return False
            
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
        timestamp = str(int(time.time() * 1000))  # millisecond precision
        internal_id = f"{timestamp}_{operation_id}"
        
        client_dir = self._get_client_dir(client_id)
        log_file_path = self._get_log_file_path(client_dir, internal_id)
        
        message_id = str(operation_id)
        
        if client_id not in self.processed_ids:
            self.processed_ids[client_id] = None
            
        # Check if we have a max processed ID for this client
        max_processed_id = self.processed_ids[client_id]
        if max_processed_id is not None:
            # Compare the current message_id with the max_processed_id
            # if message_id <= max_processed_id, it's already processed
            try:
                message_id_int = int(message_id)
                max_processed_id_int = int(max_processed_id)
                if message_id_int <= max_processed_id_int:
                    logging.info(f"Message ID {message_id} <= max processed ID {max_processed_id} for client {client_id}, skipping")
                    return True
            except Exception as e:
                logging.warning(f"Error comparing message IDs: {e}")
                return False
        
        try:
            # Let the state interpreter format the data as a string representation
            # The format_data method MUST return a JSON string representing a dictionary with data and _wal_metadata fields
            intermediate_data = self.state_interpreter.format_data(data)
            
            # Parse the intermediate data and add WAL-specific metadata
            parsed_data = json.loads(intermediate_data)
            
            # Validate the contract with StateInterpreter
            if not isinstance(parsed_data, dict) or "data" not in parsed_data or "_wal_metadata" not in parsed_data:
                raise ValueError(f"StateInterpreter.format_data must return a JSON string with 'data' and '_wal_metadata' fields")
                
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
            
            # Successfully persisted - update max processed ID
            message_id_int = int(message_id)
            if self.processed_ids[client_id] is None:
                self.processed_ids[client_id] = message_id
                logging.info(f"Set initial max processed ID to {message_id} for client {client_id}")
            else:
                max_processed_id_int = int(self.processed_ids[client_id])
                if message_id_int > max_processed_id_int:
                    self.processed_ids[client_id] = message_id
                    logging.info(f"Updated max processed ID to {message_id} for client {client_id}")
            
            log_files = self._get_all_logs(client_dir)
            self.log_count[client_id] = len(log_files)
            
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
        
        business_data_items = []
        
        latest_checkpoint = self._get_latest_checkpoint(client_dir)
        
        if latest_checkpoint:
            checkpoint_content = self._read_completed_file(latest_checkpoint)
            if checkpoint_content:
                checkpoint_data = self._parse_file_content(checkpoint_content)
                
                # Extract just business data, not WAL metadata
                if isinstance(checkpoint_data, dict):
                    business_data = {"content": checkpoint_data.get("content", "")}
                    
                    if business_data["content"]:
                        business_data_items.append(business_data)
                        logging.debug(f"Extracted business content from checkpoint {latest_checkpoint.name}")
        
        # Get all log files - since we delete logs after checkpoint creation,
        # all logs in the directory are newer than the latest checkpoint
        log_files = self._get_all_logs(client_dir)
        logging.debug(f"Found {len(log_files)} log files for client {client_id}")
        
        for log_file in log_files:
            log_content = self._read_completed_file(log_file)
            if log_content:
                log_data = self._parse_file_content(log_content)
                
                if isinstance(log_data, dict):
                    business_data = None
                    if "data" in log_data: 
                        business_data = log_data["data"]
                    else: 
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
        
        if not business_data_items:
            logging.info(f"No business data found for client {client_id}")
            return None
            
        # Use state interpreter to merge business data items
        try:
            if len(business_data_items) == 1:
                return business_data_items[0]
            
            consolidated_data = self.state_interpreter.merge_data(business_data_items)
            
            return consolidated_data
            
        except Exception as e:
            logging.error(f"Error merging business data: {e}")
            
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
        
        try:
            files = self.storage.list_files(client_dir)
            for file_path in files:
                self.storage.delete_file(file_path)
            
            if client_id in self.processed_ids:
                self.processed_ids[client_id] = None
                
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
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                latest_checkpoint = self._get_latest_checkpoint(Path(client_dir))
                
                if not latest_checkpoint:
                    continue
                    
                try:
                    checkpoint_content = self._read_completed_file(latest_checkpoint)
                    if not checkpoint_content:
                        continue
                        
                    checkpoint_data = self._parse_file_content(checkpoint_content)
                    
                    max_message_id = None
                    if isinstance(checkpoint_data, dict) and "max_message_id" in checkpoint_data:
                        max_message_id = checkpoint_data.get("max_message_id")
                    
                    if not max_message_id:
                        continue
                    
                    try:
                        max_message_id_int = int(max_message_id)
                    except (ValueError, TypeError):
                        logging.warning(f"Invalid max_message_id in checkpoint: {max_message_id}")
                        continue
                        
                    logs_to_delete = []
                    log_files = self._get_all_logs(Path(client_dir))
                    
                    for log_file in log_files:
                        log_content = self._read_completed_file(log_file)
                        if log_content:
                            parsed_log = self._parse_file_content(log_content)
                            
                            msg_id = None
                            if isinstance(parsed_log, dict):
                                if "_wal_metadata" in parsed_log and "message_id" in parsed_log["_wal_metadata"]:
                                    msg_id = parsed_log["_wal_metadata"]["message_id"]
                                elif "message_id" in parsed_log:
                                    msg_id = parsed_log["message_id"]
                            
                            # If the message ID is less than or equal to max_message_id, it's already covered by the checkpoint
                            if msg_id is not None:
                                try:
                                    if int(msg_id) <= max_message_id_int:
                                        logs_to_delete.append(log_file)
                                except Exception as e:
                                    logging.warning(f"Error comparing message IDs during cleanup: {e}")
                    
                    if logs_to_delete:
                        for log_file in logs_to_delete:
                            try:
                                self.storage.delete_file(log_file)
                            except Exception as e:
                                logging.warning(f"Error deleting redundant log {log_file}: {e}")
                        
                        logging.info(f"WAL: Cleaned up {len(logs_to_delete)} redundant logs for client {client_id}")
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
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                checkpoint_files = self._get_all_checkpoints(Path(client_dir))
                
                if not checkpoint_files:
                    continue
                
                checkpoint_files.sort(reverse=True)
                
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
                
                # Delete all checkpoints with PROCESSING status
                for checkpoint in processing_checkpoints:
                    try:
                        self.storage.delete_file(checkpoint)
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
                        except Exception as e:
                            logging.warning(f"Error deleting old checkpoint {checkpoint}: {e}")
                    
                    if old_checkpoints:
                        logging.info(f"WAL: Cleaned up {len(old_checkpoints)} old checkpoints for client {client_id}, keeping {latest_completed.name}")
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
            content = self.storage.read_file(file_path)
            
            content_lines = content.splitlines()
            if not content_lines or content_lines[0] != self.STATUS_COMPLETED:
                return None
                
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