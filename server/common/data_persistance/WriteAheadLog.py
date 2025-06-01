import logging
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set, Union

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
    
    CHECKPOINT_THRESHOLD = 2

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
        Load processed message IDs and log counts from existing checkpoints and logs.
        This helps prevent reprocessing of messages after system restarts and
        ensures log count continuity.
        Now works with the node-based directory structure.
        """
        try:
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Get all node directories within this client
                node_dirs = []
                for item in self.storage.list_files(client_dir):
                    if Path(item).is_dir():
                        node_dirs.append(item)
                
                # Process each node directory separately
                for node_dir in node_dirs:
                    node_id = Path(node_dir).name
                    node_key = f"{client_id}:{node_id}"
                    
                    self.processed_ids[node_key] = None
                    checkpoint_max_id = None
                    
                    # Get the latest checkpoint for this specific node
                    latest_checkpoint = self._get_latest_checkpoint(node_dir)
                    if latest_checkpoint:
                        checkpoint_content = self._read_completed_file(latest_checkpoint)
                        if checkpoint_content:
                            checkpoint_data = self._parse_file_content(checkpoint_content)
                            
                            if isinstance(checkpoint_data, dict):
                                # Get max_message_id from checkpoint metadata
                                if isinstance(checkpoint_data.get("_metadata"), dict) and "max_message_id" in checkpoint_data["_metadata"]:
                                    max_id = checkpoint_data["_metadata"]["max_message_id"]
                                    if max_id is not None:
                                        try:
                                            checkpoint_max_id = int(max_id)
                                        except (ValueError, TypeError):
                                            logging.warning(f"Invalid max_message_id in checkpoint: {max_id}")
                                            checkpoint_max_id = None
                    
                    # Get all log files for this specific node
                    log_files = self._get_all_logs(node_dir)
                    all_msg_ids = []
                    
                    if checkpoint_max_id is not None:
                        try:
                            all_msg_ids.append(int(checkpoint_max_id))
                        except (ValueError, TypeError):
                            logging.warning(f"Skipping non-integer checkpoint max ID: {checkpoint_max_id}")
                    
                    # Process logs for this specific node
                    for log_file in log_files:
                        log_content = self._read_completed_file(log_file)
                        if log_content:
                            parsed_data = self._parse_file_content(log_content)
                            
                            if isinstance(parsed_data, dict):
                                msg_id = None
                                # Check in _metadata first (standard location)
                                if isinstance(parsed_data.get("_metadata"), dict) and "message_id" in parsed_data["_metadata"]:
                                    msg_id = parsed_data["_metadata"]["message_id"]
                                # Then check direct message_id field
                                elif "message_id" in parsed_data:
                                    msg_id = parsed_data["message_id"]
                                
                                # If found a valid message ID, add to list
                                if msg_id:
                                    try:
                                        msg_id_int = int(msg_id)
                                        all_msg_ids.append(msg_id_int)
                                    except (ValueError, TypeError):
                                        logging.warning(f"Skipping non-integer message ID: {msg_id}")
                    
                    # Calculate the true max message ID considering both checkpoint and logs for this node
                    if all_msg_ids:
                        try:
                            max_id = max(all_msg_ids)
                            self.processed_ids[node_key] = max_id
                            logging.debug(f"Loaded max processed ID {max_id} for client {client_id}, node {node_id}")
                            
                        except Exception as e:
                            logging.warning(f"Error calculating max message ID for client {client_id}, node {node_id}: {e}, using checkpoint value")
                            if checkpoint_max_id is not None:
                                try:
                                    self.processed_ids[node_key] = int(checkpoint_max_id)
                                except (ValueError, TypeError):
                                    logging.warning(f"Invalid checkpoint max_id: {checkpoint_max_id}")
                                    self.processed_ids[node_key] = None
                    
                    # Update the log count based on the actual number of log files for this node
                    log_file_count = len(log_files)
                    node_log_key = f"{client_id}:{node_id}"
                    self.log_count[node_log_key] = log_file_count
                    
                    logging.debug(f"Loaded {log_file_count} log files for client {client_id}, node {node_id}")
                
        except Exception as e:
            logging.error(f"Error loading processed message IDs and log counts: {e}")
    
    def _get_client_dir(self, client_id: str) -> Path:
        """Get the directory path for a specific client's logs"""
        client_dir = self.base_dir / client_id
        self.storage.create_directory(client_dir)
        return client_dir
    
    def _get_node_dir(self, client_id: str, node_id: str) -> Path:
        """Get the directory path for a specific node within a client"""
        client_dir = self._get_client_dir(client_id)
        node_dir = client_dir / node_id
        self.storage.create_directory(node_dir)
        return node_dir
    
    def _get_log_file_path(self, node_dir: Path, message_id: Union[int, str]) -> Path:
        """Get the path for a log file based on message ID"""
        return node_dir / f"{self.LOG_PREFIX}{message_id}{self.LOG_EXTENSION}"
    
    def _get_checkpoint_file_path(self, node_dir: Path, timestamp: str) -> Path:
        """Get the path for a checkpoint file"""
        return node_dir / f"{self.CHECKPOINT_PREFIX}{timestamp}{self.CHECKPOINT_EXTENSION}"
    
    def _get_all_logs(self, node_dir: Path) -> List[Path]:
        """Get all log files for a node"""
        pattern = f"{self.LOG_PREFIX}*{self.LOG_EXTENSION}"
        return self.storage.list_files(node_dir, pattern)
    
    def _get_all_checkpoints(self, node_dir: Path) -> List[Path]:
        """Get all checkpoint files for a node"""
        pattern = f"{self.CHECKPOINT_PREFIX}*{self.CHECKPOINT_EXTENSION}"
        return self.storage.list_files(node_dir, pattern)
    
    def _get_latest_checkpoint(self, node_dir: Path) -> Optional[Path]:
        """
        Get the most recent completed checkpoint file for a specific node.
        Skips over any incomplete checkpoints (those with PROCESSING status).
        """
        checkpoint_files = self._get_all_checkpoints(node_dir)
        
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
    
    def _create_checkpoint(self, client_id: str, node_id: str) -> bool:
        """
        Create a checkpoint for a specific node by merging the latest checkpoint (if any) with recent logs.
        The old checkpoint is deleted after successful creation of the new one.
        """
        node_dir = self._get_node_dir(client_id, node_id)
        
        try:
            latest_checkpoint = self._get_latest_checkpoint(node_dir)
            
            log_files = self._get_all_logs(node_dir)
            if not log_files:
                logging.warning(f"No logs found for client {client_id}, node {node_id}, skipping checkpoint creation")
                return False
            
            # Collect business data from logs and checkpoint for this specific node
            business_data_items = []
            all_message_ids = set()  # For WAL internal tracking
            
            # First, extract data from the existing checkpoint if it exists
            if latest_checkpoint:
                checkpoint_content = self._read_completed_file(latest_checkpoint)
                if checkpoint_content:
                    checkpoint_data = self._parse_file_content(checkpoint_content)
                    
                    if isinstance(checkpoint_data, dict):
                        # Get max_message_id from checkpoint metadata
                        if isinstance(checkpoint_data.get("_metadata"), dict) and "max_message_id" in checkpoint_data["_metadata"]:
                            max_id = checkpoint_data["_metadata"]["max_message_id"]
                            if max_id is not None:
                                max_id_int = int(max_id) if not isinstance(max_id, int) else max_id
                                all_message_ids.add(max_id_int)
                                logging.info(f"Loaded max message ID {max_id_int} from checkpoint metadata for client {client_id}, node {node_id}")
                        
                        # Extract business data - it's stored directly in the checkpoint, not under "content"
                        checkpoint_business_data = {k: v for k, v in checkpoint_data.items() if k != "_metadata"}
                        if checkpoint_business_data:
                            business_data_items.append(checkpoint_business_data)
            
            # Process logs for this specific node
            for log_file in log_files:
                log_content = self._read_completed_file(log_file)
                if log_content:
                    parsed_log = self._parse_file_content(log_content)
                    
                    if isinstance(parsed_log, dict):
                        msg_id = None
                        if "_metadata" in parsed_log and "message_id" in parsed_log["_metadata"]:
                            msg_id = parsed_log["_metadata"]["message_id"]
                        elif "message_id" in parsed_log:
                            msg_id = parsed_log["message_id"]
                    
                        if msg_id:
                            try:
                                all_message_ids.add(int(msg_id))
                            except (ValueError, TypeError):
                                logging.warning(f"Skipping non-integer message ID during checkpoint creation: {msg_id}")
                        
                        business_data = None
                        if "data" in parsed_log:
                            business_data = parsed_log["data"]
                        else:
                            # Remove metadata from business data for consistency with checkpoint extraction
                            business_data = {k: v for k, v in parsed_log.items() if k != "_metadata"}
                            
                        if business_data is not None:
                            business_data_items.append(business_data)
                    
            # If no valid business data items to merge, skip checkpoint
            if not business_data_items:
                logging.warning(f"No business data found for checkpoint, client {client_id}, node {node_id}")
                return False
            
            timestamp = str(int(time.time() * 1000))  # millisecond precision
            
            checkpoint_path = self._get_checkpoint_file_path(node_dir, timestamp)

            merged_business_data = self.state_interpreter.merge_data(business_data_items)

            max_message_id = None
            if all_message_ids:
                max_message_id = max(all_message_ids)
                    
            # Store the merged business data directly with WAL metadata
            # Don't wrap it in a "content" field - store it as-is
            checkpoint_structure = merged_business_data.copy()  # Start with the business data
            checkpoint_structure["_metadata"] = {
                "timestamp": timestamp,
                "max_message_id": max_message_id,
                "node_id": node_id
            }
            
            formatted_data = json.dumps(checkpoint_structure)

            # TEST POINT 1: About to write checkpoint
            # logging.warning(f"TEST POINT 1: About to write checkpoint for client {client_id}, node {node_id} - SLEEPING 5 SECONDS")
            # time.sleep(5)


            checkpoint_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            success = self.storage.write_file(checkpoint_path, checkpoint_content)
            
            if not success:
                logging.error(f"Failed to write checkpoint file for client {client_id}, node {node_id}")
                return False
            
            # TEST POINT 2: Checkpoint is PROCESSING but not yet COMPLETED
            # logging.warning(f"TEST POINT 2: Checkpoint written with PROCESSING status for client {client_id}, node {node_id} - SLEEPING 5 SECONDS")
            # time.sleep(5)
            
            success = self.storage.update_first_line(checkpoint_path, self.STATUS_COMPLETED)
            
            if success:
                # TEST POINT 3: Right before deleting logs for this checkpoint
                # logging.warning(f"TEST POINT 3: About to delete {len(log_files)} logs for client {client_id}, node {node_id} - SLEEPING 5 SECONDS")
                # time.sleep(5)
                
                # Delete logs for this specific node
                for log_file in log_files:
                    try:
                        self.storage.delete_file(log_file)
                    except Exception as e:
                        logging.warning(f"Error deleting log file {log_file}: {e}")

                # Delete old checkpoint for this specific node
                if latest_checkpoint:
                    # TEST POINT 4: Right before deleting old checkpoints
                    # logging.warning(f"TEST POINT 4: About to delete old checkpoint {latest_checkpoint.name} for client {client_id}, node {node_id} - SLEEPING 5 SECONDS")
                    # time.sleep(5)

                    try:
                        self.storage.delete_file(latest_checkpoint)
                    except Exception as e:
                        logging.warning(f"Error deleting old checkpoint {latest_checkpoint}: {e}")
                
                # Reset log count for this node
                node_log_key = f"{client_id}:{node_id}"
                self.log_count[node_log_key] = 0
                
                logging.info(f"Successfully created checkpoint for client {client_id}, node {node_id}")
                return True
            else:
                logging.error(f"Failed to write checkpoint file for client {client_id}, node {node_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error creating checkpoint for client {client_id}, node {node_id}: {e}")
            return False
    
    def persist(self, client_id: str, node_id: str, data: Any, message_id: int) -> bool:
        """
        Persist data for a client and node with write-ahead logging.
        
        This method handles the WAL implementation details, including:
        1. Tracking message IDs for deduplication per node
        2. Managing checkpoint creation thresholds per node
        3. Storing WAL metadata appropriately
        4. Ensuring durability with two-phase writes
        
        The business data is formatted by the StateInterpreter, but all WAL metadata handling
        is contained within this method.
        
        Args:
            client_id: Client identifier
            node_id: Node identifier within the client
            data: Business data to persist (domain-specific)
            message_id: External ID used for reference as integer (used for deduplication)
            
        Returns:
            bool: True if successfully persisted
        """
        timestamp = str(int(time.time() * 1000))  # millisecond precision
        internal_id = f"{timestamp}_{message_id}"
        
        node_dir = self._get_node_dir(client_id, node_id)
        log_file_path = self._get_log_file_path(node_dir, internal_id)
        
        # Create composite key for tracking processed IDs per node
        node_key = f"{client_id}:{node_id}"
        
        if node_key not in self.processed_ids:
            self.processed_ids[node_key] = None
            
        # Check if we have a max processed ID for this client:node
        max_processed_id = self.processed_ids[node_key]
        if max_processed_id is not None:
            if message_id <= max_processed_id:
                logging.info(f"Message ID {message_id} <= max processed ID {max_processed_id} for client {client_id}, node {node_id}, skipping")
                return True
        
        # CHECK CHECKPOINT THRESHOLD BEFORE WRITING NEW LOG
        # This prevents infinite log accumulation if system crashes after writing but before checkpoint
        node_log_key = f"{client_id}:{node_id}"
        current_log_count = len(self._get_all_logs(node_dir))
        
        if current_log_count >= self.CHECKPOINT_THRESHOLD:
            logging.info(f"Log count {current_log_count} >= threshold {self.CHECKPOINT_THRESHOLD} for client {client_id}, node {node_id}, creating checkpoint before new log")
            checkpoint_success = self._create_checkpoint(client_id, node_id)
            if not checkpoint_success:
                logging.warning(f"Checkpoint creation failed for client {client_id}, node {node_id}, but continuing with log write")
        
        try:
            # Let the state interpreter format the data as a string representation
            # The format_data method MUST return a JSON string representing a dictionary with data and _metadata fields
            intermediate_data = self.state_interpreter.format_data(data)
            
            # Parse the intermediate data and add metadata
            parsed_data = json.loads(intermediate_data)
            
            # Validate the contract with StateInterpreter
            if not isinstance(parsed_data, dict) or "data" not in parsed_data or "_metadata" not in parsed_data:
                raise ValueError(f"StateInterpreter.format_data must return a JSON string with 'data' and '_metadata' fields")
                
            parsed_data["_metadata"]["timestamp"] = timestamp
            parsed_data["_metadata"]["message_id"] = message_id
            parsed_data["_metadata"]["node_id"] = node_id  # Add node_id to metadata
            formatted_data = json.dumps(parsed_data)
            
            # Two-phase commit approach:
            # 1. Write with PROCESSING status
            log_content = f"{self.STATUS_PROCESSING}\n{formatted_data}"
            if not self.storage.write_file(log_file_path, log_content):
                return False
                
            # 2. Update status to COMPLETED
            if not self.storage.update_first_line(log_file_path, self.STATUS_COMPLETED):
                return False
            
            # Successfully persisted - update max processed ID for this node
            if self.processed_ids[node_key] is None:
                self.processed_ids[node_key] = message_id
            else:
                if message_id > self.processed_ids[node_key]:
                    self.processed_ids[node_key] = message_id
            
            # Update log count for this node (after successful write)
            self.log_count[node_log_key] = len(self._get_all_logs(node_dir))
            
            return True
        
        except json.JSONDecodeError as e:
            logging.error(f"StateInterpreter.format_data returned invalid JSON for client {client_id}, node {node_id}: {e}")
            raise ValueError("StateInterpreter must return valid JSON") from e
        
        except ValueError as e:
            # Re-raise contract violations from the StateInterpreter
            logging.error(f"Contract violation with StateInterpreter: {e}")
            raise
            
        except Exception as e:
            logging.error(f"Error persisting data for client {client_id}, node {node_id}: {e}")
            # Clean up any partial writes
            if self.storage.file_exists(log_file_path):
                self.storage.delete_file(log_file_path)
            return False
    
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client by consolidating ALL nodes within that client.
        
        This method:
        1. Finds all node directories within the client directory
        2. For each node, gets the latest checkpoint and subsequent logs
        3. Consolidates all business data from all nodes
        4. Provides a single coherent view of business data without WAL implementation details
        5. Abstracts away internal WAL storage details from the consumer
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Consolidated business data from all nodes or None if not found
        """
        client_dir = self._get_client_dir(client_id)
        
        # Collect business data from ALL nodes
        all_business_data_items = []
        
        # Get all node directories within this client
        try:
            node_dirs = []
            for item in self.storage.list_files(client_dir):
                if Path(item).is_dir():
                    node_dirs.append(item)
            
            logging.debug(f"Found {len(node_dirs)} node directories for client {client_id}")
            
            # Process each node directory
            for node_dir_path in node_dirs:
                node_id = Path(node_dir_path).name
                logging.debug(f"Processing node {node_id} for client {client_id}")
                
                # Get latest checkpoint for this node
                latest_checkpoint = self._get_latest_checkpoint(node_dir_path)
                
                if latest_checkpoint:
                    checkpoint_content = self._read_completed_file(latest_checkpoint)
                    if checkpoint_content:
                        checkpoint_data = self._parse_file_content(checkpoint_content)
                        
                        # Extract business data directly (not from "content" field)
                        if isinstance(checkpoint_data, dict):
                            business_data = {k: v for k, v in checkpoint_data.items() if k != "_metadata"}
                            
                            if business_data:
                                all_business_data_items.append(business_data)
                                logging.debug(f"Extracted business content from checkpoint {latest_checkpoint.name} for node {node_id}")
                
                # Get all log files for this node - since we delete logs after checkpoint creation,
                # all logs in the directory are newer than the latest checkpoint
                log_files = self._get_all_logs(node_dir_path)
                logging.debug(f"Found {len(log_files)} log files for client {client_id}, node {node_id}")
                
                for log_file in log_files:
                    log_content = self._read_completed_file(log_file)
                    if log_content:
                        log_data = self._parse_file_content(log_content)
                        
                        if isinstance(log_data, dict):
                            business_data = None
                            if "data" in log_data: 
                                business_data = log_data["data"]
                            else: 
                                # Filter out metadata fields
                                if "_metadata" in log_data:
                                    # Remove metadata from business data
                                    business_data = {k: v for k, v in log_data.items() if k != "_metadata"}
                                else:
                                    # Assume everything is business data
                                    business_data = log_data
                            
                            # Add business data to our collection
                            if business_data is not None:
                                all_business_data_items.append(business_data)
                                logging.debug(f"Extracted business data from log {log_file.name} for node {node_id}")
            
            if not all_business_data_items:
                logging.info(f"No business data found for client {client_id} across all nodes")
                return None
                
            # Use state interpreter to merge all business data items from all nodes
            try:
                if len(all_business_data_items) == 1:
                    return all_business_data_items[0]
                
                consolidated_data = self.state_interpreter.merge_data(all_business_data_items)
                
                logging.info(f"Successfully consolidated data from {len(all_business_data_items)} items across all nodes for client {client_id}")
                return consolidated_data
                
            except Exception as e:
                logging.error(f"Error merging business data from all nodes: {e}")
                
                if all_business_data_items:
                    return all_business_data_items[-1]
                
                return None
                
        except Exception as e:
            logging.error(f"Error retrieving data for client {client_id}: {e}")
            return None
    
    def clear(self, client_id: str) -> bool:
        """
        Clear all data for a client by deleting the entire client directory.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if successfully cleared
        """
        client_dir = self._get_client_dir(client_id)
        
        try:
            # Delete the entire client directory and all its contents recursively
            self.storage.delete_file(client_dir)
            
            # Clear processed_ids and log_count for ALL nodes of this client
            keys_to_remove = []
            for key in self.processed_ids.keys():
                if key.startswith(f"{client_id}:"):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.processed_ids[key]
                if key in self.log_count:
                    del self.log_count[key]
                    
            logging.info(f"Cleared all data for client {client_id}: deleted client directory and {len(keys_to_remove)} tracking entries")
            return True
        except Exception as e:
            logging.error(f"Error clearing data for client {client_id}: {e}")
            return False
        
    def _cleanup_redundant_logs(self):
        """
        Clean up any log files whose message IDs are already included in checkpoints.
        This helps maintain consistency and prevent redundant storage.
        Called during initialization to ensure a clean state.
        Now works with the node-based directory structure.
        """
        try:
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Get all node directories within this client
                node_dirs = []
                for item in self.storage.list_files(client_dir):
                    if Path(item).is_dir():
                        node_dirs.append(item)
                
                # Process each node directory separately
                for node_dir in node_dirs:
                    node_id = Path(node_dir).name
                    
                    latest_checkpoint = self._get_latest_checkpoint(node_dir)
                    
                    if not latest_checkpoint:
                        continue
                        
                    try:
                        checkpoint_content = self._read_completed_file(latest_checkpoint)
                        if not checkpoint_content:
                            continue
                            
                        checkpoint_data = self._parse_file_content(checkpoint_content)
                        
                        max_message_id = None
                        # Get max_message_id from checkpoint metadata
                        if isinstance(checkpoint_data, dict) and isinstance(checkpoint_data.get("_metadata"), dict) and "max_message_id" in checkpoint_data["_metadata"]:
                            max_message_id = checkpoint_data["_metadata"]["max_message_id"]
                        
                        if not max_message_id:
                            continue
                        
                        if not isinstance(max_message_id, int):
                            try:
                                max_message_id = int(max_message_id)
                            except (ValueError, TypeError):
                                logging.warning(f"Invalid max_message_id in checkpoint: {max_message_id}")
                                continue
                            
                        logs_to_delete = []
                        log_files = self._get_all_logs(node_dir)
                        
                        for log_file in log_files:
                            log_content = self._read_completed_file(log_file)
                            if log_content:
                                parsed_log = self._parse_file_content(log_content)
                                
                                msg_id = None
                                if isinstance(parsed_log, dict):
                                    if "_metadata" in parsed_log and "message_id" in parsed_log["_metadata"]:
                                        msg_id = parsed_log["_metadata"]["message_id"]
                                    elif "message_id" in parsed_log:
                                        msg_id = parsed_log["message_id"]
                                
                                # If the message ID is less than or equal to max_message_id, it's already covered by the checkpoint
                                if msg_id is not None:
                                    try:
                                        msg_id_int = int(msg_id) if not isinstance(msg_id, int) else msg_id
                                        if msg_id_int <= max_message_id:
                                            logs_to_delete.append(log_file)
                                    except Exception as e:
                                        logging.warning(f"Error comparing message IDs during cleanup: {e}")
                        
                        if logs_to_delete:
                            for log_file in logs_to_delete:
                                try:
                                    self.storage.delete_file(log_file)
                                except Exception as e:
                                    logging.warning(f"Error deleting redundant log {log_file}: {e}")
                            
                            logging.info(f"WAL: Cleaned up {len(logs_to_delete)} redundant logs for client {client_id}, node {node_id}")
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
        Now works with the node-based directory structure.
        """
        try:
            client_dirs = []
            for item in self.storage.list_files(self.base_dir):
                if Path(item).is_dir():
                    client_dirs.append(item)
            
            for client_dir in client_dirs:
                client_id = Path(client_dir).name
                
                # Get all node directories within this client
                node_dirs = []
                for item in self.storage.list_files(client_dir):
                    if Path(item).is_dir():
                        node_dirs.append(item)
                
                # Process each node directory separately
                for node_dir in node_dirs:
                    node_id = Path(node_dir).name
                    
                    checkpoint_files = self._get_all_checkpoints(node_dir)
                    
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
                            logging.info(f"WAL: Cleaned up {len(old_checkpoints)} old checkpoints for client {client_id}, node {node_id}, keeping {latest_completed.name}")
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
