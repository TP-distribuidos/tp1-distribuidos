import socket
import os
import threading
import json
import signal
import time
from Protocol import Protocol
import logging
from Config import Config
from ReconnectionManager import ReconnectionManager


logging.basicConfig(level=logging.INFO)

QUERY_1 = os.getenv("QUERY_1", "1")
QUERY_3 = os.getenv("QUERY_3", "3")
QUERY_4 = os.getenv("QUERY_4", "4")
QUERY_5 = os.getenv("QUERY_5", "5")

class Client:
    def __init__(self, name: str):
        self.skt = None
        self.name = name
        self.protocol = Protocol()
        self.config = Config()
        self.receiver_running = False
        self.receiver_thread = None
        self.output_file_q1 = f"output/output_records_client_{self.name}_Q1.json"
        self.output_file_q3 = f"output/output_records_client_{self.name}_Q3.json"
        self.output_file_q4 = f"output/output_records_client_{self.name}_Q4.json"
        self.output_file_q5 = f"output/output_records_client_{self.name}_Q5.json"
        self.reconnection_manager = ReconnectionManager(self)
        self.file_position = {}  # Track position in each file for resuming

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        
    def _handle_signal(self, sig, frame):
        logging.info(f"Received signal {sig}, initiating graceful shutdown")
        self.shutdown()
    
    def __str__(self):
        return f"Client(name={self.name})"
    
    def connect(self, host: str, port: int):
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.skt.connect((host, port))
        logging.info(f"Connected to {host}:{port}")
    
    def shutdown(self):
        if hasattr(self, 'reconnection_manager'):
            self.reconnection_manager.stop()
        
        self.receiver_running = False
        
        if self.skt is None:
            return             
        try:
            self.skt.shutdown(socket.SHUT_RDWR)
            logging.info("Socket shutdown successfully")
        except OSError:
            pass
        finally:
            self.skt.close()
            self.skt = None
            
        if self.receiver_thread and self.receiver_thread.is_alive():
            self.receiver_thread.join()
            if self.receiver_thread.is_alive():
                logging.warning("Receiver thread did not terminate gracefully")

    def _send_csv(self, file_path: str = None, file_index: int = 0):
        """Send a CSV file with improved reconnection handling and completion tracking"""
        if self.skt is None:
            raise ConnectionError("Socket not connected")
            
        logging.info(f"\033[94mSending CSV file: {file_path}\033[0m")
        batch_sent = 0
        eof_sent = False
        
        # Safely track file position
        if file_path not in self.file_position:
            self.file_position[file_path] = 0
        
        try:
            # Read file in batches, starting from last successful position
            for batch in self._read_file_in_batches(file_path, self.config.get_batch_size(), 
                                        start_position=self.file_position[file_path]):
                # Ensure connection is available before each batch
                if self.skt is None:
                    logging.warning("Connection lost during file transfer, attempting reconnection")
                    if not self.reconnection_manager.reconnect():
                        raise ConnectionError("Failed to reconnect during file transfer")
                
                # Send the batch
                self.protocol.send_all(self.skt, batch, file_index)
                batch_sent += 1
                
                # Update position after successful send
                self.file_position[file_path] += len(batch)
                
                if batch_sent % 50 == 0 and "ratings" in file_path:
                    logging.info(f"Sent {batch_sent} batches so far...")
            
            # Send EOF marker - with retry logic
            max_eof_attempts = 3
            for attempt in range(max_eof_attempts):
                # Check connection before sending EOF
                if self.skt is None:
                    logging.warning("Connection lost before sending EOF, attempting reconnection")
                    if not self.reconnection_manager.reconnect():
                        raise ConnectionError("Failed to reconnect for EOF marker")
                
                try:
                    self.protocol.send_all(self.skt, self.config.get_EOF(), file_index)
                    logging.info(f"\033[94mCSV file sent successfully with EOF: {self.config.get_EOF()}\033[0m")
                    eof_sent = True
                    
                    # Reset file position for this file since it's complete
                    self.file_position[file_path] = 0
                    
                    return True  # Success
                except (ConnectionError, OSError) as e:
                    logging.warning(f"Connection error sending EOF (attempt {attempt+1}/{max_eof_attempts}): {e}")
                    if attempt < max_eof_attempts - 1:
                        if not self.reconnection_manager.reconnect():
                            raise ConnectionError("Failed to reconnect for EOF retry")
            
            if not eof_sent:
                raise ConnectionError("Failed to send EOF after maximum attempts")
                
        except (ConnectionError, OSError) as e:
            logging.error(f"Connection error while sending {file_path}: {e}")
            return False  # Indicate file was not completed
        
        return True  # Indicate file was completed successfully
        
    # Modify _read_file_in_batches to support starting from a position:
    def _read_file_in_batches(self, file_path: str, batch_size: int, start_position=0):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            with open(file_path, 'rb') as f:
                # Skip the header/first line
                header = f.readline()
                
                # Skip to the start position if needed
                if start_position > 0:
                    f.seek(start_position, 1)  # Seek relative to current position (after header)
                    
                batch = b''
                for line in f:
                    # Check if adding this line would exceed batch_size
                    if len(batch) + len(line) > batch_size and batch:
                        # If yes, yield current batch and start a new one with this line
                        yield batch
                        batch = line
                    else:
                        # Otherwise, add line to current batch
                        batch += line
                # Don't forget to yield the last batch if it has data
                if batch:
                    yield batch
        except IOError as e:
            raise IOError(f"Error reading file {file_path}: {e}")

    def start_sender_thread(self, file_paths=None):
        """
        Wrapper method to send data files with proper file completion tracking
        """
        if file_paths is None:
            file_paths = [self.config.get_movies()]
        
        def sender_task():
            try:
                file_index = 0
                
                while file_index < len(file_paths) and self.receiver_running:
                    file_path = file_paths[file_index]
                    
                    # Wait for any active reconnection to complete
                    if self.reconnection_manager.reconnecting.is_set():
                        logging.info("Reconnection in progress, waiting before sending file...")
                        while self.reconnection_manager.reconnecting.is_set() and self.receiver_running:
                            time.sleep(0.5)
                    
                    # Ensure we have a connection before attempting to send
                    if self.skt is None:
                        logging.warning("Socket not connected, attempting reconnection")
                        if not self.reconnection_manager.reconnect():
                            logging.error("Failed to reconnect, stopping file sending")
                            break
                    
                    try:
                        # Attempt to send the file
                        file_completed = self._send_csv(file_path, file_index)
                        
                        # Only advance to next file if this one was completed successfully
                        if file_completed:
                            logging.info(f"File {file_path} completed successfully with EOF")
                            file_index += 1
                        else:
                            logging.warning(f"File {file_path} was not completed, will retry")
                            
                    except Exception as e:
                        logging.error(f"Failed to send file {file_path}: {e}")
                        
                        # For connection errors, wait for reconnection before retrying
                        if isinstance(e, (ConnectionError, OSError)):
                            logging.warning(f"Connection error while sending {file_path}")
                            
                            # Wait for reconnection to complete if it's already in progress
                            if self.reconnection_manager.reconnecting.is_set():
                                logging.info("Reconnection already in progress, waiting...")
                                while self.reconnection_manager.reconnecting.is_set() and self.receiver_running:
                                    time.sleep(0.5)
                            else:
                                # Initiate reconnection if not already in progress
                                if not self.reconnection_manager.reconnect():
                                    logging.error("Failed to reconnect, aborting remaining file transfers")
                                    break
                            
                            # DO NOT continue here - we'll retry the same file
                        
                if self.receiver_running:
                    logging.info("\033[92mAll files completed successfully\033[0m")
                
            except Exception as e:
                if not self.receiver_running:
                    logging.info("Sender thread stopping due to client shutdown")
                else:
                    logging.error(f"Error in sender thread: {e}")
        
        sender_thread = threading.Thread(target=sender_task)
        sender_thread.start()
        return sender_thread
    
    
    def start_receiver_thread(self):
        """
        Wrapper method to start a receiver thread that continuously listens for messages
        """
        self.receiver_running = True
        self.receiver_thread = threading.Thread(target=self._receive_loop)
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        logging.info(f"Receiver thread started, logging to output files")
        return self.receiver_thread
    
    def _receive_loop(self):
        """Continuously receive messages and log them to a file based on query"""
        try:
            while self.receiver_running and self.skt:
                try:
                    query, response_data = self.protocol.recv_response(self.skt)
                    try:

                        parsed_data = json.loads(response_data)
                        if query == QUERY_1:
                            parsed_data = self._format_data_query_1(parsed_data)
                            self._write_to_file(self.output_file_q1, parsed_data)
                        elif query == QUERY_3:
                            self._write_to_file(self.output_file_q3, parsed_data)
                            logging.info(f"\033[94mReceived data for Query {QUERY_3}\033[0m")
                        elif query == QUERY_4:
                            parsed_data = self._format_data_query_4(parsed_data)
                            self._write_to_file(self.output_file_q4, parsed_data)
                            logging.info(f"\033[94mReceived data for Query {QUERY_4}\033[0m")
                        elif query == QUERY_5:
                            self._write_to_file(self.output_file_q5, parsed_data)
                            logging.info(f"\033[94mReceived data for Query {QUERY_5}\033[0m")
                            
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse response as JSON: {e}")
                        logging.info(f"Raw response: {response_data[:100]}...")
                except socket.timeout:
                    continue
                except ConnectionError as e:
                    if not self.receiver_running:
                        logging.info("Receiver thread stopping due to client shutdown")
                        break
                    
                    logging.warning(f"Connection lost in receiver: {e}")
                    # Attempt to reconnect
                    if self.reconnection_manager.reconnect():
                        logging.info("Reconnected. Continuing to receive data.")
                        continue
                    else:
                        logging.error("Failed to reconnect. Stopping receiver.")
                        break
                except (OSError) as e:
                    # Similar handling as ConnectionError
                    if not self.receiver_running:
                        logging.info("Receiver thread stopping due to client shutdown")
                        break
                    
                    logging.warning(f"Socket error in receiver: {e}")
                    if self.reconnection_manager.reconnect():
                        logging.info("Reconnected. Continuing to receive data.")
                        continue
                    else:
                        logging.error("Failed to reconnect. Stopping receiver.")
                        break
        except Exception as e:
            if self.receiver_running:
                logging.error(f"Error in receiver thread: {e}")
        
        logging.info("Receiver thread stopping")

    def _write_to_file(self, file_path: str, data: list):
        """
        Write processed data to a file
        """
        try:
            with open(file_path, 'a') as f:
                for record in data:
                    f.write(json.dumps(record) + "\n")
        except IOError as e:
            logging.error(f"Error writing to file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error writing to file {file_path}: {e}")

    def _format_data_query_4(self, data):
        """
        Format data for Query 4
        """
        formatted_data = []
        for actor in data:
            formatted_data.append({actor.get('name', 'Unknown'): actor.get('count', 0) })
        return formatted_data

    def _format_data_query_1(self, data):
        """
        Format data for Query 1
        """
        formatted_data = []
        for movie in data:
            genres_list = []
            try:
                genres_data = json.loads(movie.get('genres', '[]').replace("'", '"'))
                genres_list = [genre.get('name') for genre in genres_data if genre.get('name')]
            except (json.JSONDecodeError, AttributeError, TypeError):
                pass
            
            formatted_movie = {
                "Movie": movie.get('original_title', 'Unknown'),
                "Genres": genres_list
            }
            formatted_data.append(formatted_movie)
        return formatted_data
