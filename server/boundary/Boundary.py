import logging
import signal
import socket
import uuid
import os
import threading
import time
import select
import csv
from io import StringIO
from common.Serializer import Serializer
import json
from dotenv import load_dotenv
from ThreadLocalRabbitMQ import ThreadLocalRabbitMQ
from Protocol import Protocol

# Load environment variables
load_dotenv()

# Router queue names from environment variables
MOVIES_ROUTER_QUEUE = os.getenv("MOVIES_ROUTER_QUEUE")
MOVIES_ROUTER_Q5_QUEUE = os.getenv("MOVIES_ROUTER_Q5_QUEUE")
CREDITS_ROUTER_QUEUE = os.getenv("CREDITS_ROUTER_QUEUE")
RATINGS_ROUTER_QUEUE = os.getenv("RATINGS_ROUTER_QUEUE")
NODE_ID = os.getenv("NODE_ID", "boundary_node")

COLUMNS_Q1 = {'genres': 3, 'id':5, 'original_title': 8, 'production_countries': 13, 'release_date': 14}
COLUMNS_Q3 = {'id': 1, 'rating': 2}
COLUMNS_Q4 = {"cast": 0, "movie_id": 2}
COLUMNS_Q5 = {'budget': 2, 'imdb_id':6, 'original_title': 8, 'overview': 9, 'revenue': 15}
EOF_MARKER = "EOF_MARKER"
DISCONNECT_MARKER = "DISCONNECT"

RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
MOVIES_CSV = 0
CREDITS_CSV = 1
RATINGS_CSV = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

class Boundary:
  def __init__(self, port=5000, listen_backlog=100, movies_router_queue=MOVIES_ROUTER_QUEUE, 
               credits_router_queue=CREDITS_ROUTER_QUEUE, ratings_router_queue=RATINGS_ROUTER_QUEUE,
               movies_router_q5_queue=MOVIES_ROUTER_Q5_QUEUE):
    self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self._server_socket.bind(("", port))
    self._server_socket.listen(listen_backlog)
    self._running = True
    self._client_sockets = []
    self._client_threads = {}
    self._lock = threading.Lock()  # Thread synchronization lock
    self.protocol = Protocol
    self.node_id = NODE_ID
    
    self.message_counter = 0
    
    # Create RabbitMQ client instance
    self.rabbitmq = ThreadLocalRabbitMQ()  # Uses the same defaults as RabbitMQClient
    
    # Router queues for different CSV types
    self.movies_router_queue = movies_router_queue
    self.movies_router_q5_queue = movies_router_q5_queue
    self.credits_router_queue = credits_router_queue
    self.ratings_router_queue = ratings_router_queue

    signal.signal(signal.SIGINT, self._handle_shutdown)
    signal.signal(signal.SIGTERM, self._handle_shutdown)

    logging.info(self.green(f"Boundary ID: {self.node_id} successfully created"))
    logging.info(f"Using router queues: Movies={self.movies_router_queue}, Movies Q5={self.movies_router_q5_queue}, Credits={self.credits_router_queue}, Ratings={self.ratings_router_queue}")

  # TODO: Move to printer class
  def green(self, text): return f"\033[92m{text}\033[0m"

# ------------------------------------------------------------------ #
# main accept‑loop                                                   #
# ------------------------------------------------------------------ #
  def run(self):
    logging.info(f"Listening on {self._server_socket.getsockname()[1]}")
    
    # Set up RabbitMQ
    self._setup_rabbitmq()

    # Start response queue consumer thread
    response_thread = threading.Thread(target=self._handle_response_queue, daemon=True)
    response_thread.start()

    # Accept clients in the main thread
    self._accept_clients()

  def _accept_clients(self):
    """Accept new client connections in a loop"""
    while self._running:
      try:
        # Use select with a timeout to make the loop interruptible
        readable, _, _ = select.select([self._server_socket], [], [], 1.0)
        if self._server_socket in readable:
          client_sock, addr = self._server_socket.accept()
          client_id = str(uuid.uuid4())
          logging.info(self.green(f'client id {client_id}'))
          logging.info(f"New client {addr[0]}:{addr[1]}")
          
          with self._lock:
            self._client_sockets.append({client_id: client_sock})
          
          # Start client handling in a new thread
          client_thread = threading.Thread(
            target=self._handle_client_connection,
            args=(client_sock, addr, client_id),
            daemon=True
          )
          with self._lock:
            self._client_threads[client_id] = client_thread
          client_thread.start()
      except Exception as exc:
        if self._running:  # Only log if we're supposed to be running
          logging.error(f"Accept failed: {exc}")

  def _get_next_message_id(self):
    """Get the next incremental message ID for this node"""
    with self._lock:
      self.message_counter += 1
      return self.message_counter

# ------------------------------------------------------------------ #
# response queue consumer logic                                      #
# ------------------------------------------------------------------ #
  def _handle_response_queue(self):
    """
    Handle messages from the response queue in a separate thread.
    """
    try:
        logging.info(self.green(f"Starting response queue consumer thread"))
        
        # Set up RabbitMQ consumer
        self.rabbitmq.client.consume(
            queue_name=RESPONSE_QUEUE,
            callback=self._process_response_message,
            no_ack=False
        )
        
        logging.info(self.green(f"Started consuming from {RESPONSE_QUEUE}"))
        
        # Start consuming (blocking call)
        self.rabbitmq.client.start_consuming()
            
    except Exception as e:
        logging.error(f"Error in response queue handler: {e}")

  def _process_response_message(self, channel, method, properties, body):
    """Process messages from the response queue"""
    try:
        # Deserialize the message
        deserialized_message = Serializer.deserialize(body)
        
        # Extract client_id and data from the deserialized message
        client_id = deserialized_message.get("client_id")
        data = deserialized_message.get("data")
        query = deserialized_message.get("query")

        if not data:
            logging.warning(f"Response message contains no data")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        # Convert data to list if it's not already
        if not isinstance(data, list):
            data = [data]
        
        # Find the client socket by client_id
        client_socket = None
        with self._lock:
            for client_dict in self._client_sockets:
                if client_id in client_dict:
                    client_socket = client_dict[client_id]
                    break
        
        # Send the data to the client if the socket is found
        if client_socket:
            try:
                # Prepare data for sending
                proto = self.protocol()
                
                serialized_data = json.dumps(data)
                proto.send_all(client_socket, serialized_data, query)
                
            except Exception as e:
                logging.error(f"Failed to send data to client {client_id}: {e}")
        else:
            logging.warning(f"Client socket not found for client ID: {client_id}")
        
        # Acknowledge message
        channel.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logging.error(f"Error processing response message: {e}")
        # Reject message but don't requeue
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

# ------------------------------------------------------------------ #
# per‑client logic                                                   #
# ------------------------------------------------------------------ #
  def _handle_client_connection(self, sock, addr, client_id):
    proto = self.protocol()
    logging.info(self.green(f"Client ID: {client_id} successfully started"))
    csvs_received = 0
    client_addr = f"{addr[0]}:{addr[1]}"

    try:
        data = ''
        while True:
            try:
                data = self._receive_csv_batch(sock, proto)
                new_operation_id = self._get_next_message_id()
                if data == EOF_MARKER:
                    self._send_eof_marker(csvs_received, client_id, new_operation_id)
                    csvs_received += 1
                    logging.info(self.green(f"EOF received for CSV #{csvs_received} from client {client_addr}"))
                    continue
                
                if csvs_received == MOVIES_CSV:
                    filtered_data_q1, filtered_data_q5 = self._project_to_columns(data, [COLUMNS_Q1, COLUMNS_Q5])

                    # Send data for Q1 to the movies router
                    prepared_data_q1 = Serializer.add_metadata(client_id, filtered_data_q1, operation_id=new_operation_id, node_id=self.node_id)
                    self._send_data_to_rabbitmq_queue(prepared_data_q1, self.movies_router_queue)

                    # Send data for Q5 to the reviews router
                    prepared_data_q5 = Serializer.add_metadata(client_id, filtered_data_q5, operation_id=new_operation_id, node_id=self.node_id)
                    self._send_data_to_rabbitmq_queue(prepared_data_q5, self.movies_router_q5_queue)
                
                elif csvs_received == CREDITS_CSV:
                    filtered_data = self._project_to_columns(data, COLUMNS_Q4)
                    filtered_data = self._remove_cast_extra_data(filtered_data)
                    prepared_data = Serializer.add_metadata(client_id, filtered_data, operation_id=new_operation_id, node_id=self.node_id)
                    self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
                
                elif csvs_received == RATINGS_CSV:
                    filtered_data = self._project_to_columns(data, COLUMNS_Q3)
                    filtered_data = self._remove_ratings_with_0_rating(filtered_data)
                    prepared_data = Serializer.add_metadata(client_id, filtered_data, operation_id=new_operation_id, node_id=self.node_id)
                    self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)
                
            except ConnectionError:
                logging.info(f"Client {client_addr} disconnected")
                logging.info(f"Treating disconnection as DISCONNECT for client {client_id}")
                new_operation_id = self._get_next_message_id()
                self._send_disconnect_marker(client_id, new_operation_id)
                self._cleanup_client_resources(client_id)
                break
               
    except Exception as exc:
        logging.error(f"Client {client_addr} error: {exc}")
        logging.exception(exc)
        # Also send DISCONNECT markers on uncaught exceptions
        logging.info(f"Treating error as DISCONNECT for client {client_id}")
        new_operation_id = self._get_next_message_id()
        self._send_disconnect_marker(client_id, new_operation_id)
        self._cleanup_client_resources(client_id)

  def _send_disconnect_marker(self, client_id, new_operation_id):
    """
    Send DISCONNECT marker to all router queues for a specific client
    
    Args:
        client_id: The client ID for which to send DISCONNECT marker
    """
    # Create metadata with DISCONNECT flag set to True
    prepared_data = Serializer.add_metadata(client_id, None, eof_marker=False, disconnect_marker=True, operation_id=new_operation_id, node_id=self.node_id)
    
    # Send to all router queues
    for router_queue in [self.movies_router_queue, self.movies_router_q5_queue, 
                        self.credits_router_queue, self.ratings_router_queue]:
        self._send_data_to_rabbitmq_queue(prepared_data, router_queue)
    
    logging.info(f"\033[91mSent DISCONNECT markers to all routers for client {client_id}\033[0m")

  def _send_eof_marker(self, csvs_received, client_id, operation_id):
        prepared_data = Serializer.add_metadata(client_id, None, eof_marker=True, operation_id=operation_id, node_id=self.node_id)
        if csvs_received == MOVIES_CSV:
           self._send_data_to_rabbitmq_queue(prepared_data, self.movies_router_queue)
           self._send_data_to_rabbitmq_queue(prepared_data, self.movies_router_q5_queue)
        elif csvs_received == CREDITS_CSV:
            self._send_data_to_rabbitmq_queue(prepared_data, self.credits_router_queue)
        elif csvs_received == RATINGS_CSV:
           self._send_data_to_rabbitmq_queue(prepared_data, self.ratings_router_queue)
  
  def _cleanup_client_resources(self, client_id):
    """
    Clean up resources associated with a client when they disconnect

    Args:
      client_id: The ID of the client to clean up
    """
    # Remove the client socket
    with self._lock:
      for i, client_dict in enumerate(self._client_sockets):
        if client_id in client_dict:
          # Close the socket if it's still open
          try:
            sock = client_dict[client_id]
            sock.close()
          except Exception as e:
            logging.warning(f"Error closing socket for client {client_id}: {e}")
          
          # Remove from the list
          del self._client_sockets[i]
          break

      # Remove the thread from tracking dictionary
      if client_id in self._client_threads:
        del self._client_threads[client_id]
        
    logging.info(f"\033[94mCleaned up resources for client {client_id}\033[0m")

  def _remove_ratings_with_0_rating(self, data):
    """
    Remove rows with a rating of 0 from the ratings data.
    The rating field is expected to be a string that can be converted to a float.
    """
    result = []
    for row in data:
        if 'rating' in row and row['rating']:
            try:
                # Convert rating to float and check if it's greater than 0
                rating = float(row['rating'])
                if rating > 0:
                    result.append(row)
            except ValueError:
                # Skip rows with unparseable rating data
                logging.warning(f"Could not parse rating data: {row['rating']}")
                pass
    return result

  def _remove_cast_extra_data(self, data):
    """
    Remove extra data from the cast field and filter out rows without cast.
    The cast field is expected to be a list of dictionaries, and we only want the 'name' field.
    Rows without cast data are excluded from the result.
    """
    result = []
    for row in data:
        if 'cast' in row and row['cast']:
            try:
                # Handle the string format properly - it's using Python's repr format
                # First, make it valid JSON by replacing Python-style single quotes
                cast_str = row['cast']
                # Use ast.literal_eval which can safely parse Python literal structures
                import ast
                cast_data = ast.literal_eval(cast_str)
                
                # Extract only the 'name' field from each dictionary
                cast_names = [item.get('name') for item in cast_data if isinstance(item, dict) and item.get('name')]
                
                # Only include rows where we have valid cast names
                if cast_names:
                    row['cast'] = cast_names
                    result.append(row)
            except (SyntaxError, ValueError, TypeError) as e:
                # Skip rows with unparseable cast data
                logging.warning(f"Could not parse cast data: {e}")
                pass
    return result

  def _project_to_columns(self, data, query_columns):
    """
    Extract only the columns defined in query_columns from the CSV data.
    If query_columns is a list, returns multiple result sets, one for each element.
    
    Returns an array of dictionaries (or multiple arrays if query_columns is a list),
    where each dictionary represents a row with column names as keys and the corresponding values.
    
    Special processing is done for Q5 data to ensure budget and revenue values are valid.
    """
    # Use Python's csv module to correctly parse the CSV data
    input_file = StringIO(data)
    csv_reader = csv.reader(input_file)
    
    # Check if query_columns is a list of column mappings or a single mapping
    if isinstance(query_columns, list):
        # Initialize result arrays - one for each element in query_columns list
        results = [[] for _ in range(len(query_columns))]
        
        for row in csv_reader:
            if not row:
                continue
                
            # Process each query column set
            for i, columns in enumerate(query_columns):
                # Skip if row doesn't have enough columns
                if len(row) <= max(columns.values()):
                    continue
                
                # Apply different processing based on which column set we're dealing with
                if 'budget' in columns and 'revenue' in columns:  # This is Q5
                    # Create a dictionary for Q5 with required columns
                    row_dict = {col_name: row[col_idx] for col_name, col_idx in columns.items()}
                    
                    # Check if any field is empty, null or nan
                    if any(not row_dict.get(field, '') for field in columns.keys()):
                        continue  # Skip this row if any field is empty
                    
                    # Check if budget or revenue is 0 or not a valid number
                    try:
                        budget = float(row_dict.get('budget', '0'))
                        revenue = float(row_dict.get('revenue', '0'))
                        
                        # Skip rows where budget or revenue is 0
                        if budget <= 0 or revenue <= 0:
                            continue
                            
                        # Store the numeric values back in the dictionary
                        row_dict['budget'] = budget
                        row_dict['revenue'] = revenue
                        
                        # Only append if all checks pass
                        results[i].append(row_dict)
                    except ValueError:
                        # Skip if budget or revenue is not a valid number
                        continue
                else:  # This is Q1 or any other query
                    # Create a standard dictionary for this row
                    row_dict = {col_name: row[col_idx] for col_name, col_idx in columns.items()}
                    results[i].append(row_dict)
        
        return results
    else:
        # Single query column set - maintain backward compatibility
        result = []
        
        for row in csv_reader:
            if not row or len(row) <= max(query_columns.values()):
                continue
                
            # Create a dictionary for this row with column names as keys
            row_dict = {col_name: row[col_idx] for col_name, col_idx in query_columns.items()}
            result.append(row_dict)
        
        return result
  
  # TODO: Move to protocol class
  def _receive_csv_batch(self, sock, proto):
    """
    Receive a CSV batch from the socket
    First read 4 bytes to get the length, then read the actual data
    """
    length_bytes = proto.recv_exact(sock, 4)
    msg_length = int.from_bytes(length_bytes, byteorder='big')

    data_bytes = proto.recv_exact(sock, msg_length)
    data = proto.decode(data_bytes)

    return data

# ----------------------------------------------------------------------- #
# Rabbit-Related-Section                                                  #
# ----------------------------------------------------------------------- #

  def _setup_rabbitmq(self, retry_count=1):
    connected = self.rabbitmq.client.connect()
    if not connected:
        logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
        wait_time = min(30, 2 ** retry_count)
        time.sleep(wait_time)
        return self._setup_rabbitmq(retry_count + 1)
    
    # Declare all necessary queues
    self.rabbitmq.client.declare_queue(self.movies_router_queue, durable=True)
    self.rabbitmq.client.declare_queue(self.movies_router_q5_queue, durable=True)
    self.rabbitmq.client.declare_queue(self.credits_router_queue, durable=True)
    self.rabbitmq.client.declare_queue(self.ratings_router_queue, durable=True)
    self.rabbitmq.client.declare_queue(RESPONSE_QUEUE, durable=True)
    
    logging.info("All router queues declared successfully")
  
  def _send_data_to_rabbitmq_queue(self, data, queue_name):
    """
    Send the data to RabbitMQ queue after serializing it
    
    Args:
        data: The data to send
        queue_name: The queue to send to
    """
    try:
        # Serialize the data to binary
        serialized_data = Serializer.serialize(data)
        
        success = self.rabbitmq.client.publish_to_queue(
            queue_name=queue_name,
            message=serialized_data,
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to publish data to {queue_name}")
    except Exception as e:
        logging.error(f"Error publishing data to queue '{queue_name}': {e}")
         
# ------------------------------------------------------------------ #
# graceful shutdown handler                                          #
# ------------------------------------------------------------------ #

  def _handle_shutdown(self, *_):
    logging.info(f"Shutting down server")
    self._running = False
    
    # Stop RabbitMQ consumer first
    if hasattr(self, 'rabbitmq'):
        self.rabbitmq.client.stop_consuming()
    
    # Close all client sockets
    with self._lock:
        for client_dict in self._client_sockets:
            for _, sock in client_dict.items():
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
    
    # Close the server socket
    if hasattr(self, '_server_socket'):
        self._server_socket.close()
    
    # Close RabbitMQ connection
    if hasattr(self, 'rabbitmq'):
      self.rabbitmq.close_all()
