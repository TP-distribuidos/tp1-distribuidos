class Protocol:
    def __init__(self):
        """Initialize the protocol handler (no loop required)"""
        pass

    def recv_exact(self, sock, num_bytes):
        """Receive exactly num_bytes from the socket"""
        buffer = bytearray()
        while len(buffer) < num_bytes:
            chunk = sock.recv(num_bytes - len(buffer))
            if not chunk:
                raise ConnectionError("Connection closed while receiving data")
            buffer.extend(chunk)
        return bytes(buffer)

    def send_all(self, sock, data, query):
        """
        Send data with query type indicator
        First sends 4 bytes with the length (including query type byte),
        then sends 1 byte for query type, then the actual data
        """
        # Get data bytes
        data_bytes = data.encode('utf-8') if isinstance(data, str) else data
        
        # Get query type byte
        query_byte = query.encode('utf-8')[0:1]  # Just take the first byte
        
        # Calculate total length (data + query byte)
        total_length = len(data_bytes) + 1
        length_bytes = total_length.to_bytes(4, byteorder='big')
        
        # Send length bytes
        self._send_all_sync(sock, length_bytes)
        
        # Send query type byte
        self._send_all_sync(sock, query_byte)
        
        # Send data bytes
        self._send_all_sync(sock, data_bytes)

    def _send_all_sync(self, sock, data):
        """Send all data to the socket"""
        sock.sendall(data)
        return len(data)

    def _encode_data(self, data):
        """Encode data to bytes with length prefix"""
        data_bytes = data.encode('utf-8')
        length = len(data_bytes)
        length_bytes = length.to_bytes(4, byteorder='big')
        return length_bytes, data_bytes

    def decode(self, data_bytes):
        """Decode bytes to string"""
        return data_bytes.decode('utf-8')

    def decode_int(self, data_bytes):
        """Decode bytes to integer"""
        return int.from_bytes(data_bytes, byteorder='big')
