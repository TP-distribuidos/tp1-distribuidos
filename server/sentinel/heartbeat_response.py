"""
Helper module for sentinel heartbeat responses.
This ensures slaves properly acknowledge leader heartbeats.
"""

def create_heartbeat_response(original_message, sentinel_id):
    """Creates a response to a leader heartbeat message"""
    return {
        "type": "HEARTBEAT_ACK",
        "sender_id": sentinel_id,
        "original_timestamp": original_message.get("timestamp", 0)
    }
