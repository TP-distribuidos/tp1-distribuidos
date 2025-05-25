import os
import shutil
from pathlib import Path
from typing import List, Union, BinaryIO, TextIO
import logging
from common.data_persistance.StorageInterface import StorageInterface

class FileSystemStorage(StorageInterface):
    """
    Implementation of StorageInterface using the local filesystem
    """
    
    def read_file(self, path: Path) -> str:
        """Read contents of a file as a string"""
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception as e:
            logging.error(f"Error reading file {path}: {e}")
            raise
    
    def read_file_lines(self, path: Path) -> List[str]:
        """Read contents of a file as a list of lines"""
        try:
            with open(path, 'r') as f:
                return f.readlines()
        except Exception as e:
            logging.error(f"Error reading file {path}: {e}")
            raise
    
    def write_file(self, path: Path, content: str, ensure_dir: bool = True) -> bool:
        """Write content to a file, overwriting if it exists"""
        try:
            if ensure_dir:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                
            with open(path, 'w') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception as e:
            logging.error(f"Error writing file {path}: {e}")
            return False
    
    def append_to_file(self, path: Path, content: str) -> bool:
        """Append content to an existing file"""
        try:
            with open(path, 'a') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            return True
        except Exception as e:
            logging.error(f"Error appending to file {path}: {e}")
            return False
    
    def file_exists(self, path: Path) -> bool:
        """Check if a file exists"""
        return os.path.isfile(path)
    
    def delete_file(self, path: Path) -> bool:
        """Delete a file"""
        try:
            if os.path.isfile(path):
                os.remove(path)
            return True
        except Exception as e:
            logging.error(f"Error deleting file {path}: {e}")
            return False
    
    def create_directory(self, directory: Path) -> bool:
        """Create a directory and any necessary parent directories"""
        try:
            os.makedirs(directory, exist_ok=True)
            return True
        except Exception as e:
            logging.error(f"Error creating directory {directory}: {e}")
            return False
    
    def list_files(self, directory: Path, pattern: str = "*") -> List[Path]:
        """List all files in a directory matching the pattern"""
        try:
            return list(Path(directory).glob(pattern))
        except Exception as e:
            logging.error(f"Error listing files in {directory}: {e}")
            return []
    
    def move_file(self, src: Path, dst: Path) -> bool:
        """Move a file from src to dst"""
        try:
            shutil.move(src, dst)
            return True
        except Exception as e:
            logging.error(f"Error moving file from {src} to {dst}: {e}")
            return False
    
    def open_file(self, path: Path, mode: str = 'r') -> Union[TextIO, BinaryIO]:
        """Open a file and return the file object"""
        try:
            if mode.startswith('w') or mode.startswith('a'):
                os.makedirs(os.path.dirname(path), exist_ok=True)
            return open(path, mode)
        except Exception as e:
            logging.error(f"Error opening file {path}: {e}")
            raise
            
    def update_first_line(self, path: Path, new_content: str) -> bool:
        """Update only the first line of a file, preserving the rest of the content"""
        if not self.file_exists(path):
            logging.error(f"Cannot update first line of non-existent file {path}")
            return False
            
        try:
            # Read all content from the file
            with open(path, 'r') as file:
                lines = file.readlines()
                
            if not lines:
                logging.warning(f"File {path} is empty, cannot update first line")
                return False
                
            # Replace only the first line, ensuring it has a newline
            lines[0] = new_content + '\n'
            
            # Write all lines back to the file
            with open(path, 'w') as file:
                file.writelines(lines)
                file.flush()
                os.fsync(file.fileno())
                
            return True
        except Exception as e:
            logging.error(f"Error updating first line of file {path}: {e}")
            return False
