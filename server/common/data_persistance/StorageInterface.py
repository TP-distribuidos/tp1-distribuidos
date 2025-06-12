import abc
from pathlib import Path
from typing import List, Union, Optional, BinaryIO, TextIO

class StorageInterface(abc.ABC):
    """
    Abstract interface for storage operations.
    Implementations handle the actual file/storage operations.
    """
    
    @abc.abstractmethod
    def read_file(self, path: Path) -> str:
        """
        Read contents of a file as a string
        
        Args:
            path: Path to the file
            
        Returns:
            str: Contents of the file
            
        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If read operation fails
        """
        pass
    
    @abc.abstractmethod
    def read_file_lines(self, path: Path) -> List[str]:
        """
        Read contents of a file as a list of lines
        
        Args:
            path: Path to the file
            
        Returns:
            List[str]: Lines from the file
            
        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If read operation fails
        """
        pass
    
    @abc.abstractmethod
    def write_file(self, path: Path, content: str, ensure_dir: bool = True) -> bool:
        """
        Write content to a file, overwriting if it exists
        
        Args:
            path: Path to the file
            content: Content to write
            ensure_dir: Create parent directories if needed
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def append_to_file(self, path: Path, content: str) -> bool:
        """
        Append content to an existing file
        
        Args:
            path: Path to the file
            content: Content to append
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def file_exists(self, path: Path) -> bool:
        """
        Check if a file exists
        
        Args:
            path: Path to the file
            
        Returns:
            bool: True if file exists, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def delete_file(self, path: Path) -> bool:
        """
        Delete a file
        
        Args:
            path: Path to the file
            
        Returns:
            bool: True if successful or file didn't exist, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def create_directory(self, directory: Path) -> bool:
        """
        Create a directory and any necessary parent directories
        
        Args:
            directory: Path to the directory
            
        Returns:
            bool: True if successful or directory already exists, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def list_files(self, directory: Path, pattern: str = "*") -> List[Path]:
        """
        List all files in a directory matching the pattern
        
        Args:
            directory: Path to the directory
            pattern: Glob pattern to match files
            
        Returns:
            List[Path]: List of matching file paths
        """
        pass
    
    @abc.abstractmethod
    def move_file(self, src: Path, dst: Path) -> bool:
        """
        Move a file from src to dst
        
        Args:
            src: Source path
            dst: Destination path
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def open_file(self, path: Path, mode: str = 'r') -> Union[TextIO, BinaryIO]:
        """
        Open a file and return the file object
        
        Args:
            path: Path to the file
            mode: File open mode
            
        Returns:
            File object that must be closed after use
            
        Raises:
            FileNotFoundError: If file doesn't exist and 'r' mode
            IOError: If open operation fails
        """
        pass
        
    @abc.abstractmethod
    def update_first_line(self, path: Path, new_content: str) -> bool:
        """
        Update only the first line of a file, preserving the rest of the content
        
        Args:
            path: Path to the file
            new_content: New content for the first line (without newline)
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
