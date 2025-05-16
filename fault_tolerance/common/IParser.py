from abc import ABC, abstractmethod

class IParser(ABC):
    @abstractmethod
    def parse(self, data):
        """
        Parse the input data.
        
        Args:
            data: The data to be parsed.
            
        Returns:
            The parsed result.
        """
        pass

    @abstractmethod
    def format_output(self, parsed_data):
        """
        Format the parsed data for output.
        
        Args:
            parsed_data: The data to be formatted.
            
        Returns:
            The formatted output.
        """
        pass
