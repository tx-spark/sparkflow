from ftplib import FTP
from urllib.parse import urlparse
import io
import yaml
import atexit

class FTP_Connection:
    def __init__(self, host, username=None, password=None):
        """
        Initialize FTP connection to specified host.
        
        Args:
            host: FTP server hostname
            username: Optional username for FTP login
            password: Optional password for FTP login
        """
        self.host = host
        self.username = username # Is saving this secure? Does it matter?
        self.password = password
        self.ftp = None
        self.connect()
        atexit.register(self.close) # Close the FTP connection when the program exits

    def connect(self):
        """Establish FTP connection and login"""
        self.ftp = FTP(self.host)
        if self.username and self.password:
            self.ftp.login(user=self.username, passwd=self.password)
        else:
            self.ftp.login() # Anonymous login

    def _retry_on_disconnect(self, operation):
        """
        Try an FTP operation and retry once with reconnect if it fails.
        
        Args:
            operation: Function that performs the FTP operation
            
        Returns:
            Result of the operation, or None/[] if it fails
        """
        try:
            return operation()
        except Exception: # TO DO: Add specific exceptions
            try:
                self.connect()
                return operation()
            except Exception as e:
                print(f"Error after relogin attempt: {e}")
                return None
            
    def get_data(self, url):
        """
        Retrieve data from a URL on the FTP server.
        
        Args:
            url: FTP URL to retrieve data from
            
        Returns:
            Retrieved data as a string, or None if retrieval failed
        """
        parsed = urlparse(url)
        if parsed.netloc != self.host:
            print(f"Warning: URL {url} is for different host than connection")
            return None
            
        def retrieve():
            buffer = io.BytesIO()
            self.ftp.retrbinary(f'RETR {parsed.path}', buffer.write)
            buffer.seek(0)
            return buffer.read().decode('utf-8')
            
        return self._retry_on_disconnect(retrieve)
        
    def ls(self, url):
        """
        List all files and folders at the given FTP URL path.
        
        Args:
            url: FTP URL path to list contents from
            
        Returns:
            List of URLs for all files/folders in the directory
        """
        parsed = urlparse(url)
        if parsed.netloc != self.host:
            print(f"Warning: URL {url} is for different host than connection")
            return []
            
        def list_dir():
            file_list = []
            self.ftp.retrlines(f'LIST {parsed.path}', file_list.append)

            urls = []
            for item in file_list:
                # Split into parts but preserve filename with spaces
                parts = item.split()
                    
                filename = ' '.join(parts[3:])
                if parsed.path.endswith('/'):
                    path = parsed.path + filename
                else:
                    path = parsed.path + '/' + filename
                url = f"ftp://{self.host}{path}"
                urls.append(url)
                
            return urls
            
        result = self._retry_on_disconnect(list_dir)
        return result if result is not None else []
    
    def close(self):
        """Close the FTP connection"""
        if self.ftp:
            self.ftp.quit()
            self.ftp = None
