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
        self.ftp = FTP(host)
        if username and password:
            self.ftp.login(user=username, passwd=password)
        else:
            self.ftp.login() # Anonymous login
        atexit.register(self.close) # Close the FTP connection when the program exits
        
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
            
        try:
            # Create a bytes buffer to store the data
            buffer = io.BytesIO()
            
            # Retrieve the file
            self.ftp.retrbinary(f'RETR {parsed.path}', buffer.write)
            
            # Get data as string
            buffer.seek(0)
            return buffer.read().decode('utf-8')
            
        except Exception as e:
            print(f"Error retrieving data from {url}: {e}")
            return None
        
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
            
        try:
            # Get list of files/folders
            file_list = []
            self.ftp.retrlines(f'LIST {parsed.path}', file_list.append)
            
            # Extract filenames and build full URLs
            urls = []
            for item in file_list:
                # Parse the filename from the LIST output
                filename = item.split()[-1]
                # Build the full URL path
                if parsed.path.endswith('/'):
                    path = parsed.path + filename
                else:
                    path = parsed.path + '/' + filename
                # Construct full URL
                url = f"ftp://{self.host}{path}"
                urls.append(url)
                
            return urls
            
        except Exception as e:
            print(f"Error listing directory {url}: {e}")
            return []
    
    def close(self):
        """Close the FTP connection"""
        self.ftp.quit()

CONFIG_PATH = 'scraper/scraper_config.yaml'

with open(CONFIG_PATH, 'r', encoding='utf-8') as file:
    scraper_config = yaml.safe_load(file)

conn = FTP_Connection(scraper_config['sources']['ftp']['host'])
print(conn.ls('ftp://ftp.legis.state.tx.us/bills'))