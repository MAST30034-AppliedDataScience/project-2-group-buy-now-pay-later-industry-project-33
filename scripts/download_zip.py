from urllib.request import urlopen
from io import BytesIO
import os
from zipfile import ZipFile

def download_zip_file(filename, folder, src):
    """
    Downloads filename from a zipped folder from the source (src) into the specified folder.
    """
    
    # Create directory if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Download and extract specific file
    http_response = urlopen(src)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extract(filename, path=folder)