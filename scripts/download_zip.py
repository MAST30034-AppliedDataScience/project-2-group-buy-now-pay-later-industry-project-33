from urllib.request import urlopen
from io import BytesIO
import os
from zipfile import ZipFile
import ssl
import certifi

def download_zip_file(filename, folder, src, disable_ssl_verification=False):
    """
    Downloads filename from a zipped folder from the source (src) into the specified folder.
    Supports SSL verification using certifi or disabling SSL verification if required.
    
    Parameters:
        filename: str - The name of the file to extract.
        folder: str - The destination folder to save the file.
        src: str - The URL of the source zip file.
        disable_ssl_verification: bool - Option to disable SSL verification (default: False).
    """
    
    # Create directory if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Set up SSL context based on the disable_ssl_verification flag
    if disable_ssl_verification:
        ssl_context = ssl._create_unverified_context()  # Disables SSL verification
    else:
        ssl_context = ssl.create_default_context(cafile=certifi.where())  # Use certifi for SSL verification

    # Download and extract the specific file from the zip
    http_response = urlopen(src, context=ssl_context)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extract(filename, path=folder)

    print(f"File '{filename}' downloaded and extracted to {folder}.")