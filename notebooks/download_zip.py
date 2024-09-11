
from urllib.request import urlopen
from io import BytesIO
import os
from zipfile import ZipFile

# Create directory if it doesn't exist
base_path = "../data/tables/poa_dataset"
if not os.path.exists(base_path):
    os.makedirs(base_path)

# Download and extract Postcodes to SA2 codes file
url = "https://data.gov.au/data/dataset/2c79581f-600e-4560-80a8-98adb1922dfc/resource/33d822ba-138e-47ae-a15f-460279c3acc3/download/asgs2021_correspondences.zip"

http_response = urlopen(url)
zipfile = ZipFile(BytesIO(http_response.read()))
zipfile.extract("CG_POA_2021_SA2_2021.xlsx", path=base_path)