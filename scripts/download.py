import os
import pandas as pd
import requests

def download_ABS_dataset(dataflow_ids= ['C21_G33_SA2', 'C21_G38_SA2', 'C21_G40_SA2', 'C21_G02_SA2']):
    """
    Given a list a dataflow_id, the function downloads each dataset corresponding to the id,
    and save it into sa2_dataset/main/ folder.
    """
    
    # Create paths
    directory_path = '../data/tables/sa2_dataset/main/'
    os.makedirs(directory_path, exist_ok=True)
    base_path = '../data/tables/sa2_dataset/main/'

    for dataflow_id in dataflow_ids:
        url = f'https://api.data.abs.gov.au/data/{dataflow_id}/all'
        headers = {'accept': 'text/csv'}
        
        # Define paths
        file_path = os.path.join(base_path, f'{dataflow_id}.csv')
        filtered_file_path = os.path.join(base_path, f'{dataflow_id}_filtered.csv')

        try:
            # Download file
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
        
            if 'text/csv' in response.headers.get('Content-Type', ''):
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
            else:
                print(f'Unexpected content type for {dataflow_id}:', response.headers.get('Content-Type'))
                continue 
        
            # Filter location
            data = pd.read_csv(file_path)
            if dataflow_id == 'C21_G02_SA2':
                filtered_data = data[data['MEDAVG'] == 5]
                filtered_data = data[data['REGION_TYPE'] == 'SA2']
            else:
                filtered_data = data[data['REGION_TYPE'] == 'SA2']
                
            filtered_data.to_csv(filtered_file_path, index=False)
            
        except requests.RequestException as e:
            print(f'An error occurred for {dataflow_id}: {e}')
