import re
import pandas as pd
import os
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, FloatType, StructType, StructField

# Cleaning function for external datasets
def clean_single_external_df(df):
    """
    Rename columns of the given dataframe and drop unused columns.
    Return modifed dataframe.
    """
    
    rename_cols = {
        "HIND": "household_income_weekly",
        "HHCD": "household_composition",
        "REGION": "sa2_code",
        "STATE": "state",
        "OBS_VALUE": "obs_value",
        "MEDAVG": "type_of_value_code",
        "STRD": "dwelling_structure",
        "MRERD": "monthly_mortgage_repayments_ranges",
        "RNTRD": "weekly_rent_range",
        "LLDD": "landlord_type"   
    }

    df = df.drop(["DATAFLOW", "REGION_TYPE", "TIME_PERIOD"], axis=1)
    df.rename(columns=rename_cols, inplace=True)

    return df


def clean_external_df():
    """
    Apply clean function to each external dataset.
    Save clean data into curated folder.
    """

    dataflow_ids = ['C21_G33_SA2', 'C21_G38_SA2', 'C21_G40_SA2', 'C21_G02_SA2']
    base_path = '../data/tables/sa2_dataset/main/'

    # Create path in curated folder
    return_path = '../data/curated/sa2_dataset/'
    os.makedirs(return_path, exist_ok=True)

    for dataflow_id in dataflow_ids:
        file_path = os.path.join(base_path, f'{dataflow_id}_filtered.csv')
        clean_file_path = os.path.join(return_path, f'{dataflow_id}_clean.csv')

        data = pd.read_csv(file_path)
        clean_data = clean_single_external_df(data)
        clean_data.to_csv(clean_file_path, index=False)


# Cleaning functions for merchant dataset

def split_tag_value(val):
    """
    Given a value as a string, extract 3 values and store them in a list.
    Return a list of 3 extracted value.
    """
    
    values = []
    
    # Remove the parentheses
    core_string = val.strip('()[]')

    if bool(re.search(r"\), \(", core_string)):
        parts = core_string.split('), (')

    else:
        parts = core_string.split('], [')
        
    values.append(parts[0].strip().lower())
    values.append(parts[1].strip())
    details = parts[2].strip()
    
    # Extract value of take_rate
    take_rate_match = re.search(r'take rate: ([0-9.]+)', details)
    take_rate = float(take_rate_match.group(1)) if take_rate_match else None
    values.append(take_rate)

    return values


def clean_merchant_df(merchant):
    """
    Extract 3 values in the `tags` column, and save the modified file with 3 new columns in curated folder.
    Return dataframe after cleaning
    """
    merchant_csv = merchant.toPandas()  
    
    split_values = []
    
    for val in merchant_csv.tags:
        split_values.append(split_tag_value(val))
    
    # Merge three new columns to original dataframe
    split_cols = pd.DataFrame(split_values, columns=["goods", "revenue_level", "take_rate"])
    clean_merchant_csv = pd.concat([merchant_csv, split_cols], axis=1).drop("tags", axis=1)

    # Save file to curated folder
    os.makedirs("../data/curated/part_1/", exist_ok=True)
    clean_merchant_csv.to_parquet("../data/curated/part_1/tbl_merchants.parquet", )
    
    return



# Cleaning function for shapefile of SA2 region
def clean_shapefile_sa2(gdf):
    """
    Normalize column names and remove unused columns, save new data to `curated` folder.
    Return shapefile after cleaning
    """
    # Only keep SA2 columns
    gdf = gdf[['SA2_CODE21', 'SA2_NAME21', 'STE_NAME21', 'AUS_CODE21' , 'geometry']]
    
    # Only keep values with non-missing geometry
    gdf = gdf[gdf.geometry.isna() == False]

    gdf.columns = gdf.columns.str.lower()

    os.makedirs("../data/curated/sa2_boundary/", exist_ok=True)
    gdf.to_file("../data/curated/sa2_boundary/SA2_2021_AUST_GDA2020_clean.shp", driver='ESRI Shapefile')

    return gdf
