import re
import pandas as pd

def general_clean_external_dataset(df):
    """
    Rename columns of the given dataframe and drop unused columns.
    Return modifed dataframe.
    """
    
    rename_cols = {
        "HIND": "household_income_weekly",
        "HHCD": "household_composition",
        "REGION": "region_id",
        "STATE": "state",
        "OBS_VALUE": "obs_value",
        "MEDAVG": "monthly_mortgage_median",
        "STRD": "dwelling_structure",
        "MRERD": "monthly_mortgage_repayments_ranges",
        "RNTRD": "weekly_rent_range",
        "LLDD": "landlord_type"   
    }

    df = df.drop(["DATAFLOW", "REGION_TYPE", "TIME_PERIOD"], axis=1)
    df.rename(columns=rename_cols, inplace=True)

    return df


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
    
    take_rate_match = re.search(r'take rate: ([0-9.]+)', details)
    take_rate = float(take_rate_match.group(1)) if take_rate_match else None
    values.append(take_rate)

    return values


def clean_merchant_df(merchant_csv):
    """
    Extract 3 values in the tags column of the csv.
    Return the modified csv with 3 new columns.
    """

    
    split_values = []
    
    for val in merchant_csv.tags:
        split_values.append(split_tag_value(val))
    
    split_cols = pd.DataFrame(split_values, columns=["goods", "symbol", "take_rate"])

    clean_merchant_csv = pd.concat([merchant_csv, split_cols], axis=1).drop("tags", axis=1)

    return clean_merchant_csv