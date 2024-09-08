# Generic Buy Now, Pay Later Project

## Research Goal
The goal is to find the top 100 merchants that our new BNPL should accept


## Setup Instructions

1. **Install Dependencies:**
   - Navigate to the `notebooks` directory.
   - Run the following command to install required packages:
     ```sh
     pip install -r requirements.txt
     ```

2. **Configure Spark:**
   - Adjust the Spark configuration if needed. Set `spark.driver.memory` based on your system's memory:
     ```python
     spark = (
         SparkSession.builder.appName("Name")
         .config("spark.sql.repl.eagerEval.enabled", True) 
         .config("spark.sql.parquet.cacheMetadata", "true")
         .config("spark.driver.memory", "9g") 
         .config("spark.sql.session.timeZone", "Etc/UTC")
         .getOrCreate()
     )
     ```

3. **Download Data:**
   - **Main Data:** Please download it from Canvas, unzip each zip files and put them into data/table/part_1, part_2, part_3 and part_4 folders
   - **SA2 Boundary Files:** Download from [SA2_2021_AUST_SHP_GDA2020.zip](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip) and place it in `data/tables`.
   - **ABS Cencus 2021 Dataset:** Run all cells in `download_clean_external_dataset`, or run function `download_ABS_dataset` from `download.py`.

4. **Data Cleaning:**
   - Run all cells in `cleaning_main.ipynb` and `download_clean_external_dataset.ipynb`in order, or run funtion `clean_external_df`, `clean_merchant_df` and `clean_shapefile_sa2`. Please read function docstring to use it correctly.

5. **Exploratory Data Analysis (EDA):**


6. **Modeling:**

