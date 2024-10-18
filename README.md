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
3. **Run Code:**
   - To reproduce results from this project, run all notebooks under the `notebooks` directory in order.

4. **Download Data:**
   - **Main Data:** Please download it from Canvas, unzip each zip files and put them into data/table/part_1, part_2, part_3 and part_4 folders.
   - **SA2 Boundary Files:** Download from [SA2_2021_AUST_SHP_GDA2020.zip](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip) and place it in `data/tables/sa2_boundary` folder.
   - **ABS Cencus 2021 Dataset:** Run all cells in notebooks `02 download_clean_external_dataset.ipynb`, or run function `download_ABS_dataset` from `download.py`. Raw data is stored at `data/tables/sa2_dataset/main`.

5. **Data Cleaning:**
   - Run all cells in notebooks `01 cleaning_main.ipynb` and `02 download_clean_external_dataset.ipynb`in order, or run funtion `clean_external_df`, `clean_merchant_df` and `clean_shapefile_sa2`. Please read function docstring to use it correctly.

6. **Data Preprocessing:**
   - Run all cells in notebooks `03 convert_postcode.ipynb`, `04 merge_datasets.ipynb` and `05 outlier_analysis.ipynb`.

7. **Exploratory Data Analysis (EDA):**
   - For analysis results, run all cells in notebooks `06 modelling_revenue`, `07 modelling_consumer_fraud_prob.ipynb`, `08 modelling_merchant_fraud_prob.ipynb`, and `09 customer_retention.ipynb`.

8. **Modelling:**
   - For modelling results, run notebooks `06 modelling_revenue`, `07 modelling_consumer_fraud_prob.ipynb`, and `08 modelling_merchant_fraud_prob.ipynb`.

9. **Ranking:**
   - For overall ranking of merchants, run notebook `10 ranking_initial.ipynb`.
   - For ranking merchants by segments, run notebooks `11 ranking_manual_segments.ipynb` and `12 ranking_kmeans_segments.ipynb`.
   - For final rankings, run notebook `13 ranking_final.ipynb`.

10. **Visualizations:**
   - For visualizations, view / run notebook `14 visualizations.ipynb`.

11. **Results Summary:**
   - For all results summary of this project, view / run notebook `15 summary.ipynb`.


