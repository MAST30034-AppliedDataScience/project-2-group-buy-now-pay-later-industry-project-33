import math

def count_outliers(df, col):
    """
    Count outliers in a given column of df using the log IQR rule.
    Assume normal distribution.
    """
    # Step 1: Calculate the number of records
    N = df.count()

    # Step 2: Calculate Q1, Q3, and IQR
    quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    IQR = Q3 - Q1

    # Step 3: Calculate the multiplier (√logN - 0.5), with log base e
    multiplier = math.sqrt(math.log(N)) - 0.5

    # Step 4: Define the lower and upper bounds
    lower_bound = Q1 - (multiplier * IQR)
    upper_bound = Q3 + (multiplier * IQR)

    # Step 5: Count number of records outside of bounds
    outlier_count = df.filter((df[col] < lower_bound) | (df[col] > upper_bound)).count()
    print(f"{outlier_count} outliers outside of bounds {lower_bound, upper_bound}")
    return outlier_count