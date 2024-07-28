# Slowly Changing Dimension Implementation Retail Project using PySpark

This project demonstrates the implementation of Slowly Changing Dimensions (SCD) Type 2 for a retail store customer dimension, using PySpark. SCD Type 2 retains the complete history of changes, making it the most commonly used approach in the industry.

## Table of Contents

- [Directory Structure in HDFS](#directory-structure-in-hdfs)
- [Prepare Source and Target](#prepare-source-and-target)
- [Define Variables](#define-variables)
- [Define Schemas](#define-schemas)
- [Create Source dataFrame](#create-source-dataframe)
- [Write Enhanced Dataframe to Target](#write-enhanced-dataframe-to-target)
- [Create Target DataFrame](#create-target-dataframe)
- [View Source and Target DataFrames](#view-source-and-target-dataframes)
- [Handle Surrogate Keys](#handle-surrogate-keys)
- [Add New Data](#add-new-data)
- [Read Updated Source DataFrame](#read-updated-source-dataframe)
- [Create Active and Inactive DataFrames](#create-active-and-inactive-dataframes)
- [Perform Join Operation](#perform-join-operation)
- [Column Renaming and Hashing](#column-renaming-and-hashing)
- [Perform Join and Create Action Column](#perform-join-and-create-action-column)
- [Filter Records by Action](#filter-records-by-action)
- [Merge DataFrames](#merge-dataframes)
- [Key Points](#key-points)
- [Conclusion](#conclusion)
- [License](#license)

## Directory Structure in HDFS

- **Main Folder:** `Data`
    - **Subfolders:** `Source` and `Target`

## Prepare Source and Target

- **Source Folder:** Add the `customer.csv` file to the source.
- **Target Folder:** Create an empty target folder.

## Define Variables

-**Variables:**
  - `DATE_FORMAT` = `"yyyy-MM-dd"`
  - `future_date` = `"9999-12-31"`
  - `source_url` = `"../data/source/"`
  - `destination_url` = `"../data/target/"`
  - `primary_key` = `["customerid"]`
  - `slowly_changing_cols` = `[ "email", "phone", "address", "city", "state", "zipcode"]`
  - `implementation_cols` = `["effective_date", "end_date", "active_flag"]`

- **Columns to Show History:**
  - `effective_start_date`
  - `end_date`
  - `active_flag`

- **Example Values:**
  - `1st Jan 2013`, `31st Dec 2017` (history), `false`
  - `31st Dec 2017`, `31st Dec 9999` (current), `true`

## Define Schemas

- **Source Schema:**
  ```python
  customers_source_schema = "customerid long, firstname string, lastname string, email string, phone string, address string, city string, state string, zipcode long"
- **Target Schema:**
  ```python
  customers_target_schema = "customerid long, firstname string, lastname string, email string, phone string, address string, city string, state string, zipcode long, 
  customer_skey long, effective_date date, end_date date, active_flag boolean"

## Create Source Dataframe

- **Read and enhance the `customers_source_df`:**
  ```python
  customers_source_df = spark.read \
  .format("csv") \
  .option("header",True) \
  .schema(customers_source_schema) \
  .load(source_url)

  enhanced_customers_source_df = spark.read \
  .format("csv") \
  .option("header",True) \
  .schema(customers_source_schema) \
  .load(source_url) \
  .withColumn("customer_skey", row_number().over(window_def)) \
  .withColumn("effective_date", date_format(current_date(), DATE_FORMAT)) \
  .withColumn("end_date", date_format(lit(future_date), DATE_FORMAT)) \
  .withColumn("active_flag", lit(True))

## Write Enhanced DataFrame to Target

- **Write the enhanced source dataframe `enhanced_customers_source_df` to the target folder:**
  ```python
  enhanced_customers_source_df.write.mode('overwrite') \
  .option("header",True) \
  .option("delimiter",",") \
  .csv(destination_url)

## Create Target DataFrame

- **Create target dataframe `customers_target_df` from the enhanced source data:**
    ```python
    customers_target_df = spark.read \
   .format("csv") \
   .option("header",True) \
   .schema(customers_target_schema) \
   .load(destination_url)

## View Source and Target DataFrames

- **Compare Columns between source and target dataframes:**
    ```python
    customers_source_df.show()
    customers_target_df.show()

## Handle Surrogate Keys

- **Aggregate function to find maximum surrogate key:**
    ```python
    max_sk = customers_target_df.agg({"customer_skey": "max"}).collect()[0][0]

## Add New Data
- Add new data to mock the changes in the landing area due to ingestion of incremental data.
- Add `new_customers.csv` to the source folder.
- Delete the old `customers.csv` file.

## Read Updated Source DataFrame

- Observe Updates, Inserts, and Deletes in the new source data

## Create Active and Inactive DataFrames

- **Active Customers:**
    ```python
    active_customers_target_df = customers_target_df.filter(col("active_flag") == True)

- **Inactive Customers:**
    ```python
    inactive_customers_target_df = customers_target_df.filter(col("active_flag") == False)

## Perform Join Operation

- **Join active customers with the updated source data:**
    ```python
    merged_df = active_customers_target_df.join(customers_source_df, "customerid", "full_outer")

## Column Renaming and Hashing

- **Column Renaming Function:**
    ```python
    def column_renamer(df, suffix, append):
   
    if append:
        new_column_names = list(map(lambda x: x+suffix, df.columns))
        
    else:
        new_column_names = list(map(lambda x: x.replace(suffix,""), df.columns))
        
    return df.toDF(*new_column_names)

- **Get Hash Function:**
    ```python
    def get_hash(df, keys_list):
 
    columns = [col(column) for column in keys_list]
    
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit("1")))

- **Apply Functions:**
    ```python
    active_customers_target_df_hash = column_renamer(get_hash(active_customers_target_df, slowly_changing_cols), suffix="_target", append=True)
    customers_source_df_hash = column_renamer(get_hash(customers_source_df, slowly_changing_cols), suffix="_source", append=True)

## Perform Join and Create Action Column

- **Join and create the Action column to indicate INSERT, DELETE, UPDATE, and NO-CHANGE:**
    ```python
     merged_df = active_customers_target_df_hash.join(customers_source_df_hash, col("customerid_source") ==  col("customerid_target") , "full_outer") \
    .withColumn("Action", when(col("hash_md5_source") == col("hash_md5_target")  , 'NOCHANGE')\
    .when(col("customerid_source").isNull(), 'DELETE')\
    .when(col("customerid_target").isNull(), 'INSERT')\
    .otherwise('UPDATE'))

## Filter Records by Action

- **Unchanged Records:**
    ```python
    unchanged_records = column_renamer(merged_df.filter(col("action") == 'NOCHANGE'), suffix="_target", append=False).select(active_customers_target_df.columns)

- **Inserted Records:**
    ```python
    insert_records = column_renamer(merged_df.filter(col("action") == 'INSERT'), suffix="_source", append=False) \
                .select(customers_source_df.columns)\
                .withColumn("row_number",row_number().over(window_def))\
                .withColumn("customer_skey",col("row_number") + max_sk)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("end_date",date_format(lit(future_date),DATE_FORMAT))\
                .withColumn("active_flag", lit(True))\
                .drop("row_number")

- **Evaluate Maximum Customers Surrogate Key from Inserted Records:**
    ```python
    max_sk = insert_records.agg({"customer_skey": "max"}).collect()[0][0]

- **Updated Records:**
    ```python
    update_records = column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_target", append=False)\
                .select(active_customers_target_df.columns)\
                .withColumn("end_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("active_flag", lit(False))\
            .unionByName(
            column_renamer(merged_df.filter(col("action") == 'UPDATE'), suffix="_source", append=False)\
                .select(customers_source_df.columns)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("end_date",date_format(lit(future_date),DATE_FORMAT))\
                .withColumn("row_number",row_number().over(window_def))\
                .withColumn("customer_skey",col("row_number")+ max_sk)\
                .withColumn("active_flag", lit(True))\
                .drop("row_number")
                )

- **Evaluate Maximum Customers Surrogate Key from Updated Records:**
  ```python
  max_sk = update_records.agg({"customer_skey": "max"}).collect()[0][0]

- **Deleted Records:**
    ```python
    delete_records = column_renamer(merged_df.filter(col("action") == 'DELETE'), suffix="_target", append=False)\
                .select(active_customers_target_df.columns)\
                .withColumn("end_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("active_flag", lit(False))

## Merge DataFrames

- **Final union of all records:**
    ```python
    resultant_df = inactive_customers_target_df \
            .unionByName(unchanged_records)\
            .unionByName(insert_records)\
            .unionByName(update_records)\
            .unionByName(delete_records)

## Key Points

- Focus on active records.
- Mark deleted records as inactive in the target.
- Use hash approach for updates.
- Handle 4 scenarios: INSERT, DELETE, UPDATE, NO-CHANGE.

## Conclusion

- This implementation ensures that the customer dimension retains complete history, providing accurate and historical insights for the retail store.

## License

This project is licensed under the MIT License. You are free to use, modify, and distribute this software in any way you see fit, provided that this license notice appears in all copies of the software.
