-- Databricks notebook source
-- MAGIC %python
-- MAGIC import datetime
-- MAGIC
-- MAGIC ts = datetime.datetime.now()
-- MAGIC integ_name='nyc_moto_gold_logging'
-- MAGIC
-- MAGIC spark.sql(f"insert into nyc_moto_db_logs.nyc_moto_vehicles_log_header (date_created, integration_name, log_status, date_completed) values ('{ts}','{integ_name}','0',null)")
-- MAGIC
-- MAGIC log_df = spark.sql(f"select log_id from nyc_moto_db_logs.nyc_moto_vehicles_log_header where date_created == '{ts}'")
-- MAGIC log_id = log_df.collect()[0][0]

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inter AS SELECT
        to_timestamp(concat(crash_date, ' ', crash_time), 'MM/dd/yyyy HH:mm') as crash_date_time, 
        borough,
        zip_code,
        latitude,
        longitude,
        loc,
        on_street_name,
        cross_street_name,
        off_street_name,
        persons_injured,
        persons_killed,
        pedestrians_injured,
        pedestrians_killed,
        cyclists_injured,
        cyclists_killed,
        motorists_injured,
        motorists_killed,
        contributing_factor_vehicle_1,
        contributing_factor_vehicle_2,
        contributing_factor_vehicle_3,
        contributing_factor_vehicle_4,
        contributing_factor_vehicle_5,
        collision_id,
        cast(vehicle_type_code_1 != '' as INT) 
        + cast(vehicle_type_code_2 != '' as INT) 
        + cast(vehicle_type_code_3 != '' as INT) 
        + cast(vehicle_type_code_4 != '' as INT) 
        + cast(vehicle_type_code_5 != '' as INT)  as vehicles_involved,
        vehicle_type_code_1,
        vehicle_type_code_2,
        vehicle_type_code_3,
        vehicle_type_code_4,
        vehicle_type_code_5,
        current_timestamp as process_date
FROM nyc_moto_db_silver.nyc_moto_vehicles_silver

-- COMMAND ----------

MERGE INTO nyc_moto_db_gold.nyc_moto_vehicles_gold AS g USING inter AS i 
on g.collision_id = i.collision_id 
WHEN MATCHED THEN UPDATE SET * 
WHEN NOT MATCHED THEN INSERT *



-- COMMAND ----------

-- MAGIC %python
-- MAGIC num_inter = spark.sql("select count(*) from inter")
-- MAGIC num_inter_records = num_inter.collect()[0][0]
-- MAGIC spark.sql(f"insert into nyc_moto_db_logs.nyc_moto_vehicles_log_details (date_created, log_id, log_message, log_value) values (current_timestamp, '{log_id}','number of records written to nyc_moto_gold','{num_inter_records}')")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"UPDATE nyc_moto_db_logs.nyc_moto_vehicles_log_header SET log_status = '1', date_completed = current_timestamp WHERE log_id == '{log_id}'")
