from flask import Flask, request, Response
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, create_map, lit, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json
import os
import shutil # For the clear_directory_contents helper, if used.
import logging
import uuid # For unique batch IDs

app = Flask(__name__)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Spark Session - Will pick up SPARK_MASTER_URL from environment if set,
# otherwise defaults to local. For your cluster, SPARK_MASTER_URL must be set.
try:
    logger.info("Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("CSV-Processing-API-Clustered") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.memory", "1g")      \
        .config("spark.driver.memory", "1g")       \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()
    logger.info(f"SparkSession initialized. Master: {spark.sparkContext.master}")
except Exception as e:
    logger.error(f"Failed to create SparkSession: {e}", exc_info=True)
    spark = None # Ensure spark is None if initialization fails

# Define the schema (from your script)
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("eNodeB Name", StringType(), True),
    StructField(" Frequency band", DoubleType(), True),
    StructField("Cell FDD TDD Indication", StringType(), True),
    StructField("Cell Name", StringType(), True),
    StructField("Downlink EARFCN", DoubleType(), True),
    StructField("Downlink bandwidth", StringType(), True),
    StructField("LTECell Tx and Rx Mode", StringType(), True),
    StructField("LocalCell Id", DoubleType(), True),
    StructField("eNodeB Function Name", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Integrity", StringType(), True),
    StructField("FT_AVE 4G/LTE DL USER THRPUT without Last TTI(ALL) (KBPS)(kbit/s)", DoubleType(), True),
    StructField("FT_AVERAGE NB OF USERS (UEs RRC CONNECTED)", DoubleType(), True),
    StructField("FT_PHYSICAL RESOURCE BLOCKS LOAD DL(%)", DoubleType(), True),
    StructField("FT_PHYSICAL RESOURCE BLOCKS LOAD UL", DoubleType(), True),
    StructField("FT_4G/LTE DL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    StructField("FT_4G/LTE DL&UL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    StructField("FT_4G/LTE UL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    StructField("FT_4G/LTE CONGESTED CELLS RATE", DoubleType(), True),
    StructField("FT_4G/LTE CALL SETUP SUCCESS RATE", DoubleType(), True),
    StructField("FT_4G/LTE AVERAGE REPORTED CQI", DoubleType(), True),
    StructField("FT_4G/LTE PAGING DISCARD RATE", DoubleType(), True),
    StructField("FT_4G/LTE RADIO DOWNLINK DELAY(ms)", DoubleType(), True),
    StructField("FT_4G/LTE VOLTE TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    StructField("FT_AVE 4G/LTE DL USER THRPUT (ALL) (KBPS)(kB/s)", DoubleType(), True),
    StructField("FT_AVE 4G/LTE DL THRPUT (ALL) (KBITS/SEC)", DoubleType(), True),
    StructField("FT_AVERAGE NB OF CA UEs RRC CONNECTED(number)", DoubleType(), True),
    StructField("FT_AVERAGE NUMBER OF UE QUEUED DL", DoubleType(), True),
    StructField("FT_AVERAGE NUMBER OF UE QUEUED UL", DoubleType(), True),
    StructField("FT_S1 SUCCESS RATE", DoubleType(), True),
    StructField("FT_UL.Interference", DoubleType(), True),
    StructField("Average Nb of e-RAB per UE", DoubleType(), True),
    StructField("Average Nb of PRB used per Ue", DoubleType(), True),
    StructField("Average Nb of Used PRB for SRB", DoubleType(), True),
    StructField("FT_AVERAGE NUMBER OF UE SCHEDULED PER ACTIVE TTI DL (FDD)(number)", DoubleType(), True),
    StructField("FT_AVERAGE NUMBER OF UE SCHEDULED PER ACTIVE TTI UL (TDD)", DoubleType(), True),
    StructField("FT_CS FALLBACK SUCCESS RATE (4G SIDE ONLY)", DoubleType(), True),
    StructField("FT_CS FALLBACK TO WCDMA RATIO", DoubleType(), True),
    StructField("FT_ERAB SETUP SUCCESS RATE", DoubleType(), True),
    StructField("FT_ERAB SETUP SUCCESS RATE (ALL)(%)", DoubleType(), True),
    StructField("FT_ERAB SETUP SUCCESS RATE (init)", DoubleType(), True),
    StructField("FT_RRC SUCCESS RATE", DoubleType(), True),
    StructField("Nb e-RAB Setup Fail", DoubleType(), True),
    StructField("Nb HO fail to GERAN", DoubleType(), True),
    StructField("Nb HO fail to UTRA FDD", DoubleType(), True),
    StructField("Nb initial e-RAB Setup Fail", DoubleType(), True),
    StructField("Nb initial e-RAB Setup Succ", DoubleType(), True),
    StructField("Nb initial e-RAB Sucess rate(%)", DoubleType(), True),
    StructField("Nb of HO over S1 for e-RAB Fail", DoubleType(), True),
    StructField("Nb of HO over S1 for e-RAB Req", DoubleType(), True),
    StructField("Nb of HO over S1 for e-RAB Succ", DoubleType(), True),
    StructField("Nb of HO over X2 for e-RAB Fail", DoubleType(), True),
    StructField("Nb of HO over X2 for e-RAB Succ", DoubleType(), True),
    StructField("Nb of RRC connection release", DoubleType(), True),
    StructField("Nb S1 Add e-RAB Setup fail", DoubleType(), True),
    StructField("RRC Emergency SR", DoubleType(), True),
    StructField("RRC High Priority SR(%)", DoubleType(), True),
    StructField("RRC MOC SR(%)", DoubleType(), True),
    StructField("RRC MTC SR(%)", DoubleType(), True),
    StructField("RRC Succ rate(%)", DoubleType(), True),
    StructField("CSFB failure rate(%)", DoubleType(), True),
    StructField("E-RAB Resource Congestion Rate(%)", DoubleType(), True),
    StructField("RRC Resource Congestion Rate(%)", DoubleType(), True),
    StructField("Average TA", DoubleType(), True),
    StructField("AVE 4G/LTE UL USER THRPUT without Last TTI (Kbps)", DoubleType(), True),
    StructField("Governorate", StringType(), True)
])

# This is the base path INSIDE the spark-service container where the DRIVER writes Parquet.
# It's bind-mounted from your host's ./data/processed directory.
BASE_OUTPUT_PATH = "/host_data_processed" 

@app.route("/process", methods=["POST"])
def process_csv():
    if not spark:
        logger.error("SparkSession not available. Cannot process request.")
        return Response(response=json.dumps({"error": "Spark service unavailable"}), status=503, mimetype="application/json")
    try:
        csv_data = request.data.decode("utf-8")
        logger.info(f"Received CSV data, length: {len(csv_data)} bytes")

        if not csv_data.strip():
            logger.warning("Received empty CSV data.")
            return Response(response=json.dumps({"message": "No data to process"}), status=200, mimetype="application/json")

        lines = [line for line in csv_data.splitlines() if line.strip()]
        if not lines or (len(lines) == 1 and lines[0].startswith(schema.fieldNames()[0])):
             logger.warning("CSV data contains no data rows or only a header.")
             return Response(response=json.dumps({"message": "No data rows or only header"}), status=200, mimetype="application/json")

        rdd = spark.sparkContext.parallelize(lines)
        df = spark.read.option("header", "true").schema(schema).csv(rdd)

        if df.isEmpty():
            logger.warning("DataFrame is empty after reading CSV.")
            return Response(response=json.dumps({"message": "DataFrame empty after reading CSV"}), status=200, mimetype="application/json")
        
        logger.info(f"Initial DataFrame created with {df.count()} rows.")

        # Your transformations
        df_transformed = df.na.fill({"Latitude": 0.0, "Longitude": 0.0})
        df_transformed = df_transformed.withColumn(
            "location_map", # Renamed to avoid confusion if original 'location' was different
            create_map(
                lit("lat"), col("Latitude"),
                lit("lon"), col("Longitude")
            )
        )
        # To make it robust for Parquet AND potentially other sinks like CSV,
        # convert the map to a JSON string.
        df_final_for_output = df_transformed.withColumn(
            "location", to_json(col("location_map"))
        ).drop("location_map") # Drop the intermediate map column

        logger.info("Data transformations complete.")

        # --- Collect to Driver ---
        logger.info("Collecting data to driver...")
        collected_rows = df_final_for_output.collect()
        logger.info(f"Collected {len(collected_rows)} rows.")

        if not collected_rows:
            logger.warning("No rows collected. Nothing to write or send to NiFi.")
            return Response(response=json.dumps({"message": "No data after processing"}), status=200, mimetype="application/json")

        # --- Driver Writes Parquet to a Unique Subdirectory ---
        batch_id = str(uuid.uuid4()) # Generate a unique ID for this batch
        current_batch_output_path = os.path.join(BASE_OUTPUT_PATH, f"batch_{batch_id}")
        
        os.makedirs(current_batch_output_path, exist_ok=True) # Ensure batch-specific subdir exists
        logger.info(f"Output path for this batch: {current_batch_output_path}")

        df_to_write_on_driver = spark.createDataFrame(collected_rows, schema=df_final_for_output.schema)
        
        logger.info(f"Driver writing {df_to_write_on_driver.count()} rows to Parquet at: {current_batch_output_path}")
        df_to_write_on_driver.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(current_batch_output_path)
        logger.info(f"Successfully wrote Parquet for batch {batch_id} to {current_batch_output_path}")

        # --- Prepare JSON response for NiFi ---
        # The collected_rows already contain the data with 'location' as a JSON string.
        list_of_dicts = [row.asDict(recursive=True) for row in collected_rows]
        response_data = "\n".join([json.dumps(d) for d in list_of_dicts])
        
        logger.info(f"Prepared {len(collected_rows)} JSON objects for NiFi response.")
        return Response(response=response_data, status=200, mimetype="application/x-ndjson")

    except Exception as e:
        logger.error(f"Error in /process: {e}", exc_info=True)
        error_message = str(e)
        if 'py4j.protocol.Py4JJavaError' in str(type(e)):
            try:
                java_exception = e.java_exception
                error_message = f"Spark Java Error: {java_exception.getClass().getName()}: {java_exception.getMessage()}"
            except AttributeError: pass
        return Response(response=json.dumps({"error": "Failed to process data", "details": error_message}), status=500, mimetype="application/json")

@app.route('/shutdown', methods=['POST'])
def shutdown():
    logger.info("Shutdown endpoint called.")
    global spark
    try:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()
            spark = None
            logger.info("Spark session stopped.")
    except Exception as e:
        logger.error(f"Error stopping Spark: {e}", exc_info=True)

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        logger.warning("Werkzeug shutdown function not available. Forcing exit (suitable for Docker).")
        os._exit(0)
    else:
        logger.info("Attempting Werkzeug server shutdown...")
        func()
    return 'Server shutting down...'

@app.route('/health', methods=['GET'])
def health():
    if spark and spark.sparkContext and getattr(spark.sparkContext, '_jsc', None):
        try:
            spark.sql("SELECT 1").collect()
            return 'OK', 200
        except Exception as e:
            logger.error(f"Health check Spark query failed: {e}", exc_info=False)
            return 'Spark query failed', 503
    else:
        logger.warning("Health check: SparkSession unavailable.")
        return 'SparkSession unavailable', 503

@app.route("/")
def home():
    status = "SparkSession is active." if spark and spark.sparkContext._jsc else "SparkSession is NOT active."
    return f"Flask-PySpark (Cluster Mode - CollectToDriver Strategy from Original) service is running. {status}"


if __name__ == "__main__":
    logger.info("Starting Flask application...")
    try:
        os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)
        logger.info(f"Ensured base output directory exists: {BASE_OUTPUT_PATH}")
    except OSError as e:
        logger.error(f"Could not create base output directory {BASE_OUTPUT_PATH}: {e}. Check permissions.", exc_info=True)

    app.run(host="0.0.0.0", port=5001, debug=False, use_reloader=False)