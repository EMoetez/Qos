from flask import Flask, request, Response
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, create_map, lit, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json
import os
# import shutil # No longer needed as we are not clearing/writing directories
import logging
# import uuid # No longer needed for batch output paths

app = Flask(__name__)

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

SPARK_MASTER_ENDPOINT = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# --- Spark Session Initialization ---
spark = None
try:
    logger.info(f"Attempting to connect to Spark master at: {SPARK_MASTER_ENDPOINT}")
    spark = SparkSession.builder \
        .appName("NetworkDataProcessing-NiFi-Only-Response") \
        .master(SPARK_MASTER_ENDPOINT) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "768m") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()
    logger.info(f"SparkSession created successfully for cluster processing. Master: {spark.sparkContext.master}")
except Exception as e:
    logger.error(f"Failed to create SparkSession: {e}", exc_info=True)

# --- Schema Definition (from your script) ---
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

# No DRIVER_WRITES_FINAL_OUTPUT_PATH needed as spark-service won't write to disk.
# No clear_directory function needed.

@app.route("/process", methods=["POST"])
def process_csv_data():
    if not spark:
        logger.error("SparkSession not available. Cannot process request.")
        return Response(response=json.dumps({"error": "Spark service unavailable"}), status=503, mimetype="application/json")
    try:
        csv_data = request.data.decode("utf-8")
        if not csv_data.strip():
            logger.warning("Received empty CSV data. Nothing to process.")
            return Response(response=json.dumps({"message": "No data to process"}), status=200, mimetype="application/json")
        
        logger.info(f"Received CSV data, length: {len(csv_data)} bytes")

        lines = [line for line in csv_data.splitlines() if line.strip()]
        if not lines or (len(lines) == 1 and lines[0].startswith(schema.fieldNames()[0])):
             logger.warning("CSV data contains no data rows or only a header.")
             return Response(response=json.dumps({"message": "No data rows or only header"}), status=200, mimetype="application/json")

        full_csv_rdd = spark.sparkContext.parallelize(lines)
        df_initial = spark.read.option("header", "true").schema(schema).csv(full_csv_rdd)
        
        if df_initial.isEmpty():
            logger.warning("Initial DataFrame is empty after Spark read.")
            return Response(response=json.dumps({"message": "DataFrame empty after reading CSV"}), status=200, mimetype="application/json")
        
        initial_count = df_initial.count()
        logger.info(f"Initial DataFrame created by Spark cluster with {initial_count} rows.")


        df_initial = df_initial.na.fill({"Latitude": 0.0, "Longitude": 0.0})
        df_initial = df_initial.withColumn(
            "location",
            create_map(
                lit("lat"), col("Latitude"),
                lit("lon"), col("Longitude")
            )
        )
        
        # Convert the map to a JSON string for the NiFi response (and for general compatibility)
        #df_transformed = df_with_map.withColumn(
        #    "location", to_json(col("location_map_internal"))
        #).drop("location_map_internal")

        # Optional date transformations (ensure they are compatible with JSON output)
        # if "Date" in df_transformed.columns:
        #     df_initial = df_initial.withColumn("Date", to_timestamp(col("Date"), "dd-MM-yyyy"))
        #     # For JSON, timestamp is fine, or convert to ISO string:
        #     # df_initial = df_initial.withColumn("timestamp_iso", date_format(col("Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))


        logger.info("Data transformation complete on Spark cluster.")

        # --- Collect processed data to the Driver (Flask App) ---
        logger.info("Collecting processed data to the driver...")
        collected_rows = df_initial.collect()
        logger.info(f"Collected {len(collected_rows)} rows to the driver.")

        if not collected_rows:
            logger.warning("No data collected to the driver. Sending empty response to NiFi.")
            # You might want to send an empty list or a specific message
            return Response(response="[]", status=200, mimetype="application/x-ndjson") # Example: empty NDJSON array

        # --- Driver (Flask App) prepares response for NiFi ---
        # The 'location' column is now a JSON string.
        list_of_dicts = [row.asDict(recursive=True) for row in collected_rows]
        json_strings_for_nifi = [json.dumps(d) for d in list_of_dicts]
        response_body = "\n".join(json_strings_for_nifi)
        
        logger.info(f"Prepared {len(collected_rows)} JSON objects for response to NiFi.")
        return Response(response=response_body, status=200, mimetype="application/x-ndjson")

    except Exception as e:
        logger.error(f"Error processing CSV: {e}", exc_info=True)
        error_message = str(e)
        if 'py4j.protocol.Py4JJavaError' in str(type(e)):
            try:
                java_exception = e.java_exception
                error_message = f"Spark Java Error: {java_exception.getClass().getName()}: {java_exception.getMessage()}"
            except AttributeError: pass
        return Response(response=json.dumps({"error": "Failed to process data", "details": error_message}), status=500, mimetype="application/json")

# --- Shutdown, Home, Health Endpoints (mostly unchanged) ---
@app.route('/shutdown', methods=['POST'])
def shutdown():
    logger.info("Shutdown endpoint called.")
    global spark 
    if spark:
        logger.info("Stopping Spark session...")
        try:
            spark.stop()
            logger.info("Spark session stopped.")
        except Exception as e:
            logger.error(f"Error stopping Spark: {e}", exc_info=True)
        finally:
            spark = None

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        logger.warning('Not running with the Werkzeug Server or shutdown function not available. Forcing exit for Docker.')
        os._exit(0) 
    else:
        logger.info("Shutting down Flask server via Werkzeug...")
        func()
    return 'Server shutting down...'

@app.route("/")
def home():
    status = "SparkSession is active." if spark and spark.sparkContext._jsc else "SparkSession is NOT active."
    return f"SUUUUUUUUUUUUIIIII!!!!!! Flask-PySpark (NiFi Only Response Mode) service is running. {status}"

@app.route('/health', methods=['GET'])
def health_check(): 
    if spark and spark.sparkContext and getattr(spark.sparkContext, '_jsc', None):
        try:
            spark.sql("SELECT 1").collect()
            return 'OK', 200
        except Exception as e:
            logger.error(f"Health check Spark query failed: {e}", exc_info=False)
            return 'Spark query failed', 503
    else:
        logger.warning("Health check: SparkSession not available or SparkContext not initialized.")
        return 'SparkSession unavailable', 503

if __name__ == "__main__":
    logger.info("Starting Flask application - Spark processing on CLUSTER, response sent to NiFi (NO DISK WRITE from Spark service).")
    # No need to ensure DRIVER_WRITES_FINAL_OUTPUT_PATH exists as we are not writing from here.
    app.run(host="0.0.0.0", port=5001, debug=False, use_reloader=False)
