from flask import Flask, request, Response
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json

app = Flask(__name__)

# Spark Session is created only once
spark = SparkSession.builder \
    .appName("CSV-Processing-API") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.memory", "2G") \
    .config("spark.driver.memory", "2G") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Define the schema based on your dataset (keeping the same schema as before)
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



@app.route("/process", methods=["POST"])
def process_csv():
    try:
        # Get the raw CSV content from NiFi
        csv_data = request.data.decode("utf-8")

        # Create RDD
        rdd = spark.sparkContext.parallelize(csv_data.splitlines())

        # Convert RDD to DataFrame
        df = spark.read.option("header", "true").schema(schema).csv(rdd)

        # Date formatting
        #if "Date" in df.columns:
        #    df = df.withColumn("Date", to_timestamp(col("Date"), "dd-MM-yyyy"))
        #    df = df.withColumn("timestamp", date_format(col("Date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

        # Replace null lat/lon
        df = df.na.fill({"Latitude": 0.0, "Longitude": 0.0})
        df = df.withColumn(
            "location",
            create_map(
                lit("lat"), col("Latitude"),
                lit("lon"), col("Longitude")
            )
        )

        # Save Parquet version for storage/analytics
        df.write.mode("append").option("compression", "snappy").parquet("/home/moetez/data-ingestion/processed")

        # Convert to JSON strings for Elasticsearch (NDJSON)
        json_rows = df.toJSON().collect()
        response_data = "\n".join(json_rows)
        return Response(response=response_data, status=200, mimetype="application/x-ndjson")


    except Exception as e:
        return Response(response=json.dumps({"error": str(e)}), status=500, mimetype="application/json")

@app.route('/shutdown', methods=['POST'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

def shutdown_server():
    global spark
    try:
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            spark = None
    except Exception as e:
        print(f"Error stopping Spark: {e}")

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        print("No werkzeug shutdown. Forcing exit.")
        os._exit(0)
    else:
        print("Shutting down Flask server...")
        func()


#Added health endpoint to check for spark service before starting the ingestion
@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
