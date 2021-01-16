import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim
from pyspark.sql.functions import regexp_replace
import pyspark.sql.functions as f
from pyspark import SQLContext

### Global Variables ###
oij_file_name = "datos_delictivos_oij - Estadisticas-1.csv";
inec_file_name = "datos_inec.csv"

sc = pyspark.SparkContext()
sql = SQLContext(sc)
###
def initialize_frames():
    oij_data_frame = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(oij_file_name))
    oij_data_frame.createOrReplaceTempView("schedule")

    inec_data_frame = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(inec_file_name))
    inec_data_frame.createOrReplaceTempView("schedule")
    return oij_data_frame,inec_data_frame

def remove_white_space(oij_data_frame,inec_data_frame):
    oij_data_frame = oij_data_frame.withColumn("Distrito",trim(oij_data_frame.Distrito))
    oij_data_frame = oij_data_frame.withColumn("Distrito",regexp_replace("Distrito"," ",""))

    inec_data_frame = inec_data_frame.withColumn("provincia_canton_distrito",trim(inec_data_frame.provincia_canton_distrito))
    inec_data_frame = inec_data_frame.withColumn("provincia_canton_distrito",regexp_replace("provincia_canton_distrito"," ",""))

    return oij_data_frame,inec_data_frame

def to_lowercase(oij_data_frame, inec_data_frame):
    oij_data_frame = oij_data_frame.withColumn("Distrito",f.lower(f.col("Distrito")))
    inec_data_frame = inec_data_frame.withColumn("provincia_canton_distrito",f.lower(f.col("provincia_canton_distrito")))

    return oij_data_frame,inec_data_frame

def show_data_frames(oij_data_frame,inec_data_frame):
    oij_data_frame.show()
    inec_data_frame.show()

oij_data_frame,inec_data_frame = initialize_frames()
oij_data_frame,inec_data_frame = remove_white_space(oij_data_frame,inec_data_frame)
oij_data_frame,inec_data_frame = to_lowercase(oij_data_frame,inec_data_frame)
show_data_frames(oij_data_frame,inec_data_frame)
