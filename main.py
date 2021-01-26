import unicodedata
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import translate, regexp_replace
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

    inec_data_frame = (sql.read.format("com.databricks.spark.csv").option("header", "true").load(inec_file_name))

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


def make_diff_list(oij_data_frame,inec_data_frame):
    oij_data_frame.createOrReplaceTempView("OIJ")
    inec_data_frame.createOrReplaceTempView("INEC")
    diff_list = sql.sql(
        "Select OIJ.Distrito from OIJ LEFT JOIN INEC ON OIJ.distrito = INEC.provincia_canton_distrito WHERE INEC.provincia_canton_distrito is NULL GROUP BY OIJ.Distrito");
    return diff_list

def show_diff_list(oij_data_frame,inec_data_frame):
    make_diff_list(oij_data_frame,inec_data_frame).show()

def show_how_many_are_diff(oij_data_frame,inec_data_frame):
    diff_list = make_diff_list(oij_data_frame,inec_data_frame)
    diff_list.createOrReplaceTempView("diff_table")
    diff_list = sql.sql("Select count(*) as cantidad from diff_table")
    diff_list.show();


def make_trans():
    matching_string = ""
    replace_string = ""

    for i in range(ord(" "), sys.maxunicode):
        name = unicodedata.name(chr(i), "")
        if "WITH" in name:
            try:
                base = unicodedata.lookup(name.split(" WITH")[0])
                matching_string += chr(i)
                replace_string += base
            except KeyError:
                pass

    return matching_string, replace_string

def clean_text(c):
    matching_string, replace_string = make_trans()
    return translate(
        regexp_replace(c, "\p{M}", ""),
        matching_string, replace_string
    ).alias(c)

def remove_accents(inec_data_frame):
    return inec_data_frame.select(clean_text("provincia_canton_distrito"))


oij_data_frame,inec_data_frame = initialize_frames()
oij_data_frame,inec_data_frame = remove_white_space(oij_data_frame,inec_data_frame)
oij_data_frame,inec_data_frame = to_lowercase(oij_data_frame,inec_data_frame)

#show_diff_list(oij_data_frame,inec_data_frame)

inec_data_frame = remove_accents(inec_data_frame)
inec_data_frame.show()

#show_diff_list(oij_data_frame,inec_data_frame)

lista = make_diff_list(oij_data_frame,inec_data_frame);

