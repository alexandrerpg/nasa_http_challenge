#Instalando as aplicações necessarias para a execução desta tarefa.
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
!tar xf spark-2.4.4-bin-hadoop2.7.tgz
!pip install -q pyspark

#Baixando os datasets NASA_access_log_Jul95.gz e NASA_access_log_Aug95.gz
!wget --quiet --show-progress ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
!wget --quiet --show-progress ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
!mkdir raw_files prod_files
!mv NASA_access_log_Jul95.gz NASA_access_log_Aug95.gz raw_files
!gunzip raw_files/NASA_access_log_Jul95.gz
!gunzip raw_files/NASA_access_log_Aug95.gz

import sys
import re
import datetime
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import split, regexp_extract, col, sum, udf, desc, dayofmonth
from pyspark.sql import SparkSession
sc = SparkSession.builder.master('local[*]').getOrCreate()

mes = {
  'Jan': 1, 'Feb': 2,
  'Mar': 3, 'Apr': 4,
  'May': 5, 'Jun': 6,
  'Jul': 7, 'Aug': 8,
  'Sep': 9, 'Oct': 10,
  'Nov': 11,'Dec': 12
}


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

raw_data_df = sqlContext.read.text('/content/raw_files/*', wholetext=False).cache()

pre_data_df = raw_data_df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                              regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                              regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('endpoint'),
                              regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                              regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('bytes'))

pre_data_df.show(truncate=False)

def cast_time(s):
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(s[7:11]), mes[s[3:6]],
      int(s[0:2]),  int(s[12:14]),
      int(s[15:17]),int(s[18:20])
    )
p_time = udf(cast_time)
redy_data_df = pre_data_df.select('*', p_time(pre_data_df['timestamp']).cast('timestamp').alias('date')).drop('timestamp')
redy_data_df.show(truncate=False)
#redy_data_df.write.csv("/content/prod_files/http_nasa.csv")
#redy_data_df.write.parquet("/content/prod_files/http_nasa.parquet")

# Respondendo a quantidade de hosts únicos
host_unico = redy_data_df.select('host').distinct().count()
print ("Numero de host únicos: {0}".format(host_unico))

# Respondendo a quantidade de 404
redy_data_404_df = redy_data_df.filter(redy_data_df['status'] == 404).cache()
print('Total de 404 {0}'.format(redy_data_404_df.count()))

# Respondendo os 5 endpoints que mais causam 404
endpoint_404_df = redy_data_df.filter(redy_data_df['status'] == 404).cache()
top5_endpoint_404_df = (endpoint_404_df.groupBy('endpoint').count().sort('count', ascending=False)).cache()
print ('Top 5 dos endpoints com retorno 404: ')
top5_endpoint_404_df.show(5)

# Respondendo a quantidade de endpoints com retorno 404 por dia
endpoint_404_day = redy_data_404_df.withColumn('day',dayofmonth(redy_data_404_df['date'])).groupBy('day').count().sort('day')
print ('Quantidade de endpoints com retorno 404 por dia:')
endpoint_404_day.show(31)	

#Respondendo a quandidade de bytes que trafegaram no total
#TODO - Refatorar =(
from pyspark.sql.types import StructField,IntegerType, StructType,StringType
newDF=[StructField('_c7',IntegerType(),True)]
finalStruct=StructType(fields=newDF)
total_bytes_data = sqlContext.read.csv('/content/raw_files/*', sep=' ').cache()
total_bytes = total_bytes_data.select('_c7').cache()
total_bytes.show()
from pyspark.sql import functions as F
total = total_bytes.agg(F.sum("_c7")).collect()
print('O total de bytes trânsacionados foram {0}'.format(total))

sc.stop()
