"""
This Spark Streaming module  reads the "streaming input Kafka topic string (16-field string)" and convert them into  (12-fields) for Cassandra Database.
It reads the Kafka Topic, performs computations and convertions, and saves the results to Cassandra database.

This Spark streaming code converts input streaming  "16-fields-list"  germany/UK input   into    "12 fields-list" computed db output

incoming  KAFKA topic 16-fields combined multimarket indices 0 to 15
{ # kafka input
 ISIN, SecurityDesc, DE_Date, DE_Time,
 DE_euro_StartPrice, DE_euro_MaxPrice, DE_euro_MinPrice, DE_euro_EndPrice, DE_TradedVolume, DE_NumberOfTrades,
 UK_euro_StartPrice, UK_euro_MaxPrice, UK_euro_MinPrice, UK_euro_EndPrice, UK_TradedVolume, UK_NumberOfTrades,
} # kafka input

example Sending to Kafka Topic =
	{"CH0038863350","NESTLE NAM.        SF-_10",2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1,61.6024423106,61.6024423106,61.6024423106,61.6024423106,7,1}

outgoing  12-fields record to DB cassandra
{
	ISIN, SecurityDesc, DE_Date, DE_Time,
	GE_euro_Price,       	GE_TradedVolume,
	UK_PoundToEuro_Price,	UK_TradedVolume,
	perctDiffPrices,
	DiffPrices,
	GE_swingPerct, 
	UK_swingPerct
}
"""

from __future__ import print_function
import os
import decimal
from decimal import Decimal
import sys
import time
import threading
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import functions
from pyspark.sql.functions import split

from cassandra.cluster import Cluster

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

"""
This extract_data function processes input Kafka topic string and then transform it into a format ready for cassandra

@instring input 16 fields data from Kafka Topic
@return 12-fieds output
extract data from the input string and reformatted them into 12-fields output
"""
def extract_data( instring):

        listTokens = instring.split( ',')

	# type of instring=type 'unicode'
	if ( instring is None or \
	   len(instring)==0 or \
	   len(listTokens) !=  16):
		print ("WARNING: input string too short; len( listTokens )=" + str(len(  listTokens )) )
		return []

	# initialize output
	output = range(12)	# return list of 12 items=[0,1,2,....11]

	## extraction of each rdd line
        output[0] =listTokens[0].encode("utf-8")	#'ISIN'

	if (listTokens[1] is not None):
        	output[1] =listTokens[1].encode("utf-8")	#'SecurityDesc', 

	if (listTokens[2] is not None):
        	output[2] = listTokens[2].encode("utf-8")	# GE_Date

	if (listTokens[3] is not None):
        	output[3] = listTokens[3].encode("utf-8")	# GE_Time

	if (listTokens[7] is not None):
        	output[4] = listTokens[7].encode("utf-8")   	#GE_euro_Price, # Germany is Deutsche, GE===DE

	if (listTokens[8] is not None):
        	output[5] = listTokens[8].encode("utf-8") 		# DE_TradedVolume

	if (listTokens[13] is not None):
        	output[6] = listTokens[13].encode("utf-8")    		#UK_euro_EndPrice

	if (listTokens[14] is not None):
        	output[7] = listTokens[14].encode("utf-8") 		# UK_TradedVolume

	# convert unicode string to floats
	GermanPrice =	 float ( listTokens[7].encode("utf-8") )
	UkPrice =	 float ( listTokens[13].encode("utf-8") )

	output[8]= round( (abs(GermanPrice - UkPrice) / max(GermanPrice, UkPrice)) , 2 )  	# perctDiffPrices
	output[9]= round( abs(GermanPrice - UkPrice), 2)					# Germ Diff  UK price

	Gmax = ( float ( listTokens[5].encode("utf-8")) )
	Gmin = ( float ( listTokens[6].encode("utf-8")) )
	output[10] = round ( abs( Gmax -   Gmin ), 2 ) 		# GE_swingPerct

	UKmax =	 float ( listTokens[11].encode("utf-8") )
	UKmin =	 float ( listTokens[12].encode("utf-8") )
	output[11] = round ( abs(UKmax - UKmin), 2)		# UK_swingPerct

	return output


"""

This process function saves each of the 12 fields string into Cassandra database.
It executes the		folloing command to insert into mk1 table:
	INSERT INTO mk1 (ISIN, SecurityDesc, 
			GE_Date, GE_Time, 
			GE_euro_Price, GE_TradedVolume, 
			UK_PoundToEuro_Price, UK_TradedVolume, 
			PerctDiffPrices, DiffPrices ,
			 GE_swingPerct, UK_swingPerct 
			) VALUES ( ?,?,?,?,  ?,?,?,?,  ?,?,?,?  )

For each RDD, spark streaming call this processs to store the converted twelve fields data into Cassandra Table.

"""
def process( 
	time, rdd):
    
	if (rdd is None) : 
		return

	num_of_record = rdd.count()
	if  num_of_record ==0:
		return 

	lines = rdd.map(lambda somestruct: somestruct[1])   

	if  (lines is None):
		print("**** WARNING: empty  lines = rdd.map(lambda struct: struct[1]) ")
		return

	for each in lines.collect():
		# convert each line of comma-separated STRING into LIST
		pricing = extract_data(each)
		print ("@@extracted=" + str(pricing ) +"]" )

		# structured streaming  (pricing is None or pricing['ISIN']==' ' or pricing['ISIN']==''):
		if (pricing is None or\
		 pricing[0]==' ' or\
		 pricing[0]=='' or\
		 len(pricing) !=12 ):
			print ("## length of pricing list =" + len(pricing ) +"]" )
			continue

		pricing[0] = pricing[0].replace('"', "") 
		pricing[1] = pricing[1].replace('"', "") 
		pricing[2] = pricing[2].replace('"', "") 
		pricing[3] = pricing[3].replace('"', "") 
		p4str = ( "{:.2f}".format( float(  pricing[4] )   ))
		p6str = ( "{:.2f}".format( float(  pricing[6] )   ))
		p8str = ( "{:.2f}".format( float(  pricing[8] )   ))
		p9str = ( "{:.2f}".format( float(  pricing[9] )   ))
		p10str = ( "{:.2f}".format( float(  pricing[10] )   ))
		p11str = ( "{:.2f}".format( float(  pricing[11] )   ))

		session.execute(  to_cassandraDT,  \
			(pricing[0], pricing[1], \
			pricing[2], pricing[3], \
			round( float( p4str), 2) , int( pricing[5]), \
			round( float( p6str), 2) , int( pricing[7]), \
			round( float( p8str), 2) , round( float (pricing[9]), 2)   , \
			round( float( p10str), 2) , round( float(p11str), 2)  ))


    #END for loop insertign each LINE of RDD 
### END process function

"""
This Spark Streaming module reads data from Kafka Topic, performs processing, establishes connection with Cassandra, and saves the converted data to Cassandra

@outgoing  12-fields record to DB cassandra
{
	ISIN 0, SecurityDesc 1, DE_Date 2, DE_Time 3,
	GE_euro_Price 7	,       	GE_TradedVolume 8,
	UK_PoundToEuro_Price 13,	UK_TradedVolume 14,
	perctDiffPrices [7]- [13],
	DiffPrices [7]- [13],
	GE_swingPerct  [5, 6], 
	UK_swingPerct  [11, 12]
} 


"""
if __name__ == "__main__":
	cassandra_master = "ec2-34-204-117-202.compute-1.amazonaws.com"
	#if multiple nodes exist for cassandra, append your borkers as a list here
	keyspace = "playground"
	cluster = Cluster([cassandra_master])
	session = cluster.connect(keyspace)

	to_cassandraDT = session.prepare("INSERT INTO mk1 (ISIN, SecurityDesc, GE_Date, GE_Time, GE_euro_Price, GE_TradedVolume, UK_PoundToEuro_Price, UK_TradedVolume, PerctDiffPrices, DiffPrices , GE_swingPerct, UK_swingPerct ) VALUES ( ?,?,?,?,  ?,?,?,?,  ?,?,?,?  )")

	conf = SparkConf().setAppName("PySpark Cassandra").\
		set("spark.es.host", "ec2-34-204-117-202.compute-1.amazonaws.com").\
		set("spark.streaming.receiver.maxRate",2000).\
		set("spark.streaming.kafka.maxRatePerPartition",1000).\
		set("spark.streaming.backpressure.enabled",True).\
		set("spark.cassandra.connection.host","34.204.117.202")

	kafkabrokers = ["ec2-34-224-164-5.compute-1.amazonaws.com:9092", \
		"ec2-52-1-193-73.compute-1.amazonaws.com:9092",\
		"ec2-52-20-254-67.compute-1.amazonaws.com:9092",\
		"ec2-18-207-59-207.compute-1.amazonaws.com:9092"] 

	readingFromTopicName = "EukTopic"

	sc = SparkContext(appName="Germany_UK")
	ssc = StreamingContext(sc, 7)

	#Get stream data from kafka
	kafkaStream = KafkaUtils.createDirectStream( ssc,\
			topics=[ readingFromTopicName], \
			kafkaParams={"metadata.broker.list": kafkabrokers})

	kafkaStream.foreachRDD(process) 
	ssc.start()
	ssc.awaitTermination()

#End main

"""
FootNotes Recap:
Overall, this module converts 16-fields data from Kafka topic into  12-fields data for Cassandra

perctDiffPrices = abs( DE_euro_EndPrice - UK_euro_EndPrice )/ max ( DE_euro_EndPrice , UK_euro_EndPrice )
DiffPrices =  abs( DE_euro_EndPrice - UK_euro_EndPrice )

# SWING (Max- min)  as fraction of End price
GE_swingPerct = abs( DE_euro_MaxPrice- DE_euro_MinPrice) / DE_euro_EndPrice
UK_swingPerct = abs( UK_euro_MaxPrice- UK_euro_MinPrice) / UK_euro_EndPrice

Overall module converts Kafka topic into string for Cassandra database.

@input 	KAFKA topic 16-fields combined multimarket indices 0 to 15
{  kafka input
	ISIN, SecurityDesc, DE_Date, DE_Time,
	DE_euro_StartPrice, DE_euro_MaxPrice, DE_euro_MinPrice, DE_euro_EndPrice,DE_TradedVolume, DE_NumberOfTrades,
	UK_euro_StartPrice, UK_euro_MaxPrice, UK_euro_MinPrice, UK_euro_EndPrice,UK_TradedVolume, UK_NumberOfTrades,
} 

only created once in  Cassandra node ubuntu@ip-10-0-0-7: USE playground;
CREATE TABLE IF NOT EXISTS mk1 (ISIN text , SecurityDesc text, GE_Date text, GE_Time text, GE_euro_Price float, GE_TradedVolume int, UK_PoundToEuro_Price float, UK_TradedVolume int, PerctDiffPrices float, DiffPrices float, GE_swingPerct float, UK_swingPerct  float, PRIMARY KEY ((ISIN), GE_Date)  )  WITH CLUSTERING ORDER BY ( GE_Date DESC);

@output outgoing  12-fields record to DB cassandra
{
	ISIN 0, SecurityDesc 1, DE_Date 2, DE_Time 3,
	GE_euro_Price 7	,       	GE_TradedVolume 8,
	UK_PoundToEuro_Price 13,	UK_TradedVolume 14,
	perctDiffPrices [7]- [13],
	DiffPrices [7]- [13],
	GE_swingPerct  [5, 6], 
	UK_swingPerct  [11, 12]
}
"""
