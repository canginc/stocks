"""
This module reads Actual Germany stock ticker data from Frankfurt, 
performs UK simulation,  and sends the pricing data (of both markets) to the Kafka Topic.


"""
import random
from decimal import Decimal
import os
import sys
import datetime
import time
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka.producer import KafkaProducer
from kafka.client import KafkaClient
import boto

if sys.version_info < (3,):
    from cStringIO import StringIO
else:
    from io import StringIO
    xrange = range
from tokenize import generate_tokens


"""
This function parses the input string containing COMMA-delimited-fields.
This function correctly split the input string using COMMAS outside of quotes,
But ignore Within-quotes commas.

 incoming line schema contains 14 commaSeparate fields:  CAVEAT some field may contain comma. Use "" to identify fields with comma
	dataframe [ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades ]
 	each field is delimited by openQuote and closedquote

 Issue: certain field SecurityDesc contains COMMA inside openQuote and closedQuote
 "GB0059822006","DLG","DIALOG SEMICOND.   LS-,10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6
 hence split by comma can mistaken 14 fields as 15 fields or more
       e.g. #split_col = functions.split( datafrm['value'], ',')
	input:
 "GB0059822006","DLG","DIALOG SEMICOND.   LS-,10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6
	output:
 "GB0059822006","DLG","DIALOG SEMICOND.   LS-_10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6

  https://docs.python.org/2/library/tokenize.html
	tokenize.generate_tokens(readline)
The generate_tokens() generator requires one argument, readline, which must be a callable object which provides the same interface as the readline() method of built-in file objects (see section File Objects). Each call to the function should return one line of input as a string. Alternately, readline may be a callable object that signals completion by raising StopIteration.

 The generator produces 5-tuples with these members: the token type; the token string; a 2-tuple (srow, scol) of ints specifying the row and column where the token begins in the source; a 2-tuple (erow, ecol) of ints specifying the row and column where the token ends in the source; and the line on which the token was found. 

"""
def parts( inputString ):

    """Split a python-tokenizable expression on comma operators"""
    compos = [-1] 
    # compos stores the positions of the relevant commas in the argument string

    compos.extend( tokens[2][1] for tokens in generate_tokens(StringIO( inputString ).readline)\
		 if tokens[1] == ',')

    compos.append( len( inputString )  )

    #correctly spliting the inputString   using commas outside of quotes, but ignore within-quotes-comma

    return [  inputString[compos[i]+1:compos[i+1]] for i in xrange(len(compos)-1)]
    #return the correctly-split inputString using commas outside of quotes, but ignore within-quotes-comma



"""
This function takes the input germany data and, returns the multiple market data string for subsequent Kafka Topic.
@incoming string is 14 fields
@outcoming string is 16 fields

index locations:
0    1 	  2		3 	    4	     5          6    7    8          9         10       11        12         13
ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
 "CH0038863350","NESR","NESTLE NAM.        SF-,10","Common stock","EUR",2504245,2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
 "CH0303692047","ED4","EDAG ENGINEERING G.SF-,04","Common stock","EUR",2504254,2018-06-22,11:00,17.9,17.9,17.9,17.9,4,1

New List has 16 fields  and new multi market List NOW has these indices:
Using old indices 0, 2, 6, 7 ,Germany_toEuro [8,9,10,11],  GermanyVolume [12], DE_NumberOfTrades[13], 
UK_toEuro prices, UKvol, Uk
{
ISIN, SecurityDesc, DE_Date, DE_Time,	
 DE_euro_StartPrice, DE_euro_MaxPrice, DE_euro_MinPrice, DE_euro_EndPrice, 	 DE_TradedVolume, DE_NumberOfTrades,
 UK_euro_StartPrice, UK_euro_MaxPrice, UK_euro_MinPrice, UK_euro_EndPrice, 	 UK_TradedVolume, UK_NumberOfTrades,
}
 Continue only for stocks that statring with
ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
"BE0974293251","1NBA","ANHEUSER-BUSCH INBEV","Common stock","EUR",2504195,2018-06-21,11:00,84.4,84.4,84.4,84.4,30,1
"CA32076V1031","FMV","FIRST MAJESTIC SILVER","Common stock","EUR",2504197,2018-06-21,11:00,6.26,6.26,6.26,6.26,1000,1
"""
def  MultiMarkets ( cleanList ):

	##input should be 14 fields , with first field being ISIN string
        if ( len(cleanList) == 0 or cleanList[0] == "ISIN"  or  (len (cleanList) < 14)  ):
		#print  ( "INVALID input list cleanLis=["+ str(cleanList) +"]###" )
                return []

	# process Only Stocks, skip ETF,  skip ECF, skip others; Skip all US securities
        typeStr= str( cleanList[3]).replace('"', '') 
        typeStr= typeStr.replace("'", '') 
        symbolCountry= str( cleanList[0]).replace('"', '') 
        symbolCountry= symbolCountry.replace( "'", '') 

        if ( typeStr !="Common stock" ): 
                return []
        if  (symbolCountry.startswith("US") or  ("GEORG FISCHER"  in cleanList[2]) or ("STRAUMANN" in cleanList[2])  ):    
                return []

	MultiList  = ['']*16
	# ISIN
	MultiList[0]  =cleanList [0]
	# SecurityDesc 
	MultiList[1]  =cleanList [2]
	#DE_Date
	MultiList[2]  =cleanList [6]
	# DE_Time
	MultiList[3]  =cleanList [7]

	## Germany pricing
	#DE_euro_StartPrice
	MultiList[4]  =cleanList [8]
	# DE_euro_MaxPrice
	MultiList[5]  =cleanList [9]
	# DE_euro_MinPrice 
	MultiList[6]  =cleanList [10]
	# DE_euro_EndPrice       
	MultiList[7]  =cleanList [11]

	#DE_TradedVolume
	if (len(cleanList) >=14 ): 
		MultiList[8]  =cleanList [12]

	# DE_NumberOfTrades,
	if (len(cleanList) >=14 ): 
		MultiList[9]  =cleanList [13]

	# conservative simulations of currency exchange rate
	if ( float(cleanList[11] < 300.0) ): 
		EURO_TO_POUND  = random.uniform( 0.95, 1.05)
		POUND_TO_EURO = random.uniform( 0.95, 1.05)
		INTERMARKET_SPREAD = random.uniform( 0.95, 1.05)
	elif ( float(cleanList[11] >= 300.0) ): 
		EURO_TO_POUND  = random.uniform( 0.99, 1.01)
		POUND_TO_EURO = random.uniform( 0.99, 1.01)
		INTERMARKET_SPREAD = random.uniform( 0.99, 1.01)

	# UK_Pound_To_euro_StartPrice
	MultiList[10]=str( round( (float(cleanList [8]) *  EURO_TO_POUND * POUND_TO_EURO * INTERMARKET_SPREAD), 2)   )

	# UK_Pound_To_euro_MaxPrice 
	MultiList[11]=str( round( (float(cleanList [9] )*  EURO_TO_POUND * POUND_TO_EURO * INTERMARKET_SPREAD ), 2)   )

	# UK_Pound_To_euro_MinPrice 
	MultiList[12]=str( round( (float(cleanList [10] )*  EURO_TO_POUND * POUND_TO_EURO * INTERMARKET_SPREAD ), 2)  )

	# UK_Pound_To_euro_EndPrice
	MultiList[13]=str( round( (float(cleanList [11] )*  EURO_TO_POUND * POUND_TO_EURO * INTERMARKET_SPREAD), 2)  )

	# UK_TradedVolume
	MultiList[14]  = cleanList [12 ]

	# UK_NumberOfTrades
	MultiList[15]  = cleanList [13 ]

	return MultiList 

"""
This Main mmethods ingests the AWS germany stock data,  initializes bootstrap address for Kafka producer,
cleans, formats, and  repackages data

"""
if __name__ == "__main__":

	args = sys.argv
	# master node ip-10-0-0-9.ec2.internal
	ip_addr = "ec2-34-224-164-5.compute-1.amazonaws.com"
        if (len(args) > 2):
                ip_addr= str(args[1])

	producer = KafkaProducer(bootstrap_servers = 'ec2-34-224-164-5.compute-1.amazonaws.com')
	aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
	aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

	bucket_name ="germanymkt"
	conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
	bucket = conn.get_bucket(bucket_name)

	yearMonthDay = '2018-07-12'
	folder_name =yearMonthDay
        from_date = '2018-07-01'
        until_date= '2018-07-12'

        rangeDatesList = list(pd.date_range(from_date, until_date, freq='D').strftime('%Y-%m-%d'))
        print ("rangeDatesList ="+ str(rangeDatesList ) )

	# ACTIVE  Hours from 7 am to 3pm 
	#s3a://deutsche-boerse-xetra-pds/2018-06-15/2018-06-15_BINS_XETR10.csv
	for yearMonthDay in rangeDatesList:

		folder_name =yearMonthDay
		#for eachTimeSlot in range(8,20): 
		# testing 
		for eachTimeSlot in range(11, 12): 
			file_name = yearMonthDay+ "_BINS_XETR"+str(eachTimeSlot)+".csv"
			if ( eachTimeSlot  < 10 ):
				file_name = yearMonthDay+ "_BINS_XETR0"+str( eachTimeSlot )+".csv"
			keyname = folder_name +"/"+ file_name

			##writing S3 csv file into temporary log out file
			if ( bucket.get_key( keyname ) == None):
				print ("###@@@ SKIPPING this loop iteration:  cannot get S3 bucket keyname =" + keyname)
				continue

			data = bucket.get_key( keyname).get_contents_as_string()
			allLines = data.split('\n')

			# validate the META-data of ach line
			for line in allLines:
				# parse input line into list of comma-separated & Quote-enclosed elements, but NOT split at within-quote-comma
				cleanList = parts(line)

				# replace all Quote-enclosed commas with  Quote-enclosed underscore
				cleanList = [elt.replace(',', '_') for elt in cleanList]

				GermanyUKlist = MultiMarkets ( cleanList )
				# stock ID ISIN is the key or first field before comma
				cleanLineStr = ','.join( GermanyUKlist )

				if ( len(GermanyUKlist)  ==16  ):  
					producer.send ( "EukTopic", value=cleanLineStr.encode('utf-8') )
			producer.flush()
			producer= KafkaProducer(retries =3)
			## for each line
		## END for eachTimeSlot 
	##END for yearMonthDay in rangeDatesList:
##END main method 

