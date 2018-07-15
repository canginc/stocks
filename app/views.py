from flask import render_template, request,  jsonify
from app import app
from cassandra.cluster import Cluster
from struct import *
import json

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-34-204-117-202.compute-1.amazonaws.com', 'ec2-34-236-208-228.compute-1.amazonaws.com', 'ec2-34-236-15-217.compute-1.amazonaws.com'])
# cassandra node 7  ec2-34-204-117-202.compute-1.amazonaws.com

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('playground')

#
# @ assumes database mk1 is populated
#
# Get The inter-market price differentails of stocks in a portfolio of LARGE-CAP German stocks
#
#
@app.route('/portfolio', methods=['GET', 'POST'])
def real_post():
    stm_ADIDAS= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices   from mk1 WHERE ISIN='DE000A1EWWW0' ALLOW FILTERING;"
    ans_ADIDAS = session.execute( stm_ADIDAS )

    stm_BASF= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices   from mk1 WHERE ISIN='DE000BASF111' ALLOW FILTERING;"
    ans_BASF = session.execute( stm_BASF )

    stm_BAYER= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices   from mk1 WHERE ISIN='DE000BAY0017' ALLOW FILTERING;"
    ans_BAYER = session.execute( stm_BAYER )

    stm_CON= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices   from mk1 WHERE ISIN='DE0005439004' ALLOW FILTERING;"
    ans_CON = session.execute( stm_CON )

    stm_DAI= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices   from mk1 WHERE ISIN='DE0007100000' ALLOW FILTERING;"
    ans_DAI = session.execute( stm_DAI )

    stm_Henkel= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices  from mk1 WHERE ISIN='DE0006048408' ALLOW FILTERING;"
    ans_Henkel = session.execute( stm_Henkel )

    stm_LAIR= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices  from mk1 WHERE ISIN='DE0008232125' ALLOW FILTERING;"
    ans_LAIR = session.execute( stm_LAIR )

    stm_Linde= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices)  as diffprices  from mk1 WHERE ISIN='DE000A2E4L75' ALLOW FILTERING;"
    ans_Linde = session.execute( stm_Linde )

    stm_Merck= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices from mk1 WHERE ISIN='DE0006599905' ALLOW FILTERING;"
    ans_Merck = session.execute( stm_Merck )

    stm_SAP= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices from mk1 WHERE ISIN='DE0007164600' ALLOW FILTERING;"
    ans_SAP = session.execute( stm_SAP )

    stm_VOW= "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, MAX(diffprices) as diffprices from mk1 WHERE ISIN='DE0007664005' ALLOW FILTERING;"
    ans_VOW = session.execute( stm_VOW )

    response_list = []
    for val in ans_ADIDAS :
        response_list.append(val)
    for val in ans_BASF :
        response_list.append(val)
    for val in ans_BAYER :
        response_list.append(val)
    for val in ans_CON :
        response_list.append(val)
    for val in ans_DAI :
        response_list.append(val)

    for val in ans_Henkel :
        response_list.append(val)
    for val in ans_LAIR :
        response_list.append(val)
    for val in ans_Linde :
        response_list.append(val)
    for val in ans_Merck :
        response_list.append(val)
    for val in ans_SAP :
        response_list.append(val)
    for val in ans_VOW :
        response_list.append(val)

    jsonList =[] 
    for each in response_list:
	data ={}
	data['securitydesc'] = each.securitydesc 
	data['ge_date']= each.ge_date 
	data['ge_time']= each.ge_time
	data['gerpr']= round(each.gerpr,2) 
	data['ukpr']= round(each.ukpr,2) 
	data['diffprices'] = round(each.diffprices,2) 

	# crerate JSON object by converting  JSON to string-enclosed JSON
	dataStr = json.dumps( data )
	# the dumps function convert json into "STRING of json"!!! HTML cannot handle that output list of string representing JSON  ['{"ge_time": "11:59", "ge_date": "2018-01-26", "securitydesc": "BASF SE NA O.N.", "ukpr": 92.08, "gerpr": 95.22, "diffprices": 3.14}', '{"ge_time": "11:59", "ge_date": "2018-01-26", "securitydesc": "HENKEL AG+CO.KGAA ST O.N.", "ukpr": 101.39, "gerpr": 100.8, "diffprices": 0.59}' ]

	#NECESSARy to convert string-ennclosed JSON back into JSON object
	myjson = json.loads( dataStr )
	# add each json from response_list to output
	jsonList.append ( myjson )

    #jsonList.sort ( key= extract_diffprices, reverse = True)

    return render_template("portfolio.html", output=jsonList )


#
# @ input json
# @ output the diffprices portion of json
# return the specific field of input json
#
def extract_diffprices (json):
	# Also convert to int since update_time will be string.  When comparing  strings, "10" is smaller than "2".
	try:
		return float(json['diffprices'])
	except KeyError:
		return 0

#
# Automatically refreshes the webscreen 
#  queries Cassandra for the list of stocks with highest intermarket price differentials 
#  top combines base_cassandra and html to refresh
# @assume database cassandra has populated mk1  table 
#
@app.route("/top", methods=['GET', 'POST'])
def top_post():

    # return only stocks with the top-most Absolute differences of prices > 1 euro
    stmt_search = "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, diffprices from mk1 WHERE DiffPrices> 1 limit 100 ALLOW FILTERING"
    answerSrch = session.execute(stmt_search)

    # building total response list
    response_list = []
    for val in answerSrch:
        response_list.append(val)

    # cassandra  converts all input 2-decimals #s into  euro 95.8499984741
    # 2-digit conversion is necessary to avoid WEb display euro 95.8499984741
    jsonList =[] 
    for each in response_list:
	data ={}
	data['securitydesc'] = each.securitydesc 
	data['ge_date']= each.ge_date 
	data['ge_time']= each.ge_time
	data['gerpr']= round(each.gerpr,2) 
	data['ukpr']= round(each.ukpr,2) 
	data['diffprices'] = round(each.diffprices,2) 

	# converting  data  to string-enclosed JSON
	dataStr = json.dumps( data )

	#NECESSARy to convert string-ennclosed JSON back into JSON object
	myjson = json.loads( dataStr )
	# add each json from response_list to output
	jsonList.append ( myjson)

    # sorting jsonList based on diffprices
    jsonList.sort ( key= extract_diffprices, reverse = True)

    #print(  "### after _SORTED jsonList " )
    #print(str(type( jsonList )) )
    #print(  jsonList )
    return render_template("top.html", output=jsonList )

#
# button-driven retrieval of  list of inter-market stock price differentials
# price separates  base_cassandra from HTML
#
@app.route("/price", methods=['GET', 'POST'])
def price_post():
    #price_name = request.form["price_name"]

    # return only those stocks with  Absolute differences of prices > 1 euro
    stmt_search = "SELECT securitydesc, ge_date, ge_time, ge_euro_price as GerPr,uk_poundtoeuro_price as UkPr, diffprices from mk1 WHERE DiffPrices>1 limit 100 ALLOW FILTERING"
    answerSrch = session.execute(stmt_search)

    # building total response list
    response_list = []
    for val in answerSrch:
        response_list.append(val)

    # cassandra  converts all input 2-decimals #s into  euro 95.8499984741
    # 2-digit conversion is necessary to avoid WEb display euro 95.8499984741
    jsonList =[] 
    for each in response_list:
	data ={}
	data['securitydesc'] = each.securitydesc 
	data['ge_date']= each.ge_date 
	data['ge_time']= each.ge_time
	data['gerpr']= round(each.gerpr,2) 
	data['ukpr']= round(each.ukpr,2) 
	data['diffprices'] = round(each.diffprices,2) 

	# converting  data  to string-enclosed JSON
	dataStr = json.dumps( data )

	#NECESSARy to convert string-ennclosed JSON back into JSON object
	myjson = json.loads( dataStr )
	# add each json from response_list to output
	jsonList.append ( myjson)

    # sorting jsonList based on diffprices
    jsonList.sort ( key= extract_diffprices, reverse = True)

    return render_template("price.html", output=jsonList )
