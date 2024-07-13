# Import necessary libraries
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from db.dao import HBaseDao
from db.geo_map import GEO_Map
from rules.rules import is_score_rejected, is_ucl_exceeded, is_zipcode_invalid

# Utility function to calculate distance between last two locations where transactions happened 
def get_distance(trans_postcode, lkp_postcode):
    '''
        Calculate the distance between current and previous locations where transactions happened
        Input Arguments: trans_postcode (postcode from card_transactions real time data - Kafka topic in JSON format)
                         lkp_postcode (postcode from card_lookup table (Hbase) of the same card_id from card_transactions real time data).
    '''
    # Instantiate GEO_Map class object
    geo_obj = GEO_Map.get_instance()
    lat1 = float(geo_obj.get_lat(trans_postcode).values[0])
    long1 = float(geo_obj.get_long(trans_postcode).values[0])
    lat2 = float(geo_obj.get_lat(lkp_postcode).values[0])
    long2 = float(geo_obj.get_long(lkp_postcode).values[0])
    return geo_obj.distance(lat1, long1, lat2, long2)


# Utility function to calculate time difference between last two transactions happened    
def get_time_diff_secs(trans_transactiondt, lkp_transactiondt):
    '''
        Calculate the time difference between current and previous transactions timestamp.
        Input Arguments: trans_transactiondt (transaction_dt from card_transactions real time data - Kafka topic in JSON format)
                         lkp_transactiondt (transaction_dt from card_lookup table (Hbase) of the same card_id from card_transactions real time data).
    '''
    return (trans_transactiondt - lkp_transactiondt).total_seconds()


# Write card transactions final data to HBase table
def insert_card_transactions_hbase(transaction_data):
    '''
        Insert card transactions real time data (Kafka topic in JSON format) into card_transactions hbase table.
        Input Arguments: transaction_data (list object of card transactions real time data - Kafka topic in JSON format).
    '''
    # Instantiate HBaseDao class object to connect to HBase
    hbase_obj = HBaseDao.get_instance()
    trans_key = '|'.join([str(v) for v in transaction_data])
    data = {b'transaction_data:card_id':bytes(str(transaction_data[0]), encoding='utf8'),
            b'transaction_data:member_id':bytes(str(transaction_data[1]), encoding='utf8'),
            b'transaction_data:amount':bytes(str(transaction_data[2]), encoding='utf8'),
            b'transaction_data:pos_id':bytes(str(transaction_data[3]), encoding='utf8'),
            b'transaction_data:postcode':bytes(str(transaction_data[4]), encoding='utf8'),
            b'transaction_data:transaction_dt':bytes(str(transaction_data[5]), encoding='utf8'),
            b'transaction_data:status':bytes(str(transaction_data[6]), encoding='utf8')}
    hbase_obj.write_data(bytes(trans_key, encoding='utf8'), data, 'card_transactions')


# Update latest postcode and transaction date in card lookup HBase table
def update_card_lookup_hbase(trans_card_id, status_flag, lkp_data):
    '''
        Update postcode and transaction_dt attributes in card lookup table HBase for which transactions are treated as GENUINE.
        Input Arguments: trans_card_id
                         status_flag
                         lkp_data (List object of postcode and transaction_dt card transactions real time data - Kafka topic in JSON format).
    '''
    # Instantiate HBaseDao class object to connect to HBase
    hbase_obj = HBaseDao.get_instance()
    if status_flag == 'GENUINE':
        key = bytes(str(trans_card_id), encoding='utf8')
        data = {b'lkp_info:postcode':bytes(str(lkp_data[0]), encoding='utf8'),
                b'lkp_info:transaction_dt':bytes(str(lkp_data[1]), encoding='utf8')}
        hbase_obj.write_data(key, data, 'card_lookup')


# Utility function to detect fradulent
def check_status(trans_card_id, trans_member_id, trans_amount, trans_pos_id, trans_postcode, trans_transactiondt):
    '''
        This function is to detect the fradulent of the live transactions based on the rules defined and lookup data prepared as part of batch processing.
        The following three rules to be validated:
            1. Check credit score
            2. Check UCL threshold with amount
            3. Measure the speed travelled in Kilometers/second between the locations of last two transactions.
        If any of the above rule fails then the transaction is labelled as FRAUD. Otherwise it is labelled as GENUINE.
        Input Arguments: trans_card_id (card id attribute from card_transactions real time data - Kafka topic in JSON format)
                         trans_member_id (member id attribute from card_transactions real time data - Kafka topic in JSON format)
                         trans_amount (transaction amount from card_transactions real time data - Kafka topic in JSON format)
                         trans_pos_id (pos_id from card_transactions real time data - Kafka topic in JSON format)
                         trans_postcode (postcode from card_transactions real time data - Kafka topic in JSON format)
                         trans_transactiondt (transaction_dt from card_transactions real time data - Kafka topic in JSON format)
    '''
    status_flag = 'GENUINE'
    # Instantiate HBaseDao class object to connect to HBase
    hbase_obj = HBaseDao.get_instance()
    lkp_row = hbase_obj.get_data(str(trans_card_id), 'card_lookup')
    if lkp_row == {}:
        pass
    else:
        lkp_score, lkp_postcode, lkp_transactiondt, lkp_ucl = [val.decode('utf8') for val in list(lkp_row.values())]
        lkp_score = int(lkp_score)
        lkp_postcode = int(lkp_postcode)
        lkp_transactiondt = datetime.strptime(lkp_transactiondt, '%Y-%m-%d %H:%M:%S')
        lkp_ucl = float(lkp_ucl)
        
        # Check transactions rules to determine credit score, UCL and zipcode distance.
        if is_score_rejected(lkp_score):
            status_flag = 'FRAUD'
        elif is_ucl_exceeded(trans_amount, lkp_ucl):
            status_flag = 'FRAUD'
        elif is_zipcode_invalid(get_distance(str(trans_postcode), str(lkp_postcode)), get_time_diff_secs(trans_transactiondt, lkp_transactiondt)):
            status_flag = 'FRAUD'
        else:
            pass
    
    # Call insert_card_transactions_hbase() function to insert the card transaction data with status derived
    insert_card_transactions_hbase([trans_card_id, trans_member_id, trans_amount, trans_pos_id, trans_postcode, trans_transactiondt, status_flag])
    # Call update_card_lookup_hbase() to insert/update postcode and transaction_dt attributes in card_lookup HBase table
    update_card_lookup_hbase(trans_card_id, status_flag, [trans_postcode, trans_transactiondt])
    
    return status_flag
        

# Create fraud detection UDF on top of status utility function        
fraud_detection = udf(check_status, StringType())
           
            
# Create a spark session
spark = SparkSession \
        .builder \
        .appName("Credit Card Fraud Detection App") \
        .getOrCreate()
        
# Set log level to ERROR
spark.sparkContext.setLogLevel('ERROR')

# Read card transactions from Kafka topic : transactions-topic-verified
card_trans_source = spark  \
              .readStream  \
              .format("kafka")  \
              .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
              .option("subscribe","transactions-topic-verified")  \
              .option("startingOffsets", "earliest") \
              .load()

# Defining the Data Structure for source card transactions (json format).              
data_structure = StructType([
                            StructField("card_id", LongType()),
                            StructField("member_id", LongType()),
                            StructField("amount", IntegerType()),
                            StructField("pos_id", LongType()),
                            StructField("postcode", IntegerType()),
                            StructField("transaction_dt", StringType())
                      ])

# Converting source data from json format to dataframe (table).            
card_trans_formatted =  card_trans_source \
                 .select(from_json(col("value").cast("string"), data_structure).alias("card_transactions")) \
                 .select("card_transactions.*") \
                 .withColumn("transaction_dt", to_timestamp(col('transaction_dt'),'dd-MM-yyyy HH:mm:ss')) \
                 .select("card_id",
                         "member_id",
                         "amount",
                         "pos_id",
                         "postcode",
                         "transaction_dt"
                         )

# Derive status attribute: GENUINE or FRAUD based on rules defined
card_trans_transformed = card_trans_formatted \
                         .withColumn("status", fraud_detection(card_trans_formatted.card_id, card_trans_formatted.member_id, card_trans_formatted.amount, card_trans_formatted.pos_id, card_trans_formatted.postcode, card_trans_formatted.transaction_dt)) \
                         .select("card_id",
                         "member_id",
                         "amount",
                         "pos_id",
                         "postcode",
                         "transaction_dt",
                         "status"
                         )

# Write the final transformed data to sink             
card_trans_sink = card_trans_transformed \
                  .writeStream \
                  .outputMode("append") \
                  .format("console") \
                  .option("truncate", "false") \
                  .trigger(processingTime="1 minute") \
                  .start()

# Await termination                 
card_trans_sink.awaitTermination()
