# import packages

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery

# define
#project-id:dataset_id.table_id
delivered_table_spec = 'bigquery-cervello:dataset_food_orders.delivered_orders'
#project-id:dataset_id.table_id
other_table_spec = 'bigquery-cervello:dataset_food_orders.other_status_orders'

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')

# -- input is name of argument to be used in run command
# true means mandatorry command
# help means the description will be printed when help command is run
                      
path_args, pipeline_args = parser.parse_known_args()

# arguments passed in cli are captured using parse_known_args() mathod
# and storing arguments in two objects
# path_args holds > input file location
# pipeline_args holds > env related args, runner type, job name, file location

## >> to use input file location in code

inputs_pattern = path_args.input # parameter used from dest above, here path_args gets handled

options = PipelineOptions(pipeline_args) # create an object of pipelineoptions class and pass pipeline_arsgs

p = beam.Pipeline(options = options)
# pipeline is important class, controls lifecycle of it, similar to sparkcontext in spark

# ^^ this is mandate code to write pipeline in beam




def remove_last_colon(row): # OXJY167254JK,11-09-2020,8:11:21,854A854,Chow M?ein:,65,Cash,Sadabahar,Delivered,5,Awesome experience
    cols = row.split(',') # [(OXJY167254JK) (11-11-2020) (8:11:21) (854A854) (Chow M?ein:) (65) (Cash) ....]
    item = str(cols[4]) # item = Chow M?ein:
    
    if item.endswith(':'):
        cols[4] = item[:-1] # cols[4] = Chow M?ein

    return ','.join(cols) # OXJY167254JK,11-11-2020,8:11:21,854A854,Chow M?ein,65,Cash,Sadabahar,Delivered,5,Awesome experience



def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',') # [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ',' # oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1] # oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret



def print_row(row):
    print (row)

# write methods to collect the cleaned data
# all the transformations will be applied using pipe operator
# final data will be stored in cleaned_data p collection
# p will be unified storage entity of beam similar to dataset and rdd in spark

cleaned_data = (
    p
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
    | beam.Map(remove_last_colon) # remove : from food item name
    | beam.Map(lambda row: row.lower()) # make everything lowercase
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row+',1') # oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein,65,cash,sadabahar,delivered,5,awesome experience,1
    )

# map functions apply one to one mapping to each row and returns one row 
# create collection for separate table
# cleaned_data is label is unique and cannot be assigned to multiple p transforms
# helps in finding errors
# delivered_orders is a collection of records with delivered sstatus
delivered_orders = (
    cleaned_data
    | 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() == 'delivered')

)
# undelivered_orders is a collection of records with undelivered sstatus
other_orders = (
    cleaned_data
    | 'Undelivered Filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')
)

# for logging purpose taken count of recrds 
(cleaned_data
 | 'count total' >> beam.combiners.Count.Globally() # 920
 | 'total map' >> beam.Map(lambda x: 'Total Count:' +str(x)) # Total Count: 920
 | 'print total' >> beam.Map(print_row) # print

)

(delivered_orders
 | 'count delivered' >> beam.combiners.Count.Globally()
 | 'delivered map' >> beam.Map(lambda x: 'Delivered count:'+str(x))
 | 'print delivered count' >> beam.Map(print_row)
 )


(other_orders
 | 'count others' >> beam.combiners.Count.Globally()
 | 'other map' >> beam.Map(lambda x: 'Others count:'+str(x))
 | 'print undelivered' >> beam.Map(print_row)
 )

# bigquery , create client directly because we are using this script in cloud shell
# and not on local sdk so no json key is required for access

client = bigquery.Client()


dataset_id = "{}.dataset_food_orders".format(client.project)
# try except block for creating dataset and tables because this code willl rn daily and might give dataset exists error 
try:
    client.get_dataset(dataset_id)

except:
    dataset = bigquery.Dataset(dataset_id)  #if the dataset is not available in try block then except block code will create it
    dataset.location = "US"
    dataset.description = "dataset for food orders"
    dataset_ref = client.create_dataset(dataset, timeout=30)  # Make an API request.
    
# beams.io package can be used to create tables and loading data in them as well
# p collection takes tablename , schema, takes inut as json so we have to convert them (p colletctions) into json

def to_json(csv_str): # passing each record to this fn and assigning it to dictionary key and particular value to create json string
    
    fields = csv_str.split(',')
        
    json_str = {"customer_id":fields[0],
                 "date": fields[1],
                 "timestamp": fields[2],
                 "order_id": fields[3],
                 "items": fields[4],
                 "amount": fields[5],
                 "mode": fields[6],
                 "restaurant": fields[7],
                 "status": fields[8],
                 "ratings": fields[9],
                 "feedback": fields[10],
                 "new_col": fields[11]
                 }

    return json_str

table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'

(delivered_orders
    | 'delivered to json' >> beam.Map(to_json)
    | 'write delivered' >> beam.io.WriteToBigQuery(
    delivered_table_spec,
    schema=table_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # holds std strings for create and write dispsitions
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

# CREATE_IF_NEEDED CREATE_IF_NEEDED means this won't throw already exist error when the pipeline is rerun.
# It will create only if the table is not present.
# WRITE_APPEND allows the daily runs to append that data into a table.
# we partition on day basis because it is daily running pipeline, ingestion based

(other_orders
    | 'others to json' >> beam.Map(to_json)
    | 'write other_orders' >> beam.io.WriteToBigQuery(
    other_table_spec,
    schema=table_schema,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

# to get pipeline state message data
from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print('pipeline has ran successfully !')
else:
    print('Error Running beam pipeline')

## ^^ pipeline is done !
    
    
    
    
# creating view to store daily data, on delivered orders table
# data is partitioned on daily basis and monthly report will be in table itself

view_name = "daily_food_orders"
dataset_ref = client.dataset('dataset_food_orders')
view_ref = dataset_ref.table(view_name)
view_to_create = bigquery.Table(view_ref)

view_to_create.view_query = 'select * from `bigquery-cervello.dataset_food_orders.delivered_orders` where _PARTITIONDATE = DATE(current_date())' # query to run for view
view_to_create.view_use_legacy_sql = False # to use std sql syntax

try: # because in second run of pipeline this will cause error
    client.create_table(view_to_create)
    
except:
     print ('view already exists')

