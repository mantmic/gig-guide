import os
import lib.gcp as gcp 

dataset_id = os.getenv('BIGQUERY_DATASET_ID', "dev")

# list of tables to export
table_export_list = [
    {"table_id":"venue","file_name":"venue"},
    {"table_id":"artist","file_name":"artist"},
    {"table_id":"gig","file_name":"gig"}
]

for table in table_export_list:
    table_id = table.get('table_id')
    file_path = ".".join([table.get('file_name'),'json'])
    query = "select * from {}.{}".format(dataset_id,table_id)
    data = gcp.get_query(query)
    print(data)
    gcp.load_json_data(data,file_path)
    
