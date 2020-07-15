import os
import lib.gcp as gcp 

dataset_id = os.getenv('BIGQUERY_DATASET_ID', "dev")

# list of tables to export
table_export_list = [
    {"table_id":"region_gig","file_name":"region_gig"}
]

for table in table_export_list:
    table_id = table.get('table_id')
    file_path = ".".join([table.get('file_name'),'json'])
    query = "select * from {}.{}".format(dataset_id,table_id)
    data = gcp.get_query(query)
    print(data)
    gcp.load_json_data(data,file_path)
    
