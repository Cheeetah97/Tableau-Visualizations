import sqlalchemy
import urllib
import os
import time
import boto3

params = urllib.parse.quote_plus('Driver={ODBC Driver 13 for SQL Server};'
                                 'Server=;'
                                 'Database=;'
                                 'Uid=;Pwd=;'
                                 'Encrypt=yes;'
                                 'TrustServerCertificate=no;'
                                 'Connection Timeout=30;')

engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
connection = engine.raw_connection()
cursor = connection.cursor()

#%%
# Retrieve list of Clients
unique_clients = pd.read_sql("SELECT DISTINCT(CID) FROM SalesAnalytics",engine)["CID"].unique()

#%%
# Client by Client Processing

for client in unique_clients:
    
    # Make Table for Client
    
    Query = f"""
       DROP TABLE IndSalesAnalytics;
       SELECT * INTO IndSalesAnalytics FROM SalesAnalytics where cid = {client}"""
    
    print("Query Started")
    cursor.execute(Query)
    connection.commit()
    print("Query Finished")
    
    # Start Processing
    
    print(f'##### Processing Client {client} ###########')
    os.system('tabcmd refreshextracts --workbook "Test_Viz" --server "server" --username user --password pass')
    time.sleep(10)
    if not os.path.exists(f'C:/Users/Shariq/Desktop/CLIENT/{client}'):
        os.mkdir(f'C:/Users/Shariq/Desktop/CLIENT/{client}')
    else:
        print("Client Exists")
    wb_path = f'C:/Users/Shariq/Desktop/CLIENT/{client}/Client_WB.twbx'
    os.system(f'tabcmd get "/workbooks/Test_Viz.twb" -f {wb_path} --server "server" --username user --password pass')


#%%
# AWS S3 Credentials
session = boto3.Session(aws_access_key_id='',
                        aws_secret_access_key='')

s3 = session.resource('s3')
aws_bucket = s3.Bucket('')

#%%
# Delete Old Clients Folders

def delete_old_clients(active_clients,bucket):
    
    for obj in bucket.objects.all():
        client_obj = obj.key.split('/')[0]
        if client_obj in active_clients:
            continue
        else:
            bucket.delete_objects(Delete={'Objects':[{'Key':obj.key}]})

delete_old_clients(unique_clients,aws_bucket)
#%%
# Upload New Data
  
def upload_folder(path,bucket):

    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir+'/', file)
            with open(full_path, 'rb') as data:
                #print(full_path[len(path)+1:])
                bucket.put_object(Key=full_path[len(path)+1:],Body=data)

upload_folder('C:/Users/Shariq/Desktop/CLIENT',aws_bucket)
#%%
