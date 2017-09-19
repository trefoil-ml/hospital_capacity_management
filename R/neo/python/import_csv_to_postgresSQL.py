import odo
import datetime
import boto3
import calendar
import matplotlib
from sqlalchemy import create_engine
import seaborn as sns
import psycopg2
# matplotlib.style.use('ggplot')
import os
import pandas as pd
import matplotlib.pyplot as plt
%matplotlib inline
from IPython.display import display, HTML
boto3.setup_default_session(region_name='eu-west-1')
s3 = boto3.resource('s3', region_name='eu-west-1')
next_gen_cbm_bucket = s3.Bucket('next-gen-cbm-data')
 
def getCorrectShape(ds):
    field_names = ds.parameters[1]
    shape = "var * {"
    for j, name in enumerate(field_names.names):
        if name == 'ts':
            shape = shape+"ts: ?datetime"
        if name == 'v':
            shape = shape+"v: float32"
        if name == 'engine_type':
            shape = shape+"engine_type: string[20]"
        if name == 'engine_subtype':
            shape = shape+"engine_subtype: string[20]"
        if name == 'unit':
            shape = shape+"unit: string[20]"
        if name == 'sensor_description':
            shape = shape+"sensor_description: string[20]"
        if name == 'tag':
            shape = shape+"tag: string[20]"
        if name == 'installation_id':
            shape = shape+"installation_id: ?string"
        if name == 'engine_id':
            shape = shape+"engine_id: ?string"
        if j< len(field_names.names):
            shape = shape+","
         
    shape = shape+"}"   
    return shape
 
def saveTagsToPostgreSQL(installations, db_url, table):
    print("..........Loading and merging csv files............")
     
    for inst in installations:
        print(inst)
        conn = psycopg2.connect(db_url)
        files =  [object.key for object in next_gen_cbm_bucket.objects.filter(Prefix="W46F/"+inst) if object.key[-6:] == "csv.gz"]
        print(len(files))
        try:
            os.remove("temp.csv")
        except:
            pass
        print("..........inserting into postgreSQL............")
        with open("output.txt", "a") as outputfile:
                    outputfile.writelines("........."+inst+".............")
        for f in range(0,len(files)):
            try:
                print(f)
                print("s3://next-gen-cbm-data/" + files[f])
                ds = odo.dshape(getCorrectShape (odo.discover(odo.resource("s3://next-gen-cbm-data/" + files[f]))))
                csv = odo.odo("s3://next-gen-cbm-data/" + files[f],  'temp.csv', dshape=ds, usecols=["ts", "v", "tag", "engine_id","installation_id"])
                df = pd.read_csv('temp.csv',dtype=str)
                df = df[["ts", "v", "tag", "engine_id", "installation_id"]]
                df = df.dropna()
                df.to_csv('temp.csv',index=False)
#                 print(df.head())
                copy_sql = """
                   COPY """+table+""" FROM stdin WITH CSV HEADER
                   DELIMITER as ','
                   """
                cur = conn.cursor()
                with open('temp.csv', 'r') as tempfile:
                    cur.copy_expert(sql=copy_sql, file=tempfile)
                    conn.commit()
                    cur.close()
                os.remove("temp.csv")
                 
                with open("output.txt", "a") as outputfile:
                    outputfile.writelines("\n ..."+str(f)+"...s3://next-gen-cbm-data/" + files[f])
                     
            except Exception as err:
                print(err)
                print(files[f]+ " does not work")
                conn = psycopg2.connect(db_url)
                with open("output_failed.txt", "a") as outputfile:
                    outputfile.writelines("\n ..."+str(f)+"...s3://next-gen-cbm-data/" + files[f])
         
        print(".............Done.............")
 
db_url = 'postgresql://postgressadmin:postgressadmin@bender-spock-postgress-db.cnwm9vkygspi.eu-west-1.rds.amazonaws.com:5432/Bender_Spock_Postgress_DB'
installations = ['Maracanau', 'SuapeII_1','SuapeII_2', 'SuapeII_3', 'SuapeII_4']
saveTagsToPostgreSQL(installations, db_url, 'wois_data.time_series_table')