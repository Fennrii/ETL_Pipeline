# This program will extract the data from Better Health, County Data and will do a simple transform on the data by getting rid of unnessesary columns, and finally load it to a database
import requests
import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import psycopg2 as pg
import pandas as pd
from sqlalchemy import create_engine

# retrieves data from source and saves it locally to be used in the next steps
@task()
def extractData():

	url = "https://sharefulton.fultoncountyga.gov/resource/vsqn-t5t9.json" # Url location of the database json file
	response = requests.get(url)
	df = pd.DataFrame(json.loads(response.content))
	df.to_json("localBetterHealth{}.json".format(datetime.today().strftime("%Y%m%d")), orient="records")

# Takes locally saved data and removes unnessasary information
@task()
def transformData():

	df = pd.read_json("localBetterHealth{}.json".format(datetime.today().strftime("%Y%m%d")))

	df = df.drop(columns=["longitude", "latitude", "geolocation"]) # removes unnessesary location data
	df = df.drop(columns=["datasource", "data_value_unit"]) # removes columns filled only by the same entry

	df.to_json("localBetterHealth{}.json".format(datetime.today().strftime("%Y%m%d")), orient="records")

# Connects to database, creates the table and truncates it
@task()
def createTable():
	try:
		conn = BaseHook.get_connection("postgres")
		dbConnection = pg.connect(
			dbname=conn.conn_id, user=conn.login, host=conn.host, password=conn.password
		)
	except Exception as error:
		print(error)

	# Create the table if it does not already exist 
	cursor = dbConnection.cursor()
	cursor.execute("""
		CREATE TABLE if not exists CountyData(		
			year integer ,
			stateabbr character varying,
			statedesc character varying,
			locationname character varying,
			category character varying,
			measure character varying,
			data_value_type character varying,
			data_value decimal,
			low_confidence_limit decimal,
			high_confidence_limit decimal,
			totalpopulation integer,
			locationid integer,
			categoryid character varying,
			measureid character varying,
			datavaluetypeid character varying,
			short_question_text character varying
			);

		TRUNCATE TABLE CountyData;
		"""
    )
	dbConnection.commit()
	cursor.close()
	dbConnection.close()

# gets the locally saved transformed data and uploads it to the database's tables
@task()
def loadData():
	try:
		conn = BaseHook.get_connection("postgres")
		dbConnection = pg.connect(
			dbname=conn.conn_id, user=conn.login, host=conn.host, password=conn.password
		)
	except Exception as error:
		print(error)
    
	cursor = dbConnection.cursor()
	with open("localBetterHealth{}.json".format(datetime.today().strftime("%Y%m%d")), 'r') as f:
			file = json.load(f) 
			# Goes through each record in the json file and imputs the data into the table
			for row in file:
				cursor.execute("""
					INSERT INTO CountyData
					VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
				""".format(
				row.get("year"),
				row.get("stateabbr"),
				row.get("statedesc"),
				row.get("locationname"),
				row.get("category"),
				row.get("measure"),
				row.get("data_value_type"),
				row.get("data_value"),
				row.get("low_confidence_limit"),
				row.get("high_confidence_limit"),
				row.get("totalpopulation"),
				row.get("locationid"),
				row.get("categoryid"),
				row.get("measureid"),
				row.get("datavaluetypeid"),
				row.get("short_question_text")
				)
				)
	dbConnection.commit()
	cursor.close()
	dbConnection.close()     


default_args = {
	"owner": "airflow",
	"retries": 5,
	"retry_delay": timedelta(minutes=5)
}

# Creates and defines the DAG to be ran every midnight 
with DAG(dag_id="BetterHealth_ETL", default_args=default_args, start_date=datetime(2023,3,17), schedule_interval="0 0 * * *",catchup=False) as dag:
	
	# Uses TaskGroups to make it look nicer in Airflow
	with TaskGroup("Extract", tooltip="Extracts data from source") as extract:
		extracter = extractData()
		# Defines order for Extract group
		extracter

	with TaskGroup("Transform", tooltip="Transforms data and readies it to be loaded") as transform:
		transformer = transformData()
		# Defines order for Transform group
		transformer

	with TaskGroup("Load", tooltip="Creates table and loads data") as load:

		makeTable = createTable()
		addData = loadData()
		# Defines order for Load group
		makeTable >> addData

	# Defines order for the DAG
	extract >> transform >> load
