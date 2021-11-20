import mysql.connector
import datetime
import calendar
import pandas as pd
import time

mydb = mysql.connector.connect(
  host="localhost",
  user="python",
  password="Test@123",
  database="db_kettle_mode"
)

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root(phase_1: float = 0):

	mycursor = mydb.cursor()

	unixtime = datetime.datetime.utcnow()
	sql = "INSERT INTO realtime_consumption (phase_1, time) VALUES (%s, %s)"
	print(unixtime)
	val = (phase_1, unixtime)
	mycursor.execute(sql, val)
	mydb.commit()
	return {"message": "Success"}
