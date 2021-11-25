import mysql.connector
import datetime
import calendar
import pandas as pd
import time
from fastapi import FastAPI


mydb = mysql.connector.connect(
  host="localhost",
  user="python",
  password="Test@123",
  database="db_kettle_mode"
)

app = FastAPI()
unixtime = 0;

@app.get("/")
async def root(phase_1,phase_2,phase_3,voltage,powerPh1,powerPh2,powerPh3, deltaTime):

	mycursor1 = mydb.cursor()
	mycursor2 = mydb.cursor()

	unixtime = datetime.datetime.utcnow()
	sql1 = "INSERT INTO realtime_consumption (phase_1, phase_2, phase_3, voltage, powerPh1, powerPh2, powerPh3, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
	val1 = (phase_1, phase_2, phase_3,voltage,powerPh1,powerPh2,powerPh3, unixtime)



	energyPh1 = float(powerPh1) * float(deltaTime)
	energyPh2 = float(powerPh2) * float(deltaTime)
	energyPh3 = float(powerPh3) * float(deltaTime)
	totalEnergy = energyPh1 + energyPh2 + energyPh3
	sql2 = "INSERT INTO energy_consumption (time, EnergyPhase1, EnergyPhase2, EnergyPhase3, TotalEnergy) VALUES (%s, %s, %s, %s, %s)"
	val2 = (unixtime, energyPh1, energyPh2, energyPh3, totalEnergy )

	mycursor1.execute(sql1, val1)
	time.sleep(0.5)
	mycursor2.execute(sql2, val2)
	previousTime = datetime.datetime.utcnow()
	mydb.commit()
	return {"message": "Success"}
