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
	mycursor3 = mydb.cursor()
	mycursor4 = mydb.cursor()
	mycursor5 = mydb.cursor()
	
	phase_1 = float(phase_1) / 1000.0
	phase_2 = float(phase_2) / 1000.0
	phase_3 = float(phase_3) / 1000.0
	
	print(phase_1)
	print(phase_2)
	print(phase_3)
	
	unixtime = datetime.datetime.utcnow()
	sql1 = "INSERT INTO realtime_consumption (phase_1, phase_2, phase_3, voltage, powerPh1, powerPh2, powerPh3, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
	val1 = (phase_1, phase_2, phase_3,voltage,powerPh1,powerPh2,powerPh3, unixtime)
	
	energyPh1 = float(powerPh1) * float(deltaTime)
	energyPh2 = float(powerPh2) * float(deltaTime)
	energyPh3 = float(powerPh3) * float(deltaTime)
	totalEnergy = energyPh1 + energyPh2 + energyPh3
	
	
	mycursor3.execute("SELECT SUM(EnergyPhase1) FROM energy_consumption")
	phase1_record = mycursor3.fetchall()
	
	phase1_previous_energy = phase1_record[0][0]
	if phase1_previous_energy == None:
		phase1_previous_energy = 0
	phase1_new_energy = float(phase1_previous_energy) + (energyPh1)
	
	mycursor4.execute("SELECT SUM(EnergyPhase2) FROM energy_consumption")
	phase2_record = mycursor4.fetchall()
	
	phase2_previous_energy = phase2_record[0][0]
	if phase2_previous_energy == None:
		phase2_previous_energy = 0
	phase2_new_energy = float(phase2_previous_energy) + (energyPh2)
	
	mycursor5.execute("SELECT SUM(EnergyPhase3) FROM energy_consumption")
	phase3_record = mycursor5.fetchall()
	
	phase3_previous_energy = phase3_record[0][0]
	if phase3_previous_energy == None:
		phase3_previous_energy = 0
	phase3_new_energy = float(phase3_previous_energy) + (energyPh3)
	
	totalEnergy = phase1_new_energy + phase2_new_energy + phase3_new_energy
	
	totalEnergy = totalEnergy / 3600
	phase1_new_energy = phase1_new_energy / 3600
	phase2_new_energy = phase2_new_energy / 3600
	phase3_new_energy = phase3_new_energy / 3600
	
	sql2 = "INSERT INTO energy_consumption (time, EnergyPhase1, EnergyPhase2, EnergyPhase3, TotalEnergy) VALUES (%s, %s, %s, %s, %s)"
	val2 = (unixtime, phase1_new_energy, phase2_new_energy, phase3_new_energy, totalEnergy )
	
	mycursor1.execute(sql1, val1)
	mycursor2.execute(sql2, val2)
	
	
	mydb.commit()
	return {"message": "Success"}
