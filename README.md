# Aero test

**Task**: write a connector that will connect to the https://statsapi.web.nhl.com/api/v1/teams/21/stats API once every 12 hours and upload data to the database twice a day.

**Solution**.
  Logic for interaction with API is implemented in aero.py file. Sending requests, JSON conversions and uploading to the database are divided into functions. 
  DAG logic is implemented in aero_extract.py
