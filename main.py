from fastapi import FastAPI
import sqlite3
from fastapi.responses import FileResponse
import pandas as pd
import requests,json


class VinObject:
    """

    This class holds the record parameters for the vin table and represents a single VIN and its parameters.

    """

    def __init__(self, vin=None, make=None, model=None, model_year=None, body_class=None):

        """
        Constructor method.

        """
        self.vin = vin
        self.make = make
        self.model = model
        self.model_year = model_year
        self.body_class = body_class
        self.cached = False
    
    """
    Setters methods.

    """
    def set_vin(self, vin):
        self.vin = vin
    
    def set_model(self, model):
        self.model = model
    
    def set_make(self, make):
        self.make = make

    def set_model_year(self, model_year):
        self.model_year = model_year
    
    def set_body_class(self, body_class):
        self.body_class = body_class
    
    def set_cached(self, cached):
        self.cached = cached


app = FastAPI()

# Open/connect to the database and create table if it does not exist
conn = sqlite3.connect('mydatabase.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS vinTable (
                    id INTEGER PRIMARY KEY,
                    vin TEXT,
                    make TEXT,
                    model TEXT,
                    modelYear TEXT,
                    bodyClass TEXT
                )''')
conn.commit()


"""

This lookup route takes a vin, and checks validity of vin value.  If invalid, returns error message.  Otherwise, checks if 
available in cache.  If available, returns object with vin value and cached equal to true.  Otherwise, retrieves vin 
parameters from website, creates entry in the table and returns object with vin value and cached equal to false. 

"""
@app.get("/lookup/{vin}")
def lookup_vin(vin: str):
    found = True
    if len(vin) != 17:
        return {"Error" : "VIN must be 17 alphanumeric characters in length"}

    if not vin.isalnum():
        return {"Error" : "VIN must be alphanumeric characters only"}

    vin_object = VinObject()
    
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM vinTable WHERE vin = ?", (vin,))
    record = cursor.fetchone()

    if record:
        vin_object.set_vin(record[1])
        vin_object.set_make(record[2])
        vin_object.set_model(record[3])
        vin_object.set_model_year(record[4])
        vin_object.set_body_class(record[5])
        vin_object.set_cached(True)
    
    else:
        url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/' + vin + '?format=json' 
        record = requests.get(url)

        # logging.info(record.text)
        

        data = json.loads(record.text)
        vin_object.vin = vin
        for item in data['Results']:
            if item['Variable'] == "Model Year":
                if item['Value'] is not None:
                    vin_object.set_model_year(item['Value'])
                else: 
                    found = False
            if item['Variable'] == 'Make':
                if item['Value'] is not None:
                    vin_object.set_make(item['Value'])
            if item['Variable'] == 'Model':
                vin_object.set_model(item['Value'])
            if item['Variable'] == 'Body Class':
                vin_object.set_body_class(item['Value'])
            
        if found:
            cursor.execute("INSERT INTO vinTable (vin, make, model, modelYear, bodyClass) VALUES (?, ?, ?, ?, ?)", 
                (vin_object.vin, 
                vin_object.make,
                vin_object.model, 
                vin_object.model_year,
                vin_object.body_class))
            conn.commit()
        else: 
            return {"Error" : "Vin was not found in cache and does not exist in the VPIC database"}

        vin_object.set_cached(False)

    cursor.close()
    conn.close()

    return vin_object

"""

This remove route takes a vin, and checks validity of vin value.  If invalid, returns object with vin value and delete success equal to false.  
Otherwise, checks if available in cache.  If available, removes record and returns object with vin value and delete success equal to true.  
Otherwise, returns object with vin value and delete success equal to false.  

"""
@app.get("/remove/{vin}")
def remove_entry(vin: str):

    if len(vin) != 17:
        return {"Error" : "VIN must be 17 alphanumeric characters in length"}

    if not vin.isalnum():
        return {"Error" : "VIN must be alphanumeric characters only"}
    
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM vinTable WHERE vin = ?", (vin,))
    conn.commit()

    delete_success = cursor.rowcount > 0

    cursor.close()
    conn.close()

    return {"vin": vin, "delete_success": delete_success}


"""

This export route retrieves all cached records from the vin table and exports to a parquet file which is automatically downloaded.

"""
@app.get("/export")
def export_table():

    conn = sqlite3.connect('mydatabase.db')
    df = pd.read_sql_query("SELECT * FROM vinTable", conn)

    file_path = "exported_table.parquet"
    df.to_parquet(file_path, engine="pyarrow")

    conn.close()

    return FileResponse(file_path, media_type="application/octet-stream", filename="exported_table.parquet")
