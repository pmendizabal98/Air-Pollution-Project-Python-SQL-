import mysql.connector
from mysql.connector import Error

#---------------------------------------------------------
### Create a connection to MySQL Database
#---------------------------------------------------------
def create_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection
connection = create_connection("localhost", "root", "")

#------------------------------------------------------------
### Create the "pollution-db2" database
#------------------------------------------------------------
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
create_database_query = "CREATE DATABASE `pollution-db2`"
create_database(connection, create_database_query)

#-------------------------------------------------------------------------
### Create the "station" table
#-------------------------------------------------------------------------
try:
    cnx = mysql.connector.connect(user='root', password='', host='localhost', database='pollution-db2')
    cursor = cnx.cursor()
    create_stations_table_query = '''
    CREATE TABLE IF NOT EXISTS `station` (
      `stationid` INT NOT NULL,
      `location` VARCHAR(48) NULL,
      `geo_location` VARCHAR(45) NULL,
      PRIMARY KEY (`stationid`)
    );
    '''
    cursor.execute(create_stations_table_query)
    print("station table created successfully")
except Error as e:
    print(f"The error '{e}' occurred while creating the station table.")

#-------------------------------------------------------------------------
### Create the "schema" table
#-------------------------------------------------------------------------
try:
    create_schema_table_query ='''
    CREATE TABLE IF NOT EXISTS `schema` (
      `measure` VARCHAR(32) NOT NULL,
      `description` VARCHAR(64) NULL,
      `unit` VARCHAR(24) NULL,
      PRIMARY KEY (`measure`))
    '''
    cursor.execute(create_schema_table_query)
    print("schema table created successfully")
except Error as e:
    print(f"The error '{e}' occurred while creating the schema table.")

#------------------------------------------------------------------------
### Create the "readings" table
#------------------------------------------------------------------------
try:
    create_readings_table_query = '''
    CREATE TABLE IF NOT EXISTS `readings` (
      `readingid` INT NOT NULL AUTO_INCREMENT,
      `nox` FLOAT NULL,
      `no2` FLOAT NULL,
      `no` FLOAT NULL,
      `pm10` FLOAT NULL,
      `nvpm10` FLOAT NULL,
      `vpm10` FLOAT NULL,
      `nvpm2.5` FLOAT NULL,
      `pm2.5` FLOAT NULL,
      `vpm2.5` FLOAT NULL,
      `co` FLOAT NULL,
      `o3` FLOAT NULL,
      `so2` FLOAT NULL,
      `temperature` REAL NULL,
      `rh` INT NULL,
      `air_pressure` INT NULL,
      `date_start` DATETIME NULL,
      `date_end` DATETIME NULL,
      `current` TEXT(5) NULL,
      `instrument_type` VARCHAR(32) NULL,
      `date_time` DATETIME NULL,
      `station_stationid` INT NOT NULL,
      PRIMARY KEY (`readingid`),
      INDEX `fk_readings_station_idx` (`station_stationid` ASC),
      CONSTRAINT `fk_readings_station`
        FOREIGN KEY (`station_stationid`)
        REFERENCES `pollution-db2`.`station` (`stationid`)
        );
    '''
    cursor.execute(create_readings_table_query)

    cursor.execute(create_readings_table_query)
    print("readings table created successfully")
except Error as e:
    print(f"The error '{e}' occurred while creating the readings table")
    
#----------------------------------------------------------------------
#### Populate the station table with the csv file "clean.csv"
#----------------------------------------------------------------------
import csv

# Open the CSV file
with open('clean.csv', 'r') as f:
    reader = csv.DictReader(f)

    # Populate the readings table
    try:
        for row in reader:
            insert_query = '''
            INSERT IGNORE INTO station(stationid, location, geo_location) 
            VALUES(%s, %s, %s)
            '''
            values = (row['SiteID'], row['Location'], row['geo_point_2d'])
            cursor.execute(insert_query, values)

        # Commit the changes
        cnx.commit()

        print('station table successfully populated!')

    except Error as e:
        print(f"The error '{e}' occurred while populating the station table")


#---------------------------------------------------------------------------------
### Populate the "readings" table with the csv file "clean.csv"
#---------------------------------------------------------------------------------
import csv

# Open the CSV file
with open('clean.csv', 'r') as f:
    reader = csv.DictReader(f)

    # Populate the readings table
    try:
        for row in reader:
            values = (
                row['Date Time'], 
                None if row['NOx'] == '' else row['NOx'], 
                None if row['NO2'] == '' else row['NO2'], 
                None if row['NO'] == '' else row['NO'], 
                None if row['PM10'] == '' else row['PM10'], 
                None if row['NVPM10'] == '' else row['NVPM10'], 
                None if row['VPM10'] == '' else row['VPM10'], 
                None if row['NVPM2.5'] == '' else row['NVPM2.5'], 
                None if row['PM2.5'] == '' else row['PM2.5'], 
                None if row['VPM2.5'] == '' else row['VPM2.5'], 
                None if row['CO'] == '' else row['CO'], 
                None if row['O3'] == '' else row['O3'], 
                None if row['SO2'] == '' else row['SO2'], 
                None if row['Temperature'] == '' else row['Temperature'], 
                None if row['RH'] == '' else row['RH'], 
                None if row['Air Pressure'] == '' else row['Air Pressure'], 
                None if row['DateStart'] == '' else row['DateStart'], 
                None if row['DateEnd'] == '' else row['DateEnd'], 
                None if row['Current'] == '' else row['Current'], 
                None if row['Instrument Type'] == '' else row['Instrument Type'], 
                row['SiteID']
            )

            insert_query = '''
            INSERT INTO readings(date_time, nox, no2, no, pm10, `nvpm10`, vpm10, `nvpm2.5`, `pm2.5`, `vpm2.5`, co, o3, so2, temperature, rh, air_pressure, date_start, date_end, current, instrument_type, station_stationid) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s ,%s , %s , %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_query, values)

        # Commit the changes
        cnx.commit()

        print('readings table successfully populated!')

    except Exception as e:
        print("An error occurred: ", e)
#---------------------------------------------------------------------------------
### Populate the "schema" table
#---------------------------------------------------------------------------------
try:
    insert_schema_query = '''
    INSERT INTO `schema` (`measure`, `description`, `unit`)
    VALUES
    ('Date Time', 'Date and time of measurement', 'datetime'),
    ('NOx', 'Concentration of oxides of nitrogen', '㎍/m3'),
    ('NO2', 'Concentration of nitrogen dioxide', '㎍/m3'),
    ('NO', 'Concentration of nitric oxide', '㎍/m3'),
    ('SiteID', 'Site ID for the station', 'integer'),
    ('PM10', 'Concentration of particulate matter <10 micron diameter', '㎍/m3'),
    ('NVPM10', 'Concentration of non - volatile particulate matter <10 micron diameter', '㎍/m3'),
    ('VPM10', 'Concentration of volatile particulate matter <10 micron diameter', '㎍/m3'),
    ('NVPM2.5', 'Concentration of non volatile particulate matter <2.5 micron diameter', '㎍/m3'),
    ('PM2.5', 'Concentration of particulate matter <2.5 micron diameter', '㎍/m3'),
    ('VPM2.5', 'Concentration of volatile particulate matter <2.5 micron diameter', '㎍/m3'),
    ('CO', 'Concentration of carbon monoxide', '㎎/m3'),
    ('O3', 'Concentration of ozone', '㎍/m3'),
    ('SO2', 'Concentration of sulphur dioxide', '㎍/m3'),
    ('Temperature', 'Air temperature', '°C'),
    ('RH', 'Relative Humidity', '%'),
    ('Air Pressure', 'Air Pressure', 'mbar'),
    ('Location', 'Text description of location', 'text'),
    ('geo_point_2d', 'Latitude and longitude', 'geo point'),
    ('DateStart', 'The date monitoring started', 'datetime'),
    ('DateEnd', 'The date monitoring ended', 'datetime'),
    ('Current', 'Is the monitor currently operating', 'text'),
    ('Instrument Type', 'Classification of the instrument', 'text')
    '''
    cursor.execute(insert_schema_query)
    cnx.commit()
    print("schema table succesfuly inserted!")
except Exception as e:
    print("Error occurred while inserting data into schema table:", e)
finally:
    cursor.close()
    cnx.close()