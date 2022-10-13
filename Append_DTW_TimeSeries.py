# ---------------------------------------------------------------------------
# Append_DTW_TimeSeries.py
#
# Created on: 20200630
# Last Modified: 20220412
# Created by: Kirk Sherrill - Data Manager Rocky Mountain Network (ROMN), IMD, NPS
# For the Defined DTW Time Series as defined in the 'timeSeriesLoop' parameter, these fields are appended to existing Time Series at the Location
# Harvests all .csv files in all downstream directories of the defined input root directory.  CSV file must have the site name prefix in order to
# define the location in Aquarius at which the logger data will be uploaded (e.g. FLFO_705_FLFO_705_2020_1_Hourly_20220412.csv where {SiteName_EventName_OutputScale_DateofOutput_.csv}).
# Code is defined to process the following ROMN specific time series:
# DepthToWaterFromGround.DTW_g_Adjusted
# Absolute Pressure.Pressure_Baromerged
# Absolute Pressure.Pressure_Raw
# Groundwater Temp at Depth.Groundwater Temp at Depth 0-200 cm
# Absolute Pressure.Pressure_Baro

# V3 Update: Data being processed in this script is set for a -7 hour offset (i.e. for Mountain Standard - America/Denver) from the UTC+-00:00 value
#           ROMN Time Series are all defined with a -7 hour offset.
#
# V4 Update: Added logic to check if no numeric values don't import these rows.  Logic was already in place but more robust has been added using Coerce function
#
# V5 Update: Changed logic for time series 'Water Temp.Temperature_Raw' to 'Groundwater Temp at Depth' to reflect the sytematic renaming of the 'Water Temp' time series for WEI to 'Groundwater Temp at Depth' - 20220412
#
# Python Version 3.7
# Dependices: Requests, pyrfc3339, pytz
# The Aquarius 'Timeseries_client.py' wrapper class (see: Z:\MONITORING\Loggers\Documents\Aquarius\Python\NextGeneration\AquariusTimeSeries)
# Scripts: timeseries_client.py and setup.py must be in the Python Environment - 'Lib\site-packages' directory before the
# timeseries client can be used to hit the Aquarius REST endpoints
#######################################
# Start of Parameters requiring set up.
#######################################

rootDiretory = r'D:\ROMN\working\Loggers_DTW\DB_DTW\DataGathering\InSitu_DTW\2021\Aquarius'       #Root Directory - all child directories and .csv files will be processed.
timeSeriesLoop = ["DepthToWaterFromGround.DTW_g_Adjusted","Absolute Pressure.Pressure_Baromerged", "Absolute Pressure.Pressure_Raw","Groundwater Temp at Depth.Groundwater Temp at Depth 0-200 cm","Absolute Pressure.Pressure_Baro"]  #List defining the time series to be processed

#Workspace Output Parameters
workspace = r'D:\ROMN\working\Loggers_DTW\DB_DTW\DataGathering\InSitu_DTW\2021\Aquarius\Workspace'      ## Workspace for Processing
outLogFileName = "Aquarius_Append_DTW_TimeSeries_2021_DataProcessing"
logFileName = workspace + "\\" + outLogFileName + ".LogFile.txt"
###############################

import sys, string, os, glob, traceback, shutil, csv, pytz, ast
import pandas as pd
import requests,  pyrfc3339
from datetime import datetime
from pytz import timezone

def main():

    try:

        #################################################################
        #Define the .csv files to be processed - via the shutil
        #################################################################
        csvFiles = glob.glob(rootDiretory + "\\**\\*.csv", recursive= True)  #Sytnax Works for Python 3.x

        # AQUARIUS Server to connect to
        server = 'https://aquarius.nps.gov'  # NPS Aquarius Server Name
        loginName = 'AQ_User'  # Aquarius Login Name
        loginPass = 'AQ_User_2020!'  # Aquarius Login Password

        # This is the Aquarius API Wrapper Class - used to hit the Next Generation Aquarius Springboard (20.1.68.0)
        # Downlad the files from: https://github.com/AquaticInformatics/examples/tree/master/TimeSeries/PublicApis/Python
        from timeseries_client import timeseries_client
        #Hit the Aquarius Service
        timeseries = timeseries_client(server, loginName, loginPass)

        #Loop Thru all harvested csv weather station files
        for file in csvFiles:

            baseName = os.path.basename(file)
            baseNameSplit = baseName.split("_")
            locationName = (str.upper(str(baseNameSplit[0])) + "_" +  baseNameSplit[1])     #Define SiteName

            #Loop Thru the Time Series to be Append to'
            for timeSeries in timeSeriesLoop:

                #Define the Time Series name at the defined Location
                timeSeriesNameFull = timeSeries + "@" + locationName

                #Use the API getTimeSeiresUniqueId wrapper
                try:
                    timeSeriesId = timeseries.getTimeSeriesUniqueId(timeSeriesNameFull)
                    print("Time Series ID: " + timeSeriesId)
                except:
                    messageTime = timeFun()
                    scriptMsg = "WARNING Time Series - " + timeSeriesNameFull + " was not found at Site:" + locationName + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()
                    continue

                #Import CSV file to Pandas Dataframe
                df = pd.read_csv(file, ',')

                selected_columns = ["DateTime"]   #List to define the columns/fields being processed

                #fieldName = funcFieldName(timeSeries) - Create Function to Define the field Name - Expand for all time series 20200626
                #Define the Times Series Field Name
                if timeSeries == "DepthToWaterFromGround.DTW_g_Adjusted":
                    fieldName = 'DTW_g_Adjusted'
                elif timeSeries == "Absolute Pressure.Pressure_Baromerged":
                    fieldName = 'Pressure_Baromerged'
                elif timeSeries == "Absolute Pressure.Pressure_Raw":
                    fieldName = 'Pressure_Raw'
                elif timeSeries == "Groundwater Temp at Depth.Groundwater Temp at Depth 0-200 cm":
                    fieldName = 'Temperature_Raw'
                elif timeSeries == "Absolute Pressure.Pressure_Baro":
                    fieldName = 'Pressure_Baro'

                else:
                    print ("No Time Series - Field Name Match Found")
                    messageTime = timeFun()
                    scriptMsg = "WARNING Failed To Process - " + str(timeSeries) + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()
                #Add the time series field to be processed
                selected_columns.append(fieldName)


                #Function to take the defined Columns and Export the Columns to a new Pandas Data Frame
                df2 = select_columns(df, selected_columns)  #Creating new data frome with only the date and time series field being processed

                #Renmame the 'datetime' and "fieldName' to 'Time' and 'Value
                df2.rename(columns={"DateTime": "Time", fieldName: "Value"}, inplace=True)
                del df

                # Use to Numeric to force the value field to be numeric - using coerce to set as NAN if not.
                df2['Value'] = pd.to_numeric(df2.Value, errors='coerce')
                                
                #Check if data in the 'Value' field
                x = df2['Value'].sum()
                if x == 0:
                    messageTime = timeFun()
                    scriptMsg = "WARNING Time Series - " + timeSeriesNameFull + " is Null/NAN:" + locationName + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()
                    continue

                # Drop rows where value is NaN  due to Aquarius Call not handling
                df2 = df2[df2['Value'].notna()]

                #Convert the Time field to
                utc = pytz.UTC
               # MountainStandard = timezone('America/Denver')
                # Convert the 'Time' to dateTime field - with the UCT DataTimeIndex value - setting to 0 offset
                df2['Time'] = pd.to_datetime(df2['Time'], utc=utc)

                #Setting the Time zone to plus 7 hours - data will be shifted forward seven hours
                # On upload Aquarius Time Series will shift negative seven hours
                # All time series should have a -7 America/Denver offset
                OffSetTimeZone = timezone('Asia/Bangkok')
                df2['TimeOffSet'] = df2['Time'].dt.tz_convert(OffSetTimeZone)

                # Parse out the Time Values to create an Iso8601 formated string - Crude but works
                # df2['TimeZone'] = df2['Time'].dt.strftime('%SZ')
                df2['TimeZone'] = ".000000Z"  # Manually setting to UTC no time shift
                df2['IsoDateTime'] = df2['TimeOffSet'].dt.strftime('%Y-%m-%dT%H:%M:%S')
                df2['IsoTimeString'] = df2['IsoDateTime'] + df2['TimeZone']

                # Create a dataframewith one record
                DfOneValue = df2.iloc[0]

                #Hit Function defining the logic to be used in the apply below
                TooDictionary(DfOneValue)

                # Apply the logic defined in 'TooDictionary' to all the rows in df2.
                df2['Merged2'] = df2.apply(TooDictionary, axis=1)

                print(type(df2.iloc[0].Merged2))  #Verify is a Dictionary

                # Export the Dictionary Like structure in a Pandas Dataframe to a list for upload to Aquarius
                listToPush = df2['Merged2'].tolist()
                print (type(listToPush))

                try:
                    response = timeseries.acquisition.post('/timeseries/'+timeSeriesId+'/append', json={'Points': listToPush}).json()
                    print(response)
                    messageTime = timeFun()
                    scriptMsg = "Successfully Appended Time Series - " + timeSeriesNameFull + " - AT -" + locationName + " - Append ID is:" + str(response) + " - FileName: " + str(baseName) + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()
                    pass
                except:
                    messageTime = timeFun()
                    scriptMsg = "WARNING - Failed To Process - " + timeSeriesNameFull + " - AT -" + locationName + " - FileName: " + str(baseName) + " - " + messageTime
                    print(scriptMsg)
                    logFile = open(logFileName, "a")
                    logFile.write(scriptMsg + "\n")
                    logFile.close()


        #Next Generation Disconnect
        timeseries.disconnect()

        messageTime = timeFun()
        scriptMsg = "Successfully processed - AquariusNG_Append_DTW_TimeSeriesV3.py - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()


    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - AquariusNG_Append_DTW_TimeSeriesV3.py - " + messageTime
        print ("Exiting Error - AquariusNG_Append_DTW_TimeSeriesV3.py Error\nSee log file " + logFileName + " for more details - " + messageTime)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()

#Function Creates a new DataFrame from the defined Columns/Series in a pre-existing dataframe
def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame

# Function Defines the logic used to define the 'Merge' field - will use the apply function to push to all rows
def TooDictionary(DfOneValue):
    return {'Time': DfOneValue['IsoTimeString'], 'Value': DfOneValue['Value']}

def timeFun():          #Function to Grab Time
    from datetime import datetime
    b=datetime.now()
    messageTime = b.isoformat()
    return messageTime



if __name__ == '__main__':
    main()
