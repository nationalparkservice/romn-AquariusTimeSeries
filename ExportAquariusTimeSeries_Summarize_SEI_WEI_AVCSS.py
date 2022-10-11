# ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.py
# Script processes and exports the defined Aquarius Time Series as defined in the 'timeSeriesList' variable for defined site and time step (i.e. temporal scale of summary).
# Code is designed to process SEI, WEI or AVCSS Data. Sites to be processed are defined in an excel file as defined in the 'siteListFile' parameter.
# The processing time steps includes the following: Raw data values (i.e. no summary), daily, weekly, monthly, or yearly
# Mean values of the raw time step scale are derived for the daily, weekly, monthly and or yearly time steps

# Output will be .csv files with all sites aggregated and exported by defined time step in the 'timeStepList' variable
# Created on: 2021/08/18
# Last Modified: 2021/08/30
# Created by: Kirk Sherrill - Data Manager ROMN IMD
# Version Updates:
# Update 2022/7/7  - Added logic specific to SEI/WEI versus AVCSS. AVCSS processing defines the Site, Aspect and Plot, while SEI and WEI doesn't.
# Update 2022/7/8  - Added function for Weekly Time Step summary.

# Anaconda Environment (Kirk): py37
# Python Version 3.7 Dependices: Requests, pyrfc3339, pytz The Aquarius 'Timeseries_client.py' wrapper class
# (see: Z:\MONITORING\Loggers\Documents\Aquarius\Python\NextGeneration\AquariusTimeSeries) Scripts: timeseries_client.py and setup.py must be in the Python Environment
# - 'Lib\site-packages' directory before the timeseries client can be used to hit the Aquarius REST endpoints

#######################################
# Start of Parameters requiring set up.
#######################################

siteListFile = r'C:\ROMN\Monitoring\Streams\Data\Deliverable\DataPackage\2021\StreamTemperature\Output\SEI_SitesList.xlsx'   #Excel or CSV with the Sites/Locations to be processed
siteListIdentifier = "LocationIdentifier"   #Field name in 'siteListFile' used to define the Site/Location identifier
timeSeriesList = ["Water Temp.Water Temperature (C) HOBO"]  #List defining the time series to be processed
timeStepList = ["Raw","Daily","Weekly","Monthly","Yearly"]    #List defining the time steps to be processed ('Raw'|'Daily'|'Weekly'|'Monthly'|'Yearly')
protocol = "SEI"   #Defines the Protocol Being Processes ('SEI'|'WEI'|'AVCSS')

outFileName = "TemperatureLogger"    #output dataset file name prefix for each exported time step complied across all processed sites.
outDirectory = r'C:\ROMN\Monitoring\Streams\Data\Deliverable\DataPackage\2021\StreamTemperature\Output'      #Output directory
workspace = r'C:\ROMN\Monitoring\Streams\Data\Deliverable\DataPackage\2021\StreamTemperature\Output\workspace'      # Workspace for Processing

outLogFileName = "SEI_Temperature_LoggerProcessing_2021_20220707"
logFileName = workspace + "\\" + outLogFileName + ".LogFile.txt"
###############################

#Import Pacakge/Libraries, etc.
import sys, string, os, glob, traceback, shutil, csv, pytz, ast
import pandas as pd
import requests,  pyrfc3339
from datetime import datetime
from pytz import timezone
import numpy as np


def main():

    try:

        # AQUARIUS Server Connection steps
        server = 'https://aquarius.nps.gov'  # NPS Aquarius Server Name
        loginName = 'AQ_User'  # Aquarius Login Name
        loginPass = 'xxxxxx'  # Aquarius Login Password

        # This is the Aquarius API Wrapper Class - used to hit the Next Generation Aquarius Springboard (20.1.68.0)
        # Downlad the files from: https://github.com/AquaticInformatics/examples/tree/master/TimeSeries/PublicApis/Python
        from timeseries_client import timeseries_client
        # Hit the Aquarius Service
        timeseries = timeseries_client(server, loginName, loginPass)

        # Setup/Define dataframe with sites to be processed
        siteFileBaseName = os.path.basename(siteListFile)
        siteFileSplit = os.path.splitext(siteFileBaseName)
        x = len(siteFileSplit)
        siteFileSuffix = siteFileSplit[x - 1]

        if siteFileSuffix == ".csv":
            siteListDf = pd.read_csv(siteListFile)
        else:  # excel
            siteListDf = pd.read_excel(siteListFile)

        ##############################
        ##############################
        # Routine to Extract Time Series data per site in 'SiteListFile', by defined Time Series in 'timeSeriesList'
        # and by defined time step in 'timeStepList'

        shapeOutput = siteListDf.shape
        rowCount = (shapeOutput[0])
        rowRange = range(0, rowCount)

        rawList = []
        dailyList = []
        weeklyList = []
        monthlyList = []
        yearlyList = []

        for row in rowRange:

            rowValues = siteListDf.iloc[row]
            site = rowValues.get(siteListIdentifier)

            # Create Site Folder
            outDirBySite = os.path.join(outDirectory, site)
            if os.path.exists(outDirBySite):
                pass
            else:
                os.makedirs(outDirBySite)

            # Loop Thru the Time Series's to be processed
            for timeSeries in timeSeriesList:

                # Define the Time Series name at the defined Location
                timeSeriesNameFull = timeSeries + "@" + site

                # Use the API getTimeSeiresUniqueId wrapper
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

                # Pull Time Series data from via Aquarius Publish API - output is a dictionary see: https://aquarius.nps.gov/AQUARIUS/Publish/v2/json/metadata?op=TimeSeriesDataCorrectedServiceRequest
                timeseriesData = timeseries.getTimeSeriesCorrectedData(timeSeriesId)

                # Function To Setup Value Data From Processing
                outVal = setupDateValues(timeseriesData, site, protocol)
                if outVal[0].lower() != "success function":
                    print("WARNING - Function setupDateValues " + str(site) + "-" + str(timeSeries) + " - Failed - Exiting Script")
                    exit()
                else:
                    print("Success - Function setupDateValues " + str(site) + "-" + str(timeSeries))
                    # Assign the reference Data Frame
                    df2 = outVal[1]

                # Function Process Grades
                outVal = gradeValues(timeseriesData, df2)
                if outVal[0].lower() != "success function":
                    print("WARNING - Function gradeValues " + str(site) + "-" + str(timeSeries) + " - Failed - Exiting Script")
                    exit()
                else:
                    print("Success - Function gradeValues " + str(site) + "-" + str(timeSeries))
                    # Assign the reference Data Frame
                    df3 = outVal[1]
                    del df2

                # Function Process Grade Name
                outVal = defineGradeName(df3, protocol)
                if outVal[0].lower() != "success function":
                    print("WARNING - Function defineGradeName " + str(site) + "-" + str(timeSeries) + " - Failed - Exiting Script")
                    exit()
                else:
                    print("Success - Function defineGradeName " + str(site) + "-" + str(timeSeries))
                    # Assign the reference Data Frame
                    df4 = outVal[1]
                    del df3

                # Function Process Approvals
                outVal = approvalValues(timeseriesData, df4)
                if outVal[0].lower() != "success function":
                    print("WARNING - Function approvalValues " + str(site) + "-" + str(timeSeries) + " - Failed - Exiting Script")
                    exit()
                else:
                    print("Success - Function approvalValues " + str(site) + "-" + str(timeSeries))
                    # Assign the reference Data Frame
                    df5 = outVal[1]
                    del df4

                # Function Process Notes
                outVal = noteValues(timeseriesData, df5)
                if outVal[0].lower() != "success function":
                    print("WARNING - Function noteValues " + str(site) + "-" + str(timeSeries) + " - Failed - Exiting Script")
                    #If Notes function fails export the df5 without notes as the Raw Dataset
                    dfRawFinal = df5

                else:
                    print("Success - Function noteValues " + str(site) + "-" + str(timeSeries))
                    # Assign the reference Data Frame - this is the final Raw DataFrame
                    dfRawFinal = outVal[1]
                    del df5

                # Begin Routines to Export by desired time step
                for timeStep in timeStepList:

                    if timeStep.lower() == 'raw':

                        outFull = outDirBySite + "\\" + outFileName + "_" + str(site) + "_" + str(
                            timeSeries) + "_" + str(timeStep) + ".csv"
                        # Export
                        dfRawFinal.to_csv(outFull, index=False)
                        rawList.append(outFull)

                        messageTime = timeFun()
                        scriptMsg = "Successfully Exported Raw File for: " + str(site) + " - " + str(timeSeries) + " - " + str(timeStep) + " - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()


                    elif timeStep.lower() == 'daily':

                        outVal = processDaily(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, dailyList, protocol)
                        outVal0 = str(outVal[0])
                        if outVal0.lower() != "success function":
                            messageTime = timeFun()
                            scriptMsg = "WARNING - Function processDaily " + str(site) + "-" + str(timeSeries) + " - " + timeStep + " - Failed - Exiting Script - " + messageTime
                            print(scriptMsg)
                            logFile = open(logFileName, "a")
                            logFile.write(scriptMsg + "\n")
                            logFile.close()
                            exit()
                        else:
                            messageTime = timeFun()
                            dailyList = outVal[1]
                            print("Success - Exporting: " + str(site) + " - " + str(timeSeries) + " - " + str(
                                timeStep) + " - " + messageTime)
                    elif timeStep.lower() == 'weekly':
                        outVal = processWeekly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, weeklyList, protocol)
                        outVal0 = str(outVal[0])
                        if outVal0.lower() != "success function":
                            messageTime = timeFun()
                            scriptMsg = "WARNING - Function processWeekly " + str(site) + "-" + str(timeSeries) + " - " + timeStep + " - Failed - Exiting Script - " + messageTime
                            print(scriptMsg)
                            logFile = open(logFileName, "a")
                            logFile.write(scriptMsg + "\n")
                            logFile.close()
                            exit()
                        else:
                            messageTime = timeFun()
                            weeklyList = outVal[1]
                            print("Success - Exporting: " + str(site) + " - " + str(timeSeries) + " - " + str(timeStep) + " - " + messageTime)

                    elif timeStep.lower() == 'monthly':
                        outVal = processMonthly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, monthlyList, protocol)
                        outVal0 = str(outVal[0])
                        if outVal0.lower() != "success function":
                            messageTime = timeFun()
                            scriptMsg = "WARNING - Function processMonthly " + str(site) + "-" + str(timeSeries) + " - " + timeStep + " - Failed - Exiting Script - " + messageTime
                            print(scriptMsg)
                            logFile = open(logFileName, "a")
                            logFile.write(scriptMsg + "\n")
                            logFile.close()
                            exit()
                        else:
                            messageTime = timeFun()
                            monthlyList = outVal[1]
                            print("Success - Exporting: " + str(site) + " - " + str(timeSeries) + " - " + str(
                                timeStep) + " - " + messageTime)

                    elif timeStep.lower() == 'yearly':
                        outVal = processYearly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, yearlyList, protocol)
                        outVal0 = str(outVal[0])
                        if outVal0.lower() != "success function":
                            messageTime = timeFun()
                            scriptMsg = "WARNING - Function processYearly " + str(site) + "-" + str(timeSeries) + " - " + timeStep + " - Failed - Exiting Script - " + messageTime
                            print(scriptMsg)
                            logFile = open(logFileName, "a")
                            logFile.write(scriptMsg + "\n")
                            logFile.close()
                            exit()

                        else:
                            messageTime = timeFun()
                            yearlyList = outVal[1]
                            print("Success - Exporting: " + str(site) + " - " + str(timeSeries) + " - " + str(timeStep) + " - " + messageTime)

                    else:

                        print("WARNING - timeStep " + str(timeStep) + " - Not Defined")
                        messageTime = timeFun()
                        scriptMsg = "WARNING - timeStep " + str(timeStep) + " - Not Defined - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()

                # Move on to Next Time Series
                messageTime = timeFun()
                scriptMsg = "Successfully Processed - " + str(site) + " - " + str(timeSeries) + " - " + messageTime
                print(scriptMsg)
                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")
                logFile.close()

        # Loop Thru the Time Series's Lists and append to one file by time step
        for timeStep in timeStepList:
            if timeStep.lower() == 'raw':
                if len(rawList) >= 1:  # Append

                    outVal = appendFiles(rawList, timeStep)
                    if outVal.lower() != "success function":
                        messageTime = timeFun()
                        scriptMsg = "WARNING - Function appendFiles for rawList failed - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()
                    else:
                        messageTime = timeFun()

                        print("Success - Function appendFiles for rawList - " + messageTime)

            elif timeStep.lower() == 'daily':
                # Append if > 1
                if len(dailyList) >= 1:

                    outVal = appendFiles(dailyList, timeStep)
                    if outVal.lower() != "success function":
                        messageTime = timeFun()
                        scriptMsg = "WARNING - Function appendFiles for dailyList failed - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()
                    else:
                        messageTime = timeFun()

                        print("Success - Function appendFiles for dailyList - " + messageTime)

            elif timeStep.lower() == 'weekly':
                # Append if > 1
                if len(monthlyList) >= 1:

                    outVal = appendFiles(weeklyList, timeStep)
                    if outVal.lower() != "success function":
                        messageTime = timeFun()
                        scriptMsg = "WARNING - Function appendFiles for weeklyList failed - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()
                    else:
                        messageTime = timeFun()

                        print("Success - Function appendFiles for rawList - " + messageTime)

            elif timeStep.lower() == 'monthly':
                # Append if > 1
                if len(monthlyList) >= 1:

                    outVal = appendFiles(monthlyList, timeStep)
                    if outVal.lower() != "success function":
                        messageTime = timeFun()
                        scriptMsg = "WARNING - Function appendFiles for monthlyList failed - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()
                    else:
                        messageTime = timeFun()

                        print("Success - Function appendFiles for monthlyList - " + messageTime)

            elif timeStep.lower() == 'yearly':
                # Append if > 1
                if len(yearlyList) >= 1:

                    outVal = appendFiles(yearlyList, timeStep)
                    if outVal.lower() != "success function":
                        messageTime = timeFun()
                        scriptMsg = "WARNING - Function appendFiles for yearlyList failed - " + messageTime
                        print(scriptMsg)
                        logFile = open(logFileName, "a")
                        logFile.write(scriptMsg + "\n")
                        logFile.close()
                    else:
                        messageTime = timeFun()

                        print("Success - Function appendFiles for yearlyList - " + messageTime)

            else:

                print("WARNING - timeStep " + str(timeStep) + " - Not Defined")

        messageTime = timeFun()
        scriptMsg = "Successfully finished processing - ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.ipynb - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.py - " + messageTime
        print("Exiting Error - ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.py Error\nSee log file " + logFileName + " for more details - " + messageTime)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()



def timeFun():          #Function to Grab Time
    from datetime import datetime
    b=datetime.now()
    messageTime = b.isoformat()
    return messageTime


# output: dataframe with Site, Date, UTC, Values for the input time series
def setupDateValues(timeseriesData, site, protocol):
    try:

        # Created dataframe with the 'Points' element from the Aquarius REST call
        df = pd.DataFrame.from_dict(timeseriesData['Points'])
        df.rename(columns={"Values": "ValuesDic"})

        # Define the UTC
        utc = df['Timestamp'].str[-6:][0]

        # Define initial DateTime field - with UTC
        df['DateTime_UTC'] = pd.to_datetime(df['Timestamp'])
        # Create UTC field
        df['Utc'] = utc

        # Create intermediate datetime string field with UTC excluded
        df['DateTimeStr'] = df['DateTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Create DateTime with UTC excluded
        df['DateTime'] = df['DateTimeStr'].astype('datetime64')

        # Redefining as String - not sure why this is necessary
        df['Value'] = df['Value'].astype('str')

        # Crude but striping '{'Numeric': '
        df['ValueStripped'] = df['Value'].str[11:]

        # Redefining as String - not sure why this is necessary
        df['ValueStripped'] = df['ValueStripped'].astype('str')

        # Crude but striping '{'Numeric': '
        df['ValueStripped2'] = df['ValueStripped'].str[:-1]

        # Drop Value field
        df.drop(['Value'], axis=1)

        # Defined Final Value field as float
        df['Value'] = df['ValueStripped2'].astype('float64')

        # Add Site Name
        df['SiteName'] = site

        # Add Park, Summit, and Plot Fields
        siteSplit = site.split("_")
        park = siteSplit[0]

        df['Park'] = park

        if protocol.lower == 'avcss':
            summit = siteSplit[3]
            plot = siteSplit[4]
            df['Summit'] = summit
            df['Plot'] = plot

            #Subset the 'DateTime', and 'UTC' fields
            df2DTV = df[['Park', 'Summit', 'Plot', 'SiteName', 'DateTime', 'Utc', 'Value']]

        else: #SEI/WEI
            # Subset the 'DateTime', and 'UTC' fields
            df2DTV = df[['Park', 'SiteName', 'DateTime', 'Utc', 'Value']]

        #Add Grade Code to df2DTV

        shapeOutput = df2DTV.shape
        lastColumn = int(shapeOutput[1])
        df2DTV.insert(lastColumn, "GradeCode", "")
        # df2DTV.astype({'GradeCode': 'int64'}).dtypes

        # Add Approval Code to df2DTV
        df2DTV.insert(lastColumn + 1, "GradeName", "")

        # Add Approval Code to df2DTV
        df2DTV.insert(lastColumn + 2, "ApprovalCode", "")

        return "success function", df2DTV

    except:

        messageTime = timeFun()
        print("Error on setupDateValues Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'setupDateValues'"


# Process the Aquarius Time Series Grade Values found in the timeseriesData'Grade' variable
# output: dataframe with Grade Values
def gradeValues(timeseriesData, df2DTV):
    try:

        # Create Data From Aquarius 'Grades' service dictionary
        dfGrades = pd.DataFrame.from_dict(timeseriesData['Grades'])
        # Drop First Row this is the from day 1 to first approved value
        dfGrades = dfGrades.iloc[1:, :]
        # Drop Last Row this is the from day 1 to first approved value
        dfGrades = dfGrades.iloc[:-1, :]

        # Convert to dateTime Field
        dfGrades.astype({'StartTime': 'datetime64'}).dtypes
        dfGrades.astype({'EndTime': 'datetime64'}).dtypes

        ###################
        # Routine to define Start Time without UTC and a datetime field
        dfGrades['StartTime_UTC'] = pd.to_datetime(dfGrades['StartTime'])
        # Create intermediate datetime string field with UTC excluded
        dfGrades['StartTimeNoUTC'] = dfGrades['StartTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
        dfGrades.astype({'StartTimeNoUTC': 'datetime64'}).dtypes

        ###################
        # Routine to define End Time without UTC and a datetime field
        dfGrades['EndTime_UTC'] = pd.to_datetime(dfGrades['EndTime'])
        # Create intermediate datetime string field with UTC excluded
        dfGrades['EndTimeNoUTC'] = dfGrades['EndTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
        dfGrades.astype({'EndTimeNoUTC': 'datetime64'}).dtypes

        shapeOutput = dfGrades.shape
        rowCount = (shapeOutput[0])
        rowRange = range(0, rowCount)

        for row in rowRange:
            # Push Row to a panadas series
            rowValues = dfGrades.iloc[row]
            startTime = rowValues.get("StartTimeNoUTC")
            endTime = rowValues.get("EndTimeNoUTC")
            gradeCode = rowValues.get("GradeCode")

            # Assign Grade code in 'df2DTV' dataFrame
            df2DTV['GradeCode'] = np.where((df2DTV['DateTime'] >= startTime) & (df2DTV['DateTime'] <= endTime),
                                           str(gradeCode), df2DTV['GradeCode'])
        return "success function", df2DTV

    except:

        messageTime = timeFun()
        print("Error on setupDateValues Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'setupDateValues'"

    # Defien the Aquarius Data Grade Codes and Names - output dataframe dfGradeDefs

def defineGradeName(df3, protocol):

    try:

        dataGrades = {'GradeCode': [0, 51, 41, 31, 21, 11, 3, 2, 4, 50, 40, 30, 20, 10, 5, 1, -2, -1, 6],
                      'GradeName': ['UNDEF', 'EXCELLENT', 'VERY GOOD', 'GOOD', 'FAIR', 'POOR', 'ICE', 'DRY',
                                    'PARTIAL', 'EST EXCELLENT', 'EST VERY GOOD', 'EST GOOD', 'EST FAIR', 'EST POOR',
                                    'EST NO', 'UNVERIFIED', 'UNUSABLE', 'UNSP', 'SUSPECT']}
        dfGradeDefs = pd.DataFrame(dataGrades)
        # Assign GradeCode String value
        dfGradeDefs['GradeCodeStr'] = dfGradeDefs['GradeCode'].astype(str)

        # Join on df3
        df3_merge = pd.merge(df3, dfGradeDefs, left_on='GradeCode', right_on='GradeCodeStr')

        # Subset the 'SiteName, 'DateTime', 'Utc', 'Value' fields

        if protocol.lower() == 'avcss':

            df4 = df3_merge[['Park', 'Summit', 'Plot', 'SiteName', 'DateTime', 'Utc', 'Value']]
        else: #SEI/WEI
            df4 = df3_merge[['Park', 'SiteName', 'DateTime', 'Utc', 'Value']]



        shapeOutput = df4.shape
        rowCount = (shapeOutput[0])
        lastColumn = int(shapeOutput[1])
        'Push GradeCode and GradeName via insert - not index and rename field name needed'
        df4.insert(lastColumn, "GradeCode", df3_merge['GradeCode_x'])
        df4.insert(lastColumn + 1, "GradeName", df3_merge['GradeName_y'])

        return "success function", df4

    except:

        messageTime = timeFun()
        print("Error on defineGradeTableDef Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineGradeTableDef'"


def approvalValues(timeseriesData, df4):
    try:

        # Create Data From Aquarius 'Grades' service dictionary
        dfApproval = pd.DataFrame.from_dict(timeseriesData['Approvals'])

        ###################
        # Routine to define Start Time without UTC and a datetime field
        dfApproval['StartTime_UTC'] = pd.to_datetime(dfApproval['StartTime'], errors='coerce')
        # Create intermediate datetime string field with UTC excluded
        dfApproval['StartTimeNoUTC'] = dfApproval['StartTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # dfApproval.astype({'StartTimeNoUTC': 'datetime64'}).dtypes
        dfApproval['StartTimeNoUTC'] = pd.to_datetime(dfApproval['StartTimeNoUTC'], errors='coerce')
        ###################
        # Routine to define End Time without UTC and a datetime field
        dfApproval['EndTime_UTC'] = pd.to_datetime(dfApproval['EndTime'], errors='coerce')
        # Create intermediate datetime string field with UTC excluded
        dfApproval['EndTimeNoUTC'] = dfApproval['EndTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # dfApproval.astype({'EndTimeNoUTC': 'datetime64'}).dtypes
        dfApproval['EndTimeNoUTC'] = pd.to_datetime(dfApproval['EndTimeNoUTC'], errors='coerce')

        #####
        # Create StartTime and EndTime Text Fields
        dfApproval['StartTimeNoUTCStr'] = dfApproval['StartTimeNoUTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
        dfApproval['EndTimeNoUTCStr'] = dfApproval['EndTimeNoUTC'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Define Max and Min times in raw data
        minDateTime = df4.DateTime.min()
        maxDateTime = df4.DateTime.max()

        # Add Min and Max date values to 'dfApproval'
        dfApproval['MinDateTimeStr'] = minDateTime
        dfApproval['MaxDateTimeStr'] = maxDateTime

        # Create StartTime and EndTime Text Fields
        dfApproval['MinDateTimeStr'] = dfApproval['MinDateTimeStr'].dt.strftime('%Y-%m-%d %H:%M:%S')
        dfApproval['MaxDateTimeStr'] = dfApproval['MaxDateTimeStr'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Create Stand alone data frame with the StartTime values
        startTimeDf = dfApproval[['ApprovalLevel', 'LevelDescription', 'StartTimeNoUTC']]
        # Infill NAT values in the StartTimeFilled with the minDateTime value
        startTimeDf.fillna(minDateTime, inplace=True)

        # Create Stand alone data frame with the StartTime values
        endTimeDf = dfApproval[['EndTimeNoUTC']]
        # Infill NAT values in the StartTimeFilled with the minDateTime value
        endTimeDf.fillna(maxDateTime, inplace=True)

        # Concatenate (i.e. join) the dfDailyMean, and dfDailySTD dataframes
        dfStartEndTimeInFilled = pd.concat([startTimeDf, endTimeDf], axis=1, join='inner')

        # Add ApprovalCode to df4
        shapeOutput = df4.shape
        lastColumn = int(shapeOutput[1])
        df4.insert(lastColumn, "ApprovalCode", "")

        # Add ApprovalName to df4
        df4.insert(lastColumn + 1, "ApprovalName", "")

        shapeOutput = dfStartEndTimeInFilled.shape
        rowCount = (shapeOutput[0])
        rowRange = range(0, rowCount)

        for row in rowRange:
            # Push Row to a panadas series
            rowValues = dfStartEndTimeInFilled.iloc[row]
            startTime = rowValues.get("StartTimeNoUTC")
            endTime = rowValues.get("EndTimeNoUTC")
            approvalCode = rowValues.get("ApprovalLevel")
            approvalName = rowValues.get("LevelDescription")

            # Assign ApprovalCode
            df4['ApprovalCode'] = np.where((df4['DateTime'] >= startTime) & (df4['DateTime'] <= endTime),
                                           str(approvalCode), df4['ApprovalCode'])

            # Assign ApprovalName
            df4['ApprovalName'] = np.where((df4['DateTime'] >= startTime) & (df4['DateTime'] <= endTime),
                                           str(approvalName), df4['ApprovalName'])

        return "success function", df4

    except:

        messageTime = timeFun()
        print("Error on approvalValues Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'approvalValues'"


# Process the Aquarius Time Series Note Values found in the timeseriesData'Notes' variable
# output: dataframe with Approval Values
def noteValues(timeseriesData, df5):
    try:

        # Create Data From Aquarius 'Grades' service dictionary
        dfNotes = pd.DataFrame.from_dict(timeseriesData['Notes'])

        # If time series has Notes Process
        dfNotesShape = dfNotes.shape
        if dfNotesShape[0] > 0:

            # Convert to dateTime Field
            dfNotes.astype({'StartTime': 'datetime64'}).dtypes
            dfNotes.astype({'EndTime': 'datetime64'}).dtypes

            ###################
            # Routine to define Start Time without UTC and a datetime field
            dfNotes['StartTime_UTC'] = pd.to_datetime(dfNotes['StartTime'])
            # Create intermediate datetime string field with UTC excluded
            dfNotes['StartTimeNoUTC'] = dfNotes['StartTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
            dfNotes.astype({'StartTimeNoUTC': 'datetime64'}).dtypes

            ###################
            # Routine to define End Time without UTC and a datetime field
            dfNotes['EndTime_UTC'] = pd.to_datetime(dfNotes['EndTime'])
            # Create intermediate datetime string field with UTC excluded
            dfNotes['EndTimeNoUTC'] = dfNotes['EndTime_UTC'].dt.strftime('%Y-%m-%d %H:%M:%S')
            dfNotes.astype({'EndTimeNoUTC': 'datetime64'}).dtypes

            # Add ApprovalLevel to df5
            shapeOutput = df5.shape
            lastColumn = int(shapeOutput[1])
            df5.insert(lastColumn, "NoteText", "")

            shapeOutput = dfNotes.shape
            rowCount = (shapeOutput[0])
            rowRange = range(0, rowCount)

            for row in rowRange:
                # Push Row to a panadas series
                rowValues = dfNotes.iloc[row]
                startTime = rowValues.get("StartTimeNoUTC")
                endTime = rowValues.get("EndTimeNoUTC")
                noteTextLU = rowValues.get("NoteText")

                # Assign NoteText
                df5['NoteText'] = np.where((df5['DateTime'] >= startTime) & (df5['DateTime'] <= endTime),
                                           str(noteTextLU), df5['NoteText'])



        else:

            # Add ApprovalLevel field that is null
            shapeOutput = df5.shape
            lastColumn = int(shapeOutput[1])
            df5.insert(lastColumn, "NoteText", "")

        return "success function", df5

    except:

        messageTime = timeFun()
        print("Error on noteValues Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'noteValues'"


# Process Daily Summaries
def processDaily(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, dailyList, protocol):
    try:

        # Create workDataFrame
        dfDailyWork = dfRawFinal[['SiteName', 'DateTime', 'Value']]

        # Set the Row Inde value to 'DataTime' to allow for Time Series calculation, must set inplace=True so copy of 'DateTime is retained for use
        dfDailyWork.set_index(dfDailyWork['DateTime'], inplace=True)

        # Calculate the Daily Mean - output is a series
        dailyMeanSeries = dfDailyWork['Value'].resample(rule='D').mean()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfDailyMean = dailyMeanSeries.to_frame().reset_index()

        dfDailyMean.rename(columns={"Value": "DailyMean", "DateTime": "DateTime"}, inplace=True)

        # Calculate the Standard Deviation - output is a series
        dailySTDSeries = dfDailyWork['Value'].resample(rule='D').std()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfDailySTD = dailySTDSeries.to_frame().reset_index()

        # Rename Columns
        dfDailySTD.rename(columns={"Value": "DailyStandardDev", "DateTime": "DateTimeDropSTD"}, inplace=True)

        # Concatenate (i.e. join) the dfDailyMean, and dfDailySTD dataframes
        dfDailyMeanStd = pd.concat([dfDailyMean, dfDailySTD], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfDailyMeanStd.drop(['DateTimeDropSTD'], axis=1, inplace=True)

        # Calculate the Daily Count - output is a series
        dailyCountSeries = dfDailyWork['Value'].resample(rule='D').count()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfDailyCount = dailyCountSeries.to_frame().reset_index()

        # Rename Columns
        dfDailyCount.rename(columns={"Value": "DailyCount", "DateTime": "DateTimeDrop"}, inplace=True)

        # Concatenate (i.e. join) the dfDailyMeanStd, and defDailyCount dataframes
        dfDailyFinal = pd.concat([dfDailyMeanStd, dfDailyCount], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfDailyFinal.drop(['DateTimeDrop'], axis=1, inplace=True)


        #Add Park, Summit, and Plot Fields
        siteSplit = site.split("_")
        park = siteSplit[0]

        if protocol.lower() == 'avcss':

            summit = siteSplit[3]
            plot = siteSplit[4]

            # Add Park, Summit, Plot and SiteName fields
            dfDailyFinal.insert(0, "Park", park)
            dfDailyFinal.insert(1, "Summit", summit)
            dfDailyFinal.insert(2, "Plot", plot)
            dfDailyFinal.insert(3, "SiteName", site)

        else: #SEI,WEI
            # Add Park, Summit, Plot and SiteName fields
            dfDailyFinal.insert(0, "Park", park)
            dfDailyFinal.insert(1, "SiteName", site)


        outFull = outDirBySite + "\\" + outFileName + "_" + str(site) + "_" + str(timeSeries) + "_" + str(timeStep) + ".csv"

        # Export
        dfDailyFinal.to_csv(outFull, index=False)

        dailyList.append(outFull)

        messageTime = timeFun()
        scriptMsg = "Successfully Exported " + timeStep + "- " + outFull + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return "success function", dailyList

        del dfDailyWork
        del dailyMeanSeries
        del dfDailyMean
        del dfDailyCount


    except:

        messageTime = timeFun()
        print("Error on processDaily Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'processDaily'"

#Process Monthly Summaries
def processWeekly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, weeklyList, protocol):
    try:

        # Create workDataFrame
        dfWeeklyWork = dfRawFinal[['SiteName', 'DateTime', 'Value']]

        # Set the Row Inde value to 'DataTime' to allow for Time Series calculation, must set inplace=True so copy of 'DateTime is retained for use
        dfWeeklyWork.set_index(dfWeeklyWork['DateTime'], inplace=True)

        # Calculate the Daily Mean - output is a series
        WeeklyMeanSeries = dfWeeklyWork['Value'].resample(rule='W').mean()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfWeeklyMean = WeeklyMeanSeries.to_frame().reset_index()

        dfWeeklyMean.rename(columns={"Value": "WeeklyMean", "DateTime": "DateTime"}, inplace=True)

        # Calculate the Standard Deviation - output is a series
        WeeklySTDSeries = dfWeeklyWork['Value'].resample(rule='W').std()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfWeeklySTD = WeeklySTDSeries.to_frame().reset_index()

        # Rename Columns
        dfWeeklySTD.rename(columns={"Value": "WeeklyStandardDev", "DateTime": "DateTimeDropSTD"}, inplace=True)

        # Concatenate (i.e. join) the dfDailyMean, and dfDailySTD dataframes
        dfWeeklyMeanStd = pd.concat([dfWeeklyMean, dfWeeklySTD], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfWeeklyMeanStd.drop(['DateTimeDropSTD'], axis=1, inplace=True)

        # Calculate the Count - output is a series
        WeeklyCountSeries = dfWeeklyWork['Value'].resample(rule='W').count()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfWeeklyCount = WeeklyCountSeries.to_frame().reset_index()

        # Rename Columns
        dfWeeklyCount.rename(columns={"Value": "WeeklyCount", "DateTime": "DateTimeDrop"}, inplace=True)

        # Concatenate (i.e. join) the dfWeeklyMeanStd, and defWeeklyCount dataframes
        dfWeeklyFinal = pd.concat([dfWeeklyMeanStd, dfWeeklyCount], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfWeeklyFinal.drop(['DateTimeDrop'], axis=1, inplace=True)

        # Add Park, Summit, and Plot Fields
        siteSplit = site.split("_")
        park = siteSplit[0]


        if protocol.lower() == 'avcss':

            summit = siteSplit[3]
            plot = siteSplit[4]

            # Add Park, Summit, Plot and SiteName fields
            dfWeeklyFinal.insert(0, "Park", park)
            dfWeeklyFinal.insert(1, "Summit", summit)
            dfWeeklyFinal.insert(2, "Plot", plot)
            dfWeeklyFinal.insert(3, "SiteName", site)

        else: #SEI or WEI
            # Add Park, and SiteName fields
            dfWeeklyFinal.insert(0, "Park", park)
            dfWeeklyFinal.insert(1, "SiteName", site)


        outFull = outDirBySite + "\\" + outFileName + "_" + str(site) + "_" + str(timeSeries) + "_" + str(timeStep) + ".csv"

        # Export
        dfWeeklyFinal.to_csv(outFull, index=False)

        weeklyList.append(outFull)

        messageTime = timeFun()
        scriptMsg = "Successfully Exported " + timeStep + "- " + outFull + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return "success function", weeklyList

        del dfWeeklyWork
        del WeeklyMeanSeries
        del dfWeeklyMean
        del dfWeeklyCount


    except:

        messageTime = timeFun()
        print("Error on processDaily Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'processMonthly'"











#Process Monthly Summaries
def processMonthly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, monthlyList, protocol):
    try:

        # Create workDataFrame
        dfMonthlyWork = dfRawFinal[['SiteName', 'DateTime', 'Value']]

        # Set the Row Inde value to 'DataTime' to allow for Time Series calculation, must set inplace=True so copy of 'DateTime is retained for use
        dfMonthlyWork.set_index(dfMonthlyWork['DateTime'], inplace=True)

        # Calculate the Daily Mean - output is a series
        monthlyMeanSeries = dfMonthlyWork['Value'].resample(rule='M').mean()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfMonthlyMean = monthlyMeanSeries.to_frame().reset_index()

        dfMonthlyMean.rename(columns={"Value": "MonthlyMean", "DateTime": "DateTime"}, inplace=True)

        # Calculate the Standard Deviation - output is a series
        monthlySTDSeries = dfMonthlyWork['Value'].resample(rule='M').std()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfMonthlySTD = monthlySTDSeries.to_frame().reset_index()

        # Rename Columns
        dfMonthlySTD.rename(columns={"Value": "MonthlyStandardDev", "DateTime": "DateTimeDropSTD"}, inplace=True)

        # Concatenate (i.e. join) the dfDailyMean, and dfDailySTD dataframes
        dfMonthlyMeanStd = pd.concat([dfMonthlyMean, dfMonthlySTD], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfMonthlyMeanStd.drop(['DateTimeDropSTD'], axis=1, inplace=True)

        # Calculate the Count - output is a series
        monthlyCountSeries = dfMonthlyWork['Value'].resample(rule='M').count()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfMonthlyCount = monthlyCountSeries.to_frame().reset_index()

        # Rename Columns
        dfMonthlyCount.rename(columns={"Value": "MonthlyCount", "DateTime": "DateTimeDrop"}, inplace=True)

        # Concatenate (i.e. join) the dfMOnthlyMeanStd, and defMonthlyCount dataframes
        dfMonthlyFinal = pd.concat([dfMonthlyMeanStd, dfMonthlyCount], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfMonthlyFinal.drop(['DateTimeDrop'], axis=1, inplace=True)

        # Add Park, Summit, and Plot Fields
        siteSplit = site.split("_")
        park = siteSplit[0]


        if protocol.lower() == 'avcss':

            summit = siteSplit[3]
            plot = siteSplit[4]

            # Add Park, Summit, Plot and SiteName fields
            dfMonthlyFinal.insert(0, "Park", park)
            dfMonthlyFinal.insert(1, "Summit", summit)
            dfMonthlyFinal.insert(2, "Plot", plot)
            dfMonthlyFinal.insert(3, "SiteName", site)

        else: #SEI or WEI
            # Add Park, and SiteName fields
            dfMonthlyFinal.insert(0, "Park", park)
            dfMonthlyFinal.insert(1, "SiteName", site)


        outFull = outDirBySite + "\\" + outFileName + "_" + str(site) + "_" + str(timeSeries) + "_" + str(timeStep) + ".csv"

        # Export
        dfMonthlyFinal.to_csv(outFull, index=False)

        monthlyList.append(outFull)

        messageTime = timeFun()
        scriptMsg = "Successfully Exported " + timeStep + "- " + outFull + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return "success function", monthlyList

        del dfMonthlyWork
        del monthlyMeanSeries
        del dfMonthlyMean
        del dfMonthlyCount


    except:

        messageTime = timeFun()
        print("Error on processDaily Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'processMonthly'"


# Process Yearly Summaries
def processYearly(dfRawFinal, outDirBySite, site, timeSeries, outFileName, timeStep, yearlyList, protocol):
    try:

        # Create workDataFrame
        dfYearlyWork = dfRawFinal[['SiteName', 'DateTime', 'Value']]

        # Set the Row Inde value to 'DataTime' to allow for Time Series calculation, must set inplace=True so copy of 'DateTime is retained for use
        dfYearlyWork.set_index(dfYearlyWork['DateTime'], inplace=True)

        # Calculate the Daily Mean - output is a series
        yearlyMeanSeries = dfYearlyWork['Value'].resample(rule='AS').mean()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfYearlyMean = yearlyMeanSeries.to_frame().reset_index()

        dfYearlyMean.rename(columns={"Value": "YearlyMean", "DateTime": "DateTime"}, inplace=True)

        # Calculate the Standard Deviation - output is a series
        yearlySTDSeries = dfYearlyWork['Value'].resample(rule='AS').std()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfYearlySTD = yearlySTDSeries.to_frame().reset_index()

        # Rename Columns
        dfYearlySTD.rename(columns={"Value": "YearlyStandardDev", "DateTime": "DateTimeDropSTD"}, inplace=True)

        # Concatenate (i.e. join) the dfDailyMean, and dfDailySTD dataframes
        dfYearlyMeanStd = pd.concat([dfYearlyMean, dfYearlySTD], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfYearlyMeanStd.drop(['DateTimeDropSTD'], axis=1, inplace=True)

        # Calculate the Count - output is a series
        yearlyCountSeries = dfYearlyWork['Value'].resample(rule='AS').count()

        # Export dataseries to a dataframe - reset_index so both columns are retained
        dfYearlyCount = yearlyCountSeries.to_frame().reset_index()

        # Rename Columns
        dfYearlyCount.rename(columns={"Value": "YearlyCount", "DateTime": "DateTimeDrop"}, inplace=True)

        # Concatenate (i.e. join) the dfYearlyMeanStd, and defYearlyCount dataframes
        dfYearlyFinal = pd.concat([dfYearlyMeanStd, dfYearlyCount], axis=1, join='inner')

        # Drop duplicate 'DateTime' field
        dfYearlyFinal.drop(['DateTimeDrop'], axis=1, inplace=True)

        # Add Park, Summit, and Plot Fields
        siteSplit = site.split("_")
        park = siteSplit[0]


        if protocol.lower() == 'avcss':

            summit = siteSplit[3]
            plot = siteSplit[4]

            #Add Park, Summit, Plot and SiteName fields
            dfYearlyFinal.insert(0, "Park", park)
            dfYearlyFinal.insert(1, "Summit", summit)
            dfYearlyFinal.insert(2, "Plot", plot)
            dfYearlyFinal.insert(3, "SiteName", site)

        else:#SEI or WEI

            # Add Park, Summit, Plot and SiteName fields
            dfYearlyFinal.insert(0, "Park", park)
            dfYearlyFinal.insert(1, "SiteName", site)

        outFull = outDirBySite + "\\" + outFileName + "_" + str(site) + "_" + str(timeSeries) + "_" + str(timeStep) + ".csv"

        # Export
        dfYearlyFinal.to_csv(outFull, index=False)

        yearlyList.append(outFull)

        messageTime = timeFun()
        scriptMsg = "Successfully Exported " + timeStep + "- " + outFull + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return "success function", yearlyList

        del dfYearlyWork
        del yearlyMeanSeries
        del dfYearlyMean
        del dfYearlyCount


    except:

        messageTime = timeFun()
        print("Error on processDaily Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'processYearly'"


# Append Files in list to .csv file
def appendFiles(appendList, timeStep):
    try:

        for count, file in enumerate(appendList):

            dfLoop = pd.read_csv(file)
            if count == 0:  # Make new dfallFiles

                dfallFiles = dfLoop

            # Append dfLoop to dfallFiles
            else:

                dfallFiles = dfallFiles.append(dfLoop, ignore_index=True, verify_integrity=True)

            del dfLoop

        # Define Export .csv file
        outFull = outDirectory + "\\" + outFileName + "_AllSites_" + str(timeStep) + ".csv"

        # Export
        dfallFiles.to_csv(outFull, index=False)

        return "Success function"

    except:

        messageTime = timeFun()
        print("Error on appendFiles Function ")
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'appendFiles'"





if __name__ == '__main__':
    main()
