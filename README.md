# romn-AquariusTimeSeries
This github repository contains NPS IMD Rocky Mountain Network scripts used to manage continuous time series data in the NPS Water Resource Divisions Aquarius Time Series platform.


## ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.py
Script exports the defined Aquarius Time Series as defined in the 'timeSeriesList' variable for defined site(s) and time step(s) (i.e. temporal scale of summary) using the Aquarius API time series function, see https://aquarius.nps.gov/AQUARIUS/Publish/v2/docs/reference.html.
Code has been defined specifically to process Rocky Mountain Network Streams, Wetlands and Alpine Vegetation site/location time series data in the NPS Water Resource Divisions Aquarius System. Sites/locations to be processed are defined in an excel file which is defined in the 'siteListFile' parameter.

Processing time steps include: Raw date/time (i.e. no summary), daily, weekly, monthly, or yearly.
Mean values of the raw time step scale are derived for the daily, weekly, monthly and or yearly time periods.

**SitesListExample.xls** Example Excel file define the site/locations, identifier, parameter, unit, utcOffset and lable information used in processing.

**timeseries_client.zip** Zip file with the Aquarius API wrapper python scripts required to connect with Aquarius.

Files in zip include the **setup.py** and **timeseries_client.py**. 
- Copy these files into your Python Environment \Lib\site-packages directory.  This will allow the AquariusTimeSeries.py script to be accessiable in the Python Environment.

## Append_DTW_TimeSeries.py
Script appends continuous time series data to defined sites in Aquarius using the Aquarius API timeseries.acquistion.post function, see https://aquarius.nps.gov/AQUARIUS/Acquisition/v2/docs/reference.html. The continuous data in a .csv file is appended to the defined time series(s) in the timeSeriesLoop' parameter.  Processing logic harvests all .csv files in the downstream directories of the input root directory. CSV files being processed must have the site name prefix which is used to define the location in Aquarius at which the time series data will be uploaded to (e.g. FLFO_705_FLFO_705_2020_1_Hourly_20220412.csv where {SiteName_EventName_OutputScale_DateofOutput_.csv}).

