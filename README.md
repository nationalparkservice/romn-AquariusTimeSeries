# romn-AquariusTimeSeries
This github repository contains NPS IMD Rocky Mountain Network scripts used to manage continuous time series data in the Aquarius platform.


## ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS.py
Script exports the defined Aquarius Time Series as defined in the 'timeSeriesList' variable for defined site(s) and time step(s) (i.e. temporal scale of summary).
Code has been defined specifically to process Rocky Mountain Network Streams, Wetlands and Alpine Vegetation site/location time series data in the NPS Water Resource Divisions Aquarius System. Sites/locations to be processed are defined in an excel file which is defined in the 'siteListFile' parameter.

Processing time steps include: Raw date/time (i.e. no summary), daily, weekly, monthly, or yearly.
Mean values of the raw time step scale are derived for the daily, weekly, monthly and or yearly time periods.

**SitesListExample.xls** Example Excel file define the site/locations, identifier, parameter, unit, utcOffset and lable information used in processing.

**timeseries_client.zip** Zip file with the Aquarius API wrapper python scripts required to connect with Aquarius.

Scripts include the **setup.py** and **timeseries_client.py**. 
- Copy these files into your Python Environment \Lib\site-packages directory.  This will allow the AquariusTimeSeries.py script to be accessiable in the Python Environment.
