# romn-AquariusTimeSeries
This github repository contains NPS IMD Rocky Mountain Network scripts used to manage time series data in the Aquarius platform.


## ExportAquariusTimeSeries_Summarize_SEI_WEI_AVCSS
Script processes and exports the defined Aquarius Time Series as defined in the 'timeSeriesList' variable for defined site and time step (i.e. temporal scale of summary).
Code is intended to process Rocky Mountain Network Streams, Wetlands and Alpine Vegetation site/location time series data on Aquarius.  Sites/locations to be processed are defined in an excel file as defined in the 'siteListFile' parameter.

Processing time steps include: Raw date/time (i.e. no summary), daily, weekly, monthly, or yearly.
Mean values of the raw time step scale are derived for the daily, weekly, monthly and or yearly time steps.
