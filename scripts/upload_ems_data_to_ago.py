#-------------------------------------------------------------------------------
# Name:        Hullcar Aquifer EMS Data Workflow
#
# Purpose:     This script streamlines the Hullcar Aquifer EMS data processing pipeline by:
#                 (1) Connect to ArcGIS Online (AGO): Establishes a connection to AGO using provided credentials.
#                 (2) Retrieve EMS Data: Fetches EMS sampling data from AGO.
#                 (3) Retrieve CSV URLs: Fetches the direct CSV download URLs for current EMS data from the BC Data Catalog.
#                 (4) Load and Filter Data: Loads the EMS data from the CSV URLs into Pandas DataFrames and filters for specific monitoring locations.
#                 (5) Check for Data Updates: Compares the most recent records from the BC Data Catalog and AGO to check for differences.
#                 (6) Convert to dict: Converts the master dataset to dict for upload to AGOL.
#                 (7) Upload to AGO: Appends any new records to AGOL feature layer.
#              
# Input(s):    (1) AGO credentials
#              (2) CKAN API URL              
#
# Author:      Emma Armitage - GeoBC
#
# Created:     2025-02-24
# Updated:     2025-04-11
# Updates ongoing - see GitHub for details.
#-------------------------------------------------------------------------------

import requests
import pandas as pd
import geopandas as gpd
from arcgis import GIS
import pytz
from datetime import datetime, timezone
import logging
import sys
import numpy as np
import os

# CKAN API URL
CKAN_API_URL = 'https://catalogue.data.gov.bc.ca/api/3/action/resource_show'

# Resource ID for the dataset (BC Environmental Monitoring System Results)
RESOURCE_ID_CURRENT = "6aa7f376-a4d3-4fb4-a51c-b4487600d516"      # https://pub.data.gov.bc.ca/datasets/949f2233-9612-4b06-92a9-903e817da659/ems_sample_results_current_expanded.csv
RESOURCE_ID_HISTORIC = '32cc8da0-51ff-4235-9636-f84970e76fa3'     # https://pub.data.gov.bc.ca/datasets/949f2233-9612-4b06-92a9-903e817da659/ems_sample_results_historic_expanded.csv

MONITORING_LOCATION_IDS = ['E333852', 'E333952', 'E333959', 'E301112', 'E206908', 'E319193', 'E317974', 'E317972', 'E319192', 'E317950', 'E319191']
EMS_DATE_COLUMNS = ['COLLECTION_START', 'COLLECTION_END']
MERGE_COLS = ['EMS_ID', 'COLLECTION_END', 'PARAMETER_CODE', 'RESULT']
DROP_COLS = ['_merge', 'OBJECTID', 'SHAPE']

URL = os.getenv('MAPHUB_URL')
USERNAME = os.getenv('AGO_USERNAME')
PASSWORD = os.getenv('AGO_PASSWORD')

AGO_ITEM_ID = '6cb28330305f49fcbc7e81e38f8dccfa'

now_utc = datetime.now(timezone.utc)
now = now_utc.strftime('%Y-%m-%d %I:%M:%S %p')

def standardize_date_format(df, date_columns, localize_target, target_timezone):
    """ Standardize date columns to a consistent timezone and format """

    for col in date_columns:
        # convert to datetime
        df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M%S', errors='coerce')

        # localize naive datetimes
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize(localize_target)
        else:
            df[col] = df[col].dt.tz_convert('UTC')

       # ensure datetime64[ns] precision
        df[col] = df[col].astype('datetime64[ns, UTC]')

        # convert to target timezone
        if target_timezone != 'UTC':
            df[col] = df[col].dt.tz_convert(target_timezone)

    return df

def connect_to_ago(URL, USERNAME, PASSWORD):
    """ Returns AGO GIS Connection """
    gis = GIS(url=URL, username=USERNAME, password=PASSWORD)
    logging.info(f"..successfully connected to AGOL as {gis.users.me.username}")

    return gis

def get_ago_data(gis, ago_item_id, date_columns, query):
    """ Returns AGO sampling data """

    ago_item = gis.content.get(ago_item_id)
    ago_flayer = ago_item.layers[0]
    ago_fset = ago_flayer.query(where=query)
    ems_sdf = ago_fset.sdf
    logging.info("..retrieved EMS sampling data from AGO")

    # convert date columns to datetime, ensuring correct timezone
    ems_sdf = standardize_date_format(ems_sdf, date_columns, localize_target='UTC', target_timezone='UTC')

    # Fill NaN and NaT values
    ems_sdf = ems_sdf.fillna(np.nan).replace([np.nan], [None])
    logging.info("..cleaned dataframe datetimes and NaN values")

    return ems_sdf, ago_flayer

def get_csv_url(resource_id):
    """Fetch the direct CSV download URL from CKAN API."""
    
    response = requests.get(f"{CKAN_API_URL}?id={resource_id}")
    data = response.json()
    
    if data["success"]:
        logging.info(f"..data successfully returned for dataset {data['result']['name']} ")
        return data["result"]["url"]
    else:
        logging.warning("..error retrieving resource info.")
        return None

def load_csv_to_dataframe(csv_url, chunk_size=None):
    """Load the CSV into a Pandas DataFrame, optionally in chunks."""
    
    if not csv_url:
        logging.warning("..no CSV URL provided.")
        return None

    if chunk_size:
        logging.info(f"..loading CSV in chunks of {chunk_size} rows")
        chunks = []
        for chunk in pd.read_csv(csv_url, chunksize=chunk_size):
            chunks.append(chunk)
        return pd.concat(chunks, axis=0)
    else:
        logging.info("..loading full CSV")
        return pd.read_csv(csv_url)

# function to filter for specific well sampling sites
def filter_ems_sites(df, ems_ids, date_columns):
    """ Filters EMS sites by wells of interest within the Hullcar Aquifer """

    ems_df = df[df['EMS_ID'].isin(ems_ids)]

    # define pacific timezone
    pacific_timezone = pytz.timezone('America/Vancouver')

    # convert date columns to datetime, ensuring correct timezone
    ems_df = standardize_date_format(ems_df, date_columns, localize_target=pacific_timezone, target_timezone='UTC')
        
    # Fill NaN and NaT values
    ems_df = ems_df.fillna(np.nan).replace([np.nan], [None])

    logging.info("..filtered EMS data to sites of interest")

    return ems_df

def compare_dataframes(ems_df, ago_ems_sdf, merge_cols):
    """ Finds new records in EMS BCDC data that are not yet loaded to AGOL """

    diff = False

    # Convert datetime columns to naive for merging (if required)
    ems_df['COLLECTION_END'] = ems_df['COLLECTION_END'].dt.tz_localize(None)
    ago_ems_sdf['COLLECTION_END'] = ago_ems_sdf['COLLECTION_END'].dt.tz_localize(None)
        
    # convert ems_id to string, strip whitespace and convert to uppercase
    ems_df['EMS_ID'] = ems_df['EMS_ID'].astype(str).str.strip().str.upper()
    ago_ems_sdf['EMS_ID'] = ago_ems_sdf['EMS_ID'].astype(str).str.strip().str.upper()    

    # convert parameter_code to string, strip whitespace and convert to uppercase
    ems_df['PARAMETER_CODE'] = ems_df['PARAMETER_CODE'].astype(str).str.strip().str.upper()
    ago_ems_sdf['PARAMETER_CODE'] = ago_ems_sdf['PARAMETER_CODE'].astype(str).str.strip().str.upper()

    # convert result to float
    ems_df['RESULT'] = ems_df['RESULT'].astype(float).round(6)
    ago_ems_sdf['RESULT'] = ago_ems_sdf['RESULT'].astype(float).round(6)

    df_merge = pd.merge(left=ems_df, right=ago_ems_sdf, how='outer', indicator=True, on=merge_cols)
    new_ems_records = df_merge[df_merge['_merge'] != 'both']

    if len(new_ems_records) > 0:
        diff = True
        logging.info(f"..{len(new_ems_records)} new records found in BCDC EMS data")
    
    return diff, new_ems_records

def drop_duplicate_columns(new_ems_records, drop_cols, date_columns):
    """ Removes duplicated columns from the merged dataframe """

    for col in new_ems_records.columns:
        if col.endswith('_x'):
            new_col_name = col[:-2]
            new_ems_records.rename(columns={col: new_col_name}, inplace=True)
        if col.endswith('_y') or col in drop_cols: 
            new_ems_records.drop(columns=[col], inplace=True)
    
    return new_ems_records

def convert_ems_to_geojson(ems_df, today):
    """ Converts the pandas dataframe to geodataframe then dictionary for upload to AGOL """

    gdf = gpd.GeoDataFrame(ems_df.copy(), 
                           geometry=gpd.points_from_xy(ems_df.LONGITUDE, ems_df.LATITUDE), 
                           crs="EPSG:4326")
    logging.info("..successfully converted to geodataframe")

    new_features = []

    for idx, row in gdf.iterrows():
        # Convert to plain dictionary (removes references to the parent DataFrame)
        row_dict = row.to_dict()

        geom = row_dict.pop("geometry", None)
        attributes = {}

        for col, val in row_dict.items():
            if isinstance(val, (datetime, pd.Timestamp)):
                attributes[col] = val.isoformat()
            else:
                attributes[col] = val

        # Set GIS_LOAD_DATE to today
        attributes["GIS_LOAD_DATE"] = today

        # Create the feature dictionary
        feature = {
            "attributes": attributes,
            "geometry": {
                "x": geom.x,
                "y": geom.y
            } if geom else {}
        }

        new_features.append(feature)

    logging.info("..converted geodataframe to dict format for upload to AGOL")

    return new_features

def upload_to_ago(ago_flayer, new_features):
    """ Appends the new features to the existing feature layer in AGOL """

    add_result = ago_flayer.edit_features(adds=new_features)

    try:
        add_results = add_result.get('addResults', [])
        if all(res.get('success') for res in add_results):
            logging.info(f"..{len(new_features)} new features added to AGOL feature layer")
        else:
            logging.error("..some features failed to add to AGOL.")
            logging.error(f"..full add_result: {add_result}")
    except Exception as e:
        logging.exception(f"..unexpected error while processing add_result: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    SORT_COLUMN = 'COLLECTION_END'

    logging.info("Connecting to AGOL")
    gis = connect_to_ago(URL, USERNAME, PASSWORD)

    logging.info("Getting EMS csv url from the BC Data Catalog")
    csv_url_current = get_csv_url(RESOURCE_ID_CURRENT)
    
    if csv_url_current:

        chunk_size = 10000  # Set to an integer (e.g., 10000) for chunked loading

        logging.info("Loading EMS data from BC Data Catalog to pandas dataframe")
        logging.info("..loading EMS current data")
        df_current = load_csv_to_dataframe(csv_url_current, chunk_size)

        logging.info("Filtering current EMS data to points of interest")
        ems_df = filter_ems_sites(df_current, ems_ids=MONITORING_LOCATION_IDS, date_columns=EMS_DATE_COLUMNS)

        logging.info("Finding EMS start and end dates")
        start_date = ems_df[SORT_COLUMN].min()
        # format start date to 'YYYY-MM-DD' for AGOL query
        start_date = start_date.strftime('%Y-%m-%d')
        query = f""" COLLECTION_END >= DATE '{start_date}' """

        logging.info("Getting EMS data from AGOL")
        ago_ems_sdf, ago_flayer = get_ago_data(gis=gis, ago_item_id=AGO_ITEM_ID, date_columns=EMS_DATE_COLUMNS, query=query)

        logging.info("Checking for new data")
        diff, new_ems_records = compare_dataframes(ems_df, ago_ems_sdf, merge_cols=MERGE_COLS)

        if diff == False:
            logging.info("No new EMS submissions. Exiting script.")
            sys.exit()

        else:
            logging.info("New EMS data found. Proceeding with data processing.")

            logging.info("Dropping duplicate columns")
            new_ems_records = drop_duplicate_columns(new_ems_records, drop_cols=DROP_COLS, date_columns=EMS_DATE_COLUMNS)

            logging.info("Converting EMS data to spatial format")
            new_features_dict = convert_ems_to_geojson(new_ems_records, today=now)

            logging.info("Adding new EMS data to AGOL feature layer")
            upload_to_ago(ago_flayer, new_features_dict)  

            logging.info("Script Complete!")                     

    else:
        logging.error("Failed to retrieve CSV file.")

if __name__ == "__main__":
    main()