#-------------------------------------------------------------------------------
# Name:        Hullcar Aquifer EMS Data Workflow
#
# Purpose:     This script streamlines the Hullcar Aquifer EMS data processing pipeline by:
#                 (1) Connect to ArcGIS Online (AGO): Establishes a connection to AGO using provided credentials.
#                 (2) Retrieve EMS Data: Fetches EMS sampling data from AGO.
#                 (3) Retrieve CSV URLs: Fetches the direct CSV download URLs for current and historic EMS data from the BC Data Catalog.
#                 (4) Load and Filter Data: Loads the EMS data from the CSV URLs into Pandas DataFrames and filters for specific monitoring locations.
#                 (5) Check for Data Updates: Compares the most recent records from the BC Data Catalog and AGO to check for differences.
#                 (6) Create Master Dataset: Combines current and historic EMS data into a master dataset.
#                 (7) Convert to GeoJSON: Converts the master dataset to GeoJSON format.
#                 (8) Upload to AGO: Uploads the GeoJSON data to AGO and updates the feature layer.
#              
# Input(s):    (1) AGO credentials.                
#
# Author:      Emma Armitage - GeoBC
#
# Created:     2025-02-24
# Updates ongoing - see GitHub for details.
#-------------------------------------------------------------------------------

import requests
import pandas as pd
import geopandas as gpd
from arcgis import GIS
import pytz
from datetime import datetime
import json
import logging
from io import BytesIO, StringIO
import sys
import numpy as np
import os

# CKAN API URL
CKAN_API_URL = os.getenv('CKAN_API_URL')

# Resource ID for the dataset (BC Environmental Monitoring System Results)
RESOURCE_ID_CURRENT = "6aa7f376-a4d3-4fb4-a51c-b4487600d516"      # https://pub.data.gov.bc.ca/datasets/949f2233-9612-4b06-92a9-903e817da659/ems_sample_results_current_expanded.csv
RESOURCE_ID_HISTORIC = '32cc8da0-51ff-4235-9636-f84970e76fa3'      # https://pub.data.gov.bc.ca/datasets/949f2233-9612-4b06-92a9-903e817da659/ems_sample_results_historic_expanded.csv

MONITORING_LOCATION_IDS = ['E333852', 'E333952', 'E333959', 'E301112', 'E206908', 'E319193', 'E317974', 'E317972', 'E319192', 'E317950', 'E319191']
EMS_DATE_COLUMNS = ['COLLECTION_START', 'COLLECTION_END']

URL = os.getenv('MAPHUB_URL')
USERNAME = os.getenv('GSS_ES_AGO_USERNAME')
PASSWORD = os.getenv('GSS_ES_AGO_PASSWORD')

AGO_ITEM_ID = os.getenv('HULLCAR_ITEM_ID')
AGO_ITEM_TITLE = 'Hullcar_EMS_Data'
AGO_FOLDER = 'Hullcar Aquifer'
AGO_GROUP_ID = os.getenv('HULLCAR_GROUP_ID')

now = datetime.today().strftime('%Y-%m-%d %I:%M:%S %p')

def connect_to_ago(URL, USERNAME, PASSWORD):
    """ Returns AGO GIS Connection """
    gis = GIS(url=URL, username=USERNAME, password=PASSWORD)
    logging.info(f"..successfully connect to AGOL as {gis.users.me.username}")

    return gis

def get_ago_data(gis, ago_item_id, date_columns):
    """ Returns AGO sampling data """

    ago_item = gis.content.get(ago_item_id)
    ago_flayer = ago_item.layers[0]
    ago_fset = ago_flayer.query()
    ems_sdf = ago_fset.sdf
    logging.info("..retrived EMS sampling data from AGO")

    pacific_timezone = pytz.timezone('America/Vancouver')

    # Convert date columns to datetime, ensuring they are timezone aware
    for col in date_columns:
        ems_sdf[col] = pd.to_datetime(ems_sdf[col], format='%Y-%m-%d %I:%M:%S %p', errors='coerce')

        # Localize to UTC first if they are naive
        if ems_sdf[col].dt.tz is None:
            ems_sdf[col] = ems_sdf[col].dt.tz_localize('UTC')

        # convert the time to Pacific Time
        ems_sdf[col] = ems_sdf[col].dt.tz_convert(pacific_timezone)
        
    # Fill NaN and NaT values
    ems_sdf = ems_sdf.fillna(np.nan).replace([np.nan], [None])
    logging.info("..cleaned dataframe datetimes and NaN values")

    return ems_sdf

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
    
def check_for_data_updates(df_current, ago_sdf, sort_column):
    """ Compare the most recent record from BCDC and AGO data to check for differences """

    diff = False

    df_current.sort_values(by=sort_column, axis=0, ascending=False, inplace=True)
    ago_sdf.sort_values(by=sort_column, axis=0, ascending=False, inplace=True)

    most_recent_bcdc_record = df_current[sort_column].iloc[0]
    most_recent_ago_record = ago_sdf[sort_column].iloc[0]

    if most_recent_bcdc_record != most_recent_ago_record:
        diff = True
        logging.info("..differences found between AGOL and BCDC data")
    else:
        logging.info("..no new data found in BC Data Catalog")  

    return diff

# function to filter for specific well sampling sites
def filter_ems_sites(df, ems_ids, date_columns):
    """ Filters EMS sites by wells of interest within the Hullcar Aquifer """

    ems_df = df[df['EMS_ID'].isin(ems_ids)]

    # define pacific timezone
    pacific_timezone = pytz.timezone('America/Vancouver')

    # convert date columns to datetime, ensuring they are timezone aware
    for col in date_columns:
        ems_df[col] = pd.to_datetime(ems_df[col], format='%Y%m%d%H%M%S', errors='coerce').dt.tz_localize(pacific_timezone, 
                                                                                  ambiguous='NaT',
                                                                                  nonexistent='shift_forward')
        
    # Fill NaN and NaT values
    ems_df = ems_df.fillna(np.nan).replace([np.nan], [None])

    logging.info("..filtered EMS data to sites of interest")

    return ems_df
    
def create_master_df(df_historic, df_current, load_col_name, today):
    """ Combines current and historic dfs into one dataframe """

    df = pd.concat([df_historic, df_current], axis=0)

    # add GIS_LOAD_DATE column
    df[load_col_name] = today

    logging.info("..successfully combined dataframes")

    return df

def gdf_to_geojson(gdf):
    """
    Converts geodataframe to geojson format for upload to AGOL
    """

    features = []
    for _, row in gdf.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": row['geometry'].__geo_interface__
        }
        for column, value in row.items():
            if column != 'geometry':
                if isinstance(value, (datetime, pd.Timestamp)):
                    feature['properties'][column] = value.isoformat() if not pd.isna(value) else ''
                else:
                    feature['properties'][column] = value
        features.append(feature)
    
    geojson_dict = {
        "type": "FeatureCollection",
        "features": features
    }

    logging.info("..converted geodataframe to geojson format")

    return geojson_dict

# function to convert to geodataframe
def convert_ems_to_geojson(ems_df):
    """ Converts the pandas dataframe to geodataframe then geojson """
    gdf = gpd.GeoDataFrame(ems_df, geometry=gpd.points_from_xy(ems_df.LONGITUDE, ems_df.LATITUDE), crs="EPSG:4326")
    logging.info("..successfully converted to geodataframe")

    # Convert GeoDataFrame to GeoJSON
    geojson_dict = gdf_to_geojson(gdf)  

    return geojson_dict

def upload_to_ago(gis, geojson_dict, title, folder, group):
    """ Uploads the geojson to AGOL """

    try: 
        # Search for existing items (including the GeoJSON file and feature layer)
        existing_items = gis.content.search(f"(title:{title} OR title:data.geojson) AND owner:{gis.users.me.username}")

        # Delete the existing GeoJSON file
        for item in existing_items:
            if item.type == 'GeoJson':
                item.delete(force=True, permanent=True)
                logging.info(f"..existing GeoJSON item '{item.title}' permanently deleted.")

        # Find the existing feature layer
        feature_layer_item = None
        for item in existing_items:
            if item.type == 'Feature Layer':
                feature_layer_item = item
                break

        # Create a new GeoJSON item
        geojson_item_properties = {
            'title': title,
            'type': 'GeoJson',
            'tags': 'sampling points,geojson',
            'description': 'EMS water sampling data for sites within the Clchal/Hullcar water monitoring area',
            'fileName': 'ems_data.geojson'
        }
        geojson_file = BytesIO(json.dumps(geojson_dict).encode('utf-8'))
        # new_geojson_item = gis.content.add(item_properties=geojson_item_properties, data=geojson_file, folder=folder)
        ago_folder = gis.content.folders.get(folder)
        new_geojson_item = ago_folder.add(item_properties=geojson_item_properties, file=geojson_file).result()

        # Update the existing feature layer or create a new one if it doesn't exist
        if feature_layer_item:
            feature_layer_item.update(data=new_geojson_item, folder=folder)
            logging.info(f"..existing feature layer '{title}' updated successfully.")

            item_grp_sharing_mgr = feature_layer_item.sharing.groups
            item_grp_sharing_mgr.add(group=group)
            logging.info(f"..feature layer successfully shared with {group} group.")
        else:
            published_item = new_geojson_item.publish(overwrite=True)
            logging.info(f"..new feature layer '{title}' published successfully.")

            item_grp_sharing_mgr = published_item.sharing.groups
            item_grp_sharing_mgr.add(group=group)
            logging.info(f"..feature layer successfully shared with {group} group.")
            return published_item

    except Exception as e:
        error_message = f"..error publishing/updating feature layer: {str(e)}"
        raise RuntimeError(error_message)

def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    logging.info("Connecting to AGOL")
    gis = connect_to_ago(URL, USERNAME, PASSWORD)

    logging.info("Getting EMS data from AGOL")
    ago_ems_sdf = get_ago_data(gis=gis, ago_item_id=AGO_ITEM_ID, date_columns=EMS_DATE_COLUMNS)

    logging.info("Getting EMS csv url from the BC Data Catalog")
    csv_url_current = get_csv_url(RESOURCE_ID_CURRENT)
    csv_url_historic = get_csv_url(RESOURCE_ID_HISTORIC)
    
    if csv_url_current and csv_url_historic:

        chunk_size = 10000  # Set to an integer (e.g., 10000) for chunked loading

        logging.info("Loading EMS data from BC Data Catalog to pandas dataframe")
        logging.info("..loading EMS current data")
        df_current = load_csv_to_dataframe(csv_url_current, chunk_size)

        logging.info("Filtering current EMS data to points of interest")
        df_current_filter = filter_ems_sites(df_current, ems_ids=MONITORING_LOCATION_IDS, date_columns=EMS_DATE_COLUMNS)

        # check for data updates by comparing the most recent record
        logging.info("Checking for new data")
        diff = check_for_data_updates(df_current=df_current_filter, ago_sdf=ago_ems_sdf, sort_column='COLLECTION_END')

        if diff == True:
            logging.info("No new EMS submissions. Exiting script.")
            sys.exit()

        else:
            logging.info("Loading EMS data from BC Data Catalog to pandas dataframe")
            logging.info("..loading EMS historic data")
            df_historic = load_csv_to_dataframe(csv_url_historic, chunk_size)

            logging.info("Filtering current EMS data to points of interest")
            df_historic_filter = filter_ems_sites(df_historic, ems_ids=MONITORING_LOCATION_IDS, date_columns=EMS_DATE_COLUMNS)
            
            ### remove GIS LOAD DATE - most to concat function
            logging.info("Creating master dataframe")
            ems_df = create_master_df(df_historic=df_historic_filter, df_current=df_current_filter, load_col_name='GIS_LOAD_DATE', today=now)         

            logging.info("Converting EMS data to spatial format")
            geosjon_dict = convert_ems_to_geojson(ems_df)

            logging.info("Uploading EMS data to AGOL")
            upload_to_ago(gis=gis, geojson_dict=geosjon_dict, title=AGO_ITEM_TITLE, folder=AGO_FOLDER, group=AGO_GROUP_ID)   

            logging.info("Script Complete!")                     

    else:
        logging.error("Failed to retrieve CSV file.")

if __name__ == "__main__":
    main()