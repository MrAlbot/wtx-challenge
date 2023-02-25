import os
import re

import geopandas
from pyspark.sql import *
from pyspark.sql.functions import *


def enrich_ports_with_LatLon():
    # this function is supposed to enrich ALL the ports with latitudes in the future, but this isn't the case
    # for now since our information is limited by using just one scraping source.
    # on the readme file there have been identified several others that can compliment this dataset
    spark = SparkSession.builder.getOrCreate()

    pub150FileName = 'resources/UpdatedPub150.csv'
    scrapedPortsFileName = 'csv/port_info.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe 
    df_latlon = spark.read.option("header", True).option("sep", ',').csv(pub150FileName)
    scrapedPorts = spark.read.option("header", True).option("sep", ',').csv(scrapedPortsFileName).withColumn("Port ID",
                                                                                                             trim(
                                                                                                                 col("Port ID")))
    # here I'm adapting the df_latlon dataset so that we can use UN/LOCODE as a key
    # since there are sometimes two entries for each LOCODE specifying the HarborType as Coast or River
    # there are also some entries without LOCODE that were filtered out
    df_latlon_filtered = df_latlon.withColumn("UN/LOCODE", regexp_replace("UN/LOCODE", " ", "")). \
        filter((col('UN/LOCODE') != "") & (col("Harbor Size") != "Very Small") & (col('Harbor Type').contains("Coast")))

    portInfoDf = scrapedPorts.filter(col("Port ID") != "null").join(df_latlon_filtered,
                                                                    trim(scrapedPorts["Port ID"]) == trim(
                                                                        df_latlon_filtered["UN/LOCODE"]), 'left_outer'). \
        select(scrapedPorts['*'], df_latlon_filtered['Latitude'], df_latlon_filtered['Longitude'])

    ports_with_latlon = portInfoDf. \
        select(col("Port ID"), col("Latitude"), col("Longitude")). \
        filter(trim(col("Latitude")) != "null")

    outputFilePath = "csv/ports_with_latlon"
    ports_with_latlon.write. \
        mode("overwrite"). \
        option("header", True). \
        option("sep", ';'). \
        csv(outputFilePath)

    dir_src = "csv/ports_with_latlon/"
    delete_dir = "csv/ports_with_latlon"
    dir_dst = ""
    new_name = "csv/ports_with_latlon.csv"
    for file in os.listdir(dir_src):
        if re.search(r".*.csv$", file):
            src_file = os.path.join(dir_src, file)
            dst_file = os.path.join(dir_dst, new_name)
            print(dir_dst + new_name)
            print(file)

            os.rename(src_file, dst_file)
            # not working this chmod ... need to review
            os.chmod(delete_dir, 0o777)
            try:
                os.remove(delete_dir)
            except OSError as e:
                print("Error: %s : %s" % (delete_dir, e.strerror))

    # use other resources (HERE API) to enrich further the missing Latitude and Longitude coordinates of the ports
    # https://reverse.geocoder.ls.hereapi.com/6.2/multi-reversegeocode.json?
    # languages=en-US
    # &apiKey=---
    # &gen=9
    # &locationattributes=tz
    # &level=city
    # &mode=retrieveAreas
    # &prox=41.316667,19.45,300000
    # &app_id=---


def nearbyCities():
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point

    # radius of the circle arround the points (TODO:review crs https://shapely.readthedocs.io/en/latest/manual.html#object.buffer)

    # read in the seaport csv as a GeoDataFrame circle with radius 300Km
    seaports = pd.read_csv('csv/ports_with_latlon.csv', delimiter=';')

    # 1) create a Point geometry column from the latitude and longitude columns and create a GeoDataframe
    # 2) buffer the points into a circle with radius "buffer_size"
    buffer_size = 10
    seaports_gdf = gpd. \
        GeoDataFrame(geometry=gpd.points_from_xy(seaports.Longitude, seaports.Latitude, crs="EPSG:4326").
                     buffer(buffer_size), data=seaports)

    # read in a cities dataset as a GeoDataFrame
    cities = gpd.read_file(gpd.datasets.get_path("naturalearth_cities"))

    # perform a spatial join between the seaports circles and cities GeoDataFrames
    joined = gpd.sjoin(seaports_gdf, cities, 'left')

    # group the cities by seaport ID
    grouped = joined.groupby(['Port ID'])['name'].apply(list).reset_index()

    outputFilePath = "csv/nearby_cities.csv"
    grouped[["Port ID", "name"]].to_csv(outputFilePath, ';', header=True, index=False)


def enrich_ports_with_nearby_cities():
    spark = SparkSession.builder.getOrCreate()
    nearbyCitiesFileName = 'csv/nearby_cities.csv'
    portsInfoFileName = 'csv/port_info.csv'

    # load both nearbyCities and portsInfo dataframes 
    nearbyCities_df = spark.read.option("header", True).option("sep", ';').csv(nearbyCitiesFileName)
    portsInfo_df = spark.read.option("header", True).option("sep", ',').csv(portsInfoFileName)

    # join both dataframes to add the nearbyCities list to the PortsInfo Dataset
    ports_with_cities = portsInfo_df. \
        join(nearbyCities_df, portsInfo_df["Port ID"] == nearbyCities_df["Port ID"], 'left'). \
        select(portsInfo_df["*"], nearbyCities_df["name"].alias("Nearby Cities"))

    ports_with_cities.show(10)

    outputFilePath = "csv/port_info_final"
    ports_with_cities.write. \
        mode("overwrite"). \
        option("header", True). \
        option("sep", ';'). \
        csv(outputFilePath)

    dir_src = "csv/port_info_final/"
    delete_dir = "csv/port_info_final"
    dir_dst = ""
    new_name = "csv/port_info_final.csv"
    for file in os.listdir(dir_src):
        if re.search(r".*.csv$", file):
            src_file = os.path.join(dir_src, file)
            dst_file = os.path.join(dir_dst, new_name)
            print(dir_dst + new_name)
            print(file)

            os.rename(src_file, dst_file)
            os.chmod(delete_dir, 0o777)
            try:
                # for some reason this is not deleting the folder, but it should... specially after the chmmod...
                os.remove(delete_dir)
            except OSError as e:
                print("Error: %s : %s" % (delete_dir, e.strerror))
