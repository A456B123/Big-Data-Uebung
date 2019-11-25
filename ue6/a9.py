#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 20:31:11 2019

@author: ubuntubig
"""
import datetime as dt
from os import walk
from subprocess import call

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType

print('---------------AUFGABE 9---------------')

# a)
print('\n######## a) ########')
def convert_to_date(date_str):
    date = dt.datetime.strptime(date_str, "%Y%m%d")
    date = date.date()
    return date

def import_cdc_stations(spark):
    stationLines = sc.textFile('file:///home/ubuntubig/cdc/txt/TU_Stundenwerte_Beschreibung_Stationen_ohne_Kopf.txt')
    stationSplitLines = stationLines.map(
        lambda l:
        (l[0:5],
         convert_to_date(l[6:14]),
         convert_to_date(l[15:23]),
         int(l[24:38].strip()),
         float(l[39:50].strip()),
         float(l[51:60].strip()),
         l[61:101].strip(),
         l[102:201].strip()
         ))
    stationSchema = StructType([
        StructField('stationid',        StringType(),   True),
        StructField('vonDatum',         DateType(),     True),
        StructField('bisDatum',         DateType(),     True),
        StructField('stationshoehe',    IntegerType(),  True),
        StructField('geoBreite',        FloatType(),    True),
        StructField('geoLaenge',        FloatType(),    True),
        StructField('stationsname',     StringType(),   True),
        StructField('bundesland',       StringType(),   True),
    ])
    stationFrame = spark.createDataFrame(stationSplitLines,
                                         schema=stationSchema)
    stationFrame.createOrReplaceTempView("cdcstations")
    stationFrame.write.parquet("cdcstations_lw.parquet")
    stationFrame.cache()
    return stationFrame


# b)
print('\n######## b) ########')
def transform_product_data(line):
    line = [word.strip() for word in line.split(';')]
    date_with_hour = dt.datetime.strptime(line[1], '%Y%m%d%H')
    date = date_with_hour.date()
    hour = date_with_hour.hour

    transformation = (
        line[0],
        date,
        hour,
        int(line[2]),
        float(line[3]),
        float(line[4]),
        line[5]
    )
    return transformation

def import_product_data(spark, file):
    productLines = sc.textFile('file:///home/ubuntubig/cdc/txt/' + file)
    first_row = productLines.first()
    productLines = productLines.filter(lambda row: row != first_row)
    productSplitLines = productLines.map(lambda l: transform_product_data(l))

    productSchema = StructType([
        StructField('stationid',    StringType(),   True),
        StructField('mess_datum',   DateType(),     True),
        StructField('mess_stunde',  IntegerType(),  True),
        StructField('qn_9',         IntegerType(),  True),
        StructField('TT_TU',        FloatType(),    True),
        StructField('RF_TU',        FloatType(),    True),
        StructField('eor',          StringType(),   True)
    ])
    productFrame = spark.createDataFrame(productSplitLines,
                                         schema=productSchema)
    productFrame.createOrReplaceTempView("cdcproducts")
    productFrame.write.mode('append').parquet("cdcproducts_lw.parquet")
    productFrame.cache()
    return productFrame



# c)
print('\n######## c) ########')
def import_all_products_from_filesystem(spark):
    path = '/home/ubuntubig/cdc/txt/'
    files = []
    for (dirpath, dirnames, filenames) in walk(path):
        files.extend(filenames)
        break
    product_files = [file for file in files if 'produkt' in file]
    for file in product_files:
        import_product_data(spark, file)

#call("cd; cd hadoop-2.7.3; ./bin/hadoop fs -rm -R \
#         cdcstations_lw.parquet", shell=True)
#call("cd; cd hadoop-2.7.3; ./bin/hadoop fs -rm -R \
#         cdcproducts_lw.parquet", shell=True)