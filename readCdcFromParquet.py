#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 16:44:59 2019

@author: ubuntubig
"""

def readCdcFromParquet(spark, sc):
    """
    Read all data from the parquet files into Dataframes
    and create temp views
    """
    dfStations = spark.read.parquet("cdcstations_lw.parquet")
    dfStations.createOrReplaceTempView("cdcstations")
    dfStations.cache()

    dfProducts = spark.read.parquet("cdcproducts_lw.parquet")
    dfProducts.createOrReplaceTempView("cdcproducts")
    dfProducts.cache()

    return dfStations, dfProducts
