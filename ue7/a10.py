#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 16:48:10 2019

@author: ubuntubig
"""

import matplotlib.pyplot as plt

print('---------------AUFGABE 10---------------')

# a)
print('\n######## a) ########')
def station_scatter(spark):
    latlon = spark.sql('SELECT geoBreite, geoLaenge FROM cdcstations').collect()
    x = [row[1] for row in latlon]
    y = [row[0] for row in latlon]
    plt.scatter(x, y)
    plt.title('Stationen in Deutschland (CDC)')
    plt.show()


# b)
print('\n######## b) ########')
def station_scatter_weighted(spark):
    weighted = spark.sql('SELECT geoBreite, geoLaenge, (YEAR(bisDatum) - YEAR(vonDatum)) as yearDiff \
                         FROM cdcstations').collect()

    x = [row[1] for row in weighted]
    y = [row[0] for row in weighted]
    s = [row[2] for row in weighted]
    plt.scatter(x, y, s=s)
    plt.title('Stationen in Deutschland (CDC), gewichtet')
    plt.show()


# c)
print('\n######## c) ########')
def frosttage(spark):
#    frosttage = spark.sql('SELECT solo.stationid, p.jahr, p.frosttage \
#                          FROM cdcproducts solo \
#                          LEFT OUTER JOIN \
#                            (SELECT stationid, YEAR(mess_datum) AS jahr, COUNT(stationid) AS frosttage \
#                            FROM ( \
#        	                  SELECT stationid, mess_datum, MAX(TT_TU) AS temp_max \
#                                FROM cdcproducts \
#                                GROUP BY mess_datum, stationid) \
#                            WHERE temp_max < 0 \
#                            GROUP BY stationid, year(mess_datum)) as p \
#                          ON solo.stationid = p.stationid \
#                          GROUP BY solo.stationid, p.jahr, p.frosttage \
#                          ORDER BY solo.stationid, p.jahr')
#
#    frosttage.createOrReplaceTempView('frosttage')
#    frosttage.cache()

    tempMaxTBL = spark.sql("SELECT stationid, mess_datum, \
                            Max(TT_TU) as maxtemp\
                            FROM cdcproducts\
                            GROUP BY mess_datum, stationid\
                            ORDER BY stationid, mess_datum")

    tempMaxTBL.createOrReplaceTempView("maxtempstation")

    frostStations = spark.sql("SELECT stationid, year(mess_datum) as jahr, \
               CASE WHEN MIN(maxtemp) >= 0 \
               THEN 0 \
               ELSE COUNT(CASE WHEN maxtemp < 0 THEN 1 END) \
               END as frosttage \
               FROM maxtempstation\
              GROUP BY stationid, year(mess_datum) \
              ORDER BY stationid, year(mess_datum)")

    frostStations.createOrReplaceTempView("froststationen")


    return frostStations


# d)
print('\n######## d) ########')
def plot_frosttage(spark, year):
    frosttage = spark.sql('SELECT Count(stationid) as cnt, frosttage \
                          FROM froststationen \
                          WHERE jahr = {} \
                          GROUP BY frosttage'.format(year)).collect()

    x = [row[1] for row in frosttage]
    y = [row[0] for row in frosttage]
    plt.bar(x, y)
    plt.xlabel('Anzahl Frosttage')
    plt.ylabel('Anzahl Stationen')
    plt.title('Anzahl Stationen mit Anzahl an Frosttagen in Deutschland (CDC) im Jahr {}'.format(year))
    plt.show()

# e)
print('\n######## e) ########')
def plot_corr(spark):
    dfCorr = spark.sql('SELECT f.jahr, CORR(s.stationshoehe, f.frosttage) as correlation \
                       FROM cdcstations s \
                       INNER JOIN froststationen f \
                       ON s.stationid = f.stationid \
                       GROUP BY f.jahr \
                       ORDER BY f.jahr')

    # easier with pandas dataframe!
    pd_corr = dfCorr.toPandas()
    pd_corr = pd_corr.dropna()
    pd_corr.plot.bar(x='jahr', y='correlation', rot=0)
#    plt.bar(pd_corr.jahr, pd_corr.correlation)
#    plt.xlabel('Jahre')
#    plt.ylabel('Korrelation')
#    plt.title('Korrelation zwischen Stationshöhe und Frosttagen')
#    plt.show()


