#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 14:16:32 2019

@author: ubuntubig
"""

import matplotlib.pyplot as plt

print('---------------AUFGABE 11---------------')

# a)
print('\n######## a) ########')
def temperature_hierarchy(spark, station_name):
    stmt = spark.sql('SELECT MAX(p.TT_TU) AS max_temp, MIN(p.TT_TU) AS min_temp, AVG(p.TT_TU) AS avg_temp, \
                     p.mess_stunde, \
                     YEAR(p.mess_datum)*366+DAYOFYEAR(p.mess_datum) AS day, \
                     YEAR(p.mess_datum)*12+MONTH(p.mess_datum) AS month, \
                     YEAR(p.mess_datum)*4+QUARTER(p.mess_datum) AS quarter, \
                     YEAR(p.mess_datum) as year \
                     FROM cdcstations s \
                     JOIN cdcproducts p \
                     ON s.stationid = p.stationid \
                     WHERE s.stationsname LIKE "{}%" \
                     GROUP BY GROUPING SETS(p.mess_stunde, \
                                            YEAR(p.mess_datum)*366+DAYOFYEAR(p.mess_datum), \
                                            YEAR(p.mess_datum)*12+MONTH(p.mess_datum), \
                                            YEAR(p.mess_datum)*4+QUARTER(p.mess_datum),\
                                            YEAR(p.mess_datum))'.format(station_name))
    stmt.show()
    stmt.createOrReplaceTempView('temperatureHierarchy')
    stmt.cache()


def plot_temp_hierarchy_month(spark, year, span):
    begin = year * 12
    end = (year + span) * 12

    stmt = spark.sql('SELECT max_temp, min_temp, avg_temp, month \
                     FROM temperatureHierarchy \
                     WHERE month IS NOT NULL  \
                        AND month >= {} \
                        AND month <= {} \
                        AND max_temp != -999 \
                        AND min_temp != -999 \
                        AND avg_temp != -999 \
                     ORDER BY month ASC'.format(begin, end))
    stmt.show()
    df = stmt.toPandas()

    fig, ax = plt.subplots()
    ax.plot(df.month, df.max_temp, 'r', label='Max Temp')
    ax.plot(df.month, df.min_temp, 'g', label='Min Temp')
    ax.plot(df.month, df.avg_temp, 'b', label='Avg Temp')
    ax.legend(loc='lower center')
    plt.title('Tägliche Werte von {}'.format(year))
    plt.xlabel('Month')
    plt.ylabel("Temperature")
    plt.show()


def plot_temp_hierarchy_day(spark, year):
    begin = year * 366
    end = year * 366 + 366

    stmt = spark.sql('SELECT max_temp, min_temp, avg_temp, day \
                     FROM temperatureHierarchy \
                     WHERE day IS NOT NULL \
                        AND day >= {} \
                        AND day <= {} \
                        AND max_temp != -999 \
                        AND min_temp != -999 \
                        AND avg_temp != -999 \
                     ORDER BY day ASC'.format(begin, end))
    stmt.show()
    df = stmt.toPandas()

    fig, ax = plt.subplots()
    ax.plot(df.day, df.max_temp, 'r', label='Max Temp')
    ax.plot(df.day, df.min_temp, 'b', label='Min Temp')
    ax.plot(df.day, df.avg_temp, 'g', label='Avg Temp')
    ax.legend(loc='lower center')
    plt.title('Tägliche Werte von {}'.format(year))
    plt.xlabel('Day')
    plt.ylabel('Temperature')
    plt.show()

temperature_hierarchy(spark, 'Kempten')
plot_temp_hierarchy_month(spark, 1990, 20)
plot_temp_hierarchy_day(spark, 2015)


# b)
print('\n######## b) ########')
def station_rank(spark):
    stmt_load = spark.sql('SELECT s.stationsname, MIN(p.TT_TU) AS min_temp, AVG(p.TT_TU) AS avg_temp, \
                          MONTH(p.mess_datum) AS month, \
                          YEAR(p.mess_datum) as year \
                          FROM cdcstations s \
                          JOIN cdcproducts p \
                          ON s.stationid = p.stationid \
                          GROUP BY stationsname, year, month')

    stmt_load.show()
    stmt_load.createOrReplaceTempView('stationRankLoad')
    stmt_load.cache()


    stmt_final = spark.sql('SELECT stationsname, year, month, min_temp, avg_temp, \
                           RANK() OVER (PARTITION BY year ORDER BY min_temp ASC) AS rank_min_temp, \
                           RANK() OVER (PARTITION BY year ORDER BY avg_temp ASC) AS rank_avg_temp \
                           FROM stationRankLoad \
                           WHERE year IS NOT NULL AND \
                                month IS NOT NULL AND \
                                min_temp != -999 AND \
                                avg_temp != -999 \
                            GROUP BY stationsname, year, month, min_temp, avg_temp \
                            ORDER BY year, rank_min_temp')
    stmt_final.show()
    stmt_final.createOrReplaceTempView('stationRank')
    stmt_final.cache()

station_rank(spark)


def station_rank_2015(spark):
    stmt = spark.sql('SELECT stationsname, month, min_temp, rank_min_temp \
                     FROM stationRank \
                     WHERE year = 2015 \
                     ORDER BY rank_min_temp, month ASC')
    stmt.show()

station_rank_2015(spark)


def station_rank_coldest_avg(spark):
    stmt = spark.sql('SELECT stationsname, month, avg_temp, rank_avg_temp \
            FROM stationRank \
            WHERE year = 2015 \
            ORDER BY rank_avg_temp, month ASC')
    stmt.show()

station_rank_coldest_avg(spark)


# c)
print('\n######## c) ########')
def min_max_avg_temp_per_year(spark):
    stmt = spark.sql('SELECT s.stationsname, MIN(p.TT_TU) AS min_temp, AVG(p.TT_TU) AS avg_temp, \
                     MAX(p.TT_TU) AS max_temp, YEAR(p.mess_datum)*12+MONTH(p.mess_datum) AS month, \
                     YEAR(p.mess_datum) as year, s.bundesland \
                     FROM cdcstations s \
                     JOIN cdcproducts p \
                     ON s.stationid = p.stationid \
                     GROUP BY GROUPING SETS((YEAR(p.mess_datum), s.bundesland), \
                                            (YEAR(p.mess_datum), s.stationsname), \
                                            (YEAR(p.mess_datum)*12+MONTH(p.mess_datum), s.bundesland))')
    stmt.show()
    stmt.createOrReplaceTempView("minMaxTempAggregate")
    stmt.cache()

    stmt_1 = spark.sql('SELECT min_temp, avg_temp, max_temp, year, bundesland \
                       FROM minMaxTempAggregate \
                       WHERE year IS NOT NULL AND \
                            bundesland IS NOT NULL')
    stmt_1.show()

    stmt_2 = spark.sql('SELECT stationsname, min_temp, avg_temp, max_temp, year \
                       FROM minMaxTempAggregate \
                       WHERE year IS NOT NULL AND \
                            stationsname IS NOT NULL')
    stmt_2.show()

    stmt_3 = spark.sql('SELECT min_temp, avg_temp, max_temp, month, bundesland \
                       FROM minMaxTempAggregate \
                       WHERE month IS NOT NULL AND \
                            bundesland IS NOT NULL')
    stmt_3.show()

min_max_avg_temp_per_year(spark)