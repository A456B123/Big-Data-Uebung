#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 18:40:52 2019

@author: ubuntubig
"""

print('---------------AUFGABE 8---------------')

# a)
print('\n######## a) ########')
def list_all_stations():
    spark.sql('SELECT * FROM ghcndstations').show()


# b)
print('\n######## b) ########')
def list_stations_by_country():
    spark.sql('SELECT c.countrycode, c.countryname \
              FROM ghcndstations s JOIN ghcndcountries c \
              ON s.countrycode = c.countrycode GROUP BY c.countrycode').show(truncate=False)


# c)
print('\n######## c) ########')
def list_stations_in_germany():
    spark.sql('SELECT * FROM ghcndstations WHERE countrycode="GM" ORDER BY stationname ASC').show(truncate=False)


# d)
print('\n######## d) ########')
def plot_temperature(year, station):
    df = spark.sql('SELECT d.value/10\
              FROM ghcnddata d JOIN ghcndstations s \
              ON d.stationid = s.stationid \
              WHERE d.element = "TMAX" AND d.year = {} AND s.stationname like "{}%"'.format(year, station))

    xv = [x for x in range(1,len(df.collect())+1)]
    plt.plot(xv, df.collect())
    plt.title('TMAX über das Jahr {} in {}'.format(year, station))
    plt.xlabel('Tag')
    plt.ylabel('TMAX')
    plt.show()


# e)
print('\n######## e) ########')
def plot_temperature_min_max(year, station):
    df_max = spark.sql('SELECT d.value/10 \
              FROM ghcnddata d JOIN ghcndstations s \
              ON d.stationid = s.stationid \
              WHERE d.element = "TMAX" AND d.year = {} AND s.stationname like "{}%"'.format(year, station))

    df_min = spark.sql('SELECT d.value/10 \
              FROM ghcnddata d JOIN ghcndstations s \
              ON d.stationid = s.stationid \
              WHERE d.element = "TMIN" AND d.year = {} AND s.stationname like "{}%"'.format(year, station))

    xv = [x for x in range(1,len(df_max.collect())+1)]
    fig, ax = plt.subplots()
    ax.plot(xv, df_max.collect(), label='TMAX')
    ax.plot(xv, df_min.collect(), label='TMIN')
    ax.legend(loc='lower center')
    plt.title('TMAX über das Jahr {} in {}'.format(year, station))
    plt.xlabel('Tag')
    plt.ylabel('TMAX')
    plt.show()

# f)
print('\n######## f) ########')
def all_prcp(station):
    prcp = spark.sql('SELECT d.year, d.value \
              FROM ghcnddata d JOIN ghcndstations s \
              ON d.stationid = s.stationid \
              WHERE d.element = "PRCP" AND s.stationname like "{}%" \
              GROUP BY d.year, d.value'.format(station))

    prcp = prcp.collect()
    plt.bar([row[0] for row in prcp], [row[1] for row in prcp])
    plt.title('PRCP über alle Jahre in {}'.format(station))
    plt.xlabel('Jahr')
    plt.ylabel('Niederschlag')
    plt.show()


# g)
print('\n######## g) ########')
def all_prcp(station):
    prcp = spark.sql('SELECT d.year, d.value \
              FROM ghcnddata d JOIN ghcndstations s \
              ON d.stationid = s.stationid \
              WHERE d.element = "PRCP" AND s.stationname like "{}%" \
              GROUP BY d.year, d.value'.format(station))

    prcp = prcp.collect()
    plt.bar([row[0] for row in prcp], [row[1] for row in prcp])
    plt.title('PRCP über alle Jahre in {}'.format(station))
    plt.xlabel('Jahr')
    plt.ylabel('Niederschlag')
    plt.show()

# h)
print('\n######## h) ########')

# i)
print('\n######## i) ########')

# j)
print('\n######## j) ########')

