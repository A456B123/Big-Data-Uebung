#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 11 12:43:31 2019

@author: ubuntubig
"""

print('---------------AUFGABE 12---------------')

def readStockFromParquet(spark, sc):
    """
    Read all data from the parquet files into Dataframes
    and create temp views
    """
    dfStocks = spark.read.parquet("stocks.parquet")
    dfStocks.createOrReplaceTempView("stocks")
    dfStocks.cache()

    dfPortfolio = spark.read.parquet("portfolio.parquet")
    dfPortfolio.createOrReplaceTempView("portfolio")
    dfPortfolio.cache()

    return dfStocks, dfPortfolio

# a)
print('\n######## a) ########')
def symbol_notation(spark):
    oldest_latest = spark.sql('SELECT symbol, MIN(dt) AS oldest_date, MAX(dt) AS latest_date \
                              FROM stocks \
                              GROUP BY symbol')
    oldest_latest.show()


# b)
print('\n######## b) ########')
def min_max_avg_close(spark):
    min_max_avg_close = spark.sql('SELECT symbol, MIN(close) AS min_close, MAX(close) AS max_close, AVG(close) AS avg_close \
                                  FROM stocks \
                                  WHERE YEAR(dt) = 2009 \
                                  GROUP BY symbol')
    min_max_avg_close.show()


# c)
print('\n######## c) ########')
def num_stock_diff(spark):
    lat_v_portfolio = spark.sql('SELECT pid, bond.symbol, bond.num \
                                FROM portfolio LATERAL VIEW EXPLODE(bonds) AS bond')
    lat_v_portfolio.show()
    lat_v_portfolio.createOrReplaceTempView("portfolio_lateral")
    lat_v_portfolio.cache()

    stmt = spark.sql('SELECT symbol, SUM(num) AS anz, COUNT(*) AS anz_symbol, AVG(num) AS avg_symbol \
                     FROM portfolio_lateral \
                     GROUP BY symbol')
    stmt.show()


# d)
print('\n######## d) ########')
def symbols_no_portfolio(spark):
    symbols_no_portfolio = spark.sql('SELECT s.symbol FROM stocks s \
                                     LEFT ANTI JOIN portfolio_lateral p \
                                     ON s.symbol = p.symbol')
    symbols_no_portfolio.show()


# e)
print('\n######## e) ########')
def portfolio_value_2010(spark):
    last_day_2010 = spark.sql('SELECT symbol, MAX(dt) as last_day \
                              FROM stocks \
                              WHERE YEAR(dt) = 2010 \
                              GROUP BY symbol')
    last_day_2010.createOrReplaceTempView('lastDay2010Symbol')
    last_day_2010.cache()

    stmt = spark.sql('SELECT s.symbol, s.close \
                     FROM stocks s \
                     JOIN lastDay2010Symbol l \
                     ON s.symbol = l.symbol \
                     JOIN (\
                           SELECT p.* \
                           FROM portfolio_lateral p')
    stmt.show()






