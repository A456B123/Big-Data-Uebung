#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 16:57:17 2019

@author: ubuntubig
"""
import matplotlib.pyplot as plt
print('---------------AUFGABE 5---------------')

# a)
print('\n######## a) ########')
rd1 = sc.parallelize([i for i in range (1,101)])
rd2 = sc.parallelize([i for i in range (50, 151)])


# b)
print('\n######## b) ########')
cnt_rd1 = rd1.count()
cnt_rd2 = rd2.count()

print('\nrd1 sortiert:')
print(rd1.takeOrdered(cnt_rd1))

print('\nrd2 sortiert:')
print(rd2.takeOrdered(cnt_rd2))

print('\nerstes Element aus rd1:')
print(rd1.first())

print('\nerstes Element aus rd2:')
print(rd2.first())

sample_rd1 = rd1.takeSample(False, 10)
sample_rd2 = rd2.takeSample(False, 10)
print('\nSample aus rd1:')
print(sample_rd1)
print('\nSample aus rd2:')
print(sample_rd2)


# c)
print('\n######## c) ########')
intersect = rd1.intersection(rd2)
print('\nDurchschnittsmenge und Anzahl Elemente:')
print(intersect.sortBy(lambda x: x).collect())
print(intersect.count())

union = rd1.union(rd2)
print('\nVereinigungsmenge und Anzahl Elemente:')
print(union.sortBy(lambda x: x).collect())
print(union.count())

sub_rd1_2 = rd1.subtract(rd2)
print('\nDifferenzmenge rd1-rd2 und Anzahl Elemente:')
print(sub_rd1_2.sortBy(lambda x: x).collect())
print(sub_rd1_2.count())

sub_rd2_1 = rd2.subtract(rd1)
print('\nDifferenzmenge rd2-rd1 und Anzahl Elemente:')
print(sub_rd2_1.sortBy(lambda x: x).collect())
print(sub_rd2_1.count())

cart = rd1.cartesian(rd2)
print('\nKreuzprodukt und Anzahl Elemente:')
print(cart.sortBy(lambda x: x).collect())
print(cart.count())


# d)
print('\n######## d) ########')
rd3 = rd1.map(lambda x: 1/x)

print('\nNeue RDD (rd3):')
print(rd3.collect())

print('\nSumme Elemente rd3:')
print(rd3.sum())

xv = [x for x in range(1,101)]
fig, ax = plt.subplots()
ax.plot(xv, rd1.collect(), label='RDD 1 (x)')
ax.plot(xv, rd3.collect(), label='RDD 3 (1/x)')
ax.legend(loc='lower center')
plt.title('RDD 1 und 3 im Vergleich')
plt.xlabel('Zahl')
plt.ylabel('Wert')
plt.show()


# e)
print('\n######## e) ########')
print('\nMultipliziere alle Werte aus rd2:')
print(rd2.fold(1, lambda x, y: x*y))


# f)
print('\n######## f) ########')
print('\nFiltere rd1 auf Zahlen teilbar durch 7 und 5:')
print(rd1.filter(lambda x: x%7==0 and x%5==0).collect())
