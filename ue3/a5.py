#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 16:57:17 2019

@author: ubuntubig
"""

# a)
rd1 = sc.parallelize([i for i in range (1,101)])
rd2 = sc.parallelize([i for i in range (50, 151)])


# b)
cnt_rd1 = rd1.count()
cnt_rd2 = rd2.count()
print(rd1.takeOrdered(cnt_rd1))
print(rd2.takeOrdered(cnt_rd2))

print(rd1.first())
print(rd1.first())

sample_rd1 = rd1.takeSample(False, 10)
sample_rd2 = rd2.takeSample(False, 10)
print(sample_rd1)
print(sample_rd2)


# c)
intersect = rd1.intersection(rd2)
print(intersect.collect())
print(intersect.count())

union = rd1.union(rd2)
print(union.collect())
print(union.count())

sub_rd1_2 = rd1.subtract(rd2)
print(sub_rd1_2.collect())
print(sub_rd1_2.count())

sub_rd2_1 = rd2.subtract(rd1)
print(sub_rd2_1.collect())
print(sub_rd2_1.count())

cart = rd1.cartesian(rd2)
print(cart.collect())
print(cart.count())