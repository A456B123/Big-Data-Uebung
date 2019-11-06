#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 12:42:14 2019

@author: ubuntubig
"""
import string
print('---------------AUFGABE 5---------------')

# a)
print('\n######## a) ########')
text = sc.textFile('file:///home/ubuntubig/texte/robinsonCrusoe.txt')


# b)
print('\n######## b) ########')
cnt_all_characters = text.map(lambda x: len(x)).reduce(lambda x, y: x+y)
print('\nAnzahl aller Zeichen im Text:')
print(cnt_all_characters)


# c)
print('\n######## c) ########')
cnt_by_character = text.flatMap(lambda line: [(character, 1) for character in line]).reduceByKey(lambda x, y: x+y)
print('\nZeichenhäufigkeit im Text:')
print(cnt_by_character.sortByKey().collect())


# d)
print('\n######## d) ########')
def filter_words(line):
    filtered = ''
    for word in line.split(' '):
       filtered.join(c for c in word if c.isalnum())
    #''.join(c for c in word if c is c.isalnum() for word in line.split(' '))
    return filtered

word_count = text.filter(lambda line: filter_words(line)).flatMap(lambda line: [(word, 1) for word in line.split(' ')]).reduceByKey(lambda x, y: x+y)
print('\nWorthäufigkeit im Text:')
print(word_count.sortBy(lambda word: word[1], False).collect())