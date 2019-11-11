#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 12:42:14 2019

@author: ubuntubig
"""
import string
print('---------------AUFGABE 6---------------')

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
cnt_by_character = (
        text.flatMap(lambda line: [(character, 1) for character in line])
        .reduceByKey(lambda x, y: x+y)
        )

print('\nZeichenhäufigkeit im Text:')
print(cnt_by_character.sortByKey().collect())


# d)
print('\n######## d) ########')
words_with_special_chars = text.flatMap(lambda line: line.split(' '))
words = (
    words_with_special_chars
    .map(lambda word: ''.join(c for c in word if c.isalnum()))
    .filter(lambda word: word)
    )

word_count = (
    words.map(lambda word: word.lower())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda x, y: x+y)
    .sortBy(lambda word: word[1], False)
    )

print('\nWorthäufigkeit im Text:')
print(word_count.collect())
