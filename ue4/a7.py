#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  11 10:02:03 2019

@author: ubuntubig
"""
print('---------------AUFGABE 7---------------')

# a)
print('\n######## a) ########')
def wordcount(file_name):
    text = sc.textFile('file:///home/ubuntubig/texte/'+file_name)

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
        )

    return word_count


wc_crusoe = wordcount('robinsonCrusoe.txt')
print('Wordcount Robinson Crusoe:')
print(wc_crusoe.collect())

wc_shake = wordcount('Shakespeare.txt')
print('Wordcount Shakespeare:')
print(wc_shake.collect())


# b)
print('\n######## b) ########')
print('Wordcount Robinson Crusoe -- Alle Keys:')
print(wc_crusoe.keys().collect())

print('Wordcount Robinson Crusoe -- Alle Values:')
print(wc_crusoe.values().collect())

print('Wordcount Robinson Crusoe -- Anzahl der Wortpaare:')
print(wc_crusoe.count())

print('Wordcount Robinson Crusoe -- Paare sortiert nach Key:')
print(wc_crusoe.sortByKey().collect())

print('Wordcount Robinson Crusoe -- Paare sortiert nach Value:')
print(wc_crusoe.sortBy(lambda word: word[1], False).collect())


# c)
print('\n######## c) ########')
def countAllWords(rdd):
    seq_op = (lambda x, y: x+y[1])
    comb_op = (lambda x, y: x+y)
    aggr = rdd.aggregate(0, seq_op, comb_op)
    return aggr


aggr_crusoe = countAllWords(wc_crusoe)
print('Anzahl der Worte in Robinson Crusoe:')
print(aggr_crusoe)


# d)
print('\n######## d) ########')
def normalize(rdd):
    all_words_cnt = countAllWords(rdd)
    norm_cnt = rdd.map(lambda x: (x[0], x[1]/all_words_cnt))
    return norm_cnt


# e)
print('\n######## e) ########')
def replaceNoneTypeInRatio(tup):
    word = tup[0]
    r1 = tup[1][0]
    r2 = tup[1][1]
    if r1 is None:
        r1 = -r2
    if r2 is None:
        r2 = -r1
    return (word, (r1, r2))


def relDist(file1, file2):
    norm1 = normalize(wordcount(file1))
    norm2 = normalize(wordcount(file2))

    joined = norm1.fullOuterJoin(norm2)
    joined = joined.map(lambda x: replaceNoneTypeInRatio(x))

    seq_op = (lambda x, y: x + y[1][0] - y[1][1])
    comb_op = (lambda x, y: x + y)
    aggr = joined.aggregate(0, seq_op, comb_op)
    return aggr


# f)
print('\n######## f) ########')
def distOfTexts(files):
    for file1 in files:
        print('')
        for file2 in files:
            print('Distanz von {} und {}:'.format(file1, file2))
            print(relDist(file1, file2))


list_of_texts = ['AdventuresOfTomSawyer.txt',
                 'AliceInWonderland.txt',
                 'Ulysses.txt',
                 'Der_Prozess.txt',
                 'Die_Leiden_des_jungen_Werther.txt',
                 'Effi_Briest.txt']
distOfTexts(list_of_texts)



