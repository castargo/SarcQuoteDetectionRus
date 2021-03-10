import nltk
import os
import pickle
import string

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from nltk.corpus import stopwords
from pymystem3 import Mystem
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.utils import shuffle
from wordcloud import WordCloud


def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text


def stem_text(text):
    text_list = mystem.lemmatize(" ".join(text.split()))
    return " ".join(
        [elem for elem in text_list if elem not in words_to_del]
    )


def get_top_n_words(corpus, n=None):
    vec = CountVectorizer(ngram_range=(2,2)).fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0) 
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:n]


if __name__ == '__main__':
    data_path = '../results'
    
    # Get data
    with open(os.path.join(data_path, 'result.pickle'), 'rb') as f:
        df = pickle.load(f)
    
    df = shuffle(df)
    
    # Most popular authors
    items = dict()
    for item_list in df.author:
        if item_list:
            for item in item_list:
                if item in items.keys():
                    items[item] += 1
                else:
                    items[item] = 1

    print(dict(sorted(items.items(), key=lambda item: item[1], reverse=True)[:20]))
      
    # Delete punctuation
    df.quote = df.quote.apply(remove_punctuations)
    df.quote = df.quote.apply(lambda s: s.lower() if type(s) == str else s)
    df['quote'] = df['quote'].apply(lambda s: ' '.join(s.split()))
    
    # Add mystem
    mystem = Mystem()
    words_to_del = [' ', '\n'] + stopwords.words("russian")
    df.quote = df.quote.apply(stem_text)
    df = df[df['quote'].apply(lambda x: len(x) > 10)]
       
    # Get the top 20 common words
    common_words = get_top_n_words(df.quote, 20)
    for word, freq in common_words:
        print(word, '--', freq)
        
    common_words = get_top_n_words(df[df.target == 1]['quote'], 20)
    for word, freq in common_words:
        print(word, '--', freq)
