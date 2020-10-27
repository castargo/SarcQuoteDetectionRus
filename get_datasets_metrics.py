import os
import pickle
import random
import string

from nltk.corpus import stopwords
from pymystem3 import Mystem
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression as Logit
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import normalize


def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text


def stem_text(text):
    text_list = mystem.lemmatize(" ".join(text.split()))
    return " ".join(
        [elem for elem in text_list if elem not in list([' ', '\n'] + stopwords.words("russian"))]
    )


if __name__ == '__main__':
    dataset = 'bbf'
    # dataset = 'ci'
    
    data_path = '../results'
    
    # Get data
    if dataset == 'bbf':
        with open(os.path.join(data_path, 'result_bbf.pickle'), 'rb') as f:
            df = pickle.load(f)
    else:
        with open(os.path.join(data_path, 'result_ci.pickle'), 'rb') as f:
            df = pickle.load(f)
            
    # Delete punctuation
    df.quote = df.quote.apply(remove_punctuations)
    df.quote = df.quote.apply(lambda s: s.lower() if type(s) == str else s)
    df['quote'] = df['quote'].apply(lambda s: ' '.join(s.split()))
    
    # Add mystem
    mystem = Mystem()
    df.quote = df.quote.apply(stem_text)
    df = df[df['quote'].apply(lambda x: len(x) > 10)]
    
    # Split data to train and test
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    
    # Random
    y_test = test_df.target
    y_pred = [random.choice([0, 1]) for y in y_test]
    print("F1 for random:")
    print(f1_score(y_test, y_pred, average='binary'))
    
    # Bag-of-Words
    vectorizer = CountVectorizer(ngram_range=(1,1))
    X = vectorizer.fit_transform(train_df.quote)
    y = train_df.target
    
    X_test = vectorizer.transform(test_df.quote)
    y_test = test_df.target
    
    normalize(X, copy=False)
    normalize(X_test, copy=False)
    
    clf = Logit(
        solver='liblinear',
        class_weight={
            0: df[df.target == 0].shape[0], 
            1: df[df.target == 1].shape[0]
        },
        dual=True,
        fit_intercept=False,
        random_state=0
    )
    
    clf.fit(X, y)
    
    print("F1 train bag-of-words:")
    print(f1_score(y, clf.predict(X), average='binary'))
    
    print("F1 test bag-of-words:")
    y_pred = clf.predict(X_test)
    print(f1_score(y_test, y_pred, average='binary'))
    
    # Bag-of-Bi-grams
    vectorizer = CountVectorizer(ngram_range=(2,2))
    X = vectorizer.fit_transform(train_df.quote)
    y = train_df.target
    
    X_test = vectorizer.transform(test_df.quote)
    y_test = test_df.target
    
    normalize(X, copy=False)
    normalize(X_test, copy=False)
    
    clf = Logit(
        penalty='l1',
        solver='saga',
        class_weight={
            0: df[df.target == 0].shape[0], 
            1: df[df.target == 1].shape[0]
        },
        random_state=0,
    )
    
    clf.fit(X, y)
    
    print("F1 train bag-of-bigrams:")
    print(f1_score(y, clf.predict(X), average='binary'))
    
    print("F1 test bag-of-bigrams:")
    y_pred = clf.predict(X_test)
    print(f1_score(y_test, y_pred, average='binary'))
