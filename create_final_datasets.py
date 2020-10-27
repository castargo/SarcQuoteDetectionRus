import jellyfish
import os
import pickle
import re

import pandas as pd

from joblib import Parallel, delayed
from tqdm.auto import tqdm


def del_extra_word(sstr):
    return re.sub(r' +', ' ', sstr.replace('цитаты', ''))


def dropifjaro(ind):
    r1 = df.iloc[ind]
    r2 = df.iloc[ind + 1]
    if jellyfish.jaro_winkler_similarity(r1.quote, r2.quote) > 0.9:
        df.at[ind, 'source'] = r1.source or r2.source
        df.at[ind, 'tags'] = list(set(r1.tags + r2.tags))
        df.at[ind, 'author'] = list(set(r1.author + r2.author))
        df.at[ind, 'character'] = list(set(r1.character + r2.character))
        df.at[ind, 'is_dialog'] = r1.is_dialog or r2.is_dialog
        df.at[ind, 'target'] = r1.target or r2.target
        return ind + 1


def clear_text(text):
    clear_text = ''
    for letter in text: 
        if letter in alphabet:
            clear_text += letter
    return clear_text


if __name__ == '__main__':
    bbf_path = '../results/bbf'
    ci_path = '../results/citaty_info/qbq'
    
    # Get datasets
    # dataset = 'bbf'
    dataset = 'ci'
    
    if dataset == 'bbf':
        with open(os.path.join(bbf_path, 'bbf.pickle'), 'rb') as f:
            df = pickle.load(f)
    else:
        with open(os.path.join(ci_path, 'ci.pickle'), 'rb') as f:
            df = pickle.load(f)
    
    # Delete quotes with unpropreate lenght
    df = df[df['quote'].apply(lambda x: 10 < len(x) < 350)]
    
    # Prepare tags
    for ind, row in df.iterrows():
        df.at[ind, 'tags'] = [del_extra_word(tag) for tag in row.tags]
        
    # Delete non russian alphabet
    alphabet = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        ' ', '!', '#', '%', '&', '*', '+', ',', '.', '/', '~',
        ':', ';', '=', '?', '@', '\\', '^', '{', '|', '}',
        '"', '$', "'", '(', ')', '<', '>', '[', ']', '№',
        '-', '_', '̶', '‐', '‑', '‒', '–', '—', '―', '−', '─',
        'А', 'Б', 'В', 'Г', 'Д', 'Е', 'Ё', 'Ж', 'З', 'И', 'Й', 'К', 'Л', 'М', 'Н', 'О', 'П',
        'Р', 'С', 'Т', 'У', 'Ф', 'Х', 'Ц', 'Ч', 'Ш', 'Щ', 'Ъ', 'Ы', 'Ь', 'Э', 'Ю', 'Я',
        'а', 'б', 'в', 'г', 'д', 'е', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п',
        'р', 'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я', 'ё',
    }
    
    df['quote'] = df['quote'].apply(clear_text)
    df['quote'] = df['quote'].str.strip()
    df = df[df['quote'].apply(lambda x: len(x) > 10)]
    
    # Delete duplicates
    df = df.sort_values(by=['quote'])
    df.reset_index(drop=True, inplace=True)
    
    with Parallel(n_jobs=1, require='sharedmem') as parallel:
        indexes4drop = parallel(
            delayed(dropifjaro)(ind) 
            for ind in tqdm(df.index.values[:-1], total=df.shape[0])
        )
        
    df.drop(list(filter(None, indexes4drop)), inplace=True)
    df.target = pd.to_numeric(df.target, errors='coerce').astype(int)
    df.is_dialog = pd.to_numeric(df.is_dialog, errors='coerce').astype(int)
    
    df = df.sample(frac=1).reset_index(drop=True)
    df = df[df['quote'].apply(lambda x: len(x) > 10)]
    
    # Save file
    result_path = '../results'
    
    if dataset == 'bbf':
        with open(os.path.join(result_path, 'result_bbf.pickle'), 'wb') as f:
            pickle.dump(df, f)
    else:
        with open(os.path.join(result_path, 'result_ci.pickle'), 'wb') as f:
            pickle.dump(df, f)
