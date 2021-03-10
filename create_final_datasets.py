import jellyfish
import os
import pickle
import re

import pandas as pd

from joblib import Parallel, delayed
from tqdm.auto import tqdm


def del_extra_word(sstr):
    return re.sub(r' +', ' ', sstr.replace(' цитаты', ''))


def dropifjaro(ind):
    r1 = df.iloc[ind]
    r2 = df.iloc[ind + 1]
    if (jellyfish.jaro_winkler_similarity(r1.quote, r2.quote) > 0.9):
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
    with open(os.path.join(bbf_path, 'bbf.pickle'), 'rb') as f:
        bbf = pickle.load(f)
        
    with open(os.path.join(ci_path, 'ci.pickle'), 'rb') as f:
        ci = pickle.load(f)

    ci = ci.reset_index(drop=True)
    
    df = pd.concat([ci, bbf])
    df = df.reset_index(drop=True)
    
    df = df.astype(object).where(pd.notnull(df), None)
    df.submitted_date = df.submitted_date.astype(object).where(df.submitted_date.notnull(), None)
    
    # Delete quotes with unpropreate lenght
    df = df[df['quote'].apply(lambda x: len(x) > 10 and len(x) < 350)]
    
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
    df['quote'] = df['quote'].apply(lambda s: ' '.join(s.split()))
    df['quote'] = df['quote'].apply(
        lambda s: re.sub(r'''^[! ,?'"#%&*+.\/~:;=@\\\^{|}$\(\)\[\]<>№_]* ''', '', s)
    )
    
    # Strip whitespaces before punctuation
    df['quote'] = df['quote'].apply(
        lambda s: re.sub(r'''\s([! ,?'"#%&*+.\/~:;=@\\\^{|}$\(\)\[\]<>№_](?:\s|$))''', r'\1', s)
    )
    
    # Delete duplicates
    # Drop complete matches
    df = df.sort_values(by=['quote'])
    df.reset_index(drop=True, inplace=True);
    df = df[df['quote'].apply(lambda x: len(x) > 10)]
    df = df.drop_duplicates(subset='quote')
    
    # Drop duplicates
    df = df.reset_index(drop=True)
    
    with Parallel(n_jobs=1, require='sharedmem') as parallel:
        indexes4drop = parallel(
            delayed(dropifjaro)(ind) 
            for ind in tqdm(df.index.values[:-1], total=df.shape[0])
        )
        
    df.drop(list(filter(None, indexes4drop)), inplace=True)
    
    # Save file
    result_path = '../results'
    
    with open(os.path.join(result_path, 'result.pickle'), 'wb') as f:
        pickle.dump(df, f)
