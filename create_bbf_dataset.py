import json
import os
import pickle
import re

import pandas as pd


def normalize_column(tag):
    """Delete extra spaces from data"""
    if type(tag) == list:
        return list(set(
            [' '.join(item.split()) for item in tag]
        ))
    elif type(tag) == str:
        return [tag, ]
    else:
        return []


def create_list_column(tag):
    """Change mixed types of 'tag' column to list type"""
    if type(tag) == list:
        return [item.lower() for item in tag]
    elif type(tag) == str:
        return [tag.lower(), ]
    else:
        return list()


def is_dialog(text):
    """Checks if a text is a dialog"""
    if (
        (len(re.findall(r'—', text)) >= 2 and re.match(r'—', text))
        or 
        (len(re.findall(r'–', text)) >= 2 and re.match(r'–', text))
        or 
        (len(re.findall(r'−', text)) >= 2 and re.match(r'−', text))
        or 
        (len(re.findall(r'-', text)) >= 2 and re.match(r'-', text))
    ):
        return 1
    else:
        return 0


def is_character_in_text(text, characters):
    """Checks if a text contains characters"""
    text = text.lower()
    for character in [char.lower() for char in characters]:
        try:
            if re.findall(character, text):
                return True
        except:
            ...
    return False

    
def is_target_in_tags(tag, topic_list):
    """Check if tags contains target"""
    return True if set(tag).intersection(topic_list) else False


if __name__ == '__main__':
    bbf_path = 'results/bbf'
    quotes = []

    for file in os.listdir(bbf_path):
        if file.endswith(".txt"):
            with open(os.path.join(bbf_path, file)) as f:
                lines = f.readlines()
                for line in lines:
                    quote = json.loads(line)
                    quotes.append(quote)
                    
    df = pd.DataFrame(quotes)

    df['author'] = df['author'].apply(normalize_column)
    df['character'] = df['character'].apply(normalize_column)
    df['tag'] = df['tag'].apply(normalize_column)

    df.tag = df.tag.apply(create_list_column)
    df = df.rename(columns={"tag": "tags"})

    # Save raw data
    # with open(os.path.join(bbf_path, 'raw_bbf.pickle'), 'wb') as f:
    #     pickle.dump(df, f)

    # Mark all dialogs
    df['is_dialog'] = df['quote'].apply(is_dialog)
    
    for ind, row in df.iterrows():
        if len(row.character) > 1:
            if is_character_in_text(row.quote, row.character):
                df.at[ind, 'is_dialog'] = 1

    # Get specific labeles
    labels = [
        'ирония', 'ироничные', 'самоирония', 'сарказм', 'саркастичные',
        'насмешки', 'издевательство', 'сатира', 'остроумие', 'черный юмор',
    ]

    df['target'] = df['tags'].apply(lambda x: is_target_in_tags(x, labels))

    df.target = df.target.fillna(0)
    df.target = pd.to_numeric(df.target, errors='coerce').astype(int)

    # Save pickle
    with open(os.path.join(bbf_path, 'bbf.pickle'), 'wb') as f:
        pickle.dump(df, f)
