import json
import os
import pickle
import re

import pandas as pd


def get_quote_id(link):
    """Return number of tag's page"""
    return link.split('/')[-1]


def parse_dtime(dtime_str):
    """Return number of tag's page"""
    return "".join(filter(lambda x: re.match(r'[\d\.\: ]', x), dtime_str))


def create_list_column(tags):
    """Lowercase tags and delete a waste tag"""
    return [
        item.lower() for item in tags if item.lower() != 'автор неизвестен'
    ]


def del_square_brackets(text):
    """Delete [ ... ] and [ ... ]:"""
    return re.sub(r'\[[^\]^\[]+\]\:?\s*', '', text)


def del_round_brackets(text):
    """Delete ( ... ) at the end of the line"""
    return re.sub(r'[\.\!\?]+\s*(\([^\)^\(]+\)\.*\s*)+$', '.', text)


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


def normalize_column(tag):
    if type(tag) == list:
        return list(set(
            [' '.join(item.split()) for item in tag]
        ))
    elif type(tag) == str:
        return [tag, ]
    else:
        return []


def is_character_in_text(text, characters):
    text = text.lower()
    for character in [char.lower() for char in characters]:
        if character in text:
            return True
    return False


# def is_target_in_tags(tag, topic_list):
#     """Check if tags contains target"""
#     return True if set(tag).intersection(topic_list) else False

def get_tags_by_topic(tag, topic_list):
    return True if set(tag).intersection(topic_list) else False


def get_source(row):
    """Get data for source column"""
    if row.anime:
        return row.anime
    elif row.book:
        return row.book
    elif row.cartoon:
        return row.cartoon
    elif row.comic:
        return row.comic
    elif row.game:
        return row.game
    elif row.movie:
        return row.movie
    elif row.play:
        return row.play
    elif row.samizdat:
        return row.samizdat
    elif row.serial:
        return row.serial
    elif row.song:
        return row.song
    elif row.tv:
        return row.tv
    else:
        return None


def del_extra_symbols(text):
    """Delete ( ... ) at the end of the line"""
    if text:
        return re.sub(r'\s*\([^\)^\(]+\)*\s*$', '', text)


if __name__ == '__main__':
    # Quotes collected one by one
    ci_path = 'results/citaty_info/qbq'
    # Quotes collected by topics
    # ci_path = 'results/citaty_info'
    quotes = []

    for file in os.listdir(ci_path):
        if file.endswith(".txt"):
            with open(os.path.join(ci_path, file)) as f:
                lines = f.readlines()
                for line in lines:
                    quote = json.loads(line)
                    quotes.append(quote)

    df = pd.DataFrame(quotes)

    # Replace index with tag page number (for the raw dataset)
    df['link_id'] = df['link'].apply(get_quote_id)
    df.link_id = df.link_id.astype('int32')
    df = df.drop_duplicates(subset=['link_id'])

    df.set_index('link_id', inplace=True)
    df.drop(['link', ], axis=1, inplace=True)
    df.sort_index(inplace=True)

    # Replace references column with specific columns
    new_keys = {
        'Автор цитаты': 'author',
        'Исполнитель': 'performer',
        'Песня': 'song',
        'Самиздат': 'samizdat',
        'Цитата из аниме': 'anime',
        'Цитата из игры': 'game',
        'Цитата из книги': 'book',
        'Цитата из комикса': 'comic',
        'Цитата из мультфильма': 'cartoon',
        'Цитата из сериала': 'serial',
        'Цитата из спектакля': 'play',
        'Цитата из телешоу': 'tv',
        'Цитата из фильма': 'movie',
        'Цитируемый персонаж': 'character'
    }

    for val in new_keys.values():
        df[val] = None

    for ind, row in df.iterrows():
        refs = row['references']
        for key in refs.keys():
            df.at[ind, new_keys[key]] = refs[key]

    df.drop(['references', ], axis=1, inplace=True)

    # Fix rating data
    df.rating = pd.to_numeric(df.rating, errors='coerce')
    df.rating.fillna(0, inplace=True)
    df.rating = df.rating.astype('int32')

    df.rating_positive = pd.to_numeric(df.rating_positive, errors='coerce')
    df.rating_positive.fillna(0, inplace=True)
    df.rating_positive = df.rating_positive.astype('int32')

    df.rating_negative = pd.to_numeric(df.rating_negative, errors='coerce')
    df.rating_negative.fillna(0, inplace=True)
    df.rating_negative = df.rating_negative.astype('int32')

    for ind, row in df.iterrows():
        if row['rating'] and row['rating_positive'] and not row['rating_negative']:
            df.at[ind, 'rating_negative'] = row['rating_positive'] - row['rating']

    # Fix submitted_date
    df.submitted_date = pd.to_datetime(df['submitted_date'].apply(parse_dtime))

    # Convert tags to lowercase
    df.tags = df.tags.apply(create_list_column)

    # Save raw data
    # with open(os.path.join(ci_path, 'raw_ci.pickle'), 'wb') as f:
    #     pickle.dump(df, f)

    # Delete extra brackets
    df['text'] = df['text'].apply(del_square_brackets)
    df['text'] = df['text'].apply(del_round_brackets)

    # Mark all dialogs
    df['is_dialog'] = df['text'].apply(is_dialog)

    # Normalize columns
    df['tags'] = df['tags'].apply(normalize_column)
    df['author'] = df['author'].apply(normalize_column)
    df['performer'] = df['performer'].apply(normalize_column)
    df['character'] = df['character'].apply(normalize_column)
    
    # Resolve ambiguity for character
    for ind, row in df.iterrows():
        if len(row.character) > 1:
             if is_character_in_text(row.text, row.character):
                    df.at[ind, 'is_dialog'] = 1

    # Get data with specific tages
    labels = [
        'ирония', 'ироничные цитаты', 'самоирония', 'сарказм', 'саркастичные цитаты',
        'насмешки', 'издевательство', 'сатира', 'остроумие', 'черный юмор',
    ]

    df['target'] = df['tags'].apply(lambda x: get_tags_by_topic(x, labels))

    df.target = df.target.fillna(0)
    df.target = pd.to_numeric(df.target, errors='coerce').astype(int)

    # Delete extra columns
    df['author'] = df.apply(lambda x: x.author if x.author else x.performer, axis=1)

    df.drop(['performer', ], axis=1, inplace=True)

    df['source'] = df.apply(lambda x: get_source(x), axis=1)

    df.drop(
        [
            'song', 'samizdat', 'anime', 'game',
            'book', 'comic', 'cartoon', 'serial',
            'play', 'tv', 'movie'
        ],
        axis=1,
        inplace=True
    )

    # Delete extra symbols from columns (author, character, source)
    df['source'] = df['source'].apply(del_extra_symbols)
    
    for ind, row in df.iterrows():
        df.at[ind, 'author'] = [del_extra_symbols(author) for author in row.author]
        
    for ind, row in df.iterrows():
        df.at[ind, 'character'] = [del_extra_symbols(character) for character in row.character]
        
    df = df.rename(columns={"text": "quote"})

    # Save pickle
    with open(os.path.join(ci_path, 'ci.pickle'), 'wb') as f:
        pickle.dump(df, f)
