import pandas as pd
import html
import unicodedata
import re
import string
import itertools
import nltk
from nltk.tokenize import TweetTokenizer

tknzr = TweetTokenizer()

ws_pattern = re.compile(r'\s+')
first_split_pattern = re.compile('(.)([A-Z]+[a-z]+(?:[-][A-Z+a-z]+)*)')
second_split_pattern = re.compile('([a-z0-9])([A-Z])')
pattern_string = r"https?:\/\/[^ ^\r^\n]*"
web_url_pattern = re.compile(pattern_string)
first_single_end_pattern = re.compile(r'([!|?|.|;|:|,|-|_|*]{2,})$')
second_single_end_pattern = re.compile(r'(\s?[!|?|.|;|:|,|-|_|*]{2,})')
clean_end_pattern = re.compile(r'([!|?|.|;|:|,|-|_] [!|?|.|;|:|,|-|_])+')
rt_pattern = re.compile(r'\bRT\b')
at_pattern = re.compile('@\w+')
match_punctuation = re.compile(r'([.,/#!$%^&*;:{}=-_`~()])*\1')
end_pat = re.compile(r'.*[!|?|.|;]$')


def escape_html(text):
    try:
        return html.unescape(text)
    except:
        return text


def to_ascii(text):
    unicode_text = unicodedata.normalize('NFD', text)  # Fur usage in Python 2.x use unicode(text)
    try:
        return unicode_text.encode('utf-8', 'ignore').decode('utf-8')
    except:
        return text


def split_camelcase(text):
    first_split = first_split_pattern.sub(r'\1 \2', text)
    return second_split_pattern.sub(r'\1 \2', first_split)


def reduce_consecutive_letters(text):
    return ''.join(''.join(s)[:2] for _, s in itertools.groupby(text))


def delete_urls(text):
    return web_url_pattern.sub('', text)


def all_up_to_cap(text):
    for token in text.split(' '):
        if token.isupper():
            yield token.capitalize()
        else:
            yield token


def split_underscore(text):
    return text.replace('_', ' ')


def split_apostrophe(text):
    return text.replace("'", ' ')


def remove_multi_whitespace(text):
    return re.sub(ws_pattern, ' ', text).strip()


def replace_multi_punctuation(text):
    return match_punctuation.sub(r'\1', text)


def clean_end_signs(text):
    def end_replace(match):
        return match.groups()[0][-1]
    return clean_end_pattern.sub(end_replace, text)


def replace_RT_at_hash(text):
    text = rt_pattern.sub('', text)
    text = at_pattern.sub('', text)
    text = text.replace('#', '')
    return text


def clean_text(text, lower=True):
    text = replace_RT_at_hash(text)
    text = delete_urls(text)
    text = escape_html(text)
    text = split_camelcase(text)
    text = split_underscore(text)
    text = split_apostrophe(text)
    # text = reduce_consecutive_letters(text)
    # text = ' '.join(all_up_to_cap(text))
    text = remove_multi_whitespace(text)
    if lower:
        return text.lower()
    else:
        return text


def discard_ngrams_with_digits(ngrams):
    return [ngram for ngram in ngrams if not any(char.isdigit() for char in ngram)]

# def discard_ngrams_with_punctuation(ngrams):
#     return [ngram for ngram in ngrams if not any(char)]


def tokenize(text, stopwords=False, remove_punctuation=False):
    tokens = tknzr.tokenize(text)
    if stopwords:
        if isinstance(stopwords, str):
            while True:
                try:
                    stopwords = nltk.corpus.stopwords.words(stopwords)
                    break
                except LookupError:
                    nltk.download("stopwords")
        tokens = [token for token in tokens if token.lower() not in stopwords]
    if remove_punctuation:
        tokens = [token for token in tokens if token not in string.punctuation]
    return tokens


def gramify(tokens, minimum, maximum):
    assert minimum > 0
    assert maximum >= minimum
    if minimum == 1:
        grams = set(gram for gram in tokens if not any(c in string.punctuation for c in gram))
    else:
        grams = set()
    for n in range(max(minimum, 2), maximum+1):
        for i in range(len(tokens)-n+1):
            gram = ' '.join(tokens[i:i+n])
            if not any(c in string.punctuation for c in gram):
                grams.add(gram)
    return grams
