
import pandas as pd
import re

from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

class Cleaner:
    alay_dict = pd.read_csv('new_kamusalay.csv', encoding = "ISO-8859-1", header=None)
    alay_dict = alay_dict.rename(columns={0: 'original', 1: 'replacement'})

    id_stopword_dict = pd.read_csv('stopwordbahasa.csv', header=None)
    id_stopword_dict = id_stopword_dict.rename(columns={0: 'stopword'})

    abusive_dict = pd.read_csv('abusive.csv')['ABUSIVE'].tolist()

    factory = StemmerFactory()
    stemmer = factory.create_stemmer()

    def lowercase(text):
        return text.lower()

    def remove_unnecessary_char(text):
        text = re.sub('\n', ' ', text)  # Remove every '\n'
        text = re.sub('rt', ' ', text)  # Remove every retweet symbol
        text = re.sub('user', ' ', text)  # Remove every username
        text = re.sub(r'(www\.[^\s]+|https?://[^\s]+|http?://[^\s]+)', ' ', text)  # Remove every URL
        text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
        return text

    def remove_unicode(text):
        text = re.sub(r'\bx[a-fA-F0-9]{2}\b', '', text)
        text = re.sub(r'\bx([a-fA-F0-9]{2})', '', text)
        return text

    def remove_nonaplhanumeric(text):
        text = re.sub(r'[^0-9a-zA-Z]+', ' ', text)
        return text

    def normalize_alay(alay_dict, text):
        alay_dict_map = dict(zip(alay_dict.original, alay_dict.replacement))
        return ' '.join([alay_dict_map[word] if word in alay_dict_map else word for word in text.split(' ')])

    def remove_stopword(id_stopword_dict, text):
        text = ' '.join(['' if word in id_stopword_dict.stopword.values else word for word in text.split(' ')])
        text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
        text = text.strip()
        return text

    def stemming(stemmer, text):
        return stemmer.stem(text)

    def remove_extra_spaces(text):
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text