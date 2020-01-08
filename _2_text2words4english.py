import numpy as np
import os, re, wordninja, operator, yaml
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from basicAlgorithm import judLanguage
from functools import reduce
from stanfordcorenlp import StanfordCoreNLP
nlp = StanfordCoreNLP(r'D:\Python\Python36\stanford-corenlp-full-2018-10-05')


with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)
english_stopword = []
with open("stoplist-en.txt", 'r') as file:
    english_stopwords =[str(word).strip("\n").strip() for word in file.readlines()]

''' delete network address '''
def rmHttp(sentence):
    http_co_pattern = re.compile(r'(http)?[s]?[:：]?[\s]?[\/]?[\s]?[\/]?[\s]?t.co[\s]?[\/]?[\s]?[a-zA-Z0-9]+')
    sentence = http_co_pattern.sub(" ", sentence.lower())
    http_pattern = re.compile(r'http[s]?[:：][\s]?/[\s]?/[\s]?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')  # 匹配模式
    sentence = http_pattern.sub(" ", sentence.lower())
    http_res_pattern = re.compile(r'http[s]?[\s]?[:：]?[\s]?[/]?[\s]?[/]?[\s]?…')
    sentence = http_res_pattern.sub(" ", sentence.lower())
    return sentence

''' delete emoji symbols '''
def rmEmoji(sentence):
    try:
        emoji_pattern = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        emoji_pattern = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
    sentence = emoji_pattern.sub(" ", sentence)
    emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticons
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
        "+", flags=re.UNICODE)
    sentence = emoji_pattern.sub(" ", sentence)
    return sentence


''' delete RT... and @... '''
def rmRT(sentence):
    rt_pattern = re.compile("(?i)rt ")
    sentence = rt_pattern.sub(" ", sentence)
    rt_pattern = re.compile("@[\W]*([a-zA-Z]|[0-9])+:*")
    sentence = rt_pattern.sub(" ", sentence)
    rt_pattern = re.compile("&([a-zA-Z])+;+")
    sentence = rt_pattern.sub(" ", sentence)
    return sentence

def rmNum(sentence):
    num_pattern = re.compile("[\W]*[0-9]+[a-zA-Z]{0,3}[\W]*")
    sentence = num_pattern.sub(" ", sentence)
    return sentence

def rpKeywords(keywords, sentence):
    for key in keywords.keys():
        devs = sorted(keywords[key], key = lambda item: len(item), reverse=True)
        for dev in devs:
            sentence = sentence.replace(dev,  key)
    return sentence


def getlemmatizer(sent):
    wordnet_lemmatizer = WordNetLemmatizer()
    pos_tag, new_words = nlp.pos_tag(sent), []       # Enter a sentence
    for (word, tag) in pos_tag:
        if str(tag).startswith("NN"):       # Nouns(plural), Proper Nouns(plural)
            new_word = wordnet_lemmatizer.lemmatize(word, "n")
            new_words.append(new_word)
        elif str(tag).startswith("VB"):
            new_word = wordnet_lemmatizer.lemmatize(word, "v")
            new_words.append(new_word)
        elif str(tag).startswith("JJ"):     # Adjective (comparative, supreme)
            new_word = wordnet_lemmatizer.lemmatize(word, "a")
            new_words.append(new_word)
        elif str(tag).startswith("R"):      # Adverbs (comparative, supreme)
            new_word = wordnet_lemmatizer.lemmatize(word, "r")
            new_words.append(new_word)
        else:
            continue
    return new_words


def tokenizer(sentence, english_stopwords):
    if judLanguage(sentence) != 'en':   return ""
    sentence = sentence.lower()
    ''' delete network address '''
    sentence = rmHttp(sentence)
    ''' delete emoji symbols '''
    sentence = rmEmoji(sentence)
    ''' delete RT... and @... '''
    sentence = rmRT(sentence)
    ''' delete number '''
    sentence = rmNum(sentence)

    ''' replace derivatives into keywords '''
    sentence = rpKeywords(config['PNN'], sentence.lower())

    ''' tokenize and lemmatizer '''
    word_token = getlemmatizer(sentence)    # word_token = nltk.word_tokenize(sentence.strip())   # word_token = WordPunctTokenizer().tokenize(sentence.strip())
    if len(set(word_token)) <= 8: return ""
    # ''' delete punctuation marks that is alone '''
    # Punc = re.compile("\W+"); abb = re.compile("[\'\_]\w+")
    # word_noPunc = [re.sub(Punc, "", w) for w in word_token if Punc.sub("", w) != "" and abb.sub("", w) != ""]
    ''' split conjunctions '''
    Punc = re.compile("\W+")
    word_noLong = reduce(operator.add, [[w] if len(wordnet.synsets(w)) else wordninja.split(w) for w in word_token])
    word_noLong = [re.sub(Punc, "", w) for w in word_noLong if Punc.sub("", w) != ""]

    ''' delete stop-words via NLTK '''
    word_noStop = [w.lower() for w in word_noLong if w.lower() not in english_stopwords and w.upper() not in english_stopwords]

    ''' use keywords to replace the derivatives '''
    sentence = rpKeywords(config['PNN'], " ".join(word_noStop).lower())

    if len(set(sentence.split(" "))) <= 5:   return ""
    else:   return sentence


def text2word(EVENT, folder, out_path):
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    ''' tokenize and clean '''
    data_dict = dict(np.load(config['root4npy'] + EVENT + folder).item(0))
    uid_list = sorted(list(data_dict.keys()))
    for uid in uid_list:
        data_list, time_list = [], sorted(list(data_dict[uid].keys()))
        for tid in time_list:
            tweet = tokenizer(data_dict[uid][tid], english_stopwords)
            if len(tweet) >= 5:
                data_list.append(tweet)
            else:
                data_dict[uid].pop(tid)
        print('finish', folder, uid, len(data_dict[uid].keys()), len(data_list))
        if len(data_dict[uid].keys()) == len(data_list) and len(data_list) >= 12:
            with open(out_path + "/" + str(uid) + ".txt", "w", encoding="UTF-8") as f:
                f.write("\n".join(data_list))
        else:
            data_dict.pop(uid)
    ''' update data_dict '''
    return data_dict






if __name__ =='__main__' :
    EVENT = 'brexit'
    folder_list = ['/common-tweets-brexit_data_dict.npy', '/special-tweets-brexit_data_dict.npy']
    data_dict = {}

    for folder in folder_list:
        out_path = config['root4LDA'] + EVENT + str(folder).split('_data_')[0]
        data = text2word(EVENT, folder,out_path)
        data_dict.update(data)
    np.save(config['root4npy'] + EVENT + "/data_dict.npy", data_dict)
