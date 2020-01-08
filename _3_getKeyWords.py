import os, yaml
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer



with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)



def getKeyWord(corpus, common_num, special_num, max_num):
    vectorizer = CountVectorizer(min_df=2)
    transformer = TfidfTransformer()

    vector = vectorizer.fit_transform(corpus)
    TFIDF = transformer.fit_transform(vector) # calcuate the tfidf
    tfidf_list = TFIDF.toarray()
    words = vectorizer.get_feature_names()

    WORD = []
    for tfidf in tfidf_list[common_num:]:   #''' get the max tfidf words '''
        weight = sorted([(i, w) for i, w in enumerate(tfidf)], key=lambda item: item[-1], reverse = True)[:max_num]
        WORD.extend([words[i[0]] for i in weight])
    D_ = [(w, WORD.count(w)) for w in set(WORD)]
    keywords = dict(sorted(D_, key = lambda item: item[-1], reverse=True)[:max_num])
    return list(keywords.keys())



def masked(dir_path, folder_list, keywords, type):
    out_path, uid_list = os.path.join(dir_path, 'masked'), []
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    for folder in folder_list:
        in_path = os.path.join(dir_path, folder)
        for file_name in os.listdir(in_path):
            Count, data_list = 0, []
            with open(in_path + "/" + file_name, 'r', encoding='UTF-8', errors='ignore') as inF:
                for line in inF.readlines():
                    line = str(line).replace('\n', '').strip()
                    match_words = set(keywords) & set(line.split(' '))
                    for word in match_words:
                        line = str(line).replace(word, '').replace("  ", " ")
                        Count += 1
                    data_list.append(line)
            if (str(folder).startswith('common') and Count >= 0)  or (str(folder).startswith('special') and Count >= 12):
            # if (str(folder).startswith('common') and Count >= 0)  or (str(folder).startswith('special') and Count >= 0):
                with open(out_path + "/" + file_name, 'w', encoding='UTF-8', errors='ignore') as outF:
                    outF.write("\n".join(data_list))
                uid_list.append(file_name)
        print(folder, len(uid_list))
    ''' get the file list '''
    with open(dir_path + "/masked-filelist-%s.txt" % (type), "w", encoding="UTF-8") as file:
        file.write("\n".join(uid_list))


def filtered(dir_path ,folder_list, keywords, type):
    out_path, uid_list = os.path.join(dir_path, 'filtered'), []
    if not os.path.exists(out_path):    os.makedirs(out_path)
    for folder in folder_list:
        in_path = os.path.join(dir_path, folder)
        for file_name in os.listdir(in_path):
            Count, data_list = 0, []
            with open(in_path + "/" + file_name, 'r', encoding='UTF-8', errors='ignore') as inF:
                for line in inF.readlines():
                    line = str(line).replace('\n', '').strip()
                    match_words = set(keywords) & set(line.split(' '))
                    if len(match_words) > 0:    Count += 1
                    data_list.append(line)
            if (str(folder).startswith('common') and Count >= 1) or (str(folder).startswith('special') and Count >= 12):
            # if (str(folder).startswith('common') and Count >= 0) or (str(folder).startswith('special') and Count >= 0):
                with open(out_path + "/" + file_name, 'w', encoding='UTF-8', errors='ignore') as outF:
                    outF.write("\n".join(data_list))
                uid_list.append(file_name)
        print(folder, len(uid_list))
    ''' get the file list '''
    with open(dir_path + "/filtered-filelist-%s.txt" % (type), "w", encoding="UTF-8") as file:
        file.write("\n".join(uid_list))

def all(dir_path ,folder_list, keywords, type):
    out_path, uid_list = os.path.join(dir_path, 'all'), []
    if not os.path.exists(out_path):    os.makedirs(out_path)
    for folder in folder_list:
        in_path = os.path.join(dir_path, folder)
        for file_name in os.listdir(in_path):
            Count, data_list = 0, []
            with open(in_path + "/" + file_name, 'r', encoding='UTF-8', errors='ignore') as inF:
                for line in inF.readlines():
                    line = str(line).replace('\n', '').strip()
                    match_words = set(keywords) & set(line.split(' '))
                    if len(match_words) > 0:    Count += 1
                    data_list.append(line)
            if (str(folder).startswith('common') and Count >= 1) or (str(folder).startswith('special') and Count >= 13):
                # with open(out_path + "/" + file_name, 'w', encoding='UTF-8', errors='ignore') as outF:
                #     outF.write("\n".join(data_list))
                uid_list.append(file_name)
        print(folder, len(uid_list))
    ''' get the file list '''
    with open(dir_path + "/all-filelist-%s.txt" % (type), "w", encoding="UTF-8") as file:
        file.write("\n".join(uid_list))


def mergeUserLable(in_path):
    file_list = filter(lambda name: str(name).endswith("_user_dict.npy"), os.listdir(in_path))
    print(file_list)
    label_dict = {}
    for name in file_list:
        user_dict = dict(np.load(in_path + "/" + name).item(0))
        label_dict.update(user_dict)
    np.save(in_path + "/label_dict.npy", label_dict)





if __name__ =='__main__' :
    # EVENT_1 = 'zh-gun'
    # data_dict_1 = dict(np.load(config['root4npy'] + EVENT_1 + "/data_dict.npy").item(0))
    # EVENT_2 = 'zh-money'
    # data_dict_2 = dict(np.load(config['root4npy'] + EVENT_2 + "/data_dict.npy").item(0))
    # data_dict_1.update(data_dict_2)
    # data_dict_2.update(data_dict_1)
    # np.save(config['root4npy'] + EVENT_1 + "/data_dict.npy", data_dict_1)
    # np.save(config['root4npy'] + EVENT_2 + "/data_dict.npy", data_dict_2)

    EVENT = 'us'
    # data_dict = dict(np.load(config['root4npy'] + EVENT + "/data_dict.npy").item(0))
    in_path = str(os.path.join(config['root4LDA'], "all")) #EVENT))
    folder_list = list(filter(lambda s: s.startswith('common') or s.startswith('special'), list(os.listdir(in_path))))
    common_corpus, special_corpus = [], []

    for folder in folder_list:
        folder_path, data_list = os.path.join(in_path, folder), []
        # print(len(os.listdir(folder_path)), len(data_dict.keys()))
        for file_name in os.listdir(folder_path):
            uid = file_name.split(".txt")[0]
            # tid_list = data_dict[uid].keys()
            with open(folder_path + "/" + file_name, 'r', encoding="UTF-8", errors='ignore') as file:
                lines = [str(line).strip().strip('\n') for line in file.readlines()]
            # if len(lines) == len(tid_list):
                data_list.append(" ".join(lines))
            # else:
            #     print(uid, len(lines), len(tid_list))
        ''' classify by common or special '''
        if str(folder).startswith('common'):
            common_corpus.extend(data_list)
        elif str(folder).startswith('special'):
            special_corpus.extend(data_list)
    # ''' get the keywords '''
    # print(len(common_corpus), len(special_corpus))
    # corpus = list(common_corpus) + list(special_corpus)
    # keywords = getKeyWord(corpus, common_num = len(common_corpus), special_num = len(special_corpus), max_num = 35)
    # print(keywords, list(folder_list))

    # keywords = ['isi', 'exit', 'eu', 'brexit', 'ru', 'iq', 'ir', 'de', 'fr', 'ua', 'refugee', 'stray', 'congress', 'retention',
    #             'settle', 'emplace', 'prime', 'minister', 'premier', 'johnson', 'boris', 'conservative', 'london', 'paris',
    #             'moscow', 'nhs', 'david', 'william', 'donald', 'cameron', 'referendum', 'dave', 'scotland', 'downing', 'syria',
    #             'westminster',  'buckingham', 'palace', 'labour', 'helen', 'macintyre', 'pounds', 'gbp', 'theresa', 'mary',
    #             'may', 'cabinet', 'minister', 'financial', 'crisis', 'fbpe', 'parliamentary', 'oxfordshire', 'whig', 'tory',
    #             'thames', 'brexit', 'bbc', 'ireland', 'jeremy', 'corbyn', 'queen', 'elizabeth', 'borisjohnson', 'conservativeparty',
    #             'brexitbritain', 'boristheliar', 'brexitbill', 'bbcnews', 'tories', 'dailymailuk', 'leadership', 'impeachment']
    # keywords = ['gui', 'wen', 'guo', 'zh', 'hk', 'law', 'police', 'usa', 'fugitive', 'violence', 'ordinance', 'opposition', "freedom"
    #  'march', 'lie', 'stability', 'thug', 'rule', 'ant', 'mob', 'government', 'violent', 'offender', 'trade', 'parade', 'public', 'free'
    #  'force', 'supp', 'prosperity', 'cheat', 'political', 'anti', 'protect', 'legislative', 'punish', 'peaceful', 'council','protest']
    keywords = ['usa', 'ru', 'putin', 'ministry', 'news', 'eu', 'ruble', 'ko', 'ov', 'agency', 'foreign', 'minister', 'football',
    'bre', 'police', 'kov', 'rbc', 'president', 'quote', 'syria', 'petersburg', 'terrorist', 'defense', 'vladimir', 'crimea', 'obama',
    'nuclear', 'donetsk', 'trump', 'disaster', 'fukushima', 'force', 'fire', 'npp', 'kill', 'chernobyl', 'army', 'military', 'quotes',
    'pravda', 'zap', 'accident', 'explode', 'oro', 'samara']

    # ''' get the masked corpus '''
    # masked(in_path, folder_list, keywords, type = EVENT)
    # ''' get the filtered corpus '''
    # filtered(in_path, folder_list, keywords, type = EVENT)
    ''' get the all corpus '''
    all(in_path, folder_list, keywords, type=EVENT)
    # ''' get the merged user dictionary '''
    # mergeUserLable(os.path.join(config['root4npy'], EVENT))

