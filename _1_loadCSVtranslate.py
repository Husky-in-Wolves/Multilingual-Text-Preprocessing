import random, time, numpy, os
from time import sleep
import multiprocessing, yaml
from basicAlgorithm import baidu_translate, google_translate, freeTranslate


with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)

token_dict = {'token_1': True, 'token_2': True, 'token_3': True, 'token_4': True, 'token_5': True, 'token_6': True}


def myCallback(userD, dataD, user_dict_path, data_dict_path):
    if not os.path.isfile(user_dict_path):  user_dict = {}
    else: user_dict = dict(numpy.load(user_dict_path).item(0))
    if not os.path.isfile(data_dict_path): data_dict = {}
    else: data_dict = dict(numpy.load(data_dict_path).item(0))
    user_dict.update(userD)
    for screen_name in dataD.keys():
        if screen_name not in data_dict.keys():
            data_dict[screen_name] = {}
        data_dict[screen_name].update(dataD[screen_name])
    numpy.save(user_dict_path, user_dict)
    numpy.save(data_dict_path, data_dict)

def multiTranslate(token, toTrans_dict, dataD):
    global ip, proxies_list
    for i, key in enumerate(toTrans_dict.keys()):
        screen_name, timestamp, tweet = key[0], key[-1], toTrans_dict[key]
        # sentence = baidu_translate(token, tweet)
        # sentence = google_translate(token, tweet)
        sentence = freeTranslate(token, tweet) if i%2 == 1 else baidu_translate(token, tweet)
        time.sleep(0.5)
        if sentence != "" and sentence != "error":
            dataD[screen_name][timestamp] = sentence
    return dataD



def loadCSV(token_name, dir_path, file_name, user_dict_path, data_dict_path):
    token = config[token_name]; print(token['appid'], token['secretKey'], 'for', file_name)
    data_dict, userD, dataD, toTrans_dict = {}, {}, {}, {}
    if os.path.isfile(data_dict_path):
        data_dict = numpy.load(data_dict_path).item(0)

    with open(dir_path + '/' + file_name, 'r', encoding='UTF-8', errors='ignore') as file:
        line_list = list(file.readlines())
        line_list.reverse()
        for i, str_line in enumerate(line_list):
            if len(toTrans_dict.keys()) >= 5:
                dataD = multiTranslate(token, toTrans_dict, dataD)
                toTrans_dict = {}
            if i % (500) == 1 and len(dataD.keys()) > 0:
                myCallback(userD, dataD, user_dict_path, data_dict_path)
                print(file_name, 'the %d-th line has been recorded.' % (i))
                userD, dataD = {}, {}
            try:
                line = str_line.strip("\n").strip().split("\t")
                ''' record the information '''
                user_id, screen_name, description, special = line[1], str(line[3]).strip(), line[5], True
                ''' record the tweets and timestamps '''
                lang, tweet, timestr = line[11], str(line[12]).strip(" "), line[13]
                timeArray = time.strptime(timestr, "%Y-%m-%d %H:%M")
                timestamp = int(time.mktime(timeArray))
            except:
                continue
            else:
                if screen_name not in data_dict.keys() or timestamp not in data_dict[screen_name].keys():
                    toTrans_dict[(screen_name, timestamp)] = tweet
                    if screen_name not in dataD.keys():
                        dataD[screen_name] = {}
                        userD[screen_name] = {'description': description, 'special': special}

    myCallback(userD, dataD, user_dict_path, data_dict_path)
    global token_dict
    token_dict[token_name] = True




if __name__ =='__main__' :
    # user_dict, data_dict, user_dict_path, data_dict_path = {}, {}, "", ""
    pool = multiprocessing.Pool(6)
    # for i, folder in enumerate(os.listdir(config['root4csvnew'])):
    for i, folder in enumerate(["special-twitter-us-plus"]):
        in_path = os.path.join(config['root4csvnew'], folder)
        dir_path = os.path.join(config['root4npy'], folder)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        for file in os.listdir(in_path):
            user_dict_path = dir_path + "/%s_user_dict.npy" % (file)
            data_dict_path = dir_path + "/%s_data_dict.npy" % (file)

            token_name, Flag = "", 'no idle processing'
            while Flag == 'no idle processing':
                try:
                    token_name = list(token_dict.keys())[list(token_dict.values()).index(True)]
                except:
                    print('sorry, there is no idle processing:\t',token_dict )
                    Flag = 'no idle processing'
                    sleep(10**4)
                else:
                    Flag = ""
                    token_dict[token_name] = False
                    print('creat one processing is on call %s'%(token_name), token_dict)
            # loadCSV("", in_path, file, user_dict_path, data_dict_path)
            pool.apply_async(func=loadCSV, args=(token_name, in_path, file, user_dict_path, data_dict_path))
    pool.close()
    pool.join()





