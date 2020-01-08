import time, numpy, os
from basicAlgorithm import google_translate
from dateutil.parser import parse
import multiprocessing, yaml

with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)
data_dict, data_dict_path = {}, ""


def loadProfile(profile):
    userD, info_dict, key, value = {}, {}, "", ""
    with open(profile, 'r', encoding="UTF-8", errors='ignore') as file:
        for line in file.readlines():
            if line == '\n':
                try:
                    screen_name, description, special = info_dict['screen_name'], info_dict['description'], False
                except:
                    continue
                else:
                    if screen_name not in userD.keys():
                        userD[screen_name] = {'description': description, 'special': special}
                info_dict, key, value = {}, "", ""
            else:
                try:
                    list_ = line.split(':\t')
                except:
                    if key != "":   info_dict[key] = str(info_dict[key]) + str(line).strip('\n').strip()
                else:
                    key, value = list_[0], list_[-1]
                    info_dict[key] = value
    return userD


''' load the profile file... '''
def run4profile():
    folders = os.listdir(config['root4prof'])
    for folder in folders:
        user_dict = {}
        in_path = os.path.join(config['root4prof'], folder)
        profile_list = os.listdir(in_path)
        for profile in profile_list:
            userD = loadProfile(in_path + "/" + profile)
            user_dict.update(userD)
        numpy.save(config['root4npy'] + "%s_user_dict.npy" % (folder), user_dict)


def loadTXT(file_path, screen_name):
    print('starting load tweets for %s' % (file_path))
    dataD, tweet_dict, key, value = {screen_name:{}}, {}, "", ""
    with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
        for line in file.readlines():
            if line == '\n':
                try:
                    timestr = parse(tweet_dict['created_at'])
                    timestr = str(timestr).split('+')[0]
                    timeArray = time.strptime(timestr, "%Y-%m-%d %H:%M:%S")
                    timestamp = int(time.mktime(timeArray))
                    text, lang = tweet_dict['text'], tweet_dict['lang']
                except:
                    continue
                else:
                    if lang == 'en' and len(text) > 10:
                        dataD[screen_name][timestamp] = text
                    elif lang != 'en' and lang in config['lang4translate'] and len(text) > 10:
                        ''' translate the tweets and record the result '''
                        sentence = google_translate("", text)
                        if sentence != '':
                            dataD[screen_name][timestamp] = sentence
                tweet_dict, key, value = {}, "", ""
            else:
                try:
                    list = line.split(':\t')
                except:
                    if key != "":   tweet_dict[key] = str(tweet_dict[key]) + str(line)
                else:
                    key, value = list[0].strip(), str(list[-1]).strip("\n").strip()
                    tweet_dict[key] = value
    return dataD


def myCallback(dataD):
    global data_dict, data_dict_path
    for uid in dataD.keys():
        if uid not in data_dict.keys():
            data_dict[uid] = {}
        data_dict[uid].update(dataD[uid])
    numpy.save(data_dict_path, data_dict)


''' load the tweets files... '''
def run4txt():
    global data_dict_path, data_dict
    for folder in os.listdir(config['root4txt']):
        pool = multiprocessing.Pool(3)
        data_dict_path = config['root4npy'] + "%s_data_dict.npy" % (folder)
        try:
            data_dict = numpy.load(data_dict_path).item(0)
            print('exist the file %s'%(data_dict_path), len(data_dict.keys()), data_dict.keys())
        except:
            data_dict = {}
            print('doooooo not exist the file %s' % (data_dict_path))

        in_path = os.path.join(config['root4txt'], folder)
        for file_name in os.listdir(in_path):
            screen_name = file_name.split(".txt")[0]
            if screen_name not in data_dict.keys():
                pool.apply_async(func = loadTXT, args = (in_path + "/" + file_name, screen_name,), callback=myCallback)
        pool.close()
        pool.join()





if __name__ =='__main__' :
    run4profile()
    # run4txt()



