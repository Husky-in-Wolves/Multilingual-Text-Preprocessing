import difflib, csv, numpy, time
from basicAlgorithm import baidu_translate, google_translate
import yaml, os
from time import sleep


with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)


def myCallback(userD, dataD, user_dict_path, data_dict_path):
    if not os.path.isfile(user_dict_path):  user_dict = {}
    else: user_dict = numpy.load(user_dict_path).item(0)
    if not os.path.isfile(data_dict_path): data_dict = {}
    else: data_dict = numpy.load(data_dict_path).item(0)
    user_dict.update(userD)
    for uid in dataD.keys():
        if uid not in data_dict.keys():
            data_dict[uid] = {}
        data_dict[uid].update(dataD[uid])
    numpy.save(user_dict_path, user_dict)
    numpy.save(data_dict_path, data_dict)


def reLoadTXT(line, file_path):
    with open(file_path + ".txt", "a", encoding='UTF-8', errors='ignore') as f:
        string = "\t".join(line)
        f.write(string)
        f.write('\n')

def loadCSV(dir_path, file_name, user_dict_path, data_dict_path):
    try:
        haveTrans_dict = numpy.load("haveTrans_dict_%s.npy" % (str(file_name).split('.csv')[0])).item(0)
        print('finish load the haveTrans_dict.')
    except:
        haveTrans_dict = {}
        print("dooooo not exist the haveTrans_dict_%s.npy" % (str(file_name).split('.csv')[0]))

    userD, dataD = {}, {}
    with open(dir_path + '/' + file_name, 'r', encoding='UTF-8', errors='ignore') as file:
        csv_file = csv.reader(list(map(lambda line: line.replace('\0', ''), file)))     # to avoid the null bytes error
        for i, line in enumerate(csv_file):
            if i%(5*10**5) == 1:
                myCallback(userD, dataD, user_dict_path, data_dict_path)
                userD, dataD = {}, {}
                print(file_name, 'the %d-th line has been recorded.' % (i))

            ''' record the informations '''
            user_id, screen_name, description, special = line[1], str(line[3]).strip(), line[5], True
            if screen_name not in userD.keys() and screen_name not in dataD.keys():
                userD[screen_name], dataD[screen_name] = {'description': description, 'special': special}, {}
            ''' record the tweets '''
            lang, tweet, timestr = line[11], str(line[12]).strip(" "), line[13]
            try:
                timeArray = time.strptime(timestr, "%Y-%m-%d %H:%M")
                timestamp = int(time.mktime(timeArray))
            except:
                continue
            else:
                if lang == 'en' and len(tweet) > 10:
                    dataD[screen_name][timestamp] = tweet
                elif lang != 'en' and lang in config['lang4translate'] and len(tweet) > 10:
                    try:
                        t = haveTrans_dict[tweet]
                    except:
                        if not os.path.exists(dir_path + "_new/"): os.mkdir(dir_path + "_new/")
                        reLoadTXT(line, dir_path + "_new/" + str(file_name).split('.txt')[0])
                    else:
                        ''' save the english tweets, the similarity of tweet and translated one should be low than 0.5 '''
                        ratio = difflib.SequenceMatcher(None, tweet, haveTrans_dict[tweet]).quick_ratio()
                        if haveTrans_dict[tweet] != '' and ratio < 0.5:
                            dataD[screen_name][timestamp] = haveTrans_dict[tweet]
                        else:
                            haveTrans_dict[tweet] = ''
    myCallback(userD, dataD, user_dict_path, data_dict_path)



def run4csv():
    import multiprocessing
    pool = multiprocessing.Pool(7)
    for i, folder in enumerate(os.listdir(config['root4csv'])):
        in_path = os.path.join(config['root4csv'], folder)
        dir_path = os.path.join(config['root4npy'], folder)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        for file in os.listdir(in_path):
            user_dict_path = dir_path + "/%s_user_dict.npy" % (file)
            data_dict_path = dir_path + "/%s_data_dict.npy" % (file)
            # loadCSV(in_path, file, min_time_stamps[folder], user_dict_path, data_dict_path)
            pool.apply_async(func = loadCSV, args = (in_path, file, user_dict_path, data_dict_path))
    pool.close()
    pool.join()


if __name__ =='__main__' :
    run4csv()