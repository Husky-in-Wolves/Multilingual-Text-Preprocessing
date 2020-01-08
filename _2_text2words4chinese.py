import numpy as np, re, yaml
import jieba, os
jieba.load_userdict("myWord.txt",)



with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)
with open('stoplist-zh.txt') as stopfile:
    stopwords = [str(l).strip("\n").strip(" ") for l in stopfile.readlines()]




PNN4gun= {"枪支型号": ["火狐","骚本","捷克","山猫","雷神","HK50","hk50","秃鹰","飞龙","秃子","ed","ED","鲁格","仿戴安娜","戴安娜","黛安娜","绿瓦","仿绿","兔子","气狗","hk","HK"],
          "枪支性能": ["口径","威力","危力","初速","射程","制式","高精度","准确度","回压","内径","外径","精确度","五碗","精准度","膛线"],
          "枪支部件":["缩口","弹托","双缩口","固钉器","退壳器","脚架","汽瓶","气室","气筒","击针","击簧","恒压阀","打气筒","狙击镜","瞄准镜","活塞杆","扳机","握把","管子","消声器","盖板孔","燕尾","套件","夹具","狗包","瓶套","相机架","固定环","瓶消瞄阀","阀消瓶套","消瓶阀","护木弧圈","瓶阀","枪管","钢炮管","消音器","激光","红外","狙击架","红外线"],
          "枪支类型": ["气枪","散弹枪","猎枪","手枪","射钉枪","钉枪","手枪","板球","军用枪"],
          "持枪方式": ["中握","后握","狙击"],
          "射击方式": ["五连","连点","连发","几发"],
          "枪械": ["枪械","整枪","整秃","枪"],
          "子弹类型": ["弹头","弹壳","发射药","水泥钉","射钉","子弹","弹夹","钢珠","铅弹","狗粮","母鸡","蛋蛋","粮食"],
          "射击猎物": ["斑鸠","野鸡","兔子","鸽子","蜥蜴","鳄鱼","狐狸","野猪"],
          "收货地址": ["省","市","镇","区","县","街道","街","地址","小区","单元","室","路"]}



def is_uchar(uchar):
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5': return True
    """判断一个unicode是否是数字"""
    if uchar >= u'\u0030' and uchar<=u'\u0039': return True
    """判断一个unicode是否是英文字母"""
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):  return True
    return False

def rmChar(data):
    http_pattern = re.compile(r'[a-zA-Z]*[*%]?http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    data = http_pattern.sub("", data)
    www_pattern = re.compile(r'[a-zA-Z]*www[.]?([a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    data = www_pattern.sub("", data)
    tag_pattern = re.compile("([\[【]([^\]】])*[】\]])")
    data = tag_pattern.sub("", data)
    and_pattern = re.compile("&(\S)+;+")
    data = and_pattern.sub("", data)
    at_pattern = re.compile("^@(.*?)[\,,\，,\s,\。,]$")
    data = at_pattern.sub("", data)
    chat_pattern = re.compile('([qQ]{1,2}[:：群]?[0-9]*)|(,[FT])')
    data = chat_pattern.sub(" ", data)

    # 删除为空或者较短的数据，最小句长为5
    data_nochar = [c for c in data if is_uchar(c) == True]
    data = "".join(data_nochar)
    char_pattern = re.compile("([0-9]{5,})|([0-9a-zA-Z]{4,})")
    data = char_pattern.sub("", data)
    return data

''' replace number by <num> '''
def rpNum(word_list):
    num_pattern = re.compile("[0-9]+")
    for i, w in enumerate(word_list):
        w = num_pattern.sub("<num>", w)
        word_list[i] = w
    return word_list


def label_repeat(seg_nostop, lable = PNN4gun):
    for key in lable:
        if len(set(seg_nostop) & set(lable[key])) > 0:
            seg_nostop.extend(list(set(seg_nostop) & set(lable[key])))
            seg_nostop.append(key)
    return seg_nostop

def tokenize(sentence):
    #中文字符大于5个
    if len(re.findall(u'[\u4e00-\u9fff]+', sentence)) < 5:
        return ""
    ''' clear char '''
    sentence = rmChar(sentence)
    "remove stopwords"
    for sw in stopwords:
        sentence = sentence.replace(str(sw), "")
    ''' tokenize '''
    word_list = list(jieba.cut(sentence))         #seg = pynlpir.segment(data, pos_tagging=False)
    ''' replace number by <num> '''
    word_list = rpNum(word_list)

    # 合并相邻的两个单字
    i = 0
    while i + 1 < len(word_list):
        if len(word_list[i]) <= 1 and len(word_list[i + 1]) <= 1:
            word_list[i] = word_list[i] + word_list[i + 1]
            word_list.pop(i + 1)
        i += 1
    for i, w in enumerate(word_list):
        if w.startswith("h"):
            word_list[i] = ""

    #将关键词重复并且打标签
    label_word_list=label_repeat(word_list)
    if len(set(label_word_list)) <= 5:   return ""
    else:   return " ".join(label_word_list)









if __name__ =='__main__' :
    Event = "zh-gun"
    in_dir = str(os.path.join(config['root4npy'], Event))
    out_dir = str(os.path.join(config['root4LDA'], Event))
    data_dict = dict(np.load(in_dir + "/data_dict.npy").item(0))
    label_dict = dict(np.load(in_dir + "/label_dict.npy").item(0))
    # Sess_dict = dict(np.load(in_dir + "/Sess_dict.npy").item(0))

    uid_list = list(data_dict.keys())
    for uid in uid_list:
        sentence_list = []
        sentence_dict = {ts: tokenize(data_dict[uid][ts]) for ts in data_dict[uid]}
        for ts in list(sentence_dict.keys()):
            if len(sentence_dict[ts]) >= 5: sentence_list.append(sentence_dict[ts])
            else: data_dict[uid].pop(ts)
        if len(sentence_list) > 0 and label_dict[uid] == False:
            out_folder = out_dir + "/common-messages-" + Event
            if not os.path.exists(out_folder):  os.mkdir(out_folder)
            with open(out_folder + "/" + uid + ".txt", 'w', encoding="UTF-8", errors='ignore') as file:
                file.write("\n".join(sentence_list))
        elif len(sentence_list) > 0:
            out_folder = out_dir + "/special-messages-" + Event
            if not os.path.exists(out_folder):  os.mkdir(out_folder)
            with open(out_folder + "/" + uid + ".txt", 'w', encoding="UTF-8", errors='ignore') as file:
                file.write("\n".join(sentence_list))
        else:
            data_dict.pop(uid)

    np.save(in_dir + "/data_dict.npy", data_dict)


