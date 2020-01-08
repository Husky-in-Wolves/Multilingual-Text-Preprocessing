from urllib import request, parse
import requests, hashlib, http.client, urllib
import sys, random, json, re, yaml
from time import sleep
import difflib

with open('config.yaml') as fd_conf:
    config = yaml.load(fd_conf, Loader = yaml.SafeLoader)

ip = "222.189.190.139:9999"
Request_URL = "http://fanyi.youdao.com/translate?smartresult=dict&smartresult=rule"
form_data = {}
form_data['i'] = "hello"
form_data['from'] = 'AUTO'
form_data['to'] = 'AUTO'
form_data['smartresult'] = 'dict'
form_data['doctype'] = 'json'
form_data['version'] = '2.1'
form_data['keyfrom'] = 'fanyi.web'
form_data['action'] = 'FY_BY_CLICKBUTTION'
form_data['typoResult'] = 'false'


handler_list=['Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
              "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
              'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
              'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
              'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11']


''' translate a minority language into English '''
def judLanguage(sentence):
    zh_pattern = u'[\u4e00-\u9fff]+'
    fr_pattern = u'[\u00C0-\u00FF]+'
    ru_pattern = u'[\u0400-\u052f]+'
    jpn_pattern = u'[\u0800-\u4e00]+'
    kr_pattern = u'[\uac00-\ud7a3]+'
    if len(re.findall(zh_pattern, sentence)) > 0: return 'zh'
    elif len(re.findall(fr_pattern, sentence)) > 0: return 'fr'
    elif len(re.findall(ru_pattern, sentence)) > 0: return 'ru'
    elif len(re.findall(jpn_pattern, sentence)) > 0: return 'ja'
    elif len(re.findall(kr_pattern, sentence)) > 0: return 'ko'
    else: return "en"


def youdao_translate(ip, proxies_list, sentence):
    ERR, maxCount, translate_res = True, 10, ""
    while ERR and maxCount > 0:
        proxies, handler = {"http": ip}, [('User-Agent', random.choice(handler_list))]
        proxy_support = request.ProxyHandler(proxies)
        opener = request.build_opener(proxy_support)
        opener.add_handler = handler
        request.install_opener(opener)
        try:
            ''' auto to zh  '''
            form_data['i'], form_data['from'], form_data['to'] = sentence, "AUTO", "zh"
            data = parse.urlencode(form_data).encode('utf-8')
            response = request.urlopen(Request_URL, data, timeout=2)
            translate_results = json.loads(response.read().decode('utf-8'))
            zh_translate = translate_results["translateResult"][0][0]['tgt']
            ''' zh to en '''
            form_data['i'], form_data['from'], form_data['to'] = zh_translate, 'zh', 'en'
            data = parse.urlencode(form_data).encode('utf-8')
            response = request.urlopen(Request_URL, data, timeout=2)
            translate_results = json.loads(response.read().decode('utf-8'))
            en_translate = translate_results["translateResult"][0][0]['tgt']
        except:
            try:
                ind = list(proxies_list).index(ip)
            except:
                continue
            else:
                list(proxies_list).pop(ind)
            maxCount -= 1
            ip = random.choice(proxies_list)
            print("the original ip is error, the next is ", ip)
        else:
            ''' save the english tweets, the similarity of tweet and translated one should be low than 0.5 '''
            ratio = difflib.SequenceMatcher(None, sentence, en_translate).quick_ratio()
            if ratio < 0.5 and judLanguage(en_translate) == 'en':
                print("from", sentence, 'to', en_translate)
                return ip, proxies_list, en_translate
    return ip, proxies_list, ""





def baidu_translate(token, content, from_lang = 'auto', to_lang = 'en'):
    content = content.lower().replace('\n', ' ')
    httpClient, dst = None, None
    salt = random.randint(32768, 65536)
    sign = token['appid'] + content + str(salt) + token['secretKey']
    sign = hashlib.md5(sign.encode()).hexdigest()
    myurl = config['baidu_translate'] + '?appid=' + token['appid'] + '&q=' + urllib.parse.quote(content) \
            + '&from=' + from_lang + '&to=' + to_lang + '&salt=' + str(salt) + '&sign=' + sign
    try:
        httpClient = http.client.HTTPConnection('api.fanyi.baidu.com', timeout=2.5)
        httpClient.request('GET', myurl)
        response = httpClient.getresponse()
        result_all = response.read().decode("utf-8")
        result = json.loads(result_all)
        trans_result = result['trans_result'][0]
        src, dst = trans_result['src'], trans_result['dst']
    except Exception as e:
        return 'error'
    finally:
        if httpClient: httpClient.close()
        if dst != None and len(dst) > 10:
            ''' save the english tweets, the similarity of tweet and translated one should be low than 0.5 '''
            ratio = difflib.SequenceMatcher(None, content, dst).quick_ratio()
            if ratio < 0.5 and judLanguage(dst) == 'en':
                # print('from', content, 'to', dst)
                return dst
        return ''


def google_translate(token, content, from_lang = 'auto', to_lang = 'en'):
    from GoogleFreeTrans import Translator
    content = content.lower().replace('\n', ' ')
    try:
        translator = Translator.translator(src=from_lang, dest=to_lang)
        dst = translator.translate(content)
    except:
        print('from', content, 'to', 'error')
    else:
        if dst != None and len(dst) > 10:
            ''' save the english tweets, the similarity of tweet and translated one should be low than 0.5 '''
            ratio = difflib.SequenceMatcher(None, content, dst).quick_ratio()
            if ratio < 0.5 and judLanguage(dst) == 'en':
                # print('from', content, 'to', dst)
                return dst
    return ""

def freeTranslate(token, content):
    import BaiduTranslate
    translator = BaiduTranslate.Dict()
    try:
        json = translator.dictionary(content, dst='en')
        dst = json['trans_result']['data'][0]['dst']
    except:
        return "error"
    else:
        if dst != None and len(dst) > 10:
            ''' save the english tweets, the similarity of tweet and translated one should be low than 0.5 '''
            ratio = difflib.SequenceMatcher(None, content, dst).quick_ratio()
            if ratio < 0.5 and judLanguage(dst) == 'en':
                # print('from', content, 'to', dst)
                return dst
    return ""



if __name__ =='__main__' :
    baidu_translate('写了几个正则，发现也很不理想')


