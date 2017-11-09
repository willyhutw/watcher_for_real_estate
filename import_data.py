#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import csv
import json
import logging
import datetime
from zipfile import ZipFile
from elasticsearch import Elasticsearch
from elasticsearch import helpers


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
logging.basicConfig(filename='import_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


CITIES = {
    'A': u'臺北市',
    'B': u'臺中市',
    'C': u'基隆市',
    'D': u'臺南市',
    'E': u'高雄市',
    'F': u'新北市',
    'G': u'宜蘭縣',
    'H': u'桃園市',
    'I': u'嘉義市',
    'J': u'新竹縣',
    'K': u'苗栗縣',
    'M': u'南投縣',
    'N': u'彰化縣',
    'O': u'新竹市',
    'P': u'雲林縣',
    'Q': u'嘉義縣',
    'T': u'屏東縣',
    'U': u'花蓮縣',
    'V': u'臺東縣',
    'W': u'金門縣',
    'X': u'澎湖縣',
    'Z': u'連江縣'
}


def del_indices(name):
    logging.info("=== Check and delete exist index : " + name)
    if es.indices.exists(name):
        logging.info("=== Index exist, deleting index : " + name)
        res = es.indices.delete(name)
        logging.info(res)


def update_template(json_file, name):
    payload = {}
    logging.info("=== Update template for index : " + name)
    with open(json_file) as index_template:
        logging.info("=== Parsing template json file : " + json_file)
        payload = json.loads(index_template.read())
    logging.info("=== Puting template for index : " + name)
    res = es.indices.put_template(name, body=payload)
    logging.info(res)


def unzip_data_files(target_dir):
    logging.info("=== Unzip data files in " + target_dir)
    pwd = os.getcwd()
    for re_path in glob.glob(target_dir + '/*.zip'):
        with ZipFile(re_path, 'r') as zip_file:
            logging.info("=== Unzipping file : " + re_path)
            zip_file.extractall(pwd + '/' + re_path[:-4])


def list_files(target_dir):
    files = []
    for d in os.listdir(target_dir):
        if os.path.isdir(target_dir + '/' + d):
            for re_path in glob.glob(target_dir + '/' + d +'/[A-Za-z]_*_*_[A-Za-z].CSV'):
                files.append(re_path)
    logging.info("=== Raw files in " + target_dir)
    logging.info(files)
    return files


def import_to_es(re_path):
    logging.info("=== Reading and parsing raw data of " + re_path)
    with open(re_path, 'r') as csvfile:
        types = re_path.upper()[-5:-4]
        city_code = re_path.upper()[-16:-15]
        csv_rows = csv.reader(csvfile, delimiter=',')
        header = csv_rows.next()
        actions = []
        for row in csv_rows:
            doc = {}
            for h, v in zip(header, row):
                if isinstance(h, str):
                    h = h.decode('big5', 'ignore')
                if isinstance(v, str):
                    v = v.decode('big5', 'ignore')
                if h in [u'交易年月日', u'建築完成年月', u'租賃年月日']:
                    v = t_date_parse(t_date=v.strip())
                doc[h] = v
                doc[u'縣市'] = CITIES[city_code]
                doc['@timestamp'] = datetime.datetime.utcnow()
            action = {}
            action['_op_type'] = 'index'
            action['_index'] = 'real_estate'
            action['_type'] = types
            action['_source'] = doc
            actions.append(action)
        logging.info("=== Inserting data ...")
        res = helpers.bulk(es, actions)
        logging.info(res)


def t_date_parse(t_date):
    date = datetime.date.min
    if t_date != '' and len(t_date) == 7:
        year = int(t_date[:3]) + 1911
        month = int(t_date[3:5])
        day = int(t_date[5:7])
        try:
            date = datetime.date(year, month, day)
        except Exception as e:
            logging.error("Error while parsing t_date : " + t_date)
            logging.error(e)
    return date


def main():
    del_indices(name='real_estate')
    update_template(json_file='index_template.json', name='template_real_estate')
    unzip_data_files(target_dir='data')
    files = list_files(target_dir='data')
    for re_path in files:
        import_to_es(re_path=re_path)


if __name__ == '__main__':
    main()
