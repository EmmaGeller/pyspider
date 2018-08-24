#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-08-17 14:12:08
# Project: division

import re
import psycopg2
import time

from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {
    }



    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        a = response.doc('.center_list_contlist > li:first-child >a')
        url = a.attr.href
        self.crawl(url=url, callback=self.parse_province)

    def on_finished(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    @config(priority=2)
    def parse_province(self, response):
        process_item=Process_item()
        for each in response.doc('.provincetr > td').items():
            a = each.children('a')
            url = a.attr.href
            province_name = a.text()
            province_code = re.findall('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/\d+/(\d+).html', url)[0]
            item = {}
            item['id'] = province_code.ljust(12,'0')
            item['name'] = province_name
            item['parent_id'] ='0'
            item['type'] = '1'
            item['level'] = '1'
            process_item.save(**item)
            self.crawl(url=url, callback=self.parse_city, save={'province_id': item['id']})

    def parse_city(self, response):
        process_item=Process_item()
        province_id = response.save['province_id']
        for each in response.doc('.citytr').items():
            city_node = each('td').eq(0)
            next_url = city_node.children('a').attr.href
            city_code = city_node.text()
            city_name = each('td').eq(1).text()
            item = {}
            item['name'] = city_name
            item['id'] = city_code
            item['parent_id'] = province_id
            item['type'] ='2'
            item['level'] = '2'
            process_item.save(**item)
            self.crawl(url=next_url, callback=self.parse_count, save={'city_id': item['id']})

    def parse_count(self, response):
        process_item=Process_item()
        city_id = response.save['city_id']
        for each in response.doc('.countytr').items():
            count_node = each('td').eq(0)
            next_url = count_node.children('a').attr.href
            count_code = count_node.text()
            count_name = each('td').eq(1).text()
            item = {}
            item['id'] = count_code
            item['name'] = count_name
            item['parent_id'] = city_id
            item['type'] = '3'
            item['level'] = '3'
            process_item.save(**item)
            self.crawl(url=next_url, callback=self.parse_town, save={'count_id': item['id']})

    def parse_town(self, response):
        process_item=Process_item()
        count_id = response.save['count_id']
        for each in response.doc('.towntr').items():
            town_node = each('td').eq(0)
            next_url = town_node.children('a').attr.href
            town_code = town_node.text()
            town_name = each('td').eq(1).text()
            item = {}
            item['id'] = town_code
            item['name'] = town_name
            item['parent_id'] = count_id
            item['level'] = '4'
            type_code = int(town_code[6:9])
            if (599 > type_code > 400):
                type = '4_400'
            elif (type_code > 200):
                type = '4_200'
            elif (type_code > 100):
                type = '4_100'
            elif (type_code > 1):
                type = '4_001'
            else:
                type = '500'
            item['type'] = str(type)
            process_item.save(**item)
            self.crawl(url=next_url, callback=self.parse_village, save={'town_id':item['id']})

    def parse_village(self, response):
        process_item=Process_item()
        town_id = response.save['town_id']
        for each in response.doc('.villagetr').items():
            village_node = each('td').eq(0)
            village_code = village_node.text()
            village_type = each('td').eq(1).text()
            village_name = each('td').eq(2).text()
            item = {}
            item['id'] = village_code
            item['name'] = village_name
            item['parent_id'] = town_id
            item['level'] = '5'
            item['type'] = village_type
            process_item.save(**item)

class Process_item:
    def __init__(self):
        self.conn = psycopg2.connect(dbname="************", user="*******", password="********", host="**********", port="***********")
        self.cursor = self.conn.cursor()

    def save(self,**item):
        try:
            if not hasattr(self, 'conn'):
                self.conn = psycopg2.connect(dbname="*********", user="**********", password="**********", host="********",
                                             port="5433")
            table_name = 'division'
            abs_id = 'id'
            col_str = ''
            row_str = ''
            for key in item.keys():
                col_str = col_str + " " + key + ","
                row_str = "{}'{}',".format(row_str,
                                           item[key] if "'" not in item[key] else item[key].replace("'", "\\'"))
            sql = "insert INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET ".format(table_name,
                                                                                                       col_str[1:-1],
                                                                                                       row_str[:-1],
                                                                                                       abs_id)
            for (key, value) in six.iteritems(item):
                sql += "{} = '{}', ".format(key, value if "'" not in value else value.replace("'", "\\'"))
            sql = sql[:-2]
            self.cursor.execute(sql)
        except Exception as e:
            raise e
        self.conn.commit()
