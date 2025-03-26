#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-08-17 14:12:08
# Project: division
import logging
import random
import re
import psycopg2

from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {

    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://www.mca.gov.cn/article/sj/xzqh/2018/', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('.article >tr').items():
            title = each('.arlisttd').text()
            print(title)
            url = each.find('a').attr.href
            update_time = each('.timedefault').text()
            self.crawl(url=url, callback=self.parse_url, save={'update_time': update_time,'title':title})

    def parse_url(self, response):
        update_time = response.save['update_time']
        title=response.save['title']
        pq = response.doc('body>script:first-child').text()
        url = re.findall('window.location.href="([^\s]*)";', pq)[0]
        self.crawl(url=url, callback=self.parse_area, save={'update_time': update_time,'title':title})

    def parse_area(self, response):
        process_item = Process_item()
        update_time = response.save['update_time']
        title=response.save['title']
        print(title)
        if '行政区划代码' in title:
            for each in response.doc('tr[style="mso-height-source:userset;height:14.25pt"][height="19"]').items():
                id = each('td').eq(1).text().ljust(12, '0')
                name = each('td').eq(2).text()
                if id != "" or id!='000000000000':
                    item = {}
                    item['id'] = id
                    item['name'] = name
                    item['updated'] = update_time
                    item['flag']=0
                    item['old_id']=None
                    # process_item.save(**item)

        elif '变更情况' in title:
            reason = ""
            approval_doc = ""
            for each in response.doc('tr[style="mso-height-source:userset;height:20.1pt"]').items():
                old_id = each('td').eq(1).text()
                old_name = each('td').eq(2).text()
                # \32 018年3月县以下区划变更情况_7058 > table > tbody > tr:nth-child(30) > td.xl927058 > font:nth-child(1)

                if (each('td').eq(3).text() != None):
                    reason = each('td').eq(3).text()
                    new_id = each('td').eq(4).text()
                    new_name = each('td').eq(5).text()
                elif (each('td').eq(6).text() != None):
                    approval_doc = each('td').eq(6).text()
                else:
                    new_id = each('td').eq(3).text()
                    new_name = each('td').eq(4).text()
                    approval_doc = each('td').eq(5).text()
                item={}
                item['status']='原行政区划代码为{}改为{}，名称从{}改为{}，原因是{},批准文件为{}'.format(old_id,new_id,old_name,new_name,reason,approval_doc)
                item['old_id']=old_id
                item['id']=new_id
                item['name']=new_name
                item['flag']=1
                print(item)
                process_item.save(**item)

    def on_finished(self):
        if hasattr(self, 'conn'):
            self.conn.close()


class Process_item:
    def __init__(self):
        self.conn = psycopg2.connect(dbname="ascs", user="ascs", password="ascs.tech", host="10.10.1.213",
                                     port="5433")
        self.cursor = self.conn.cursor()

    def save(self, **item):
        try:
            if not hasattr(self, 'conn'):
                self.conn = psycopg2.connect(dbname="ascs", user="ascs", password="ascs.tech", host="10.10.1.213",
                                             port="5433")
            flag=item.pop('flag')
            table_name = 'division'
            old_id = item.pop('old_id')
            abs_id = 'id'
            col_str = ''
            row_str = ''
            sql=''
            for key in item.keys():
                col_str = col_str + " " + key + ","
                row_str = "{}'{}',".format(row_str,
                                           item[key] if "'" not in item[key] else item[key].replace("'", "\\'"))
            if flag==0:
                sql = "insert INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET  ".format(table_name,
                                                                                            col_str[1:-1],
                                                                                            row_str[:-1],
                                                                                            abs_id)
            for (key, value) in six.iteritems(item):
                sql += "{} = '{}', ".format(key, value if "'" not in value else value.replace("'", "\\'"))
            sql = sql[:-2]
            if flag == 1:
                sql = 'UPDATE {} SET {} where id={}'.format(table_name,sql,old_id)
            print(sql)
            self.cursor.execute(sql)
        except Exception as e:
            raise e
        self.conn.commit()
