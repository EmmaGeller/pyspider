#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-08-23 05:56:15
# Project: deals

from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://ris.szpl.gov.cn/credit/showcjgs/ysfcjgs.aspx?cjType=0', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        td = response.doc('#ctl00_ContentPlaceHolder1_updatepanel1>table>tr:nth-child(2)>td:nth-child(2)')
        for each in td('a').items():
            self.crawl('http://ris.szpl.gov.cn/credit/showcjgs/ysfcjgs.aspx?cjType=0', fetch_type='js', js_script="""
                   function() {
                     setTimeout("$('.
                     """
                      + each.attr.id +
                     """
                     ').click()", 1000);                                                                                 
                     }""",callback=self.detail_page)

            print(each.attr.id)

    @config(priority=2)
    def detail_page(self, response):
        response.doc().html()
