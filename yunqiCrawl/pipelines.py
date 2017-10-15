# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import re
from yunqiCrawl.items import YunqiBookDetailItem,YunqiBookListItem


class YunqicrawlPipeline(object):

    def __init__(self, mongo_uri, mongo_db, replicaset):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.replicaset = replicaset

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'yunqi'),
            replicaset=crawler.settings.get('REPLICASET')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri, replicaset=self.replicaset)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, YunqiBookListItem):
            self._process_booklist_item(item)
        else:
            self._process_bookDetail_item(item)
        return item

    def _process_booklist_item(self, item):
        """
        处理小说信息
        :param item:
        :return:
        """
        self.db.bookInfo.insert(dict(item))

    def _process_bookDetail_item(self, item):
        """
        处理小说热度
        :param item:
        :return:
        """
        item['novelLabel'] = item['novelLabel'].strip().replace('\n','')

        for key, value in item.items():
            if u'Click' in key or u'Popular' in key or u'Comm' in key:
                value = self._process_item_num(value)

    def _process_item_num(self, content):
        pattern = re.compile(r'\d+')
        match = pattern.search(content)
        return match.group() if match else content