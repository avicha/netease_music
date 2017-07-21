import scrapy
import re
import pymongo
import datetime
from netease_music.items import SingerItem


class SingerSpider(scrapy.Spider):
    name = 'singer'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.SingerPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'COLLECTION_NAME': 'singers',
        'DOWNLOAD_DELAY': 0.25
    }

    def start_requests(self):
        self.singer_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        self.collection = self.db[self.settings.get('COLLECTION_NAME')]
        week = datetime.timedelta(days=7)
        q = self.collection.find({'$or': [{'status': 1, 'source': 1}, {'status': 0, 'source': 1, 'updated_at': {'$lte': datetime.datetime.now() - week}}]})
        for x in q:
            self.singer_num += 1
            yield scrapy.Request('http://music.163.com/artist/desc?id=%s' % x.get('remote_id'), self.parse)
        print 'find %s singer ids.' % self.singer_num

    def parse(self, response):
        artist = {
            'name': response.css('#artist-name::text').extract_first(),
            'alias': response.css('#artist-alias::text').extract_first().replace('\n', ''),
            'avatar': response.css('.n-artist img::attr(src)').extract_first(),
            'description': response.css('.n-artdesc').extract_first(),
            'remote_id': response.css('#artist-name::attr(data-rid)').extract_first(),
            'source': 1,
            'status': 2
        }
        singer_item = SingerItem(**artist)
        yield singer_item

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,singer number %s', reason, self.singer_num)
