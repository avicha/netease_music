import scrapy
import re
import pymongo


class SingerIdSpider(scrapy.Spider):
    name = 'singer_id'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.SingerIdPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'COLLECTION_NAME': 'singers',
        'DOWNLOAD_DELAY': 0
    }

    def start_requests(self):
        self.singer_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        self.collection = self.db[self.settings.get('COLLECTION_NAME')]
        yield scrapy.Request('http://music.163.com/discover/artist/cat?id=1001&initial=0', self.parse)

    def parse(self, response):
        for href in response.css('#initial-selector li a::attr(href)'):
            yield response.follow(href, callback=self.parse)
        for a in response.css('#m-artist-box li .s-fc0'):
            self.singer_num += 1
            text = a.xpath('./text()').extract_first()
            artist_id = a.xpath('./@href').re_first(r'id=(\d+)')
            yield {
                'name': text,
                'remote_id': artist_id,
                'source': 1
            }

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,singer number %s', reason, self.singer_num)
