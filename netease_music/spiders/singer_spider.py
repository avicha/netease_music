import scrapy
import pymongo
import datetime


class SingerSpider(scrapy.Spider):
    name = 'singer'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.SingerPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'DOWNLOAD_DELAY': 0.2
    }

    def start_requests(self):
        self.singer_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        week = datetime.timedelta(days=7)
        q = self.db.singers.find({'$or': [{'status': 1, 'source': 1}, {'status': 2, 'source': 1, 'updated_at': {'$lte': datetime.datetime.now() - week}}]})
        for x in q:
            self.singer_num += 1
            yield scrapy.Request('http://music.163.com/artist/desc?id=%s' % x.get('remote_id'), self.parse)

    def parse(self, response):
        name = response.css('#artist-name::text').extract_first()
        alias = response.css('#artist-alias::text').extract_first()
        artist = {
            'name': name.strip('\n') if name else None,
            'alias': alias.strip('\n') if alias else None,
            'avatar': response.css('.n-artist img::attr(src)').extract_first(),
            'description': response.css('.n-artdesc').extract_first(),
            'remote_id': response.css('#artist-name::attr(data-rid)').extract_first(),
            'source': 1,
            'status': 2,
            'updated_at': datetime.datetime.now()
        }
        yield artist

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,singer number %s', reason, self.singer_num)
