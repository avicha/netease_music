import scrapy
import pymongo
import datetime
import json


class LyricSpider(scrapy.Spider):
    name = 'lyric'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.LyricPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'DOWNLOAD_DELAY': 0.1,
        'LIMIT': 10000
    }

    def start_requests(self):
        self.lyric_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        week = datetime.timedelta(days=7)
        q = self.db.songs.find({'status': 1, 'source': 1}).limit(self.settings.get('LIMIT'))
        for x in q:
            yield scrapy.Request('http://music.163.com/api/song/media?id=%s' % x.get('remote_id'), self.parse, meta={'song_id': x.get('remote_id')})

    def parse(self, response):
        result = json.loads(response.text)
        yield {
            'lyric': result.get('lyric'),
            'status': -1 if result.get('songStatus') == -1 else 2,
            'remote_id': response.meta.get('song_id'),
            'source': 1,
            'updated_at': datetime.datetime.now()
        }

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,lyric number %s', reason, self.lyric_num)
