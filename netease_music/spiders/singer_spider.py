import scrapy
import pymongo
import datetime
from bs4 import BeautifulSoup


class SingerSpider(scrapy.Spider):
    name = 'singer'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.SingerPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'DOWNLOAD_DELAY': 0,
        'LIMIT': 10000
    }

    def start_requests(self):
        self.singer_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        week = datetime.timedelta(days=7)
        q = self.db.singers.find({'name': None}).limit(self.settings.get('LIMIT'))
        for x in q:
            self.singer_num += 1
            yield scrapy.Request('http://music.163.com/artist/desc?id=%s' % x.get('remote_id'), self.parse, meta={'singer_id': x.get('remote_id')})

    def parse(self, response):
        try:
            soup = BeautifulSoup(response.text, 'html5lib')
            name_dom = soup.select_one('#artist-name')
            alias_dom = soup.select_one('#artist-alias')
            avatar_dom = soup.select_one('.n-artist img')
            desc_dom = soup.select_one('.n-artdesc')
            artist = {
                'name': name_dom.get_text().strip('\n') if name_dom else None,
                'alias': alias_dom.get_text().strip('\n') if alias_dom else None,
                'avatar': avatar_dom.get('src') if avatar_dom else None,
                'description': desc_dom.get_text() if desc_dom else None,
                'remote_id': response.meta.get('singer_id'),
                'source': 1,
                'status': 2,
                'updated_at': datetime.datetime.now()
            }
            yield artist
        except Exception as e:
            raise e

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,singer number %s', reason, self.singer_num)
