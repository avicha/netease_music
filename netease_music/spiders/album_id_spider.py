import scrapy
import pymongo
import datetime


class AlbumIdSpider(scrapy.Spider):
    name = 'album_id'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.AlbumIdPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'DOWNLOAD_DELAY': 1,
        'PAGE_SIZE': 100
    }

    def start_requests(self):
        self.album_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        week = datetime.timedelta(days=7)
        q = self.db.singers.find({'$or': [{'status': 2, 'source': 1}, {'status': 3, 'source': 1, 'updated_at': {'$lte': datetime.datetime.now() - week}}]})
        for x in q:
            yield scrapy.Request('http://music.163.com/artist/album?id=%s&limit=%s&offset=0' % (x.get('remote_id'), self.settings.get('PAGE_SIZE')), self.parse)
            singer_album_num = self.db.albums.count({'remote_singer_id': x.get('remote_id')})
            self.db.singers.update_one({'remote_id': x.get('remote_id')}, {'$set': {'status': 3, 'album_num': singer_album_num, 'updated_at': datetime.datetime.now()}})

    def parse(self, response):
        singer_id = response.css('#m-song-module::attr(data-id)').extract_first()
        for href in response.css('.u-page .zpgi:not(.js-selected)::attr(href)'):
            yield response.follow(href, callback=self.parse)
        for li in response.css('#m-song-module li'):
            self.album_num += 1
            yield {
                'title': li.css('.s-fc0::text').extract_first(),
                'folder': li.css('img::attr(src)').extract_first(),
                'remote_id': li.css('.s-fc0::attr(href)').re_first(r'id=(\d+)'),
                'remote_singer_id': singer_id,
                'source': 1
            }

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,album number %s', reason, self.album_num)
