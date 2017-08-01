import scrapy
from store import client, db
import datetime


class AlbumIdSpider(scrapy.Spider):
    name = 'album_id'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.AlbumIdPipeline': 300},
        'DOWNLOAD_DELAY': 0,
        'PAGE_SIZE': 1000
    }

    def __init__(self, limit=10000, *args, **kwargs):
        super(AlbumIdSpider, self).__init__(*args, **kwargs)
        self.limit = int(limit)

    def start_requests(self):
        self.album_num = 0
        self.client = client
        self.db = db
        q = self.db.singers.find({'status': 2, 'source': 1}).limit(self.limit)
        for x in q:
            singer_id = x.get('remote_id')
            yield scrapy.Request('http://music.163.com/artist/album?id=%s&limit=%s&offset=0' % (singer_id, self.settings.get('PAGE_SIZE')), self.parse, meta={'remote_singer_id': singer_id, 'singer_name': x.get('name')})
            self.db.singers.update_one({'remote_id': singer_id, 'source': 1}, {'$set': {'status': 3}})

    def parse(self, response):
        for href in response.css('.u-page .zpgi:not(.js-selected)::attr(href)'):
            yield response.follow(href, callback=self.parse)
        for li in response.css('#m-song-module li'):
            self.album_num += 1
            yield {
                'name': li.css('.s-fc0::text').extract_first(),
                'folder': li.css('img::attr(src)').extract_first(),
                'remote_id': li.css('.s-fc0::attr(href)').re_first(r'id=(\d+)'),
                'remote_singer_id': response.meta.get('remote_singer_id'),
                'singer_name': response.meta.get('singer_name'),
                'source': 1
            }

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,album number %s', reason, self.album_num)
