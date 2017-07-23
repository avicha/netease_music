import scrapy
import pymongo
import datetime


class AlbumSpider(scrapy.Spider):
    name = 'album'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.AlbumPipeline': 300},
        'MONGO_URI': 'localhost:27017',
        'MONGO_DATABASE': 'smart_tv',
        'DOWNLOAD_DELAY': 0.2
    }

    def start_requests(self):
        self.song_num = 0
        self.client = pymongo.MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client[self.settings.get('MONGO_DATABASE')]
        week = datetime.timedelta(days=7)
        q = self.db.albums.find({'$or': [{'status': 1, 'source': 1}, {'status': 2, 'source': 1, 'updated_at': {'$lte': datetime.datetime.now() - week}}]})
        for x in q:
            yield scrapy.Request('http://music.163.com/album?id=%s' % x.get('remote_id'), self.parse)

    def parse(self, response):
        publish_time = response.css('.intr:nth-child(3)::text').extract_first()
        publish_company = response.css('.intr:nth-child(4)::text').extract_first()
        if response.css('#album-desc-more').extract_first():
            description = '<br>&nbsp;&nbsp;'.join(map(lambda x: x.strip('\n'), response.css('#album-desc-more .f-brk::text').extract()))
        else:
            description = response.css('.f-brk').extract_first()
        songListPreCache = response.css('#song-list-pre-cache textarea::text').extract_first()
        if songListPreCache:
            songs = []
            import json
            songListPreCache = json.loads(songListPreCache)
            for x in songListPreCache:
                songs.append({
                    'name': x.get('name'),
                    'alias': x.get('alias'),
                    'duration': x.get('duration'),
                    'artists': x.get('artists'),
                    'remote_id': str(x.get('id')),
                    'remote_album_id': str(x.get('album').get('id')),
                    'source': 1
                })
        else:
            songs = []
        album = {
            'publish_time': publish_time.strip('\n') if publish_time else None,
            'publish_company': publish_company.strip('\n') if publish_company else None,
            'description': description.strip('\n') if description else None,
            'song_num': len(songs),
            'remote_id': response.css('#content-operation::attr(data-rid)').extract_first(),
            'source': 1,
            'status': 2,
            'updated_at': datetime.datetime.now(),
            'songs': songs
        }
        yield album

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,song number %s', reason, self.song_num)
