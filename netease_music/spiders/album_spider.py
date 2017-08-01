# coding=utf-8
import scrapy
from store import client, db
import datetime
from bs4 import BeautifulSoup


class AlbumSpider(scrapy.Spider):
    name = 'album'
    custom_settings = {
        'ITEM_PIPELINES': {'netease_music.pipelines.AlbumPipeline': 300},
        'DOWNLOAD_DELAY': 0
    }

    def __init__(self, limit=10000, *args, **kwargs):
        super(AlbumSpider, self).__init__(*args, **kwargs)
        self.limit = int(limit)

    def start_requests(self):
        self.song_num = 0
        self.client = client
        self.db = db
        q = self.db.albums.find({'status': 1, 'source': 1}).limit(self.limit)
        for x in q:
            yield scrapy.Request('http://music.163.com/album?id=%s' % x.get('remote_id'), self.parse, meta={'album_id': x.get('remote_id'), 'remote_singer_id': x.get('remote_singer_id'), 'singer_name': x.get('singer_name')})

    def parse(self, response):
        try:
            soup = BeautifulSoup(response.text, 'html5lib')
            publish_time = response.xpath(u'//p[contains(.,"发行时间")]/text()').extract_first()
            publish_company = response.xpath(u'//p[contains(.,"发行公司")]/text()').extract_first()
            if response.css('#album-desc-more').extract_first():
                description = '<br>&nbsp;&nbsp;'.join(map(lambda x: x.strip('\n'), response.css('#album-desc-more .f-brk::text').extract()))
            else:
                description = response.css('.n-albdesc .f-brk::text').extract_first()
            songListPreCache = soup.select_one('#song-list-pre-cache textarea').get_text()
            alias = None
            if songListPreCache:
                songs = []
                import json
                songListPreCache = json.loads(songListPreCache)
                for x in songListPreCache:
                    album = x.get('album')
                    songs.append({
                        'name': x.get('name'),
                        'alias': x.get('alias'),
                        'duration': x.get('duration'),
                        'artists': x.get('artists'),
                        'remote_id': str(x.get('id')),
                        'remote_album_id': str(album.get('id')),
                        'remote_singer_id': response.meta.get('remote_singer_id'),
                        'album_name': album.get('name'),
                        'singer_name': response.meta.get('singer_name'),
                        'source': 1
                    })
                    if not alias and album.get('alia'):
                        alias = ','.join(album.get('alia'))
            else:
                songs = []
            album = {
                'alias': alias,
                'publish_time': publish_time.strip('\n') if publish_time else None,
                'publish_company': publish_company.strip('\n') if publish_company else None,
                'description': description.strip('\n') if description else None,
                'song_num': len(songs),
                'songs': songs,
                'remote_id': response.meta.get('album_id'),
                'source': 1,
                'status': 2,
                'updated_at': datetime.datetime.now()
            }
        except Exception as e:
            print e
            album = {
                'alias': '',
                'remote_id': response.meta.get('album_id'),
                'source': 1,
                'status': -1,
                'updated_at': datetime.datetime.now(),
                'songs': []
            }
        finally:
            yield album

    def closed(self, reason):
        self.client.close()
        self.logger.info('spider closed because %s,song number %s', reason, self.song_num)
