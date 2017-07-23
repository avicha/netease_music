import datetime
from scrapy.exceptions import DropItem


class SingerPipeline(object):

    def process_item(self, item, spider):
        spider.db.singers.update_one({'remote_id': item.get('remote_id'), 'source': item.get('source')}, {'$set': item}, upsert=True)
        return item


class AlbumPipeline(object):

    def process_item(self, item, spider):
        songs = item.get('songs')
        del item['songs']
        spider.db.albums.update_one({'remote_id': item.get('remote_id'), 'source': item.get('source')}, {'$set': item}, upsert=True)
        for song_data in songs:
            song = spider.db.songs.find_one({'remote_id': song_data.get('remote_id'), 'source': song_data.get('source')})
            if not song:
                song_data.update({'created_at': datetime.datetime.now(), 'updated_at': datetime.datetime.now(), 'deleted_at': None, 'status': 1})
                spider.db.songs.insert_one(song_data)
        return item


class SingerIdPipeline(object):

    def process_item(self, item, spider):
        singer = spider.db.singers.find_one({'remote_id': item.get('remote_id'), 'source': item.get('source')})
        if not singer:
            item.update({'created_at': datetime.datetime.now(), 'updated_at': datetime.datetime.now(), 'deleted_at': None, 'status': 1})
            spider.db.singers.insert_one(item)
            return item
        else:
            raise DropItem("Duplicate singer found: %s" % item)


class AlbumIdPipeline(object):

    def process_item(self, item, spider):
        album = spider.db.albums.find_one({'remote_id': item.get('remote_id'), 'source': item.get('source')})
        if not album:
            item.update({'created_at': datetime.datetime.now(), 'updated_at': datetime.datetime.now(), 'deleted_at': None, 'status': 1})
            spider.db.albums.insert_one(item)
            return item
        else:
            raise DropItem("Duplicate album found: %s" % item)


class LyricPipeline(object):

    def process_item(self, item, spider):
        spider.db.songs.update_one({'remote_id': item.get('remote_id'), 'source': item.get('source')}, {'$set': item})
        return item
