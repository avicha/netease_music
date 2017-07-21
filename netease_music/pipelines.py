import datetime
from scrapy.exceptions import DropItem


class SingerPipeline(object):

    def process_item(self, item, spider):
        spider.collection.update_one({'remote_id': item.get('remote_id'), 'source': 1}, {'$set': dict(item)}, upsert=True)
        return item


class SingerIdPipeline(object):

    def process_item(self, item, spider):
        singer = spider.collection.find_one({'remote_id': item.get('remote_id'), 'source': item.get('source')})
        if not singer:
            item.update({'created_at': datetime.datetime.now(), 'updated_at': datetime.datetime.now(), 'status': 1})
            spider.collection.insert_one(item)
            return item
        else:
            raise DropItem("Duplicate item found: %s" % item)
