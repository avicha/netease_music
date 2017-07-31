from elasticsearch import Elasticsearch
import pymongo


es = Elasticsearch(['localhost:9200'])
index = 'smart_tv'
settings = {
    'number_of_shards': 1,
    'number_of_replicas': 0
}
mappings = {
    'singers': {
        'properties': {
            'name': {'type': 'text'},
            'alias': {'type': 'text'},
            'updated_at': {'type': 'date'}
        }
    },
    'albums': {
        'properties': {
            'name': {'type': 'text'},
            'alias': {'type': 'text'},
            'singer_name': {'type': 'text'},
            'publish_time': {'type': 'date', 'format': 'yyyy-MM-dd'},
            'updated_at': {'type': 'date'}
        }
    },
    'songs': {
        'properties': {
            'name': {'type': 'text'},
            'alias': {'type': 'text'},
            'album_name': {'type': 'text'},
            'singer_name': {'type': 'text'},
            'artists': {'type': 'text'},
            'lyric': {'type': 'text'},
            'duration': {'type': 'integer'},
            'updated_at': {'type': 'date'}
        }
    }
}
if es.indices.exists(index):
    es.indices.delete(index)
resp = es.indices.create(index, body={'settings': settings, 'mappings': mappings}, update_all_types=True, wait_for_active_shards=1)
print repr(resp)
client = pymongo.MongoClient('localhost:27017')
db = client.smart_tv
q = db.singers.find({})
for x in q:
    print 'singer: %s' % x.get('_id')
    es.index(index, doc_type='singers', id=x.get('_id'), body={'name': x.get('name'), 'alias': x.get('alias'), 'updated_at': x.get('updated_at')})

q = db.albums.find({})
for x in q:
    print 'album: %s' % x.get('_id')
    es.index(index, doc_type='albums', id=x.get('_id'), body={'name': x.get('name'), 'alias': x.get('alias'), 'singer_name': x.get('singer_name'), 'publish_time': x.get('publish_time'), 'updated_at': x.get('updated_at')})

# q = db.songs.find({}).limit(0)
# for x in q:
#     artists = []
#     for artist in x.get('artists'):
#         name = artist.get('name')
#         artists.append(name)
#         for alias_name in artist.get('alia'):
#             if alias_name != name:
#                 artists.append(alias_name)
#     es.index(index, doc_type='songs', id=x.get('_id'), body={'name': x.get('name'), 'alias': ','.join(x.get('alias')), 'album_name': x.get('album_name'), 'singer_name': x.get('singer_name'), 'artists': artists, 'lyric': x.get('lyric'), 'duration': x.get('duration'), 'updated_at': x.get('updated_at')})
