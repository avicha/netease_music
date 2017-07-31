import pymongo

client = pymongo.MongoClient('localhost:27017')
db = client.smart_tv
q = db.singers.find({'source': 1})
for x in q:
    singer_album_num = db.albums.count({'remote_singer_id': x.get('remote_id'), 'source': 1})
    db.singers.update_one({'remote_id': x.get('remote_id'), 'source': 1}, {'$set': {'album_num': singer_album_num}})
