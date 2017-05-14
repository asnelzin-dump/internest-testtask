
def parse_and_save():
    con = sqlite3.connect(BASE_FILENAME)
    cur = con.cursor()
    cur.execute('create table if not exists Sites(sid integer, geo integer)')

    with open(LOG_FILENAME, 'r', encoding='ISO-8859-1') as log:
        for line in log:
            useragent, sid, geo, bid = line.split('*')
            cur.execute('insert into Sites (sid, geo) values ({}, {})'.format(int(sid), int(geo)))
    con.commit()
    cur.close()


def parse_and_save_mongo():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['base']
    collection = db['log']
    data = []
    with open(LOG_FILENAME, 'r', encoding='ISO-8859-1') as log:
        for line in log:
            useragent, sid, geo, bid = line.split('*')
            entry = {
                'sid': sid,
                'geo': geo
            }
            data.append(entry)
            if len(data) == 1000:
                collection.insert(data)
                data = []


def mongo_count_geos():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['base']
    collection = db['log']
    response = collection.aggregate([
        { '$group' : {
            '_id' : { 'sid': '$sid', 'geo' : '$geo' }
        }},
        { '$group' : {
            '_id' : '$_id.sid',
            'geo_count' : { '$sum': 1 }
        }}
    ])
    data = []
    for entry in response['result']:
        data.append((entry['_id'], entry['geo_count']))
    print(data[0])
    write_to_csv(data, FIRST_ANSWER)

def count_geos():
    con = sqlite3.connect(BASE_FILENAME)
    cur = con.cursor()
    cur.execute('select sid, count(distinct geo) from Sites group by sid')
    write_to_csv(cur, FIRST_ANSWER)
    cur.close()

