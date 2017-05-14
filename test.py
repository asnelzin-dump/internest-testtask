import time
import pymongo
import re

LOG_FILENAME = 'test3.min.log'


def useragent_to_browser(useragent):
    valid_browser_version = re.compile(r'(Chrome|Firefox|Opera|MSIE)[ /](\d+)')
    match = re.search(valid_browser_version, useragent)
    if match is not None:
        return '/'.join(match.groups())
    else:
        return 'Other'


def main():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['base']
    collection = db['log']

    print('Saving data...')
    start = time.clock()
    with open(LOG_FILENAME, 'r', encoding='ISO-8859-1') as log:
        for number, line in enumerate(log):
            if (number % 10000) == 0:
                print('Inserted {} entries'.format(number))
            useragent, sid, geo, bid = line.rstrip().split('*')
            browser = useragent_to_browser(useragent)
            entry = {
                'browser': browser,
                'sid': sid,
                'geo': geo,
                'bid': bid
            }
            collection.insert(entry)
    elapsed = time.clock() - start
    print('Data has been successfully saved in {:.3} seconds!\n'.format(elapsed))

if __name__ == '__main__':
    main()