import csv
import re
import time

LOG_FILENAME = 'test3.log'
ANSWER_FILENAMES = ('1.csv', '2.csv', '3.csv')

task1_counter = {}
# {
#     sid: set(geo, ...),
# }

task2_counter = {}
# {
#     geo: set(sid, ...),
# }
task2_data = {}
# {
#     (geo, bid): {
#         browser: freq,
#         ...
#     }
#     ...
# }

task3_counter = {}
# {
#     sid: {
#         browser: freq,
#         ...
#     }
#     ...
# }

task3_data = {}
# {
#     (sid, bid): {
#         browser: freq,
#         ...
#     }
#     ...
# }


def useragent_to_browser(useragent):
    valid_browser_version = re.compile(r'(Chrome|Firefox|Opera|MSIE)[ /](\d+)')
    match = re.search(valid_browser_version, useragent)
    if match is not None:
        return '/'.join(match.groups())
    else:
        return 'Other'


def write_to_csv(data, filename):
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_NONE)
        writer.writerows(data)
    print('Writing done!')
    print()


def task1_save_line(sid, geo):
    if sid not in task1_counter:
        task1_counter[sid] = {geo}
    else:
        task1_counter[sid].add(geo)


def task2_save_line(browser, sid, geo, bid):
    if geo not in task2_counter:
        task2_counter[geo] = {sid}
    else:
        task2_counter[geo].add(sid)

    key = (geo, bid)
    if key not in task2_data:
        task2_data[key] = {
            browser: 1
        }
    else:
        if browser not in task2_data[key]:
            task2_data[key][browser] = 1
        else:
            task2_data[key][browser] += 1


def task3_save_line(browser, sid, bid):
    if sid not in task3_counter:
        task3_counter[sid] = {
            browser: 1
        }
    else:
        if browser not in task3_counter[sid]:
            task3_counter[sid][browser] = 1
        else:
            task3_counter[sid][browser] += 1

    key = (sid, bid)
    if key not in task3_data:
        task3_data[key] = {
            browser: 1
        }
    else:
        if browser not in task3_data[key]:
            task3_data[key][browser] = 1
        else:
            task3_data[key][browser] += 1


def task1_write_data():
    write_to_csv(
        [(sid, len(geos)) for sid, geos in task1_counter.items()],
        ANSWER_FILENAMES[0])


def task2_write_data():
    data_to_save = []

    print('Processing data for second task...')
    start = time.clock()

    #target_geos - geo ids, from which visited more than 6 sites
    target_geos = [geo for geo, sids in task2_counter.items() if len(sids) > 6]

    for geo in target_geos:
        target_keys = [key for key in task2_data.keys() if key[0] == geo]
        for _, bid in target_keys:
            for browser, freq in task2_data[(geo, bid)].items():
                data_to_save.append((geo, bid, browser, freq))

    elapsed = time.clock() - start
    print('Data has been successfully processed in {:.3} seconds!\n'.format(elapsed))

    write_to_csv(data_to_save, ANSWER_FILENAMES[1])


def task3_write_data():
    data_to_save = []

    print('Processing data for third task...')
    start = time.clock()

    #target_sids - sites ids, which have 3 or more browsers with freq > 1000
    target_sids = [sid for sid, browsers in task3_counter.items()
                   if len([freq for freq in browsers.values() if freq > 1000]) >= 3]

    for sid in target_sids:
        target_keys = [key for key in task3_data.keys() if key[0] == sid]
        for _, bid in target_keys:
            for browser, freq in task3_data[(sid, bid)].items():
                data_to_save.append((sid, bid, browser, freq))

    elapsed = time.clock() - start
    print('Data has been successfully processed in {:.3} seconds!\n'.format(elapsed))

    write_to_csv(data_to_save, ANSWER_FILENAMES[2])


def main():
    print('Saving data to memory...')

    with open(LOG_FILENAME, 'r', encoding='ISO-8859-1') as log:
        for line in log:
            useragent, sid, geo, bid = line.rstrip().split('*')
            browser = useragent_to_browser(useragent)

            task1_save_line(sid, geo)
            task2_save_line(browser, sid, geo, bid)
            task3_save_line(browser, sid, bid)

    print('Done!\n')

    task1_write_data()
    task2_write_data()
    task3_write_data()

if __name__ == '__main__':
    main()