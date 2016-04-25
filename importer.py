#!/usr/bin/env python
#coding: utf-8


def open_remote(source):
    import urllib2
    from config import sources

    # TODO: Notice. You can implement switch for proto and auth type here
    remote_source_obj = filter(lambda source_obj: source_obj['name'] == source, sources)
    top_level_url = remote_source_obj[0]['remote_proto'] + '://' \
        + remote_source_obj[0]['remote_user'] + ':' + remote_source_obj[0]['remote_password'] + '@' \
        + remote_source_obj[0]['remote_host'] \
        + remote_source_obj[0]['path_to_file']

    opener = urllib2.build_opener()
    return opener.open(top_level_url)


def get_local_fn(source):
    return source + '.csv'


def update_local(source):
    import shutil
    from contextlib import closing

    local_file_fn = get_local_fn(source)

    with closing(open_remote(source)) as remote_file:
        with open(local_file_fn, 'wb') as local_file:
            shutil.copyfileobj(remote_file, local_file)

    return local_file_fn


def get_tickets(source):
    import csv

    local_source_fn = update_local(source)
    with open(local_source_fn, "rb") as csvfile:
        datareader=csv.reader(csvfile)
        for row in datareader:
            yield row


def import_charter_tickets(source):
    count = 0
    for row in get_tickets(source):
        count += 1
    print count


if __name__ == '__main__':
    import_charter_tickets('test')
