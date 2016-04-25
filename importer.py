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


def get_tickets_from_local(csv_fn):
    import csv

    with open(csv_fn, "rb") as csvfile:
        datareader=csv.reader(csvfile, delimiter='\t', lineterminator='\r\n')
        is_first_line = True
        for row in datareader:
            if is_first_line:
                is_first_line = False
                continue
            yield row


def save_ticket_in_local(tkt, csv_fn):
    import csv

    with open(csv_fn, 'a') as csvfile:
        a = csv.writer(csvfile, delimiter='\t', lineterminator='\r\n')
        a.writerow(tkt)


def delete_local(fn):
    try:
        with open(fn, "w"):
            pass
    except:
        pass


def check_is_title(tkt_item):
    flight_num = 0

    try:
        flight_num = int(tkt_item[7])
    except:
        flight_num = 0

    if flight_num == 0:
        return True

    return False


def check_is_zero_price(tkt_item):
    ticket_price = 0

    try:
        # Notice: I don't check float format, float(tkt_item[10].replace(',', '.'))
        ticket_price = float(tkt_item[10])
    except:
        ticket_price = 0

    if ticket_price == 0:
        return True

    return False


def convert_icao_to_iata(tkt_item):
    from config import icao_to_iata

    flag = 0
    # AirCompanyCode - 1, AirCompanyCode Return - 19
    for i in (1, 19):
        if tkt_item[i] in icao_to_iata:
            tkt_item[i] = icao_to_iata[ tkt_item[i] ]
            flag = 1

    return tkt_item, flag


def preimport_handler(in_fn, source):
    out_fn = in_fn + '.import'
    delete_local(out_fn)

    count = 0
    count_is_title = count_is_zero_price = 0
    count_icao = 0
    for tkt in get_tickets_from_local(in_fn):
        if check_is_title(tkt):
            count_is_title += 1
            continue
        if check_is_zero_price(tkt):
            count_is_zero_price += 1
            continue

        count_icao_fl = 0
        tkt, count_icao_fl = convert_icao_to_iata(tkt)
        count_icao += count_icao_fl

        tkt.append(source)

        save_ticket_in_local(tkt, out_fn)

        count += 1

    # Debug
    print count
    print 'Filtered (titles):', count_is_title
    print 'Filtered (zero prices):', count_is_zero_price
    print 'Converted tkts (icao to iata):', count_icao

    return out_fn


def import_charter_tickets(source):
    local_source_fn = update_local(source)
    local_source_filtered_fn = preimport_handler(local_source_fn, source)


if __name__ == '__main__':
    import_charter_tickets('test')
