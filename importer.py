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


def get_tmp_dir():
    from config import tmp_dir

    return tmp_dir


def get_local_fn(source):
    from time import time

    ts = time()
    return get_tmp_dir() + '/' + source + '-' + str(ts) + '.csv'


def update_local(source):
    import shutil
    from contextlib import closing

    local_file_fn = get_local_fn(source)

    with closing(open_remote(source)) as remote_file:
        with open(local_file_fn, 'wb') as local_file:
            shutil.copyfileobj(remote_file, local_file)

    return local_file_fn


def copy_local(source, stored_fn):
    import shutil

    local_file_fn = get_local_fn(source)
    shutil.copy(stored_fn, local_file_fn)

    return local_file_fn


def get_tickets_from_local(csv_fn):
    import csv

    print csv_fn
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
    print 'Rows:', count
    print 'Filtered (titles):', count_is_title
    print 'Filtered (zero prices):', count_is_zero_price
    print 'Converted tkts (icao to iata):', count_icao

    return out_fn


def db_execute(sql, *args):
    """Execute sql query

    :param str sql: Query to execute. Query string contains variables {i}.

    :param list args: List of query variables. The 0-th variable should be the table name.

    :return: Number of affected rows
    :rtype: int
    """
    import pymysql.cursors
    from config import db

    count_affected_rows = 0
    connection = pymysql.connect(host = db['host'],
                            user = db['user'],
                            password = db['password'],
                            db = db['db'],
                            local_infile = True,
                            cursorclass = pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            count_affected_rows = cursor.execute( sql.format(db['charter_table'], *args) )
        connection.commit()
    finally:
        connection.close()

    return count_affected_rows


def db_import_from_local(csv_fn, source):
    sql_delete_exist_tkts = 'delete from `{0}` where source = \'{1}\';'
    count_deleted_tkts = db_execute(sql_delete_exist_tkts, source)

    sql_load_tkts_from_file = '''LOAD DATA LOCAL INFILE '{1}'
        INTO TABLE `{0}`
        FIELDS TERMINATED BY '\\t'
        LINES TERMINATED BY '\\r\\n'
        (ExternalID,AirCompanyCode,DepartureIATA,ArrivalIATA,DepartureTerminal,ArrivalTerminal,AirCraft,FlightNum,@DepartureDate,@ArrivalDate,Price,Currency,Class,Seats,CostUSD,CostRUB,CostEUR,OrderLimit,ReturnWay,AirCompanyCodeReturn,DepartureIATAReturn,ArrivalIATAReturn,DepartureTerminalReturn,ArrivalTerminalReturn,AirCraftReturn,FlightNumReturn,@DepartureDateReturn,@ArrivalDateReturn,ClassReturn,SeatsReturn,CostUSDReturn,CostRUBReturn,CostEURReturn,Baggage,BaggageBack,AirRules,TourOperator, source)
        SET
            DepartureDate = STR_TO_DATE(@DepartureDate, '%Y-%m-%d %H:%i:%s'),
            ArrivalDate = STR_TO_DATE(@ArrivalDate, '%Y-%m-%d %H:%i:%s'),
            DepartureDateReturn = STR_TO_DATE(@DepartureDateReturn, '%Y-%m-%d %H:%i:%s'),
            ArrivalDateReturn = STR_TO_DATE(@ArrivalDateReturn, '%Y-%m-%d %H:%i:%s')
    ;
    '''
    count_imported_tickets = db_execute(sql_load_tkts_from_file, csv_fn)

    sql_activate_tkts = 'update `{0}` set active = 1 where source = \'{1}\';'
    count_activated_tickets = db_execute(sql_activate_tkts, source)

    # Debug
    print 'Deleted tickets:', count_deleted_tkts
    print 'Imported tickets:', count_imported_tickets
    print 'Activated tickets:', count_activated_tickets


def import_charter_tickets(source, stored_file = None):
    local_source_fn = update_local(source) if stored_file is None else \
                      copy_local(source, stored_file)
    local_source_import_ready_fn = preimport_handler(local_source_fn, source)
    db_import_from_local(local_source_import_ready_fn, source)


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description='Charter tickets importer')
    parser.add_argument('source', metavar='source', type=str, help = 'source name')
    parser.add_argument('-f', metavar='file', dest='file', type=str, help = 'path to local file with tickets')

    return parser.parse_args()


def main():
    import argparse

    args = parse_args()
    import_charter_tickets(args.source, args.file)


if __name__ == '__main__':
    main()
