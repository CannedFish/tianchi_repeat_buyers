#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3, math

def _get_total_page(conn, table, num):
    cur = conn.cursor()
    cur.execute("select count(*) from %s" % table)
    total = int(cur.fetchone()[0])
    page = math.ceil(total/num)
    return (total, page)

def do_split(conn, table, num):
    total, page = _get_total_page(conn, table, num)
    print total, page
    drop_old_table = "drop table if exists {0}"
    create_table = "create table {0} (user_id text, item_id text, cat_id text, merchant_id text, brand_id text, time_tamp text, action_type integer)"
    get_data = "select * from {0} limit {1} offset {2}"
    insert_data = "insert into {0} values (?, ?, ?, ?, ?, ?, ?)"
    idx = 0
    for i in range(num):
        cur = conn.cursor()
        subtable = table + "_" + str(i)
        cur.execute(drop_old_table.format(subtable))
        cur.execute(create_table.format(subtable))
        cur.execute(get_data.format(table, page, idx))
        data = cur.fetchall()
        cur.executemany(insert_data.format(subtable), data)
        idx += page
        print "%8d/%d" % (idx, total)
    conn.commit()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print "Usage split $table $num"
        sys.exit(1)

    conn = sqlite3.connect('./Data/data.db')
    do_split(conn, sys.argv[1], int(sys.argv[2]))
    conn.close()

# vim: set sw=4 ts=4 softtabstop=4
