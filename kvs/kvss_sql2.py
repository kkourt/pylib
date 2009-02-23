_schema = '''
CREATE TABLE IF NOT EXISTS kvss_keys (
	kid	INTEGER NOT NULL,
	key	TEXT UNIQUE,
	PRIMARY KEY(kid)
);

CREATE TABLE IF NOT EXISTS kvss_vals (
	vid	INTEGER NOT NULL,
	val	TEXT UNIQUE,
	PRIMARY KEY(vid)
);

CREATE TABLE IF NOT EXISTS kvss_ids (
	id	INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS kvss_kvs (
	eid	INTEGER NOT NULL,
	kid	INTEGER NOT NULL,
	vid	INTEGER NOT NULL,
	FOREIGN KEY(eid) REFERENCES kvss_ids(id),
	FOREIGN KEY(kid) REFERENCES kvss_keys(kid),
	FOREIGN KEY(vid) REFERENCES kvss_vals(vid)
);
'''

import sqlite3
import os
from itertools import izip

_q_insert_id = "INSERT INTO kvss_ids DEFAULT VALUES"
_q_insert_key = "INSERT OR IGNORE INTO kvss_keys ( key ) VALUES (?)"
_q_insert_val = "INSERT OR IGNORE INTO kvss_vals ( val ) VALUES (?)"
_q_associate_kv = "INSERT INTO kvss_kvs (eid, kid, vid) VALUES (?,?,?)"

_q_select_lrid = "SELECT last_insert_rowid()"
_q_select_kid = "SELECT kid FROM kvss_keys WHERE key=\"%s\""
_q_select_vid = "SELECT vid FROM kvss_vals WHERE val=\"%s\""


class  Kvss_SQL(object):
	def __init__(self, connstr=os.path.realpath('kvss.db'), debug=False):
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		con.executescript(_schema)
	
	def _insert_kvs(self, d):
		""" insert list-of-dicts """
		con = self._con
		con.execute(_q_insert_id)
		id = con.execute(_q_select_lrid).next()[0]

		keys = []
		vals = []
		for key, val in d.iteritems():
			con.execute(_q_insert_key, (key,))
			keys.append(con.execute(_q_select_kid % key).next()[0])

			con.execute(_q_insert_val, (val,))
			vals.append(con.execute(_q_select_vid % val).next()[0])

		for k,v in izip(keys, vals):
			con.execute(_q_associate_kv, (id, k, v))
	
if __name__ == '__main__':
	_tmp_lod = (
		{ 'owner': 'kornilios', 'type':'PC', 'processor':'AMD' },
		{ 'owner': 'kornilios', 'type':'CAR', 'brand':'Peugeot' },
		{ 'owner': 'vkoukis', 'type':'PC', 'processor':'INTEL' },
		{ 'owner': 'gtsouk', 'type':'CAR', 'brand':'KIA'}
	)
	kvss = Kvss_SQL(connstr=":memory:", debug=True)
	for d in _tmp_lod:
		kvss._insert_kvs(d)
