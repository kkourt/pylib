_schema = '''
CREATE TABLE  kvhs_ord (
	eid	INTEGER NOT NULL,
	pid	INTEGER UNIQUE,
	PRIMARY KEY(eid),
	FOREIGN KEY(pid) REFERENCES kvhs_ord(id)
);

CREATE TABLE kvhs_keys (
	kid	INTEGER NOT NULL,
	key	TEXT UNIQUE,
	PRIMARY KEY(kid)
);

CREATE TABLE kvhs_vals (
	vid	INTEGER NOT NULL,
	key	TEXT UNIQUE,
	PRIMARY KEY(vid)
);

CREATE TABLE kvhs_kvs (
	eid	INTEGER NOT NULL,
	kid	INTEGER NOT NULL,
	vid	INTEGER NOT NULL,
	FOREIGN KEY(eid) REFERENCES kvhs_ord(eid),
	FOREIGN KEY(kid) REFERENCES kvhs_keys(kid),
	FOREIGN KEY(vid) REFERENCES kvhs_vals(vid)
);

CREATE TRIGGER remove_parent_check BEFORE DELETE ON kvhs_ord
FOR EACH ROW BEGIN
	SELECT RAISE(ROLLBACK, "entry has childern: can't delete")
	WHERE
		(SELECT eid FROM kvhs_ord WHERE pid = OLD.eid)
	IS NOT NULL;
END;
'''

import sqlite3
import os

class  Kvhs_SQL(object):
	def __init__(self, connstr=os.path.realpath('kvhs.db'), debug=False):
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		try:
			con.executescript(_schema)
		except sqlite3.OperationalError,x:
			if debug:
				print "Can't create initial schema:", x
				
if __name__ == '__main__':
	kvhs = Kvhs_SQL(debug=True)
