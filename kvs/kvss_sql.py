_schema = '''
CREATE TABLE IF NOT EXISTS kvss_ids (
	id	INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS kvss_kvs (
	eid	INTEGER NOT NULL,
	key	TEXT NOT NULL,
	val	TEXT NOT NULL,
	FOREIGN KEY(eid) REFERENCES kvss_ids(id),
	UNIQUE(eid, key, val)
);

CREATE TABLE IF NOT EXISTS kvss_hier (
	hid	    INTEGER NOT NULL,
	pid		INTEGER NOT NULL,
	key		TEXT NOT NULL,
	val		TEXT NOT NULL,
	PRIMARY KEY(hid),
	FOREIGN KEY(pid) REFERENCES kvss_hier(hid)
);

CREATE INDEX IF NOT EXISTS idx_eid ON kvss_kvs(eid);
CREATE INDEX IF NOT EXISTS idx_key ON kvss_kvs(key);
CREATE INDEX IF NOT EXISTS idx_kvs ON kvss_kvs(key, val);
'''

import sqlite3
import os
from itertools import repeat

_q_insert_id = "INSERT INTO kvss_ids DEFAULT VALUES"
_q_associate_kv = "INSERT INTO kvss_kvs (eid, key, val) VALUES (?,?,?)"

_q_count_kvs = "SELECT count(key) FROM kvss_kvs WHERE eid = \"%s\""
_q_select_lrid = "SELECT last_insert_rowid()"
_q_select_kvs = "SELECT key,val FROM \"%s\" WHERE eid = \"%s\""
_q_select_keys = "SELECT DISTINCT key FROM \"%s\""
_q_select_ids = "SELECT id FROM %s"
_q_select_vals = "SELECT DISTINCT val FROM \"%s\" WHERE key = \"%s\""

_q_select_hier = '''
SELECT hid
	FROM kvss_hier
	WHERE pid = \"%d\"
	  AND key = \"%s\"
	  AND val = \"%s\"
'''

_q_insert_hier = '''
INSERT
 INTO kvss_hier (pid, key, val)
 VALUES (?, ?, ?)
'''

_q_cr_ro_view = "CREATE VIEW %s AS %s"
_q_cr_ro_table = "CREATE TABLE %s AS %s"
_q_create_ro = None

_q_create_ro_idx = '''
	CREATE INDEX IF NOT EXISTS %s_idx_eid ON %s(eid);
	CREATE INDEX IF NOT EXISTS %s_idx_key ON %s(key);
	CREATE INDEX IF NOT EXISTS %s_idx_kvs ON %s(key, val);
'''

_q_drop_view = "DROP VIEW %s"
_q_drop_table = "DROP TABLE %s"
_q_drop_ro = None

_q_select_filtered_ids = '''
SELECT DISTINCT eid AS id
	FROM %s
	WHERE key="%s" AND val="%s"
'''

_q_select_filtered_kvs = '''
SELECT eid, key, val
	FROM %s, ( %s )
	WHERE eid = id
	      AND (key <> "%s" AND val <> "%s")
'''


class  KvssSQL(object):
	def __init__(self, connstr=os.path.realpath('kvss.db'), debug=False, tempstore="VIEW"):
		global _q_create_ro, _q_drop_ro
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		self._ctx = []
		self._ctx_hids = []
		_q_create_ro = _q_cr_ro_table if tempstore == "TABLE" else _q_cr_ro_view
		_q_drop_ro = _q_drop_table if tempstore == "TABLE" else _q_drop_view
		con.executescript(_schema)

	def _insert_kvs(self, d, unique=True):
		""" insert list-of-dicts """
		con = self._con
		con.execute(_q_insert_id)
		id = con.execute(_q_select_lrid).next()[0]

		entry = tuple( d.iteritems() )
		if unique and tuple(self.find(entry)):
			if self._debug:
				print "entry:", d, "exists => ignoring"
			return

		for k,v in entry:
			con.execute(_q_associate_kv, (id, k, v))

		con.commit()

	def _insert_kvs_many(self, datasource, lod):
		cur = self._con.cursor()
		cur_exec = cur.execute
		cur_execmany = cur.executemany
		for d in lod:
			cur_exec(_q_insert_id)
			id = cur_exec(_q_select_lrid).next()[0]
			cur_execmany(_q_associate_kv, ( (id, k, v) for (k,v) in d.iteritems()))
			#for k,v in d.iteritems():
			#	cur_exec(_q_associate_kv, (id, k, v))
		self._con.commit()

	def find(self, tot):
		con = self._con

		h = "SELECT id FROM kvss_ids,"
		m_t = "(SELECT eid AS id%d FROM kvss_kvs WHERE key=\"%s\" AND val=\"%s\")"
		m = ',\n'.join([ m_t % (i, tot[i][0], tot[i][1]) for i in xrange(len(tot)) ])
		e = " AND ".join([ "id%d == id" % (i) for i in xrange(len(tot)) ])
		q = h + m + " WHERE " + e

		for (id,) in con.execute(q):
			if con.execute(_q_count_kvs % id).next()[0] == len(tot):
				yield id

	def _ctx_hid(self):
		ctx_hids = self._ctx_hids
		return ctx_hids[-1] if ctx_hids else 0

	def _ctx_ids(self,hid=None):
		if hid is None:
			hid = self._ctx_hid()
		return "kvss_ids" if hid == 0 else ("RO_IDS_%d" % hid)

	def _ctx_kvs(self,hid=None):
		if hid is None:
			hid = self._ctx_hid()
		return "kvss_kvs" if hid == 0 else ("RO_KVS_%d" % hid)

	def _iterate_entries(self,ctx=True):
		ids = self._ctx_ids() if ctx else "kvss_ids"
		q = _q_select_ids % ids
		for e in self._con.execute(q):
			yield e[0]

	def _iterate_keys(self,ctx=True):
		kvs = self._ctx_kvs() if ctx else "kvss_kvs"
		for k in self._con.execute(_q_select_keys % kvs):
			yield k[0]

	def _iterate_vals(self, key, ctx=True):
		kvs = self._ctx_kvs() if ctx else "kvss_kvs"
		for v in self._con.execute(_q_select_vals % (kvs,key)):
			yield v[0]

	def _iter_entry_kv(self, id, ctx=True):
		kvs = self._ctx_kvs() if ctx else "kvss_kvs"
		q = _q_select_kvs % (kvs, str(id))
		for x in self._con.execute(q):
			yield (x[0], x[1])

	def _get_entry(self, id):
		return [ "%s:%s" % x for x in self._iterate_entry_kv(id) ]

	def _ctx_push(self, key, val):
		con = self._con
		hid = self._ctx_hid()
		# http://bugs.python.org/issue4995
		old_isolation_level = con.isolation_level
		con.isolation_level = None

		q_check = _q_select_hier % (hid, key, val)
		#print q_check
		result = list(con.execute(q_check))
		if result:
			hid_new = result[0][0]
		else:
			con.execute(_q_insert_hier, (hid, key, val))
			hid_new = con.execute(_q_select_lrid).next()[0]

			ctx_kvs_prev = self._ctx_kvs(hid)
			ctx_ids = self._ctx_ids(hid_new)
			ctx_kvs = self._ctx_kvs(hid_new)

			s0 = _q_select_filtered_ids % (ctx_kvs_prev, key, val)
			q = _q_create_ro % (ctx_ids, s0)
			#print q
			con.execute(q)

			s1 = _q_select_filtered_kvs % (ctx_kvs_prev, s0, key, val)
			#print q
			q = _q_create_ro % (ctx_kvs, s1)
			con.execute(q)

			q = _q_create_ro_idx % tuple(x for x in repeat(ctx_kvs, 6))
			con.executescript(q)


		con.commit()
		con.isolation_level = old_isolation_level
		self._ctx.append((key,val))
		self._ctx_hids.append(hid_new)

	def _ctx_pop(self):
		if not self._ctx:
			return
		self._ctx.pop()
		self._ctx_hids.pop()

	def _clear_ro_cache(self):
		con = self._con
		hids = list(con.execute("SELECT hid FROM kvss_hier"))
		for hid, in hids:
			con.execute(_q_drop_table % self._ctx_ids(hid))
			con.execute(_q_drop_table % self._ctx_kvs(hid))
		con.execute("DELETE FROM kvss_hier WHERE 1")
		con.commit()

class KvssShell(object):
	def __init__(self, kvss):
		self._kvss = kvss
		self._key = None

	def entries(self):
		kvss = self._kvss
		for id in kvss._iterate_entries():
			print id, " [",
			for k, v in kvss._iter_entry_kv(id):
				print ("%s:%s" % (k,v)),
			print "]"

	def cd(self):
		cmd = self._cmd
		if len(cmd) < 2:
			return
		x = cmd[1]
		if x == '..':
			self._cd_pop()
		else:
			self._cd_push(x)

	def _cd_push(self, x):
		kvss = self._kvss
		if self._key is None:
			for k in kvss._iterate_keys():
				if k == x:
					self._key = x
					return
		else:
			for v in kvss._iterate_vals(self._key):
				if v == x:
					kvss._ctx_push(self._key, v)
					self._key = None
					return
		print "%s does not exist" % x

	def _cd_pop(self):
		kvss = self._kvss
		if self._key is None:
			kvss._ctx_pop()
		else:
			self._key = None


	def ls(self):
		kvss = self._kvss
		if self._key is None:
			for k in kvss._iterate_keys():
				print k
		else:
			for v in kvss._iterate_vals(self._key):
				print v

	def default(self):
		print "%s: Unknown command" % self._cmd[0]

	def go(self):
		kvss = self._kvss
		ctx = kvss._ctx
		while True:

			prompt = '/'.join([''] + [ '%s=%s' % (x[0],x[1]) for x in ctx ] + [''])
			if self._key:
				prompt += '%s' % self._key
			try:
				cmd = self._cmd = raw_input(prompt + '> ').split()
			except EOFError:
				print
				break

			if len(cmd) == 0:
				continue

			if cmd[0] == 'entries':
				self.entries()
			elif cmd[0] == 'ls':
				self.ls()
			elif cmd[0] == 'cd':
				self.cd()
			elif cmd[0] == 'clear_cache':
				self._kvss._clear_ro_cache()
			elif cmd[0] == 'exit':
				break
			else:
				self.default()

if __name__ == '__main__':
	_tmp_lod = (
		{ 'owner': 'kornilios', 'type':'PC', 'processor':'AMD' },
		{ 'owner': 'kornilios', 'type':'Laptop', 'processor':'Intel' },
		{ 'owner': 'kornilios', 'type':'CAR', 'brand':'Peugeot' },
		{ 'owner': 'vkoukis', 'type':'PC', 'processor':'INTEL' },
		{ 'owner': 'gtsouk', 'type':'CAR', 'brand':'KIA'}
	)
	kvss = KvssSQL(debug=True)
	for d in _tmp_lod:
		kvss._insert_kvs(d)

	sh = KvssShell(kvss)
	sh.go()
