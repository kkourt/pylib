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
'''

import sqlite3
import os

_q_insert_id = "INSERT INTO kvss_ids DEFAULT VALUES"
_q_associate_kv = "INSERT INTO kvss_kvs (eid, key, val) VALUES (?,?,?)"

_q_count_kvs = "SELECT count(key) FROM kvss_kvs WHERE eid = \"%s\""
_q_select_lrid = "SELECT last_insert_rowid()"
_q_select_kvs = "SELECT key,val FROM \"%s\" WHERE eid = \"%s\""
_q_select_keys = "SELECT DISTINCT key FROM \"%s\""
_q_select_ids = "SELECT id FROM %s"
_q_select_vals = "SELECT DISTINCT val FROM \"%s\" WHERE key = \"%s\""

_q_cr_tmp_view = "CREATE TEMP VIEW %s AS %s"
_q_cr_tmp_table = "CREATE TEMP TABLE %s AS %s"
_q_create_temp = None

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
		global _q_create_temp
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		self._ctx = []
		_q_create_temp = _q_cr_tmp_table if tempstore == "TABLE" else _q_cr_tmp_view
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

	def _ctx_ids(self):
		return "kvss_ids" if not self._ctx else ("TMP_IDS%d" % len(self._ctx))

	def _ctx_kvs(self):
		return "kvss_kvs" if not self._ctx else ("TMP_KVS%d" % len(self._ctx))

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

		ctx_kvs_prev = self._ctx_kvs()
		self._ctx.append((key,val))
		ctx_ids = self._ctx_ids()
		ctx_kvs = self._ctx_kvs()

		s0 = _q_select_filtered_ids % (ctx_kvs_prev, key,val)
		q = _q_create_temp % (ctx_ids, s0)
		con.execute(q)

		s1 = _q_select_filtered_kvs % (ctx_kvs_prev, s0, key, val)
		q = _q_create_temp % (ctx_kvs, s1)
		con.execute(q)

	def _ctx_pop(self):
		pass

class KvssShell(object):
	def __init__(self, kvss):
		self._kvss = kvss
		self._path = "/"
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
		kvss = self._kvss
		if self._key is None:
			for k in kvss._iterate_keys():
				if k == x:
					self._key = x
					self._path += x
					return
			else:
				print "%s does not exist" % x
		else:
			for v in kvss._iterate_vals(self._key):
				if v == x:
					kvss._ctx_push(self._key, v)
					self._key = None
					self._path += "=%s/" % v
					return
			else:
				print "%s does not exist" % x

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
		while True:
			try:
				cmd = self._cmd = raw_input(self._path + '> ').split()
			except EOFError:
				print
				break
			if cmd[0] == 'entries':
				self.entries()
			elif cmd[0] == 'ls':
				self.ls()
			elif cmd[0] == 'cd':
				self.cd()
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
