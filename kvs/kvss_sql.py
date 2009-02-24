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

CREATE TABLE IF NOT EXISTS kvss_ds (
	ds	TEXT NOT NULL,
	UNIQUE(ds)
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

_q_select_filtered_ids_exclude = '''
SELECT DISTINCT eid AS id
	FROM %s
	EXCEPT SELECT DISTINCT eid AS id FROM %s WHERE key="%s"
'''

_q_select_ds = "SELECT ds FROM kvss_ds WHERE ds=\"%s\""
_q_insert_ds = "INSERT INTO kvss_ds(ds)  VALUES(?)"

import types

class  KvssSQL(object):
	class _CTX(types.ListType):
		def __init__(self, *args, **kwargs):
			super(KvssSQL._CTX,self).__init__(*args, **kwargs)
			self.__hids = []

	def __init__(self, connstr=os.path.realpath('kvss.db'), debug=False, tempstore="TABLE"):
		global _q_create_ro, _q_drop_ro
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		_q_create_ro = _q_cr_ro_table if tempstore == "TABLE" else _q_cr_ro_view
		_q_drop_ro = _q_drop_table if tempstore == "TABLE" else _q_drop_view
		con.executescript(_schema)

	def get_context(self):
		return self._CTX()

	def _insert_kvs(self, d, unique=True):
		""" insert list-of-dicts """
		entry = tuple( d.iteritems() )
		if unique and tuple(self.find(entry)):
			if self._debug:
				print "entry:", d, "exists => ignoring"
			return

		con = self._con
		con.execute(_q_insert_id)
		id = con.execute(_q_select_lrid).next()[0]
		for k,v in entry:
			con.execute(_q_associate_kv, (id, k, v))

		con.commit()

	def _insert_kvs_many(self, lod):
		cur = self._con.cursor()
		cur_exec = cur.execute
		cur_execmany = cur.executemany
		for d in lod:
			cur_exec(_q_insert_id)
			id = cur_exec(_q_select_lrid).next()[0]
			cur_execmany(_q_associate_kv, ( (id, k, v) for (k,v) in d.iteritems()))
		self._con.commit()

	def insert_kvs_ds(self, lod, datasource):
		con = self._con
		q = _q_select_ds % datasource
		res = list(con.execute(q))
		if res:
			print "datasource %s exists: doing nothing" % datasource
			return
		self._clear_ro_cache()
		self._insert_kvs_many(lod)
		con.execute(_q_insert_ds, (datasource,))
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

	def _ctx_hid(self, ctx):
		ctx_hids = ctx._CTX__hids
		return ctx_hids[-1] if ctx_hids else 0

	def _ctx_ids(self, ctx, hid=None):
		if hid is None:
			hid = self._ctx_hid(ctx)
		return "kvss_ids" if hid == 0 else ("RO_IDS_%d" % hid)

	def _ctx_kvs(self, ctx, hid=None):
		if hid is None:
			hid = self._ctx_hid(ctx)
		return "kvss_kvs" if hid == 0 else ("RO_KVS_%d" % hid)

	def _iterate_entries(self,ctx):
		ids = self._ctx_ids(ctx) if ctx else "kvss_ids"
		q = _q_select_ids % ids
		for e in self._con.execute(q):
			yield e[0]

	def _iterate_keys(self,ctx):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		for k in self._con.execute(_q_select_keys % kvs):
			yield k[0]

	def _iterate_vals(self, key, ctx):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		for v in self._con.execute(_q_select_vals % (kvs,key)):
			yield v[0]

	def _iter_entry_kv(self, id, ctx=None):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		q = _q_select_kvs % (kvs, str(id))
		for x in self._con.execute(q):
			yield (x[0], x[1])

	def _get_entry(self, id):
		return [ "%s:%s" % x for x in self._iterate_entry_kv(id) ]

	def _ctx_push(self, ctx, key, val):
		con = self._con
		hid = self._ctx_hid(ctx)
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

			ctx_kvs_prev = self._ctx_kvs(ctx, hid)
			ctx_ids = self._ctx_ids(ctx, hid_new)
			ctx_kvs = self._ctx_kvs(ctx, hid_new)

			if len(val) > 0:
				s0 = _q_select_filtered_ids % (ctx_kvs_prev, key, val)
			else:
				s0 = _q_select_filtered_ids_exclude % (ctx_kvs_prev, ctx_kvs_prev, key)
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
		if len(val) == 0:
			val = "None"
		ctx.append((key,val))
		ctx._CTX__hids.append(hid_new)
		return ctx

	def _ctx_pop(self, ctx):
		if not ctx:
			return
		ctx.pop()
		ctx._CTX__hids.pop()

	def _clear_ro_cache(self):
		con = self._con
		hids = list(con.execute("SELECT hid FROM kvss_hier"))
		for hid, in hids:
			con.execute(_q_drop_table % self._ctx_ids(hid))
			con.execute(_q_drop_table % self._ctx_kvs(hid))
		con.execute("DELETE FROM kvss_hier WHERE 1")
		con.commit()


from cmdparse import CmdParser, CmdPyParser
import readline as rl
class KvssShell(CmdParser, CmdPyParser):
	commands = ("entries", "ls", "cd", "clear_cache")
	def __init__(self, *args, **kwargs):
		self._kvss = kwargs.get("kvss")
		self._key = None
		super(KvssShell, self).__init__(*args, **kwargs)
		self._namespace["kvss"] = self._kvss
		self._ctx = self._kvss.get_context()

	def _list_iter(self):
		kvss = self._kvss
		if self._key is None:
			for k in kvss._iterate_keys(self._ctx):
				yield k
		else:
			for v in kvss._iterate_vals(self._key, self._ctx):
				yield v

	def parse_entries(self, lex):
		""" list entries at this specific context """
		kvss = self._kvss
		for id in kvss._iterate_entries(self._ctx):
			print id, " [",
			for k, v in kvss._iter_entry_kv(id):
				print ("%s:%s" % (k,v)),
			print "]"

	def complete_cd(self, lex):
		x = lex.get_token()
		x_next = lex.get_token()
		if x_next:
			return []

		if not x:
			ret = [l for l in self._list_iter()]
			return ret
		else:
			xlen = len(x)
			return filter(lambda l: x == l[:xlen], (self._list_iter() ))

	def parse_cd(self, lex):
		""" enter a key/value (depending on the context) """
		x = lex.get_token()
		if not x:
			levels = len(self._ctx)
			for i in xrange(levels):
				self._cd_pop()
		elif x == '..':
			self._cd_pop()
		else:
			self._cd_push(x)

	def parse_clear_cache(self, lex):
		""" drop the cache tables """
		self._kvss._clear_ro_cache()

	def _cd_push(self, x):
		kvss = self._kvss
		if x[0] == '!':
			if self._key is not None:
				print "not command in key context not supported"
				return
			for k in kvss._iterate_keys():
				if k == x[1:]:
					kvss._ctx_push(self._ctx, k, '')
					return
		elif self._key is None:
			for k in kvss._iterate_keys(self._ctx):
				if k == x:
					self._key = x
					return
		else:
			for v in kvss._iterate_vals(self._key, self._ctx):
				if v == x:
					kvss._ctx_push(self._ctx, self._key, v)
					self._key = None
					return
		print "%s does not exist" % x

	def _cd_pop(self):
		kvss = self._kvss
		if self._key is None:
			kvss._ctx_pop(self._ctx)
		else:
			self._key = None

	def parse_ls(self, lex):
		""" list available keys or vals (depending on the context) """
		for l in self._list_iter():
			print l

	def go(self):
		kvss = self._kvss
		ctx = self._ctx
		rl.set_history_length(1000)
		rl.parse_and_bind('tab: complete')
		while True:
			prompt = '/'.join([''] + [ '%s=%s' % (x[0],x[1]) for x in ctx ] + [''])
			if self._key:
				prompt += '%s' % self._key
			try:
				cmd = raw_input(prompt + '> ')
			except EOFError:
				print
				break
			out = self.parse(cmd)
			if out is not None:
				print out

if __name__ == '__main__':
	_tmp_lod = (
		{ 'owner': 'kornilios', 'type':'PC', 'processor':'AMD' },
		{ 'owner': 'kornilios', 'type':'Laptop', 'processor':'Intel' },
		{ 'owner': 'kornilios', 'type':'CAR', 'brand':'Peugeot' },
		{ 'owner': 'vkoukis', 'type':'PC', 'processor':'Intel' },
		{ 'owner': 'gtsouk', 'type':'CAR', 'brand':'KIA' }
	)
	kvss = KvssSQL(debug=True)
	for d in _tmp_lod:
		kvss._insert_kvs(d)

	kvss_sh = KvssShell(kvss=kvss)
	kvss_sh.go()

