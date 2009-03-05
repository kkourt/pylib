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


_q_insert_id_def = "INSERT INTO kvss_ids DEFAULT VALUES"
_q_insert_id = "INSERT INTO %s(id) VALUES (?)"
_q_associate_kv = "INSERT INTO kvss_kvs (eid, key, val) VALUES (?,?,?)"

_q_count_kvs = "SELECT count(key) FROM kvss_kvs WHERE eid = \"%s\""
_q_select_lrid = "SELECT last_insert_rowid()"
_q_select_kvs = "SELECT key,val FROM \"%s\" WHERE eid = \"%s\""
_q_select_keys = "SELECT DISTINCT key FROM \"%s\""
_q_select_ids = "SELECT id FROM %s"
_q_select_vals = "SELECT DISTINCT val FROM \"%s\" WHERE key = \"%s\""
_q_select_vals_ids = "SELECT eid, val FROM \"%s\" WHERE key = \"%s\""

_q_select_hier = '''
SELECT hid
	FROM kvss_hier
	WHERE pid = \"%d\"
	  AND key = \"%s\"
	  AND val = \"%s\"
'''

_q_select_hier_key = '''
SELECT val FROM kvss_hier WHERE pid = \"%d\" AND key = \"%s\"
'''

_q_select_hier_kv = '''
SELECT key, val FROM kvss_hier WHERE pid =\"%d\"
'''

_q_insert_hier = '''
INSERT
 INTO kvss_hier (pid, key, val)
 VALUES (?, ?, ?)
'''

_q_create_q = "CREATE TABLE %s AS %s"

_q_create_ids = "CREATE TABLE %s (id INTEGER PRIMARY KEY)"
_q_create_kvs = '''
CREATE TABLE IF NOT EXISTS %s (
	eid	INTEGER NOT NULL,
	key	TEXT NOT NULL,
	val	TEXT NOT NULL,
	FOREIGN KEY(eid) REFERENCES %s(id),
	UNIQUE(eid, key, val)
);
'''

_q_create_ro_idx = '''
	CREATE INDEX IF NOT EXISTS %s_idx_eid ON %s(eid);
	CREATE INDEX IF NOT EXISTS %s_idx_key ON %s(key);
	CREATE INDEX IF NOT EXISTS %s_idx_kvs ON %s(key, val);
'''
_q_drop_table = "DROP TABLE %s"

_q_select_filtered_ids = '''
SELECT DISTINCT eid AS id
	FROM %s
	WHERE key="%s" AND val="%s"
'''

_q_select_filtered_kvs = '''
SELECT eid, key, val
	FROM %s, %s
	WHERE eid = id AND (key <> "%s" OR val <> "%s")
'''

_q_select_filtered_ids_exclude = '''
SELECT DISTINCT eid AS id
	FROM %s
	EXCEPT SELECT DISTINCT eid AS id FROM %s WHERE key="%s"
'''

_q_select_filtered_ids_exclude2 = '''
SELECT DISTINCT eid AS id
	FROM %s
	WHERE NOT EXISTS (
		SELECT eid AS id0 FROM %s
		WHERE id=id0 AND key="%s"
	)
	LIMIT 1
'''

_q_select_ds = "SELECT ds FROM kvss_ds WHERE ds=\"%s\""
_q_insert_ds = "INSERT INTO kvss_ds(ds)  VALUES(?)"

import sqlite3
import os
from itertools import repeat, ifilter
from functools import partial
import types
import re

class  KvssSQL(object):
	class _CTX(types.ListType):
		def __init__(self, *args, **kwargs):
			super(KvssSQL._CTX,self).__init__(*args, **kwargs)
			self.__hids = list(args[0].__hids) if len(args) > 0 else []

	def __init__(self, connstr=os.path.realpath('kvss.db'), debug=False):
		self._con = con = sqlite3.connect(connstr)
		self._debug = debug
		con.executescript(_schema)

	def get_context(self, ctx=None):
		if ctx is None:
			return self._CTX()
		else:
			return self._CTX(ctx)

	def _insert_kvs(self, d, unique=True):
		""" insert list-of-dicts """
		entry = tuple( d.iteritems() )
		if unique and tuple(self.find(entry)):
			if self._debug:
				print "entry:", d, "exists => ignoring"
			return

		con = self._con
		con.execute(_q_insert_id_def)
		id = con.execute(_q_select_lrid).next()[0]
		for k,v in entry:
			con.execute(_q_associate_kv, (id, k, v))

		con.commit()

	def _insert_kvs_many(self, lod):
		cur = self._con.cursor()
		cur_exec = cur.execute
		cur_execmany = cur.executemany
		for d in lod:
			cur_exec(_q_insert_id_def)
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

	def _hid_ids(self, hid):
		return "kvss_ids" if hid == 0 else ("RO_IDS_%d" % hid)

	def _hid_kvs(self, hid):
		return "kvss_kvs" if hid == 0 else ("RO_KVS_%d" % hid)

	def _ctx_ids(self, ctx):
		hid = self._ctx_hid(ctx)
		return self._hid_ids(hid)

	def _ctx_kvs(self, ctx):
		hid = self._ctx_hid(ctx)
		return self._hid_kvs(hid)

	def _iterate_entries(self,ctx):
		ids = self._ctx_ids(ctx) if ctx else "kvss_ids"
		q = _q_select_ids % ids
		for e in self._con.execute(q):
			yield e[0]

	def cnt_entries(self, ctx):
		return reduce(lambda x,y: x+1, self._iterate_entries(ctx), 0)

	def iterate_entries(self, ctx):
		for id in self._iterate_entries(ctx):
			entry = {}
			for k, v in self._iter_entry_kv(ctx, id):
				entry[k] = v
			yield entry

	# XXX: Does not seem to give considerable speedup
	def iterate_entries_keys(self, ctx, *keys):
		""" iterate entries and vals only for the specified keys.
		If an entry does not contain the specified key then show
		nothing.
		"""
		nr_keys = len(keys)
		if nr_keys <= 0:
			raise ValueError, "keys error: %d" % nr_keys

		_and = " AND " if nr_keys > 1 else " "
		kvs = self._ctx_kvs(ctx)
		q = "SELECT kvs0.eid, "  + \
		','.join((" kvs%d.key, kvs%d.val" % (i,i) for i in xrange(nr_keys))) + \
		" FROM " + \
		','.join(( " %s AS kvs%s" % (kvs,i) for i in xrange(nr_keys))) + \
		" WHERE " + \
		'AND'.join(( " kvs%d.eid = kvs%d.eid" % (i-1,i) for i in xrange(1,nr_keys))) + \
		_and + \
		'AND'.join(( " kvs%d.key = \"%s\" " % (i, keys[i]) for i in xrange(nr_keys)))

		for t in self._con.execute(q):
			yield dict([ (t[i], t[i+1]) for i in xrange(1, nr_keys*2, 2) ])


	def _iterate_keys(self,ctx):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		for k in self._con.execute(_q_select_keys % kvs):
			yield k[0]

	def _iterate_vals(self, key, ctx):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		for v in self._con.execute(_q_select_vals % (kvs,key)):
			yield v[0]

	def _iterate_cache(self, ctx, key=None):
		hid = self._ctx_hid(ctx)
		con = self._con
		if key is None:
			q = _q_select_hier_kv % hid
			for key, val in con.execute(q):
				yield key,val
		else:
			q = _q_select_hier_key % (hid, key)
			for val in con.execute(q):
				yield val

	def _check_empty_blah(self, key, ctx):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		res = list(self._con.execute(_q_select_filtered_ids_exclude2 % (kvs, kvs, key)))
		return True if len(res) > 0 else False

	def _check_empty(self, ctx, key):
		# This is slow the first time, but it uses the cache
		self._ctx_push(ctx, key, '')
		l = list(self._iterate_keys(ctx))
		ret = False
		if len(l):
			ret = True
		self._ctx_pop(ctx)
		return ret

	def _iter_entry_kv(self, ctx, id):
		kvs = self._ctx_kvs(ctx) if ctx else "kvss_kvs"
		q = _q_select_kvs % (kvs, str(id))
		for x in self._con.execute(q):
			yield (x[0], x[1])

	def _get_entry(self, id):
		return [ "%s:%s" % x for x in self._iterate_entry_kv(id) ]

	def _ctx_exists(self, ctx, key, val):
		for k in self._iterate_keys(ctx):
			if k == key:
				break
		else:
			return False

		# entries with no k in their keys
		if val == '':
			return True

		for v in self._iterate_vals(k, ctx):
			if v == val:
				return True
		else:
			return False

	def _get_push_q(self, key, val, kvs_prev):
		if len(val) > 0:
			q = _q_select_filtered_ids % (kvs_prev, key, val)
		else:
			q = _q_select_filtered_ids_exclude % (kvs_prev, kvs_prev, key)
		return q


	def _ctx_push(self, ctx, key, val, filter_fn=None):
		con = self._con
		hid = self._ctx_hid(ctx)
		# http://bugs.python.org/issue4995
		old_isolation_level = con.isolation_level
		con.isolation_level = None

		# check if the hierarchy already exists based on:
		# (parent hid, key, val)
		q_check = _q_select_hier % (hid, key, val)
		#print q_check
		result = list(con.execute(q_check))

		if result:
			# hierarchy exists, just get hid
			hid_new = result[0][0]
		else:
			# create new hierarchy
			# - insert new entry in hier, get new hid
			con.execute(_q_insert_hier, (hid, key, val))
			hid_new = con.execute(_q_select_lrid).next()[0]

			# - get matrices names
			ctx_kvs_prev = self._hid_kvs(hid) # old kvs
			ctx_ids = self._hid_ids(hid_new)  # new ids
			ctx_kvs = self._hid_kvs(hid_new)  # new kvs

			# - create new ids tables
			if filter_fn is None:
				s0 = self._get_push_q(key, val, ctx_kvs_prev)
				q = _q_create_q % (ctx_ids, s0)
				con.execute(q)
			else:
				q = _q_create_ids % (ctx_ids,)
				con.execute(q)
				q_i = _q_insert_id % ctx_ids
				q = _q_select_vals_ids % (ctx_kvs_prev, key)
				for id, v in con.execute(q):
					if filter_fn(v):
						con.execute(q_i, (id,) )

			s1 = _q_select_filtered_kvs % (ctx_kvs_prev, ctx_ids, key, val)
			q = _q_create_q % (ctx_kvs, s1)
			con.execute(q)

			# - create indices
			q = _q_create_ro_idx % tuple(x for x in repeat(ctx_kvs, 6))
			#print q
			con.executescript(q)

		con.commit()
		con.isolation_level = old_isolation_level
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
			con.execute(_q_drop_table % self._hid_ids(hid))
			con.execute(_q_drop_table % self._hid_kvs(hid))
		con.execute("DELETE FROM kvss_hier WHERE 1")
		con.commit()

class KvssCore(object):
	re_filters = re.compile(r'(>=?|<=?|~|`)(.+)$')
	ineq_types = { 'int': types.IntType, 'float': types.FloatType }
	ops_ineq = set(("<", ">", ">=", "<="))
	re_ineq_rterm = re.compile(r'(\w+)\(([A-Za-z0-9.]+)\)')

	def get_ctx_from_path(self, path, ctx=None):
		if ctx is None:
			ctx = self.get_context()
		for k,v in path:
			if self._ctx_exists(ctx, k, v):
				self._ctx_push(ctx, k, v)
			else:
				return None
		return ctx

	def _ineq_filter(self, op, rterm):
		(ineq_t, ineq_rterm) = self.re_ineq_rterm.match(rterm).groups()
		ineq_t = self.ineq_types[ineq_t]
		ineq_rterm = ineq_t(ineq_rterm)
		mycmp = partial(cmp, ineq_t(ineq_rterm))
		if op == "<":
			filter_fn = lambda x: mycmp(ineq_t(x)) == 1
		elif op == ">":
			filter_fn = lambda x: mycmp(ineq_t(x)) == -1
		elif op == "<=":
			filter_fn = lambda x: mycmp(ineq_t(x)) != -1
		elif op == ">=":
			filter_fn = lambda x: mycmp(indeq_t(x)) != 1
		else:
			raise ValueError, "Unexpected error"
		return filter_fn

	def _get_filter(self, op, rterm):
		# push into context based in a filter
		if op in self.ops_ineq:
			# inequality filter
			return self._ineq_filter(op, rterm)
		elif op == '~':
			# regular expression filter
			raise NotImplementedError
		elif op == '`':
			# python evaluation filter
			raise NotImplementedError
		else:
			raise ValueError, "Unexpected Error"

	def ctx_push(self, ctx, key, val):
		# special case #1 : '^' (first value) or '$' (last value)
		if (val == '^') or (val == '$'):
			vals = [ v for v in self._iterate_vals(key, ctx) ]
			if len(vals) == 0:
				raise ValueError, "k=%s does not have values in %s" % (key, ctx)
			vals.sort()
			vidx = 0 if val == '^' else -1
			return self._ctx_push(ctx, key, vals[vidx])

		# special case #2: filters (>,>=,<,<=,~,`)
		m = self.re_filters.match(val)
		if m:
			op, rterm = m.groups()
			filter_fn = self._get_filter(op, rterm)
			return self._ctx_push(ctx, key, val, filter_fn)

		# standard case: just use the specified value
		if not self._ctx_exists(ctx, key, val):
			raise ValueError, "(%s,%s) does not exist in ctx:%s" % (key, val, str(ctx))
		self._ctx_push(ctx, key, val)

	def ctx_kk_op_iter(self, ctx0, ctx1, k_key, k_val, *op_fns):
		d0 = {}
		for e0 in self.iterate_entries(ctx0):
			if (k_key not in e0):
				raise ValueError, "k_key:%s does not exist in entry:%s" % (k_key, str(e0))
			if (k_val not in e0):
				raise ValueError, "k_val:%s does not exist in entry:%s" % (k_val, str(e0))
			k = e0[k_key]
			assert k not in d0
			d0[k] = e0[k_val]

		d1 = {}
		for e1 in self.iterate_entries(ctx1):
			if (k_key not in e1):
				raise ValueError, "k_key:%s does not exist in entry:%s" % (k_key, str(e1))
			if (k_val not in e1):
				raise ValueError, "k_val:%s does not exist in entry:%s" % (k_val, str(e1))
			k = e1[k_key]
			assert k not in d1
			d1[k] = e1[k_val]

		r = {}
		common_keys = set(d0.iterkeys()).intersection(set(d1.iterkeys()))
		for k in d0.iterkeys():
			yield k, [fn(d0[k],d1[k]) for fn in op_fns]

		del d0
		del d1
		#del common_keys

	def ctx_expand_all_iter(self, ctx, key, check_empty=False):
		""" expand context specified for all values for the specified key.
			If check_empty is defined, a value equal to '', will also be included
			for entries in this context that do not have the specified key """
		for val in self._iterate_vals(key, ctx):
			ctx_new = self.get_context(ctx)
			self._ctx_push(ctx_new, key, val)
			yield ctx_new
		if check_empty and self._check_empty(ctx, key):
			ctx_new = self.get_context(ctx)
			self._ctx_push(ctx_new, key, '')
			yield ctx_new

	def ctx_expand_iter(self, ctxl, key, val):
		""" expand contexts using wildcards """
		if (val != '?') and (val != '*'):
			for ctx in ctxl:
				self.ctx_push(ctx, key, val)
				yield ctx
		else:
			check_empty = (val == '*')
			for ctx in ctxl:
				for ctx_n in self.ctx_expand_all_iter(ctx, key, check_empty):
					yield ctx_n


	def ctx_expand_path(self, path, ctx=None):
		# /elem_type=double/host=clone/mt_conf=*/method=?/
		#  ?=> all entries under specified key
		#  *=> ? + entries which do not define specified key
		if ctx is None:
			ctx =  self.get_context()
		ctx_list = [ ctx ]
		paths = path.split('/')
		assert paths[0] == '' # full path for now
		assert paths[-1] == ''
		for path in paths[1:-1]:
			(key, val) = path.split(':')
			ctx_list = list(self.ctx_expand_iter(ctx_list, key, val))
		return ctx_list

	@staticmethod
	def path_from_ctx(ctx):
		return '/' + '/'.join([ '%s=%s' % (kv[0],kv[1]) for kv in ctx ]) + '/'

class Kvss(KvssCore, KvssSQL):
	pass

from cmdparse import CmdParser, CmdPyParser
import readline as rl
class KvssShell(CmdParser, CmdPyParser):
	commands = ("entries", "ls", "cd", "cc", "lsc", "cnt")
	def __init__(self, *args, **kwargs):
		self._kvss = kwargs.get("kvss")
		self._key = None
		super(KvssShell, self).__init__(*args, **kwargs)
		self._namespace["kvss"] = self._kvss
		ctx = kwargs.get("ctx", None)
		self._ctx = self._kvss.get_context(ctx)

	def _list_iter(self):
		kvss = self._kvss
		if self._key is None:
			for k in kvss._iterate_keys(self._ctx):
				yield k
		else:
			for v in kvss._iterate_vals(self._key, self._ctx):
				yield v

	def parse_entries(self, tokens):
		""" list entries at this specific context """
		toks = set(tokens)
		ctx = self._ctx
		kvss = self._kvss
		for id in kvss._iterate_entries(ctx):
			print id, " [",
			for k, v in kvss._iter_entry_kv(ctx, id):
				if (not toks) or (k in toks):
					print ("%s:%s" % (k,v)),
			print "]"

	def parse_cnt(self, tokens):
		""" count entries """
		print self._kvss.cnt_entries(self._ctx)

	def complete_cd(self, tokens):
		if not tokens:
			return [l for l in self._list_iter()]

		x = tokens.pop(0)
		if tokens:
			return []

		xlen = len(x)
		return filter(lambda l: x == l[:xlen], (self._list_iter() ))

	def parse_cd(self, tokens):
		""" enter a key/value (depending on the context) """
		if tokens:
			x = tokens.pop(0)
			if x == '..':
				self._cd_pop()
			else:
				self._cd_push(x)
		else:
			levels = len(self._ctx)
			if self._key:
				levels += 1
			for i in xrange(levels):
				self._cd_pop()
			return

	def parse_cc(self, tokens):
		""" drop the cache tables """
		self._kvss._clear_ro_cache()

	def _cd_push(self, x):
		kvss = self._kvss
		if x[0] == '!':
			if self._key is not None:
				print "not command in key context not supported"
				return
			for k in kvss._iterate_keys(self._ctx):
				if k == x[1:]:
					kvss._ctx_push(self._ctx, k, '')
					return
		elif self._key is None:
			for k in kvss._iterate_keys(self._ctx):
				if k == x:
					self._key = x
					return
			else:
				print "key:%s was not found" % x
		else:
			try:
				kvss.ctx_push(self._ctx, self._key, x)
				self._key = None
			except ValueError, ve:
				print ve
			return

	def _cd_pop(self):
		kvss = self._kvss
		if self._key is None:
			kvss._ctx_pop(self._ctx)
		else:
			self._key = None

	def parse_ls(self, tokens):
		""" list available keys or vals (depending on the context) """
		for l in self._list_iter():
			print l

	def parse_lsc(self, tokens):
		""" list available cache entries (depending on the context) """
		for i in self._kvss._iterate_cache(self._ctx, self._key):
			print i

	def go(self):
		kvss = self._kvss
		ctx = self._ctx
		rl.set_history_length(1000)
		rl.parse_and_bind('tab: complete')
		while True:
			prompt = '/'.join([''] + [ '%s:%s' % (x[0],x[1]) for x in ctx ] + [''])
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
	#ctx = kvss.get_context()

