from cPickle import dumps, loads
from bsddb import rnopen, btopen
from os import makedirs
from os.path import isdir
from itertools import imap

from bdb_utils import bdb_del_under
from kvss_sql import KvssShell, KvssCore

from cBitmap import Bitmap

# indices store:
# datasources        => pickled set of datasources
# keys               => pickled set of keys
# keys/%s/bitmap     => bitmap for k
# keys/%s/%s/bitmap  => bitmap for (k,v)
# keys/%s/%s/keys    => pickled set of keys
# cache/vals/

class KvssBitmap(object):
	def __init__(self, dbdir='kvss.bmp', debug=False):
		if not isdir(dbdir):
			makedirs(dbdir)
		self._info = "bmp"
		self._entries_db = rnopen(dbdir + '/entries.db')
		self._indices_db = btopen(dbdir + '/indices.db')

	def get_context(self, ctx=None):
		return list() if ctx is None else list(ctx)

	def _do_iter_entries(self, bmp=None):
		edb = self._entries_db
		e_iter = xrange(0, len(edb)) if bmp is None else list(imap(int, bmp.iter_set_bits()))
		for e_idx in e_iter:
			yield e_idx, loads(edb[e_idx + 1])

	def _do_get_bitmap(self, pred_fn, bmp_parent=None):
		bmp = Bitmap()
		for entry_id, entry in self._do_iter_entries(bmp_parent):
			if pred_fn(entry):
				bmp.set_bit(entry_id)
		return bmp

	def _get_bitmap_k(self, key):
		bmp_id = "keys/%s/bitmap" % (key,)
		idb = self._indices_db
		if bmp_id in idb:
			bmp = Bitmap()
			# Note that we are oversizing the array here
			bmp.init_frombuffer(idb[bmp_id],len(self._entries_db))
		else:
			pred_fn = lambda e: (key in e)
			bmp = self._do_get_bitmap(pred_fn)
			idb[bmp_id] = str(buffer(bmp))
			idb.sync()
		return bmp

	def _set_bitmap_notk(self, key):
		bmp_id = "keys/%s/%s/bitmap" % (key, '')
		idb = self._indices_db
		if bmp_id in idb:
			return
		bmp = self._get_bitmap_k(key)
		bmp.expand(len(self._entries_db))
		bmp = ~bmp
		idb[bmp_id] = str(buffer(bmp))
		idb.sync()

	def _set_bitmap_kv(self, key, val, pred_fn=None):
		bmp_id = "keys/%s/%s/bitmap" % (key, val)
		idb = self._indices_db
		if bmp_id in idb:
			return
		bmp_parent = self._get_bitmap_k(key)
		if pred_fn is None:
			pred_fn = lambda e: (e[key] == val)
		bmp = self._do_get_bitmap(pred_fn, bmp_parent)
		idb[bmp_id] = str(buffer(bmp))
		idb.sync()

	def _get_bitmap_kv(self, key, val):
		bmp_id = "keys/%s/%s/bitmap" % (key, val)
		idb = self._indices_db
		bmp = Bitmap()
		# Note that we are oversizing the array here
		bmp.init_frombuffer(idb[bmp_id], len(self._entries_db))
		return bmp

	def _bmp_from_ctx(self, ctx):
		idb = self._indices_db
		imap_fn = lambda kv: self._get_bitmap_kv(kv[0],kv[1])
		reduce_fn = lambda b0, b1: b0 & b1
		ret = reduce(reduce_fn, imap(imap_fn, ctx))
		return ret

	def _get_keys(self, key, val):
		idb = self._indices_db
		keys_id = "/keys/%s/%s/keys" % (key,val)
		if keys_id in idb:
			keys = loads(idb[keys_id])
		else:
			keys = set()
			bmp = self._get_bitmap_kv(key, val)
			for entry_id, entry in self._do_iter_entries(bmp):
				keys.update(entry.iterkeys())
			idb[keys_id] = dumps(keys)
			idb.sync()
		return keys

	#######################
	  ### Interface ###
	#######################

	def _insert_kvs(self, lentries):
		entries_db = self._entries_db
		entries_nr = len(entries_db)
		indices_db = self._indices_db
		keys = set()
		# add entries
		for entry in lentries:
			keys.update(entry.iterkeys())
			entries_nr += 1
			for k,v in entry.iteritems():
				entry[k] = str(v)
			entries_db[entries_nr] = dumps(entry)
		if "keys" in indices_db:
			old_keys = loads(indices_db['keys'])
			for key in keys.intersection(old_keys):
				bdb_del_under(indices_db.db, "keys/%s/" % key)
			keys.update(old_keys)
		indices_db['keys'] = dumps(keys)
		# delete all cache entries
		bdb_del_under(indices_db.db, "cache/")
		entries_db.sync()
		indices_db.sync()

	def insert_kvs_ds(self, lentries, datasource):
		idb = self._indices_db
		if 'datasources' in idb:
			ds_set = loads(idb['datasources'])
			if datasource in ds_set:
				print "datasource %s exists allready" % datasource
				return
		else:
			ds_set = set()
		self._insert_kvs(lentries)
		ds_set.add(datasource)
		idb['datasources'] = dumps(ds_set)
		idb.sync()
		return

	def _iterate_keys(self, ctx):
		indices_db = self._indices_db
		keys = loads(indices_db['keys'])
		ctx_keys = set()
		for key,val in ctx:
			ks = self._get_keys(key, val)
			keys = keys.intersection(ks)
			ctx_keys.add(key)
		return iter(keys.difference(ctx_keys))

	def _get_vals(self, key, ctx):
		bmp = self._get_bitmap_k(key)
		if ctx:
			ctx_bmp = self._bmp_from_ctx(ctx)
			bmp = bmp & ctx_bmp
		vals = set()
		for eid, entry in self._do_iter_entries(bmp):
			vals.add(entry[key])
		return vals

	@staticmethod
	def _ctx_id(ctx):
		ret = ''
		if ctx:
			ret = '/'.join([ '%s=%s' % (kv[0],kv[1]) for kv in ctx ]) + '/'
		return ret

	@staticmethod
	def _ctx_getval(ctx, key):
		for k,v in ctx:
			if k == key:
				return v
		else:
			return None

	def _iterate_vals(self, key, ctx):
		idb = self._indices_db
		cid = "cache/vals/" + self._ctx_id(ctx) + key
		if cid in idb:
			ret = iter(loads(idb[cid]))
		else:
			vals = self._get_vals(key, ctx)
			idb[cid] = dumps(vals)
			idb.sync()
			ret = iter(vals)
		return ret

	def iterate_entries(self, ctx):
		bmp = self._bmp_from_ctx(ctx) if ctx else None
		for eid, entry in self._do_iter_entries(bmp):
			yield entry
	ientries = iterate_entries

	def cnt_entries(self, ctx):
		if not ctx:
			ret = len(self._entries_db)
		else:
			bmp = self._bmp_from_ctx(ctx)
			ret = reduce(lambda x,y: x+1, bmp.iter_set_bits(), 0)
		return ret

	def _ctx_push(self, ctx, key, val, filter_fn=None):
		if len(val) > 0:
			pred_fn = None
			if filter_fn is not None:
				pred_fn = lambda x : filter_fn(x[key])
			self._set_bitmap_kv(key, val, pred_fn)
		else:
			self._set_bitmap_notk(key)
		ctx.append((key,val))
		return ctx

	def _ctx_pop(self, ctx):
		if ctx:
			ctx.pop()

	def _clear_ro_cache(self):
		idb = self._indices_db
		bdb_del_under(idb.db, "cache/")
		idb.sync()

	def _check_empty(self, ctx, key):
		self._set_bitmap_notk(key)
		nctx = ctx + [(key, '')]
		bmp = self._bmp_from_ctx(nctx)
		return bool(bmp)


if __name__ == '__main__':
	_tmp_lod = (
		{ 'owner': 'kornilios', 'type':'PC', 'processor':'AMD' },
		{ 'owner': 'kornilios', 'type':'Laptop', 'processor':'Intel' },
		{ 'owner': 'kornilios', 'type':'CAR', 'brand':'Peugeot' },
		{ 'owner': 'vkoukis', 'type':'PC', 'processor':'Intel' },
		{ 'owner': 'gtsouk', 'type':'CAR', 'brand':'KIA' }
	)

	class Kvss(KvssCore, KvssBitmap):
	    pass

	kvss = Kvss()
	kvss.insert_kvs_ds(_tmp_lod, 'DS0')
	kvss_sh = KvssShell(kvss=kvss)
	kvss_sh.go()
