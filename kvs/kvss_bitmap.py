from cPickle import dumps, loads
from bsddb import rnopen, btopen
from os import makedirs
from os.path import isdir
from itertools import repeat, imap
import math
import array

from bdb_utils import bdb_del_under
from kvss_sql import KvssShell, KvssCore

# indices store:
# datasources        => pickled set of datasources
# keys               => pickled set of keys
# keys/%s/bitmap     => bitmap for k
# keys/%s/%s/bitmap  => bitmap for (k,v)
# keys/%s/%s/keys    => pickled set of keys

class Bitmap(object):
	def __init__(self):
		self.array = array.array('B')
		f,i = math.modf(math.log(self.array.itemsize<<3,2))
		assert(f == 0.0)
		self.shift = int(i)
		self.mask = (1<<self.shift) - 1
		self.bits_nr = 0

	def test_bit(self, bit_nr):
		array = self.array
		w_idx = bit_nr >> self.shift
		if w_idx >= len(array):
			return 0
		return 1 if (array[w_idx] & (1<<(bit_nr & self.mask))) else 0

	def set_bits_nr(self, bits_nr):
		if bits_nr < self.bits_nr:
			raise NotImplementedError
		newlen = 1 + (bits_nr>>self.shift)
		a = self.array
		alen = len(array)
		if newlen > alen:
			a.extend(repeat(0, (newlen - alen)))
		self.bits_nr = bits_nr


	def set_bit(self, bit_nr):
		array = self.array
		w_idx = bit_nr >> self.shift
		array_len = len(array)
		if w_idx >= array_len:
			array.extend(repeat(0, (w_idx + 1 - array_len)))
		array[w_idx] |= (1<<(bit_nr & self.mask))
		if bit_nr + 1 > self.bits_nr:
			self.bits_nr = bit_nr + 1

	def uset_bit(self, bit_nr):
		raise NotImplementedError

	def __and__(self, other):
		ret = Bitmap()
		nbits_nr = min(self.bits_nr, other.bits_nr)
		ret.set_bits_nr(nbits_nr)
		na, a0, a1 = ret.array, self.array, other.array
		for i in xrange(len(na)):
			na[i] = a0[i] & a1[i]
		return ret

	def __neg__(self):
		ret = Bitmap()
		bits_nr = self.bits_nr
		ret.set_bits_nr(bits_nr)
		oa = self.array
		na = ret.array
		size = len(na) - 1
		for i in xrange(size):
			na[i] = ~oa[i]
		na[size] = (~os[size]) & (~(bits_nr & self.mask))
		return ret

	def iter_set_bits(self):
		array = self.array
		shift = self.shift
		wbits = 8*array.itemsize
		for widx in xrange(len(array)):
			w = array[widx]
			for i in xrange(wbits):
				if w & (1<<i):
					yield (widx << shift) + i

import random
def test_bitmap_and(times=1024, ints=1024, maxint=10000):
	for t in xrange(times):
		s0 = set()
		b0 = Bitmap()
		for i in xrange(ints):
			n = random.randint(0, maxint)
			s0.add(n)
			b0.set_bit(n)

		s1 = set()
		b1 = Bitmap()
		for i in xrange(ints):
			n = random.randint(0, maxint)
			s1.add(n)
			b1.set_bit(n)

		s = s0.intersection(s1)
		l = sorted(s)
		b = b0 & b1
		i = 0
		for x in b.iter_set_bits():
			assert x == l[i]
			i += 1

def test_bitmap(times=1024, ints=1024, maxint=100000):
	for t in xrange(times):
		rand_set = set()
		bitmap = Bitmap()
		for i in xrange(ints):
			n = random.randint(0, maxint)
			rand_set.add(n)
			bitmap.set_bit(n)
		l = sorted(rand_set)
		i = 0
		for x in bitmap.iter_set_bits():
			assert x == l[i]
			i += 1

class KvssBitmap(object):
	def __init__(self, dbdir):
		if not isdir(dbdir):
			makedirs(dbdir)
		self._entries_db = rnopen(dbdir + '/entries.db')
		self._indices_db = btopen(dbdir + '/indices.db')

	def get_context(self, ctx=None):
		return list() if ctx is None else list(ctx)

	def _do_iter_entries(self, bmp=None):
		edb = self._entries_db
		e_iter = xrange(1, len(edb) +1) if bmp is None else bmp.iter_set_bits()
		for e_idx in e_iter:
			yield e_idx, loads(edb[e_idx])

	def _do_get_bitmap(self, pred_fn, bmp_id, bmp_parent=None):
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
			bmp.array.fromstring(idb[bmp_id])
		else:
			pred_fn = lambda e: (key in e)
			bmp = self._do_get_bitmap(pred_fn, bmp_id)
			idb[bmp_id] = bmp.array.tostring()
			idb.sync()
		return bmp

	def _set_bitmap_notk(self, key, val):
		bmp_id = "keys/%s/%s/bitmap" % (key, val)
		idb = self._indices_db
		if bmp_id in idb:
			return
		bmp_parent = self._get_bitmap_k(key)

	def _set_bitmap_kv(self, key, val, pred_fn=None):
		bmp_id = "keys/%s/%s/bitmap" % (key, val)
		idb = self._indices_db
		if bmp_id in idb:
			return
		bmp_parent = self._get_bitmap_k(key)
		if pred_fn is None:
			pred_fn = lambda e: (e[key] == val)
		bmp = self._do_get_bitmap(pred_fn, bmp_id, bmp_parent)
		idb[bmp_id] = bmp.array.tostring()
		idb.sync()

	def _get_bitmap_kv(self, key, val):
		bmp_id = "keys/%s/%s/bitmap" % (key, val)
		idb = self._indices_db
		bmp = Bitmap()
		bmp.array.fromstring(idb[bmp_id])
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

	def _insert_kvs(self, lentries):
		entries_db = self._entries_db
		entries_nr = len(entries_db)
		indices_db = self._indices_db
		keys = set()
		# add entries
		for entry in lentries:
			keys.update(entry.iterkeys())
			entries_nr += 1
			entries_db[entries_nr] = dumps(entry)
		indices_db['keys'] = dumps(keys)
		# delete indices have changed
		try:
			old_keys = loads(indices_db['keys'])
			for key in keys.intersection(old_keys):
				bdb_del_under(indices_db.db, "keys/%s/" % key)
		except KeyError:
			pass
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

	def _iterate_vals(self, key, ctx):
		bmp = self._get_bitmap_k(key)
		if ctx:
			ctx_bmp = self._bmp_from_ctx(ctx)
			bmp = bmp & ctx_bmp
		vals = set()
		for eid, entry in self._do_iter_entries(bmp):
			vals.add(entry[key])
		return iter(vals)

	def iterate_entries(self, ctx):
		bmp = self._bmp_from_ctx(ctx) if ctx else None
		for eid, entry in self._do_iter_entries(bmp):
			yield entry

	def _ctx_push(self, ctx, key, val, filter_fn=None):
		if filter_fn:
			pred_fn = lambda x : filter_fn(x[key])
		self._set_bitmap_kv(key, val)
		ctx.append((key,val))
		return ctx

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

	kvss = Kvss(dbdir='kvss.bmp')
	kvss.insert_kvs_ds(_tmp_lod, 'DS0')
	kvss_sh = KvssShell(kvss=kvss)
	kvss_sh.go()
