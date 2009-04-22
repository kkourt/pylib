""" Berkeley DB frequency hash """

from bsddb.db import DB, DB_HASH, DB_CREATE
from os import makedirs, sysconf
from os.path import dirname, isdir
from struct import pack, unpack

cache_perc = 0.5 # percentage of total memory allocated to the cache
_cache_size = int((sysconf('SC_PHYS_PAGES')*sysconf('SC_PAGESIZE'))*cache_perc)

def bdb_init_hash(db_file, cache_size=None):
	""" initialize a bdb hash """
	db_dir = dirname(db_file)
	if not isdir(db_dir):
		makedirs(db_dir)
	db = DB()
	if cache_size is None:
		cache_size = _cache_size
	db.set_cachesize (
		cache_size / (1024*1024*1024),
		cache_size % (1024*1024*1024)
	)
	db.open(db_file, dbtype=DB_HASH, flags=DB_CREATE)
	return db

def hfreq_bdb_init(db_file, cache_size=None):
	""" initialize (open) a bdb frequency hash """
	return bdb_init_hash(db_file, cache_size)

def set_bdb_init(db_file, cache_size=None):
	return bdb_init_hash(db_file, cache_size)

def set_bdb_add(db, v):
	if not db.has_key(v):
		db[v] = '1'

def set_bdb_rem(db, v):
	if db.has_key(v):
		del db[v]

def hfreq_bdb_add(db, v, freq=1):
	try:
		db[v] = pack("L", unpack("L", db[v])[0] + long(freq))
	except KeyError:
		db[v] = pack("L", long(freq))

def hfreq_bdb_get(db, v):
	return unpack("L", db[v])[0]

def bdb_iteritems(db):
	c = db.cursor()
	while True:
		item = c.next()
		if item is None:
			break
		yield item

def hfreq_bdb_iteritems(db):
	c = db.cursor()
	while True:
		item = c.next()
		if item is None:
			break
		yield (item[0], unpack("L", item[1])[0])

def bdb_len(db):
	return reduce(lambda x,y: x+1, bdb_iteritems(db), 0)

def bdb_del_under(db, under):
	# intended for btree dbs
	cursor = db.cursor()
	key = cursor.set_range(under, dlen=0, doff=0)[0]
	while True:
		if not key.startswith(under):
			break
		cursor.delete()
		try:
			key = cursor.next(dlen=0, doff=0)[0]
		except KeyError:
			break
	cursor.close()
