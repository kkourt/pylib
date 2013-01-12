import types

def dhier_reduce(lod, key, nonexistant=None):
	ret = {}
	for d in lod:
		if not isinstance(d, dict):
			d = dict(d)
		v = d.pop(key, nonexistant)
		if v not in ret:
			ret[v] = []
		ret[v].append(d)
	return {key: ret}

def dhier_reduce_many(lod, keys, map_fn=None, nonexistant=None):

	if len(keys) == 0:
		return  map_fn(lod) if map_fn is not None else lod

	k = keys[0]
	ret = {}
	d = dhier_reduce(lod, k, nonexistant)[k]
	for xk, xd in d.iteritems():
		ret[xk] = dhier_reduce_many(xd, keys[1:], map_fn, nonexistant)
	return ret

def dhier_iterate_leafs(d):
	for v in d.itervalues():
		if isinstance(v, types.DictType):
			for v_ in dhier_iterate_leafs(v):
				yield v_
		else:
			yield v
