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

## returns a tuple of
#  ret1: a hierarchical dict with key=parameter value 
#                                 val=hiearchical dict or data list
#  ret2: a dict with key->set of parameters
def dhier_reduce_many2(lod, keys, map_fn=None, nonexistant=None):

	if len(keys) == 0:
		ret1 =  map_fn(lod) if map_fn is not None else lod
		ret2 = {}
		return (ret1, ret2)

	k = keys[0]
	ret1  = {}
	ret2v = []
	ret2  = {}
	d = dhier_reduce(lod, k, nonexistant)[k]
	for xk, xd in d.iteritems():
		ret1[xk], ret2_prev = dhier_reduce_many2(xd, keys[1:], map_fn, nonexistant)
		ret2v.append(xk)
		for param_key, param_set in ret2_prev.iteritems():
			if param_key not in ret2:
				ret2[param_key] = param_set
			else:
				ret2[param_key] = ret2[param_key].union(param_set)

	assert k not in ret2
	ret2[k] = set(ret2v)
	return (ret1, ret2)

def dhier_iterate_leafs(d):
	for v in d.itervalues():
		if isinstance(v, types.DictType):
			for v_ in dhier_iterate_leafs(v):
				yield v_
		else:
			yield v
