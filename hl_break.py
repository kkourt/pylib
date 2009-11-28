from math import floor, ceil

# high-low break

def hl_break(n, high, low):
	""" return a list of integers with a sum of n.
	The elements of the list should be <= high and >= low """

	rem = n % high
	if rem == 0:
		return [ high for i in xrange(n / high) ]

	max_elems = int(floor(float(n)/float(low)))
	min_elems = int(ceil(float(n)/float(high)))

	elems = min_elems
	while True:
		if elems > max_elems:
			raise ValueError
		if elems*low <= n:
			break
		elems += 1

	ret = [ high for i in xrange(elems -1) ]
	while rem < low:
		x = min(len(ret), rem)
		for i in xrange(x):
			ret[i] -= 1
		rem += x

	ret.append(rem)
	return ret
