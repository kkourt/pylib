def enc_delta_iter(items):
	prev = items[0]
	yield prev
	for item in items[1:]:
		yield (item - prev)
		prev = item

def enc_delta(items):
	return list(enc_delta_iter(items))

def enc_rle_iter(items):
	prev = items[0]
	freq = 1
	for item in items[1:]:
		if item == prev:
			freq += 1
		else:
			yield (prev, freq)
			prev = item
			freq = 1
		prev = item
	yield prev, freq

def enc_rle(items):
	return list(enc_rle_iter(items))

def rev_range(items):

	if len(items) == 0:
		return (1,1,1)

	if len(items) == 1:
		return (items[0], items[0] +1, 1)

	deltas = enc_delta(items)
	x0 = deltas[0]
	rles = enc_rle(deltas[1:])
	if (len(rles) != 1):
		raise ValueError, "Not a range"
	rle = rles[0]
	return (x0, x0 + (rle[1]+1)*rle[0], rle[0])

def test_rev_range(start, end, step):
	l0 = range(start, end, step)
	l1 = range(*rev_range(l0))
	if l0 != l1:
		print "Trying : ", start, end, step
		print "*********** Error"
		print l0
		print l1
		raise

def test_rev_range_rand(loops, l=100):
	import random
	for i in xrange(loops):
		start = random.randint(-l, l)
		end = random.randint(-l, l)
		step = 0
		while step == 0:
			step = random.randint(-l, l)
		test_rev_range(start, end, step)

if __name__ == '__main__':
	test_rev_range_rand(1024)
