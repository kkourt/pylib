
class VerifyCnt(object):
	def __init__(self, cnt):
		self.cnt = cnt
		self.i = 0

	def add(self, i):
		self.i += 1

	def verify(self):
		assert self.i == self.cnt, "cnt != expected (%d != %d)" % (self.i, self.cnt)

def iter_assert(iterator, verifier):
	for i in iterator:
		verifier.add(i)
		yield i
	verifier.verify()

def iter_assert_cnt(iterator, count):
	return iter_assert(iterator, VerifyCnt(count))

if __name__ == '__main__':
	print "this should be OK"
	list(iter_assert_cnt(xrange(10), 10))
	print "this should Fail:"
	try:
		list(iter_assert_cnt(xrange(10), 9))
	except AssertionError:
		print "\tIt did!"
	else:
		print "\tIt didn't!"
