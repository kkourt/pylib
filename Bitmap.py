import math
import array
from itertools import repeat

class Bitmap(object):
	def __init__(self):
		self.array = array.array('B')
		f,i = math.modf(math.log(self.array.itemsize<<3,2))
		assert(f == 0.0)
		self.shift = int(i)
		self.mask = (1<<self.shift) - 1
		self.last_bit = 0

	def test_bit(self, bit_nr):
		array = self.array
		w_idx = bit_nr >> self.shift
		if w_idx >= len(array):
			return 0
		return 1 if (array[w_idx] & (1<<(bit_nr & self.mask))) else 0

	def set_last_bit(self, last_bit):
		if last_bit < self.last_bit:
			raise NotImplementedError
		newlen = 1 + (last_bit>>self.shift)
		a = self.array
		alen = len(a)
		if newlen > alen:
			a.extend(repeat(0, (newlen - alen)))
		self.last_bit = last_bit

	def set_bit(self, bit_nr):
		array = self.array
		w_idx = bit_nr >> self.shift
		array_len = len(array)
		if w_idx >= array_len:
			array.extend(repeat(0, (w_idx + 1 - array_len)))
		array[w_idx] |= (1<<(bit_nr & self.mask))
		if bit_nr > self.last_bit:
			self.last_bit = bit_nr

	def uset_bit(self, bit_nr):
		raise NotImplementedError

	def __repr__(self):
		return "<Bitmap last_bit:%s %s>" % (self.last_bit, self.array)

	def __and__(self, other):
		ret = Bitmap()
		nlast_bit = min(self.last_bit, other.last_bit)
		ret.set_last_bit(nlast_bit)
		na, a0, a1 = ret.array, self.array, other.array
		for i in xrange(len(na)):
			na[i] = a0[i] & a1[i]
		return ret

	def __eq__(self, other):
		if self.last_bit != other.last_bit:
			return False
		return (self.array == other.array)

	def __nonzero__(self):
		for w in self.array:
			if w != 0:
				return True
		return False

	def __invert__(self):
		ret = Bitmap()
		last_bit = self.last_bit
		ret.set_last_bit(last_bit)
		oa = self.array
		na = ret.array
		size = len(na) - 1
		bmask = (1<<(oa.itemsize*8)) - 1 # ugly hack, to avoid overflow
		for i in xrange(size):
			na[i] = bmask & (~oa[i])
		bmask = (1<<(1 + (last_bit & self.mask))) - 1
		na[size] = bmask & (~oa[size])
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


if __name__ == '__main__':
	test_bitmap()
	test_bitmap_and()
	test_bitmap_invert()
