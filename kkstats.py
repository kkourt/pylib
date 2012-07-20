
def mean(items):
	sum = 0.0
	nr = 0
	for i in items:
		sum += i
		nr += 1
	return sum/nr if nr > 0 else 0

from types import TupleType, ListType
from math import sqrt

def deviation(data):
	if not (isinstance(data, TupleType) or isinstance(data, ListType)):
		data = tuple(data)
	m = mean(data)
	dev = sqrt(float(sum(( (m-d)**2 for d in data )))/len(data))
	return dev

class StatList(list):
	"""
	Overload a list, adding some simple statistics functions

	It allows the user to define a function to map the elements of the list,
	before performing the calculations

	Example:
	>>> x = StatList((10,10,11,12))
	>>> print x.samples, x.avg
	4 10.75
	>>> x = StatList((10,10,11,12), fn=lambda x: x/10.0)
	>>> print x.samples, x.avg
	4 1.075

	"""
	def __init__(self, l=[], fn=None):
		list.__init__(self, l)
		if fn is None:
			fn = lambda x: x
		self._fn = fn

	@property
	def max(self):
		return max(map(self._fn,self))

	@property
	def min(self):
		return min(map(self._fn,self))

	@property
	def avg(self):
		return mean(map(self._fn,self))

	@property
	def samples(self):
		return len(self)

	@property
	def dev(self):
		return deviation(map(self._fn,self))
