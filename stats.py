
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
