#!/usr/bin/env python
from itertools import imap
from math import pow

kilo = 1000
sizes = dict(imap(
	lambda x:(pow(kilo,x[0]),x[1]),
	((1, 'k'), (2, 'M'), (3, 'G'))
))

ks = sorted(sizes.iterkeys(), reverse=True)

def humanize_nr(val):
	post = ''
	val = float(val)
	for k in ks:
		if val >= k:
			val = val / k
			post = sizes[k]
	return (val,post)


def humanize(file_in, file_out):
	while True:
		line = file_in.readline()
		if line == '':
			break
		val = float(line)
		print "%8.1f%s\n" % humanize_nr(val)
		#file_out.write("%3f%s\n" % (val,post))
		#file_out.flush()

import sys
if __name__ == '__main__':
	humanize(sys.stdin, sys.stdout)
