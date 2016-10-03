#!/usr/bin/env python
from itertools import imap
from math import pow

kilo = 1000
sizes_10 = dict(imap(
	lambda x:(pow(kilo,x[0]),x[1]),
	((1, 'k'), (2, 'M'), (3, 'G'))
))

ks_10 = sorted(sizes_10.iterkeys(), reverse=True)

kib = 1024
sizes_p2 = dict(imap(
	lambda x:(pow(kib,x[0]),x[1]),
	((1, 'ki'), (2, 'Mi'), (3, 'Gi'))
))

ks_p2 = sorted(sizes_p2.iterkeys(), reverse=True)

def humanize_nr(val, p2=False):
	post = ''
	val = float(val)
	ks = ks_p2 if p2 else ks_10
	sizes = sizes_p2 if p2 else sizes_10
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
