import Gnuplot as G
import types
from functools import partial

_g = G.Gnuplot(persist=1)

def draw_data(data,f,logscale=False):
	d = G.Data(data)
	_g('set terminal png size 640 480')
	_g("set output '" + f + "'")
	_g.plot(d)
	if logscale:
		_g("set logscale y")
		_g("set output 'logscale-" + f + "'")
		_g.plot(d)
		_g("unset logscale y")

class BarsMultiple(object):
	def __init__(self, y_max, y_min):
		self.dir = mkdtemp(prefix="bars-mult")
		self.g = G.Gnuplot()
		self.g('set terminal png size 640 480')
		self.g("set yrange [%.30f:%.30f]" % (y_min,y_max))

	def add(self, f, data):
		d = G.Data(data)

		self.g("set output \'%s" % self.dir + '/' + f + '-scale.png\'')
		self.g.plot(d)
		#self.g("set logscale y")
		#self.g("set output \'%s" % self.dir + '/' + f + '-logscale.png\'')
		#self.g.plot(d)
		#self.g("unset logscale y")

def _xtics(items):
	xtics = ("\"%s\" %d" % (str(items[i]), i) for i in xrange(len(items)))
	xtics = "set xtics (" + ", ".join(xtics) +")"
	_g(xtics)

def plot_lps(data, file=None, **kwargs):
	title = kwargs.get("title", '')
	canv_size = kwargs.get("canvas_size", None)

	_g("set output '" + file + "'")
	_g("set style fill solid 1")
	_g("set title \"%s\"" % title)
	_g("set key left top")

	if isinstance(data, types.DictType):
		keys = sorted(data.iterkeys())
		klen = len(keys)
		if isinstance(data[keys[0]], types.DictType):
			# we assume similar structure for all items
			keys2 = sorted(data[keys[0]])
			k2len = len(keys2)
			data = [ [ data[k1][k2] for k1 in keys] for k2 in keys2 ]
			_xtics(keys2)
			mkD = partial(G.Data, data, with_="linespoints", filename=file + '.gp')
			_using = lambda i : "0:%d" % (i+1)
			ds = [mkD(using=_using(i), title=str(keys[i])) for i in xrange(klen)]
		else:
			data = [ data[k] for k in keys ]
			ds = [ G.Data(data, with_="linespoints", filename=file + '.gp') ]
			_xtics(keys)

	canv_size = "" if canv_size is None else "size %s,%s" % canv_size
	if file is None:
		_g("set terminal wxt %s" % canv_size)
	else:
		_g("set terminal png %s" % canv_size)

	fname = file + '.gp'
	#yrange = kwargs.get("yrange", (y_min,y_max))
	#_g("set yrange [%s:%s]" % yrange)
	_g.plot(*ds)

def plot_bars(data, file=None, **kwargs):
	ksort_key = kwargs.get("ksort_key", None)
	ksort_key2 = kwargs.get("ksort_key2", None)
	bw = kwargs.get("bw", 0.15)
	title = kwargs.get("title", '')
	canv_size = kwargs.get("canvas_size", None)

	_g("set output '" + file + "'")
	_g("set boxwidth %f" % bw)
	_g("set style fill solid 1")
	_g("set title \"%s\"" % title)
	_g("set key left top")
	if isinstance(data, types.DictType):
		keys = sorted(data.iterkeys(), ksort_key)

		klen = len(keys)
		xtics = ("\"%s\" %d" % (str(keys[i]), i) for i in xrange(klen))
		xtics = "set xtics (" + ", ".join(xtics) +")"
		_g(xtics)

		x_step = bw
		if isinstance(data[keys[0]], types.DictType):
			# we assume similar structure for all items
			keys2 = sorted(data[keys[0]], ksort_key2)
			k2len = len(keys2)
			data = [ [ data[k1][k2] for k2 in keys2] for k1 in keys ]
			d_max = max(max(d) for d in data)
			d_min = min(min(d) for d in data)
			x_min = -x_step*(k2len/2.0)  +  (x_step/2.0)
			mkD = partial(G.Data, data, with_="boxes", filename=file + '.gp')
			_using = lambda i : "($0%+f):%d"  % (x_min + x_step*i, i+1)
			ds = [mkD(using=_using(i), title=keys2[i]) for i in xrange(k2len)]
		else:
			k2len = 1
			data = [ data[k] for k in keys ]
			d_max = max(data)
			d_min = min(data)
			ds = [ G.Data(data, with_="boxes", filename=file + '.gp') ]

		canv_size = None
		x_size = klen*k2len*25
		canv_size = (max(x_size, 650), 500)
		y_min = 0 # (d_min - (d_max - d_min)*.1)
		y_max = d_max + (d_max - d_min)*.1

	canv_size = "" if canv_size is None else "size %s,%s" % canv_size
	if file is None:
		_g("set terminal wxt %s" % canv_size)
	else:
		_g("set terminal png %s" % canv_size)

	fname = file + '.gp'
	yrange = kwargs.get("yrange", (y_min,y_max))
	_g("set yrange [%s:%s]" % yrange)
	_g.plot(*ds)

def test_bars():
	d0 = {1:20, 2:30, 3:35}
	d1 = {'A':20,  'B':45, 'C':30}
	d2 = {'A': {'0': 20, '1': 30}, 'B': {'0': 40, '1': 10}}
	d3 = {
		'A': {'0': 20, '1': 30, '2': 10, '3': 14},
		'B': {'0': 40, '1': 10, '2': 27, '3': 32},
	}
	d4 = {
		'A': {'0': 20, '1': 30, '2': 10, '3': 14, '4': 20},
		'B': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'C': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'D': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'E': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'F': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'G': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'H': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'I': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
		'J': {'0': 40, '1': 10, '2': 27, '3': 32, '4': 32},
	}
	draw_bars(d0, "plots/1.png", title="foola")
	#draw_bars(d1, "plots/1.png")
	draw_bars(d2, "plots/2.png")
	draw_bars(d3, "plots/3.png")
	draw_bars(d4, "plots/4.png")

def test_data():
	draw_data(range(100))

if __name__ == '__main__':
	#test_bars()
	d3 = {
		'0' : {'A' : 10, 'B': 12},
		'1' : {'A' : 15, 'B': 18},
		'2' : {'A' : 20, 'B': 22},
		'3' : {'A' : 28, 'B': 32},
	}
	plot_lps(d3, "plots/lp-3.png")
