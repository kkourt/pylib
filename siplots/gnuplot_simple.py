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
	if isinstance(items[0], types.IntType):
		return
	xtics = ("\"%s\" %d" % (str(items[i]), i) for i in xrange(len(items)))
	xtics = "set xtics (" + ", ".join(xtics) +")"
	_g(xtics)

_terms = {
	''   : 'png',
	'png': 'png',
	'eps': 'postscript',
	'tex': 'epslatex'
}
def _prepare(file=None, **kwargs):
	canv_size = kwargs.get("canvas_size", None)
	canv_size = "" if canv_size is None else "size %s,%s" % canv_size
	title = kwargs.get("title", '')
	_g("set output '" + file + "'")
	_g("set title \"%s\"" % title)
	_g("set key left top")
	_g("set grid")
	_g("set size .9,.9")
	_g("set style fill solid 1")
	yrange = kwargs.get("yrange", None)
	if yrange is None:
		_g("set yrange[*:*]")
	else:
		_g("set yrange[%f:%f]" % yrange)
	if file is None:
		_g("set terminal wxt %s" % canv_size)
	else:
		_g("set terminal %s %s" % (_terms[file.split('.')[-1]], canv_size))


_bw_def = 0.15
def _data_dict(indata, file, with_, **kwargs):
	keys = sorted(indata.iterkeys())
	x0 = indata[keys[0]]
	if isinstance(x0, types.DictType): # check the first element
		if "keys2" in kwargs:
			keys2 = kwargs["keys2"]
		else:
			keys2 = sorted(x0) # we assume the same structure for all
		try:
			data = [ [ indata[k1][k2] for k1 in keys] for k2 in keys2 ]
		except KeyError:
			raise
		_xtics(keys2)
		if with_ in ("linespoints", "lines"):
			using_ = lambda i : "0:%d" % (i+1)
		elif with_ == "points":
			for i in xrange(len(keys2)):
				# use both x and y
				data[i] = [keys2[i]] + data[i]
			using_ = lambda i : "($1/(1024.0*1024.0)):%d" % (i+2)
		elif with_ == "boxes":
			bw = kwargs.get("bw", _bw_def)
			x_step = bw
			x_min = -bw*(len(keys)/2.0) + (x_step/2.0)
			using_ = lambda i : "($0%+f):%d"  % (x_min + x_step*i, i+1)
		else:
			raise NotImplementedError
		mkD = partial(G.Data, data, with_=with_, filename=file + '.gp')
		ds = [mkD(using=using_(i), title=str(keys[i])) for i in xrange(len(keys))]
	else:
		data = [ indata[k] for k in keys ]
		_xtics(keys)
		ds = [ G.Data(data, with_=with_, filename=gpfile) ]
	return ds

def plot_lps(data, file=None, **kwargs):
	if isinstance(data, types.DictType):
		ds = _data_dict(data, file, with_="linespoints", **kwargs)
	_prepare(file, **kwargs)
	_g.plot(*ds)

def plot_pnts(data, file=None, **kwargs):
	if isinstance(data, types.DictType):
		ds = _data_dict(data, file, with_="points", **kwargs)
	_prepare(file, **kwargs)
	_g.plot(*ds)

def plot_bars(data, file=None, **kwargs):
	if isinstance(data, types.DictType):
		ds = _data_dict(data, file, with_="boxes", **kwargs)
	bw = kwargs.get("bw", _bw_def)
	_g("set boxwidth %f" % bw)
	_prepare(file, **kwargs)
	_g.plot(*ds)


def tests():
	d0 = {
		'Pizza'  : {'May': 20, 'June': 17, 'July': 15 },
		'Burger' : {'May': 13, 'June': 15, 'July': 7  }
	}
	plot_bars(d0,"plots/d0-bar.png")
	plot_lps(d0,"plots/d0-lps.png")

if __name__ == '__main__':
	tests()
