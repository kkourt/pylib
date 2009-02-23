from cStringIO import StringIO
import re

class Var(object):
	def __init__(self, name, val):
		self.name = name
		self.value = val
		self.old_value = None

class Vars(object):
	def __init__(self):
		self._vars = {}
		self.data = []
	
	def assign(self, name, val):
		if name not in self._vars:
			self._vars[name] = Var(name, val)
			return

		v = self._vars[name]
		if v.value is not None:
			self.flush()
		v.old_value = v.value
		v.value = val

	def flush(self):
		d = {}
		for v in self._vars.itervalues():
			if v.value is None:
				d[v.name] = v.old_value
			else:
				d[v.name] = v.old_value = v.value
				v.value = None
		self.data.append(d)

class LogParser(object):
	re_regex = re.compile(r'^/(.*)/$')
	re_ws = re.compile(r'^\s+$')
	re_assign = re.compile(r'^\s+(\w\S*)\s*=\s*([^#\n]+).*$')
	re_flush = re.compile(r'^\s+flush\s*$')
	re_clear = re.compile(r'^\s+clear((?:\s+\w+){0,})\s*$')

	def __init__(self, parse_data, debug=False):
		self._debug = debug
		self._rules = []
		self.lterms = set()
		self._init(parse_data)
		self._current_data = {}
		self.data = []
	
	def _init_regex(self, parse_data, match):
		commands = []
		regex = match.groups()[0]
		if self._debug:
			print "got regex: %s" % regex
		try:
			regex = re.compile(regex)
		except:
			print "Failed to compile regex '%s'" % regex
			raise

		re_ws = self.re_ws
		re_assign = self.re_assign
		re_flush = self.re_flush
		re_clear = self.re_clear
		while True:
			l = parse_data.readline()
			if l == '' or (re_ws.match(l) is not None):
				break

			match = re_assign.match(l)
			if match is not None:
				(lterm, rterm) = match.groups()
				commands.append(('=', lterm, rterm))
				self.lterms.add(lterm)
				continue

			match = re_flush.match(l)
			if match is not None:
				commands.append(('FL',))
				continue

			match = re_clear.match(l)
			if match is not None:
				cl_cmd = [ "CL" ]
				terms, = match.groups()
				if terms:
					cl_cmd.append(terms.split())
				commands.append(cl_cmd)
				continue
				
			raise ValueError, "parse error <%s> (not an assignment/flush)" % l[:-1]

		self._rules.append((regex, commands))

	def _init(self, parse_data):
		if isinstance(parse_data, str):
			parse_data = StringIO(parse_data)
		
		re_regex = self.re_regex
		re_ws = self.re_ws
		while True:
			l = parse_data.readline()
			if l == '':
				break
			if l.startswith('#') or (re_ws.match(l) is not None):
				continue

			match = re_regex.match(l)
			if match is not None:
				self._init_regex(parse_data, match)
				continue

			raise ValueError, "parse error %s (not a regexp)" % l[:-1]
	
	def _execute_commands(self, commands, match):
		for command in commands:
			if command[0] == 'FL':
				if self._debug:
					print 'FLUSH'
				self.data.append(dict(self._current_data))
			elif command[0] == 'CL':
				if self._debug:
					print 'CLEAR',
				if len(command) == 1:
					if self._debug:
						print 'ALL'
					self._current_data.clear()
				else:
					if self._debug:
						print 'TERMS: ', ' '.join(command[1])
					for term in command[1]:
						if term in self._current_data:
							del self._current_data[term]
			elif command[0] == '=':
				lterm, rterm = command[1:]
				rterm = match.expand(rterm)
				rterm = eval(rterm)
				if self._debug:
					print 'ASSIGN ', lterm, '=', rterm, '--'
				self._current_data[lterm] = rterm
			else:
				raise ValueError, "Unknown command: %s" % ','.join(command)
		
	def go(self, f):
		if self._debug:
			print 'STARTED PARSING'

		while True:
			l = f.readline()
			if l == '':
				break

			for pattern, commands in self._rules:
				match = pattern.match(l)
				if match is not None:
					self._execute_commands(commands, match)
				# Only one match (first) allowed
				continue
