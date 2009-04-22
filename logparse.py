from cStringIO import StringIO
import re

class LogParser(object):
	re_regex = re.compile(r'^/(.*)/$')
	re_regex_ws = re.compile(r'^\s+/(.*)/$')
	re_initial_ws = re.compile(r'^(\s+)')
	re_ws = re.compile(r'^\s+$')
	re_assign = re.compile(r'^\s+(\w\S*)\s*=\s*([^#\n]+).*$')
	re_flush = re.compile(r'^\s+flush\s*$')
	re_clear = re.compile(r'^\s+clear((?:\s+\w+){0,})\s*$')

	def __init__(self, parse_data, debug=False, globals=None):
		self._debug = debug
		self._globals = globals() if globals is None else globals
		self._rules = []
		self.lterms = set()
		self._init(parse_data)
		self._current_data = {}
		self.data = []

	def _init_commands(self, parse_data, initial_ws=''):
		commands = [] # commands for this regular expression
		re_ws = self.re_ws
		re_regex = self.re_regex_ws
		re_assign = self.re_assign
		re_flush = self.re_flush
		re_clear = self.re_clear

		# first line
		pp = parse_data.tell() # previous position
		l = parse_data.readline()
		assert(l.startswith(initial_ws))
		current_ws = self.re_initial_ws.match(l).groups()[0]
		assert(len(current_ws) > len(initial_ws))
		parse_data.seek(pp)

		while True:
			# get next line
			pp = parse_data.tell()
			l = parse_data.readline()
			if l == '':
				break
			if not l.startswith(current_ws):
				# go back
				parse_data.seek(pp)
				break

			# regular expression command
			match = re_regex.match(l)
			if match is not None:
				regex = self._compile_regex(match.groups()[0])
				new_commands = self._init_commands(parse_data, current_ws)
				commands.append(('RE', regex, new_commands))
				continue

			# assignment command
			match = re_assign.match(l)
			if match is not None:
				(lterm, rterm) = match.groups()
				commands.append(('=', lterm, rterm))
				self.lterms.add(lterm)
				continue

			# flush command
			match = re_flush.match(l)
			if match is not None:
				commands.append(('FL',))
				continue

			# clear command
			match = re_clear.match(l)
			if match is not None:
				cl_cmd = [ "CL" ]
				terms, = match.groups()
				if terms:
					cl_cmd.append(terms.split())
				commands.append(cl_cmd)
				continue

			# Unknown command
			raise ValueError, "parse error <%s> (not a valid command)" % (l[:-1],)

		return commands

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

			# match a regular expression
			match = re_regex.match(l)
			if match is not None:
				regex_str =  match.groups()[0]
				regex = self._compile_regex(regex_str)
				commands = self._init_commands(parse_data)
				self._rules.append((regex, commands))
				continue

			raise ValueError, "parse error %s (not a regexp)" % l[:-1]

	def _compile_regex(self, regex):
		if self._debug:
			print "got regex: %s" % regex
		try:
			regex = re.compile(regex)
		except:
			print "Failed to compile regex '%s'" % regex
			raise
		return regex


	def _execute_commands(self, commands, match):
		for command in commands:
			if command[0] == 'FL':
				if self._debug:
					print 'FLUSH'
				yield dict(self._current_data)
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
				globs = dict(self._globals)
				globs['__match_obj'] = match
				rterm = eval(rterm, globs)
				if self._debug:
					print 'ASSIGN ', lterm, '=', rterm, '--'
				self._current_data[lterm] = rterm
			elif command[0] == 'RE':
				nregex, ncommands = command[1:]
				nmatch = nregex.match(match.group(0))
				if nmatch is not None:
					rets = self._execute_commands(ncommands, nmatch)
					for ret in rets:
						yield ret
			else:
				raise ValueError, "Unknown command: %s" % command[0]

	def go_iter(self, f):
		if self._debug:
			print 'STARTED PARSING'
		while True:
			l = f.readline()
			if l == '':
				break
			for pattern, commands in self._rules:
				match = pattern.match(l)
				if match is not None:
					rets = self._execute_commands(commands, match)
					for ret in rets:
						yield ret
				# Only one match required

	def go(self, f):
		self.data = list(self.go_iter(f))
