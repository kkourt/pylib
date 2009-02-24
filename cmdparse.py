import shlex
import code
import sys
import StringIO
import readline as rl

_lex_extra_chars = '-/.,!='

class CmdParser(object):
	commands = ("echo", "help")
	def __init__(self, *args, **kwargs):
		self.cmds = set()
		self._completions = []
		for klass in self.__class__.__mro__:
			cmds = getattr(klass, 'commands', ())
			for cmd in cmds:
				if cmd in self.cmds:
					print "Overwriting command", cmd
				self.cmds.add(cmd)
		super(CmdParser, self).__init__(*args, **kwargs)
		rl.set_completer(self._complete)

	def _complete(self, text, state):
		if state == 0:
			if rl.get_begidx() == 0:
				self._completions = filter(lambda s: s.find(text) == 0, self.cmds)
			else:
				input = rl.get_line_buffer()
				lex = shlex.shlex(input, posix=True)
				lex.wordchars += _lex_extra_chars
				token = lex.get_token()
				comp_fn = getattr(self, 'complete_' + token, lambda x: [])
				self._completions = comp_fn(lex)
		if state < len(self._completions):
			return self._completions[state]
		else:
			return None

	def parse(self, input):
		lex = shlex.shlex(input, posix=True)
		lex.wordchars += _lex_extra_chars
		token = lex.get_token()
		if token not in self.cmds:
			ret = "Available Commands: %s" % ' '.join(self.cmds)
		else:
			fn = getattr(self, 'parse_' + token, None)
			if fn is None:
				ret = "Errror : parse_%s not implemented" % token
			else:
				rl.set_completer()
				ret = fn(lex)
				rl.set_completer(self._complete)
		return ret

	def parse_echo(self, lex):
		""" Just return the input """
		return ' '.join([t for t in lex])

	def parse_help(self, lex):
		""" Print command help """
		cmds = []
		for t in lex:
			cmds.append(t)
		if not cmds:
			cmds = self.cmds

		ret = ""
		cmds = list(cmds)
		cmds.sort()
		for cmd in cmds:
			if cmd not in self.cmds:
				ret += "%-12s : unknown\n" % cmd
				continue
			fn = getattr(self, 'parse_' + cmd, None)
			if fn is None:
				ret += "%-12s : not implemented\n" % cmd
				continue
			ret += "%-12s : %s\n" % (cmd, fn.__doc__)
		return ret

class CmdCodeConsole(code.InteractiveConsole):
	""" Use stdout for writing data (code.InteractiveConsole uses stderr)"""
	def write(self, data):
		sys.stdout.write(data)

class CmdPyParser(object):
	commands = ("__python__",)

	def __init__(self, *args, **kwargs):
		self._namespace = {}
		super(CmdPyParser, self).__init__(*args, **kwargs)

	def parse_python(self, lex):
		""" python interactive mode """
		buff = StringIO.StringIO()
		console = CmdCodeConsole(self._namespace)
		prompt = ">>> "
		while True:
			input = raw_input(prompt)
			if input == '__python__':
				break
			buff.write(input)
			buff.write("\n")
			try:
				more = console.runsource(buff.getvalue(), '$ctl$')
			except SyntaxError, e:
				print str(e)
			if more:
				prompt = "..."
			else:
				buff.truncate(0)
				prompt = ">>> "
		del console
		del buff
		return "Python interactive mode ended"

	parse___python__ = parse_python

if __name__ == '__main__':
	class MyParser(CmdParser, CmdPyParser):
		commands = tuple()
	myparser = MyParser()
	rl.set_history_length(1000)
	rl.parse_and_bind('tab: complete')
	while True:
		input = raw_input("> ")
		ret = myparser.parse(input)
		print ret
