# simple wrapper that allows .accessors for dicts
class xdict(dict):
    def __init__(self, init={}):
        dict.__init__(self, init)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

    def __getattr__(self, name):
        return super(xdict, self).__getitem__(name)

