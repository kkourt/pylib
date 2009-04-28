#!/usr/bin/env python
# vim:expandtab:softtabstop=4:shiftwidth=4:tabstop=8

import traceback

def debug(func):
    """Decorator function. Prints the arguments and the return value of a function."""

    def quote(x):
        if type(x) == str: return '"%s"' % x[:100]
        else: return repr(x)[:500]

    def wrapper(*args, **kwargs):
        params = [quote(x) for x in args] + ["%s=%s" % (k, quote(v)) for k, v in kwargs.items()]
        print "%s(%s)" % (func.__name__, ", ".join(params))
        try:
            ret = func(*args, **kwargs)
        except Exception, e:
            ret = e
            raise
        finally:
            print "%s() -> %s" % (func.__name__, quote(ret))
        return ret

    return wrapper

def debugexc(func):
    """Decorator function. Prints the arguments and the return value of a function."""

    def quote(x):
        if type(x) == str: return '"%s"' % x[:100]
        else: return repr(x)[:300]

    def wrapper(*args, **kwargs):
        #traceback.print_tb()
        params = [quote(x) for x in args] + ["%s=%s" % (k, quote(v)) for k, v in kwargs.items()]
        try:
            ret = func(*args, **kwargs)
        except Exception, e:
            traceback.print_exc()
            ret = e
            raise
        return ret

    return wrapper

class DBG_Trace(object):
    """ This call instanciates a decorator that prints every subsequent call
        made after the (decorated) function is called and before it returns.
    """
    def __init__(self, trace_filter=lambda x: True):
        """ trace_filter : trace a function only if trace_filter(fn) == True """
        self.calls = 0   # trace recursive calls
        self.trace_filter = trace_filter

    def __call__(self, func):
        import sys
        import inspect

        def wrapper(*args, **kwargs):
            if self.calls == 0:
                sys.settrace(tracer)
            self.calls += 1
            try:
                ret = func(*args, **kwargs)
            except Exception, e:
                raise
            finally:
                self.calls -= 1
                if self.calls == 0:
                    sys.settrace(None)

            return ret

        def tracer(frame,event,arg):
            if frame.f_code == wrapper.func_code:
                return tracer
            if self.trace_filter(frame.f_code.co_name) is not True:
                return tracer

            if event == 'call':
                a = [ "%s => %s" % (v, frame.f_locals[v]) for v in frame.f_locals]
                print "TRACE: CALL: %s (%s)" % (frame.f_code.co_name, ", ".join(a))
            elif event == 'return':
                print "TRACE:  RET: %s (%s) " % (frame.f_code.co_name, arg)
            return tracer

        return wrapper

def fn_str(fn):
    code = fn.func_code
    return fn.__name__ + '(' + ','.join(code.co_varnames[:code.co_argcount]) + ')'

def _contr_str(container):
    if not container:
        return ''
    ret = '{'
    for k in container.keys():
        d,t = container[k]
        ret += (k + ': (')
        if len(d) > 128:
            ret += ('...data...len:' + str(len(d)) + '...')
        else:
            ret += str(d)
        ret += (', ' + t + ') ')
    ret += '}'
    return ret

def print_msg(op, subject, subj_id, arg_list, container):
    #print op, subject, subj_id, arg_list, container
    #print op, subject, subj_id, arg_list, _contr_str(container)
    pass

def generator_str(gen):
    # from trial-and-error, so it may be bogus
    return "%s (%s)" % (str(gen), gen.gi_frame.f_code.co_name)

class DummyLock(object):
    def __init__(self, name=''):
        self.name = name
    def __enter__(self):
        print 'dummylock %s ENTER' % self.name
    def __exit__(self, *args):
        print 'dummylock %s EXIT' % self.name
