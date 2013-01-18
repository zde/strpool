#! /usr/bin/python
import strpool

def enc(t, *args):
    print '%-22s %-19s' % (t, repr(args)),
    buf = strpool.buf()
    if t.endswith('pool'): buf.dump_pool(*args)
    else: buf.dump(*args)
    print '=>', repr(str(buf))[1:-1]

enc('integers are var-sized', 0, 128, 16512)
enc('lists get size prefix', [(5, 6)])
enc('strings get prefix too', [(5, 'abc')])

enc('None & () are special', 3, 1)
enc('6 = 3*2 + 0', 3, None)
enc('7 = 3*2 + 1', 3, ('abc',))

enc('dump vs', [('a',), ('b',)])
enc('dump_pool', [('a',), ('b',)])
enc('dump_pool', ['a', 'b'])
