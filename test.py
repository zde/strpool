#! /usr/bin/python
import os, strpool, re

class Names(dict):
    def add(self, s):
        # update count
        self[s] = self.get(s, 0) - 1

    def save(self, buf):
        # sort by frequency
        tmp = sorted(self, key = lambda s: self[s])
        buf.dump_pool(tmp)
        buf.dump_raw('name')

        # assign IDs, renumber
        n = 0
        for s in tmp:
            self[s] = n
            n += 1

_rx_evr = re.compile('^(?:0*(\d*):)?([^-]+)(?:-([^-]+))?$')
_rx_ver = re.compile('(?:0*(\d+))|(~?[a-zA-Z]+)')

class Versions(dict):
    def add(self, s):
        if s in self: return
        e, v, r = _rx_evr.match(s).groups()

        # translate evr
        ret = bytearray(32)
        ret *= 0 # prealloc
        if e:
            ret += '>'
            ret += '='*(len(e) - 1)
            ret += e
        while v:
            for e, c in _rx_ver.findall(v):
                if e: c = e; e = '='*(len(e) - 1)
                elif c[0] == '~': e = '-'; c = c[1:]
                else: e = '.'
                ret += e
                ret += c
            v = r; r = None
        ret += '.'
        self[s] = str(ret)

    def save(self, buf):
        # invert
        inv = dict()
        for s in self:
            o = inv.setdefault(self[s], s)
            if len(s) < len(o):
                inv[self[s]] = s

        # sort by parsed evr
        tmp = sorted(inv)
        buf.dump_pool(map(inv.get, tmp))
        buf.dump_raw('vers')

        # assign IDs, save
        n = 0
        for evr in tmp:
            inv[evr] = n
            n += 1

        # renumber
        for s in self:
            self[s] = inv[self[s]]

class DB:
    def __init__(self, filename):
        # map database to memory
        buf = strpool.mmap(open(filename))
        buf = strpool.chunk(buf)

        # parse tables
        while buf:
            tag = buf.load_raw(4)
            pool = buf.load_pool()
            setattr(self, tag, pool)

if 1:
    n = Names()
    for i in 'a ab abc'.split():
        n.add(i)
    v = Versions()
    for i in '1 1.2 0:1.2 0:1.2b-17el6 1:3.4-5 1.2~'.split():
        v.add(i)

    buf = strpool.buf()
    n.save(buf)
    v.save(buf)
    open('/tmp/test', 'w').write(buf)

    db = DB('/tmp/test')
    #print map(str, db.name)
    #print map(str, db.vers)

if 1:
    import re
    for a, b, c in re.findall('(.+) (.+) (.+)\n', '''
1.0 1.0 0
1.0 2.0 -1
2.0 1.0 1
2.0.1 2.0.1 0
2.0 2.0.1 -1
2.0.1 2.0 1
2.0.1a 2.0.1a 0
2.0.1a 2.0.1 1
2.0.1 2.0.1a -1
5.5p1 5.5p1 0
5.5p1 5.5p2 -1
5.5p2 5.5p1 1
5.5p10 5.5p10 0
5.5p1 5.5p10 -1
5.5p10 5.5p1 1
10xyz 10.1xyz -1
10.1xyz 10xyz 1
xyz10 xyz10 0
xyz10 xyz10.1 -1
xyz10.1 xyz10 1
xyz.4 xyz.4 0
xyz.4 8 -1
8 xyz.4 1
xyz.4 2 -1
2 xyz.4 1
5.5p2 5.6p1 -1
5.6p1 5.5p2 1
5.6p1 6.5p1 -1
6.5p1 5.6p1 1
6.0.rc1 6.0 1
6.0 6.0.rc1 -1
10b2 10a1 1
10a2 10b2 -1
1.0aa 1.0aa 0
1.0a 1.0aa -1
1.0aa 1.0a 1
10.0001 10.0001 0
10.0001 10.1 0
10.1 10.0001 0
10.0001 10.0039 -1
10.0039 10.0001 1
4.999.9 5.0 -1
5.0 4.999.9 1
20101121 20101121 0
20101121 20101122 -1
20101122 20101121 1
2_0 2_0 0
2.0 2_0 0
2_0 2.0 0
a a 0
a+ a+ 0
a+ a_ 0
a_ a+ 0
+a +a 0
+a _a 0
_a +a 0
+_ +_ 0
_+ +_ 0
_+ _+ 0
+ _ 0
_ + 0
1.0~rc1 1.0~rc1 0
1.0~rc1 1.0 -1
1.0 1.0~rc1 1
1.0~rc1 1.0~rc2 -1
1.0~rc2 1.0~rc1 1
1.0~rc1~git123 1.0~rc1~git123 0
1.0~rc1~git123 1.0~rc1 -1
1.0~rc1 1.0~rc1~git123 1
10 9 1
100 99 1
1000 999 1
'''):
        v.add(a); v.add(b)
        d = str(cmp(v[a], v[b]))
        if d != c:
            print 'XXX', a, b, c, d
            print v[a], v[b]

    buf = strpool.buf()
    buf.dump(5, [1, 2], [('a', 1, None), ('b', 2, ('x',))])
    print repr(str(buf))
    ch = strpool.chunk(buf)
    print ch.load(0, [0], [('', 0, ('',))])
    print repr(str(ch))
