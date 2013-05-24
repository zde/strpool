#! /usr/bin/python
import bsddb
from struct import unpack

class Package(object):
    __slots__ = 'name version arch provides requires'.split()
    use = set((
        1000, 1001, 1002, 1003, 1022, # nvrea
        1047, 1112, 1113, # provides
        1049, 1048, 1050, # requires
    ))

    def __init__(self, hdr, i=0):
        n, s = unpack('>2I', hdr[i:i + 8])
        i += 8; b = i + n*16
        d = {}
        print 'NEW'
        prev = 0
        while n:
            tag, t, o, c = unpack('>4I', hdr[i:i + 16])
            i += 16; n -= 1
            print 'TAG', tag
            assert tag > prev, (prev, tag); prev = tag
            if tag not in self.use:
                continue
            o += b
            v = []
            while c:
                if t == 6 or t == 8:
                    p = hdr.index('\0', o)
                    v.append(hdr[o:p])
                    o = p + 1
                elif t == 4:
                    v.append(unpack('>I', hdr[o:o + 4])[0])
                    o += 4
                else: raise ValueError, (tag, t)
                c -= 1
            d[tag] = v

        # nevra
        self.name = d[1000][0]
        epoch = d.get(1003, (None,))[0]
        epoch = epoch and '%d:' % epoch or ''
        self.version = '%s%s-%s' % (epoch, d[1001][0], d[1002][0])
        self.arch  = d.get(1022, (None,))[0]

        # prco
        xlat = None, 1, 4, 5, 2, 3, 6, None
        def decode(flag, version):
            flag = xlat[flag >> 1 & 7]
            if flag: flag = flag, version
            return flag
        def prco(*tags):
            return [(name, decode(flag, version))
                    for name, flag, version in zip(*[d.get(t, ()) for t in tags])]
        self.provides = prco(1047, 1112, 1113)
        self.requires = prco(1049, 1048, 1050)

    def __str__(self):
        if not self.arch:
            return '%s-%s' % (self.name, self.version)
        return '%s-%s.%s' % (self.name, self.version, self.arch)

def installed(name='/var/lib/rpm/Packages'):
    db = bsddb.hashopen(name, 'r')
    k, v = db.first()
    assert k == '\0\0\0\0'
    assert len(v) == 4
    while 1:
        try: k, v = db.next()
        except: break
        assert len(k) == 4
        yield Package(v)
    db.close()

if __name__ == '__main__':
    for pkg in installed():
        print pkg
        for p in pkg.requires:
            print '\t', p
