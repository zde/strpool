#! /usr/bin/python
import bsddb
from struct import unpack

class Package(object):
    def __init__(self, hdr, i=0):
        tags = {}
        n, s = unpack('>2I', hdr[i:i + 8])
        i += 8
        while n:
            tag, typ, offset, count = unpack('>4I', hdr[i:i + 16])
            tags[tag] = typ, offset, count
            i += 16; n -= 1
        if i + s > len(hdr):
            raise ValueError
        self.tags = tags
        self.hdr = hdr
        self.base = i
 
    def _tag(self, tag):
        try: typ, offset, count = self.tags[tag]
        except KeyError: return
        hdr = self.hdr; offset += self.base
        while count:
            if typ == 6 or typ == 8:
                p = hdr.index('\0', offset)
                yield hdr[offset:p]
                offset = p + 1
            elif typ == 4:
                p = offset + 4
                yield unpack('>I', hdr[offset:p])[0]
                offset = p
            else:
                raise ValueError
            count -= 1

    def __str__(self):
        name, = self._tag(1000)
        version, = self._tag(1001)
        release, = self._tag(1002)
        try: epoch = '%d:' % tuple(self._tag(1003))
        except: epoch = ''
        try: arch = '.%s' % tuple(self._tag(1022))
        except: arch = ''
        return '%s-%s%s-%s%s' % (name, epoch, version, release, arch)

    def _prco(self, *arg):
        name, flag, ver = map(self._tag, arg)
        while True:
            n = name.next()
            f = flag.next()
            v = ver.next()
            f = (None, 1, 4, 5, 2, 3, 6, None)[f >> 1 & 7]
            if f: f = f, v
            yield n, f

    provides = property(lambda self: self._prco(1047, 1112, 1113))
    requires = property(lambda self: self._prco(1049, 1048, 1050))


    if 0:
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
