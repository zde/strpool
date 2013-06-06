#! /usr/bin/python
import bsddb, re
from struct import unpack

class Package(object):
    location = None

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
            if typ == 6 or typ == 8 or typ == 9:
                p = hdr.index('\0', offset)
                yield hdr[offset:p]
                offset = p + 1
            elif typ == 4:
                p = offset + 4
                yield unpack('>I', hdr[offset:p])[0]
                offset = p
            else:
                raise ValueError, typ
            count -= 1

    name2tag = {
        'name': 1000,
        'version': 1001,
        'release': 1002,
        'epoch': 1003,
        'summary': 1004,
        'description': 1005,
        'arch': 1022,
    }

    def __getattr__(self, name):
        try:
            return self._tag(self.name2tag[name]).next()
        except StopIteration:
            if name == 'arch': return None
            raise

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

class Rpmdb:
    def __init__(self, name='/var/lib/rpm/Packages'):
        db = bsddb.hashopen(name, 'r')
        k, v = db.first()
        assert k == '\0\0\0\0'
        assert len(v) == 4
        self.packages = []
        while 1:
            try: k, v = db.next()
            except: break
            assert len(k) == 4
            self.packages.append(Package(v))
        db.close()

    def __str__(self): return 'installed'
    def __len__(self): return len(self.packages)
    def __getitem__(self, n):
        return self.packages[n]

    def search(self, patterns, provides):
        pat = re.compile('^(%s)$' % '|'.join(
            p[-1:] == '*' and re.escape(p[:-1])+'.*' or re.escape(p)
            for p in patterns))
        pkgids = []
        for pkgid, pkg in enumerate(self.packages):
            if pat.match(pkg.name):
                pkgids.append(pkgid)
        return pkgids
