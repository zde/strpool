#! /usr/bin/python
import bsddb
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
    def __init__(self, path='/var/lib/rpm/'):
        self.packages = bsddb.hashopen(path + 'Packages', 'r')
        self.provides = bsddb.btopen(path + 'Providename', 'r')

    def __str__(self): return 'installed'
    def __len__(self): return len(self.packages) - 1
    def __getitem__(self, pkgid):
        return Package(self.packages[pkgid])

    def search(self, patterns, provides):
        dup = set()
        for pat in patterns:
            if pat[-1:] == '*':
                pat = pat[:-1]
                check = lambda name: name.startswith(pat)
            else:
                check = lambda name: name == pat
            name, p = self.provides.set_location(pat)
            while check(name):
                i = 0
                while i < len(p):
                    pkgid = p[i:i + 4]; i += 8
                    if pkgid in dup: continue
                    if provides or self[pkgid].name == name:
                        yield pkgid; dup.add(pkgid)
                name, p = self.provides.next()
