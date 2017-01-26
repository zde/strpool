#! /usr/bin/python
import bsddb
from struct import unpack
from bisect import bisect

class Package(object):
    location = None

    def __init__(self, hdr, i=0):
        n, s = unpack('>2I', hdr[i:i + 8])
        i += 8; b = i + n*16; assert b + s <= len(hdr)
        tags = {}
        while n:
            tag, typ, offset, count = unpack('>4I', hdr[i:i + 16]); assert offset < s
            tags[tag] = b + offset, count; i += 16; n -= 1
        self._hdr = hdr
        self._tags = tags
        self.name = self[1000]

    def __str__(self):
        return '%s-%s.%s' % (self.name, self.evr, self.arch)

    def __getitem__(self, tag):
        i = self._tags[tag][0]; hdr = self._hdr
        return hdr[i:hdr.index('\0', i)]

    @property
    def evr(self):
        evr = '%s-%s' % (self[1001], self[1002])
        if 1003 in self._tags:
            e = self._tags[1003][0]
            e = unpack('>I', self._hdr[e:e + 4])[0]
            if e: evr = '%d:%s' % (e, evr)
        return evr
    @property
    def arch(self): return 1022 in self._tags and self[1022] or 'noarch'
    @property
    def summary(self): return self[1004]
    @property
    def description(self): return self[1005]

    def _list(self, tag):
        try: i, count = self._tags[tag]
        except KeyError: return
        hdr = self._hdr
        while count:
            p = hdr.index('\0', i); yield hdr[i:p]
            i = p + 1; count -= 1
    def _list_n(self, tag):
        try: i, count = self._tags[tag]
        except KeyError: return
        hdr = self._hdr
        while count:
            yield unpack('>I', hdr[i:i + 4])[0]
            i += 4; count -= 1
    def _prco(self, name, flag, ver):
        name = self._list(name)
        flag = self._list_n(flag)
        ver = self._list(ver)
        while 1:
            n = name.next()
            f = flag.next() >> 1 & 7
            v = ver.next()
            yield n, f & 1 | f >> 1 & 2 | f << 1 & 4, v

    @property
    def provides(self): return self._prco(1047, 1112, 1113)
    @property
    def requires(self): return self._prco(1049, 1048, 1050)
    @property
    def conflicts(self): return self._prco(1054, 1053, 1055)
    @property
    def obsoletes(self): return self._prco(1090, 1114, 1115)
    @property
    def files(self):
        assert 1027 not in self._tags # OLDFILENAMES
        dirs = list(self._list(1118))
        dirindex = self._list_n(1116)
        basename = self._list(1117)
        flag = self._list_n(1037)
        csum = self._list(1035)
        while 1:
            yield dirs[dirindex.next()] + basename.next(), flag.next(), csum.next()

class PackageFile(Package):
    def __init__(self, filename):
        read = open(filename, 'rb').read
        magic = read(8)
        if magic[:4] == '\xed\xab\xee\xdb':
            magic = read(96)[-8:] # skip lead
        for i in 'sig', 'rpm': # two header structures
            if magic[:3] != '\x8e\xad\xe8':
                raise ValueError
            hdr = read(8); n, s = unpack('>2I', hdr)
            hdr += read(n*16 + (s + 7 & -8))
            magic = read(8)
        Package.__init__(self, hdr)

class Rpmdb:
    def __init__(self, path='/var/lib/rpm/'):
        self.path = path
        self.prov = sorted(bsddb.hashopen(path + 'Providename', 'r').items())
        self.packages = bsddb.hashopen(path + 'Packages', 'r')
        self.cache = {}

    def __str__(self): return 'installed'
    def __len__(self): return len(self.packages) - 1
    def __getitem__(self, pkgid):
        pkg = self.cache.get(pkgid)
        if not pkg:
            pkg = self.cache[pkgid] = Package(self.packages[pkgid])
        return pkg
    def __iter__(self):
        self.packages.first()
        while 1:
            try: key, pkg = self.packages.next()
            except: break
            yield self[key]

    def search(self, patterns, provides):
        dup = set()
        for pat in patterns:
            if pat[-1:] == '*':
                pat = pat[:-1]
                check = lambda name: name.startswith(pat)
            else:
                check = lambda name: name == pat
            try:
                pos = bisect(self.prov, (pat, None))
                name, p = self.prov[pos]
            except: continue
            while check(name):
                i = 0
                while i < len(p):
                    pkgid = p[i:i + 4]; i += 8
                    if pkgid in dup: continue
                    if provides or self[pkgid].name == name:
                        yield pkgid; dup.add(pkgid)
                try:
                    pos += 1
                    name, p = self.prov[pos]
                except: break
