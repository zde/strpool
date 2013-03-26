#! /usr/bin/python
import glob, re, sys, os
import xml.etree.cElementTree as ET

# configuration
var = {
    'releasever': '17',
    'basearch': 'x86_64',
}

def expand(s):
    s = re.split('\\$(\w+)', s)
    for i in range(1, len(s), 2):
        s[i] = var[s[i]]
    return ''.join(s)

def repos(path='/etc/yum.repos.d'):
    def enabled():
        if not n: return False
        return d.get('enabled') == '1'
    for fn in glob.glob(path +'/*.repo'):
        n = None
        for l in open(fn):
            m = re.search('^\[\s*(.+?)\s*\]', l)
            if m:
                if enabled(): yield n, d
                n, d = m.group(1), {}
                continue
            m = re.search('^\s*(\w+?)\s*=\s*(.+?)\s*$', l)
            if m: d[m.group(1)] = expand(m.group(2))
        if enabled(): yield n, d

# urlgrabber ;-)
import socket, hashlib
conn = {}

class Http:
    def __init__(self, url, csum=None, tee=None):
        m = re.search('^(\w+)://(.+)', url)
        if m:
            proto, url = m.groups()
            if proto not in ('http', 'https'):
                raise NotImplementedError
        self.host, url = re.search('(.+?)(/.+)', url).groups()
        get = 'GET %s HTTP/1.1\r\nHost: %s\r\nConnection: Keep-alive\r\n\r\n' % (url, self.host)
        self.sock = conn.pop(self.host, None)
        if self.sock:
            self.sock.sendall(get)
            self.buf = self.sock.recv(0x4000)
        if not self.sock or not self.buf:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, 80))
            self.sock.sendall(get)
            self.buf = self.sock.recv(0x4000)
        while True:
            i = self.buf.find('\r\n\r\n')
            if i != -1: break
            more = self.sock.recv(0x4000)
            if not more: raise IOError, 'No header'
            self.buf += more
        self.cl = int(re.search('\nContent-Length: (\d+)', self.buf).group(1))
        assert self.cl > 0, self.cl
        self.buf = self.buf[i + 4:]
        assert len(self.buf) <= self.cl, self.buf
        self.csum = csum and (hashlib.new(csum[0]), csum[1])
        self.tee = tee

    def read(self, n=0x4000):
        if self.cl == 0:
            return ''
        buf = self.buf or self.sock.recv(self.cl)
        buf, self.buf = buf[:n], buf[n:]
        if self.csum: self.csum[0].update(buf)
        if self.tee: self.tee(buf)
        self.cl -= len(buf)
        if self.cl == 0:
            conn[self.host] = self.sock
            if self.csum:
                h, csum = self.csum
                if type(csum) is not list:
                    csum = [csum]
                if h.hexdigest() not in csum:
                    raise IOError, '%s: checksum failed' % self.host
        return buf

    def __iter__(self):
        l = ''
        while 1:
            buf = self.read()
            if not buf: break
            buf = (l + buf).split('\n')
            l = buf.pop()
            for i in buf: yield i
        if l: yield l

def retrieve(fn, base, csum):
    tee = open(fn +'.repomd', 'wb').write
    for ev, e in ET.iterparse(Http(base +'repodata/repomd.xml', csum, tee=tee)):
        if e.tag != '{http://linux.duke.edu/metadata/repo}data': continue
        if e.get('type') != 'primary': continue
        href = e.find('{http://linux.duke.edu/metadata/repo}location').get('href')
        csum = e.find('{http://linux.duke.edu/metadata/repo}checksum')
        csum = csum.get('type'), csum.text
    tee = os.popen('gzip -d >%s.xml' % fn, 'wb').write
    read = Http(base + href, csum, tee=tee).read
    while read(): pass

def sync(fn, d):
    csum = None
    try: base = d['baseurl'].split()
    except KeyError:
        url = d['mirrorlist']
        tee = open(fn +'.metalink', 'wb').write
        base = Http(url, tee=tee)
        if '/metalink?' in url:
            csum = {}; best = None; out = []
            for ev, e in ET.iterparse(base):
                if e.tag == '{http://www.metalinker.org/}hash':
                    if e.get('type') in hashlib.algorithms:
                        best = e.get('type')
                        csum.setdefault(best, []).append(e.text)
                if e.tag == '{http://www.metalinker.org/}url':
                    out.append(e.text)
            csum = best, csum[best]
            base = out
    for base in base:
        if not base.startswith('http://'): continue
        if base.endswith('/repodata/repomd.xml'): base = base[:-19]
        elif not base.endswith('/'): base += '/'
        try: retrieve(fn, base, csum); return True
        except IOError, e: print e

class Pool(dict):
    def __init__(self):
        self.keys = {}
    def __call__(self, s, key=None):
        if type(key) is int:
            l = self.keys.setdefault(s, [])
            if not key in l: l.append(key)
        return self.setdefault(s, s)
    def list(self):
        ret = sorted(self)
        n = 0
        for s in ret:
            self[s] = n
            n += 1
        if self.keys:
            def add_keys(s):
                if s in self.keys:
                    buf = strpool.buf()
                    buf.dump(0, *tuple(self.keys[s]))
                    buf.dump_raw(s)
                    s = str(buf)
                return s
            ret = map(add_keys, ret)
        return ret

def evr(get):
    ret = get('ver')
    e = get('epoch')
    if e and e != '0': ret = e +':'+ ret
    e = get('rel')
    if e: ret = ret +'-'+ e
    return ret

def parse(fn):
    arches = Pool()
    provides = Pool()
    versions = Pool()
    packages = []
    for _, e in ET.iterparse(open(fn)):
        if e.tag != '{http://linux.duke.edu/metadata/common}package': continue
        arch = arches(e.find('{http://linux.duke.edu/metadata/common}arch').text)
        name = e.find('{http://linux.duke.edu/metadata/common}name').text
        name = provides(name, len(packages))
        ver = versions(evr(e.find('{http://linux.duke.edu/metadata/common}version').get))
        loc = e.find('{http://linux.duke.edu/metadata/common}location').get('href')
        summ = e.find('{http://linux.duke.edu/metadata/common}summary').text
        desc = e.find('{http://linux.duke.edu/metadata/common}description').text
        if type(summ) is unicode: summ = summ.encode('UTF-8')
        if type(desc) is unicode: desc = desc.encode('UTF-8')
        fil = []; prco = [(name, (2, ver))], [], [], [], []; tgt = None
        for v in e.find('{http://linux.duke.edu/metadata/common}format').getiterator():
            if v.tag == '{http://linux.duke.edu/metadata/common}file': fil.append(v.text)
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}provides': tgt = 0
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}requires': tgt = 1
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}conflicts': tgt = 3
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}obsoletes': tgt = 4
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}entry':
                t = tgt; get = v.get
                if t == 1 and get('pre') == '1': t = 2
                f = get('flags')
                if f:
                    if   f == 'EQ': f = 2
                    elif f == 'LT': f = 1
                    elif f == 'GT': f = 4
                    elif f == 'LE': f = 3
                    elif f == 'GE': f = 6
                    else: raise ValueError, f
                    f = f, versions(evr(get))
                v = provides(get('name'), t == 0 and len(packages)), f
                if v not in prco[t]: prco[t].append(v)
        prco[0].extend([(provides(n), None) for n in fil])
        packages.append((arch, prco, loc, summ, desc))
        e.clear()
    return arches, provides, versions, packages

import strpool

def dump(fn, (arches, provides, versions, packages)):
    write = open(fn, 'wb').write
    # write header
    write('PKGS')
    for i in arches, provides, versions:
        buf = strpool.buf()
        buf.dump_pool(i.list())
        write(buf)
    # write packages
    def enc((name, f)):
        if f: f = f[0], versions[f[1]]
        return provides[name], f
    packages = [(arches[arch],) + tuple(map(enc, p) for p in prco) + (loc, summ, desc) 
                for arch, prco, loc, summ, desc in packages]
    buf = strpool.buf()
    buf.dump_pool(packages)
    write(buf)

from time import time
_curr = None

def tm(title=None, *args):
    global _curr
    if title:
        elapsed = time() - _curr
        sys.stderr.write('%6.2fms %s\n' % (elapsed*1e3, title % args))
    _curr = time()

class Repo:
    def __init__(self, name, fn):
        self.name = name
        tm()
        db = strpool.chunk(strpool.mmap(open(fn)))
        if db.load_raw(4) != 'PKGS':
            raise IOError, 'Repository signature not found'
        self.arches   = db.load_pool()
        tm('open %s', fn)
        self.provides = db.load_pool(1)
        tm('%d provides', len(self.provides))
        self.versions = db.load_pool()
        tm('%d versions', len(self.versions))
        self.packages = db.load_pool()
        tm('%d packages', len(self.packages))

    def __str__(self):
        return self.name
    def __len__(self):
        return len(self.packages)

class Sack(set):
    def search(self, patterns):
        for repo in self:
            prov = repo.provides
            for pat in patterns:
                exact = True
                if pat[-1:] == '*':
                    pat = pat[:-1]
                    exact = False
                i = prov.find(pat)
                while i < len(prov):
                    name, keys = prov[i]
                    if not name.startswith(pat): break
                    if exact and len(name) != len(pat): break
                    for k in keys:
                        pkg = repo.packages[k]
                        arch, x, n, (x, v) = pkg.load(0, 0, 0, (0, 0))
                        if n == i: yield name, repo.versions[v], repo.arches[arch]
                    i += 1

if __name__ == '__main__':
    sack = Sack()
    for n, d in repos():
        fn = '%s-%s-%s' % (n, var['releasever'], var['basearch'])
        if not os.access(fn +'.db', os.R_OK):
            if not os.access(fn +'.xml', os.R_OK):
                sync(fn, d)
            dump(fn +'.db', parse(fn +'.xml'))
        sack.add(Repo(n, fn +'.db'))
    for pkg in sack.search(sys.argv[1:]):
        print '%s-%s.%s' % pkg
