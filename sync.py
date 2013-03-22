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
    def __init__(self, url, csum=None):
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

    def read(self, n=0x4000):
        if self.cl == 0:
            return ''
        buf = self.buf or self.sock.recv(self.cl)
        buf, self.buf = buf[:n], buf[n:]
        if self.csum:
            self.csum[0].update(buf)
        self.cl -= len(buf)
        if self.cl == 0:
            conn[self.host] = self.sock
            if self.csum and self.csum[0].hexdigest() != self.csum[1]:
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
    for ev, e in ET.iterparse(Http(base +'repodata/repomd.xml', csum)):
        if e.tag != '{http://linux.duke.edu/metadata/repo}data': continue
        if e.get('type') != 'primary': continue
        href = e.find('{http://linux.duke.edu/metadata/repo}location').get('href')
        csum = e.find('{http://linux.duke.edu/metadata/repo}checksum')
        csum = csum.get('type'), csum.text
    read = Http(base + href, csum).read
    write = os.popen('gzip -d >%s' % fn, 'wb').write
    while 1:
        buf = read()
        if not buf: break
        write(buf)

def sync(fn, d):
    csum = None
    try: base = d['baseurl'].split()
    except KeyError:
        url = d['mirrorlist']
        base = Http(url)
        if '/metalink?' in url:
            i = ET.iterparse(base); base = []
            for ev, e in i:
                if e.tag == '{http://www.metalinker.org/}hash':
                    if e.get('type') in hashlib.algorithms:
                        csum = e.get('type'), e.text
                if e.tag == '{http://www.metalinker.org/}url':
                    base.append(e.text)
    for base in base:
        if not base.startswith('http://'): continue
        if base.endswith('/repodata/repomd.xml'): base = base[:-19]
        elif not base.endswith('/'): base += '/'
        try: retrieve(fn, base, csum); return True
        except IOError, e: print e

def evr(get):
    ret = get('ver')
    e = get('epoch')
    if e and e != '0': ret = e +':'+ ret
    e = get('rel')
    if e: ret = ret +'-'+ e
    return ret

class Pool(dict):
    def __call__(self, s):
        ret = self.get(s)
        if ret is None:
            ret = self[s] = len(self)
        return ret
    def list(self):
        l = [None] * len(self)
        for s in self: l[self[s]] = s
        return l

import strpool

def xml2db(fn, db):
    arches = Pool()
    provides = Pool()
    versions = Pool()
    packages = []
    for _, e in ET.iterparse(open(fn)):
        if e.tag != '{http://linux.duke.edu/metadata/common}package': continue
        name = provides(e.find('{http://linux.duke.edu/metadata/common}name').text)
        arch = arches(e.find('{http://linux.duke.edu/metadata/common}arch').text)
        ver = versions(evr(e.find('{http://linux.duke.edu/metadata/common}version').get))
        loc = e.find('{http://linux.duke.edu/metadata/common}location').get('href')
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
                v = provides(get('name')), f
                if v not in prco[t]: prco[t].append(v)
        buf = strpool.buf()
        buf.dump(arch, loc, *prco)
        packages.append(str(buf))
        e.clear()
    buf = strpool.buf()
    buf.dump_pool(packages)
    for i in versions, provides, arches:
        buf.dump_pool(i.list())
    buf.dump_raw('PKGS')
    open(db, 'wb').write(buf)

class Repo:
    def __init__(self, fn):
        db = strpool.chunk(strpool.mmap(open(fn)))
        assert db.load_raw(4) == 'PKGS'
        self.arch = db.load_pool()
        self.prov = db.load_pool()
        self.ver  = db.load_pool()
        self.pkgs = db.load_pool()
    def __len__(self):
        return len(self.pkgs)
    def __getitem__(self, n):
        pkg = self.pkgs[n]
        arch, loc = pkg.load(0, '')
        arch = self.arch[arch]
        def decode((n, f)):
            return self.prov[n], f and (f[0], self.ver[f[1]])
        prco = [map(decode, pkg.load([(0, (0, 0))])[0]) for i in range(5)]
        return arch, loc, prco

if __name__ == '__main__':
    for n, d in repos():
        if n == 'updates': continue
        fn = '%s-%s-%s' % (n, var['releasever'], var['basearch'])
        if not os.access(fn +'.db', os.R_OK):
            if not os.access(fn +'.xml', os.R_OK):
                sync(fn +'.xml', d)
            xml2db(fn +'.xml', fn +'.db')

        # attempt to use it
        repo = Repo(fn +'.db')
        for arch, loc, prco in repo:
            name, (x, ver) = prco[0][0]
            print name, arch, ver
