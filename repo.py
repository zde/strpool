#! /usr/bin/python
import glob, re, os
import xml.etree.cElementTree as ET

# config
var = {
    'releasever': '18',
    'basearch': 'x86_64',
}
def expand(s):
    s = re.split('\\$(\w+)', s)
    for i in range(1, len(s), 2):
        s[i] = var[s[i]]
    return ''.join(s)

# url grabber :)
import socket
conn = {}

class Http:
    def __init__(self, url):
        m = re.search('^(\w+)://(.+)', url)
        if m:
            proto, url = m.groups()
            if proto not in ('http', 'https'):
                raise NotImplementedError
        self.host, url = re.search('(.+?)(/.+)', url).groups()
        get = 'GET %s HTTP/1.1\nHost: %s\nConnection: Keep-alive\n\n' % (url, self.host)
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
        self.l = int(re.search('\nContent-Length: (\d+)', self.buf).group(1))
        self.buf = self.buf[i + 4:]

    def read(self, n = None):
        if self.l == 0:
            conn[self.host] = self.sock
            return ''
        if self.buf: buf = self.buf; self.buf = None
        else: buf = self.sock.recv(self.l)
        self.l -= len(buf)
        return buf

    def __iter__(self):
        buf = self.read()
        while True:
            try: l, buf = buf.split('\n', 1)
            except:
                more = self.read()
                if not more: break
                buf += more
            yield l

packages = []
provides = {}

def parse_repo(name):
    print 'parsing', name
    for _, e in iterparse(open('%s-primary.xml' % name, 'rb')):
        if e.tag != '{http://linux.duke.edu/metadata/common}package': continue

        # name, arch, version, location
        name = e.find('{http://linux.duke.edu/metadata/common}name').text
        arch = e.find('{http://linux.duke.edu/metadata/common}arch').text
        ver = e.find('{http://linux.duke.edu/metadata/common}version').get
        ver = ver('epoch'), ver('ver'), ver('rel')
        loc = e.find('{http://linux.duke.edu/metadata/common}location').get('href')

        # provides, requires, conflicts, obsoletes, files
        pro = []; req = []; con = []; obs = []; tgt = None
        for v in e.find('{http://linux.duke.edu/metadata/common}format').getiterator():
            if v.tag == '{http://linux.duke.edu/metadata/common}file': pro.append(v.text)
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}provides': tgt = pro
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}requires': tgt = req
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}conflicts': tgt = con
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}obsoletes': tgt = obs
            elif v.tag == '{http://linux.duke.edu/metadata/rpm}entry':
                v = v.get; x = v('name'); f = v('flags')
                if f: x = x, (f, v('epoch'), v('ver'), v('rel'))
                tgt.append(x)

        # add to packages
        pkg = name, arch, ver, loc, pro, req, con, obs
        packages.append(pkg)
        # add to provides
        for i in pro:
            f = None
            if type(i) == tuple: i, f = i
            provides.setdefault(i, {}).setdefault(f, []).append(pkg)
        e.clear()

def repo_list(path):
    for fn in glob.glob(path +'/*.repo'):
        name = None
        for l in open(fn):
            m = re.search('^\[(.+)\]', l)
            if m:
                if name and dic.get('enabled') == '1':
                    yield name, dic
                name = m.group(1)
                dic = {}
                continue
            m = re.search('^(\w+)=(.+)', l)
            if m: dic[m.group(1)] = expand(m.group(2))
        if name and dic.get('enabled') == '1':
            yield name, dic

def repo_get(name, dic):
    base = dic.get('baseurl')
    if base: base = base.split()
    else:
        url = dic.get('mirrorlist')
        base = Http(url)
        if '/metalink?' in url:
            base = [e.text for ev, e in ET.iterparse(base)
                    if e.tag == '{http://www.metalinker.org/}url']
    base = [e for e in base if e.startswith('http://')]
    if not base: raise ValueError, 'No mirrors for %s' % name
    base = base[0]
    if base.endswith('/repodata/repomd.xml'): base = base[:-19]
    elif not base.endswith('/'): base += '/'
    data = {}
    for ev, e in ET.iterparse(Http(base +'repodata/repomd.xml')):
        if e.tag == '{http://linux.duke.edu/metadata/repo}data':
            if e.get('type') in ('primary', 'filelists', 'other'):
                l = e.find('{http://linux.duke.edu/metadata/repo}location')
                data[e.get('type')] = base + l.get('href')
    # retrieve primary
    print name
    read = Http(data['primary']).read
    write = open(name +'.xml.gz', 'wb').write
    while True:
        buf = read()
        if not buf: break
        write(buf)

if __name__ == '__main__':
    for name, dic in repo_list('/etc/yum.repos.d'):
        try: os.stat(name +'.xml.gz')
        except: repo_get(name, dic)
