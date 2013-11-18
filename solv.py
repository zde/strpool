#! /usr/bin/python
from collections import deque

class Solver(set):
    def __init__(self, installed):
        self.provides = {}
        for repo in installed:
            for pkg in repo:
                self.add(pkg)

    def add(self, pkg):
        assert pkg not in self
        set.add(self, pkg)
        for n, f, v in pkg.provides:
            self.provides.setdefault(n, {}).setdefault(pkg, []).append((f, v))
        for n, f, v in pkg.files:
            self.provides.setdefault(n, set()).add(pkg)

    def verify(self):
        print len(self), 'packages', len(self.provides), 'provides'
        for n in self.provides:
            d = self.provides[n]
            if type(d) == set and len(d) != 1:
                print n, ' '.join(map(str, d))
