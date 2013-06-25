#! /usr/bin/python
from collections import deque

class Solver(set):
    def __init__(self, installed):
        self.provides = {}
        for repo in installed:
            for pkg in repo:
                assert pkg not in self; self.add(pkg)
                for n, f in pkg.provides:
                    self.provides.setdefault(n, {}).setdefault(pkg, []).append(f)
                for n in pkg.files:
                    self.provides.setdefault(n, {})[pkg] = True

    def verify(self):
        print 'verify', len(self), len(self.provides)
