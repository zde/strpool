MODULES = strpoolmodule.so

all: $(MODULES)
	python sync.py

.SUFFIXES: .so
.c.so:
	gcc -g -o $@ --shared $<
