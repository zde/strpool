MODULES = strpoolmodule.so

all: $(MODULES)
	python test.py

.SUFFIXES: .so
.c.so:
	gcc -g -o $@ --shared $<
