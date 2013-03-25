MODULES = strpoolmodule.so

all: $(MODULES)
	python sync.py gnome-t

.SUFFIXES: .so
.c.so:
	gcc -g -o $@ --shared $<
