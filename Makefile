MODULES = strpool.so

all: $(MODULES)
	python sync.py provides "gnome-t*" yum

.SUFFIXES: .so
.c.so:
	gcc -g -o $@ --shared $<
