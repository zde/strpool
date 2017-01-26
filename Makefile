MODULES = strpool.so

all: $(MODULES)
	./mgmt search "gnome-t*"

.SUFFIXES: .so
.c.so:
	gcc -g -o $@ -fPIC --shared $<
