#include <python2.7/Python.h>
#include <sys/mman.h>

/* mmap type */

struct mmap {
    PyObject_HEAD
    void *buf;
    size_t size;
};

static struct mmap*
mmap_new(PyTypeObject *type, PyObject *args)
{
    PyFileObject *file;
    struct stat sb;
    void *buf;
    struct mmap *self;

    if (!PyArg_ParseTuple(args, "O!:mmap", &PyFile_Type, &file)) goto err;
    if (fstat(fileno(file->f_fp), &sb)) goto err_io;
    buf = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fileno(file->f_fp), 0);
    if (buf == MAP_FAILED) goto err_io;

    self = PyObject_NEW(struct mmap, type);
    if (!self) goto err;
    self->buf = buf;
    self->size = sb.st_size;
    return self;

 err_io:
    PyErr_SetFromErrno(PyExc_IOError);
 err:
    return NULL;
}

static void
mmap_dealloc(struct mmap *self)
{
    munmap(self->buf, self->size);
    PyObject_DEL(self);
}

static PyObject*
mmap_repr(struct mmap *self)
{
    return PyString_FromFormat(
        "<%s %p %d>", Py_TYPE(self)->tp_name, self->buf, self->size);
}

static Py_ssize_t
mmap_getsegcount(struct mmap *self, Py_ssize_t *lenp)
{
    if (lenp) *lenp = self->size;
    return 1;
}

static Py_ssize_t
mmap_getreadbuf(struct mmap *self, Py_ssize_t idx, void **pp)
{
    assert(idx == 0);
    *pp = self->buf;
    return self->size;
}

static PyBufferProcs mmap_as_buffer = {
    .bf_getsegcount = (segcountproc)mmap_getsegcount,
    .bf_getreadbuffer = (readbufferproc)mmap_getreadbuf,
    .bf_getcharbuffer = (getcharbufferproc)mmap_getreadbuf,
};

static Py_ssize_t
mmap_length(struct mmap *self)
{
    return self->size;
}

static PyObject*
mmap_slice(struct mmap *self, Py_ssize_t i, Py_ssize_t j)
{
    return PyString_FromStringAndSize(self->buf + i, j - i);
}

static PySequenceMethods mmap_as_sequence = {
    .sq_length = (lenfunc)mmap_length,
    .sq_slice = (ssizessizeargfunc)mmap_slice,
};

static PyObject*
mmap_str(struct mmap *self)
{
    return PyString_FromStringAndSize(self->buf, self->size);
}

static PyTypeObject mmap_type = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "mmap",
    .tp_basicsize = sizeof(struct mmap),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = (newfunc)mmap_new,
    .tp_dealloc = (destructor)mmap_dealloc,
    .tp_repr = (reprfunc)mmap_repr,
    .tp_as_buffer = &mmap_as_buffer,
    .tp_as_sequence = &mmap_as_sequence,
    .tp_str = (reprfunc)mmap_str,
};

/* buf type */

struct buf {
    PyObject_HEAD
    void *buf;
    size_t size;
    void *base;
};

static struct buf*
buf_new(PyTypeObject *type, PyObject *args)
{
    size_t alloc = 256;
    void *base;
    struct buf *self;

    if (!PyArg_ParseTuple(args, "|I:chunk", &alloc)) goto err;
    base = malloc(alloc);
    if (!base) goto err_mem;

    self = PyObject_NEW(struct buf, type);
    if (!self) goto err_free;
    self->base = base;
    self->buf = base + alloc;
    self->size = 0;
    return self;

 err_free:
    free(base);
 err_mem:
    PyErr_NoMemory();
 err:
    return NULL;
}

static void
buf_dealloc(struct buf *self)
{
    free(self->base);
    PyObject_DEL(self);
}

static void*
_buf_grow(struct buf *self, void *pos, size_t size)
{
    if (pos - self->base < size) {
        size_t used = self->buf - pos;
        void *base = malloc(size += used*2);
        if (!base) {
            self->buf = pos;
            self->size = used;
            return PyErr_NoMemory();
        }

        self->buf = base + size;
        memcpy(self->buf - used, pos, used);
        free(self->base), self->base = base;
        pos = self->buf - used;
    }
    return pos;
}

static uint8_t*
_buf_dump_tuple(struct buf *self, uint8_t *pos, PyObject *arg)
{
    PyObject **args = &arg;
    size_t i = 1;

    if (PyTuple_CheckExact(arg))
        args = &PyTuple_GET_ITEM(arg, 0), i = Py_SIZE(arg);
    while (i--) {
        PyObject *item = args[i];
        if (PyInt_CheckExact(item)) {
            size_t s = PyInt_AS_LONG(item);
            pos = _buf_grow(self, pos, 10);
            if (!pos) return NULL;
            *--pos = s & 0x7f;
            while (s >= 0x80)
                s >>= 7, *--pos = --s | 0x80;
        } else
        if (PyString_CheckExact(item)) {
            size_t s = Py_SIZE(item);
            pos = _buf_grow(self, pos, 10 + s);
            if (!pos) return NULL;
            memcpy(pos -= s, PyString_AS_STRING(item), s);
            *--pos = s & 0x7f;
            while (s >= 0x80)
                s >>= 7, *--pos = --s | 0x80;
        } else
        if (PyList_CheckExact(item)) {
            size_t s = Py_SIZE(item), j = s;
            while (j--) {
                PyObject *val = PyList_GET_ITEM(item, j);
                pos = _buf_dump_tuple(self, pos, val);
                if (!pos) return NULL;
            }
            pos = _buf_grow(self, pos, 10);
            if (!pos) return NULL;
            *--pos = s & 0x7f;
            while (s >= 0x80)
                s >>= 7, *--pos = --s | 0x80;
        } else
        if (i-- && PyInt_CheckExact(args[i])) {
            size_t s = PyInt_AS_LONG(args[i]) << 1;
            if (PyTuple_CheckExact(item)) {
                pos = _buf_dump_tuple(self, pos, item);
                if (!pos) return NULL;
                s |= 1;
            }
            pos = _buf_grow(self, pos, 10);
            if (!pos) return NULL;
            *--pos = s & 0x7f;
            while (s >= 0x80)
                s >>= 7, *--pos = --s | 0x80;
        } else return NULL;
    }
    return pos;
}

static PyObject*
buf_dump(struct buf *self, PyObject *arg)
{
    PyObject *ret;
    void *pos;
    size_t size;

    pos = self->buf, self->buf += self->size;
    pos = _buf_dump_tuple(self, pos, arg);
    if (!pos) return NULL;
    size = self->buf - pos, self->buf = pos;
    ret = PyInt_FromLong(size - self->size);
    self->size = size;
    return ret;
}

static PyObject*
buf_dump_pool(struct buf *self, PyObject *list)
{
    PyObject *ret;
    size_t n, *items, i, size, s;
    uint8_t *pos;

    if (!PyList_Check(list))
        return PyErr_SetString(PyExc_TypeError, "need a list"), NULL;
    n = Py_SIZE(list);
    items = malloc(n*sizeof(size_t));
    if (!items)
        return PyErr_NoMemory();
    pos = self->buf, self->buf += self->size;
    for (i = n; i--;) {
        const void *buf;
        PyObject *item = PyList_GET_ITEM(list, i);
        if (PyObject_CheckReadBuffer(item)) {
            PyObject_AsReadBuffer(item, &buf, &size);
            pos = _buf_grow(self, pos, size);
            if (!pos) return NULL;
            memcpy(pos -= size, buf, size);
            items[i] = size;
        } else {
            size = self->buf - (void*)pos;
            pos = _buf_dump_tuple(self, pos, item);
            if (!pos) return NULL;
            items[i] = self->buf - (void*)pos - size;
        }
    }
    pos = _buf_grow(self, pos, (n + 1)*10);
    if (!pos) return NULL;
    for (i = n; i--;) {
        s = items[i];
        *--pos = s & 0x7f;
        while (s >= 0x80)
            s >>= 7, *--pos = --s | 0x80;
    }
    free(items);
    s = n;
    *--pos = s & 0x7f;
    while (s >= 0x80)
        s >>= 7, *--pos = --s | 0x80;
    size = self->buf - (void*)pos, self->buf = pos;
    ret = PyInt_FromLong(size - self->size);
    self->size = size;
    return ret;
}

static PyObject*
buf_dump_raw(struct buf *self, PyObject *arg)
{
    const void *buf;
    size_t size;
    void *pos;

    if (PyObject_AsReadBuffer(arg, (const void**)&buf, &size))
        return NULL;
    pos = self->buf, self->buf += self->size;
    pos = _buf_grow(self, pos, size);
    if (!pos) return NULL;
    memcpy(pos -= size, buf, size);
    self->size = self->buf - pos;
    self->buf = pos;
    return PyInt_FromLong(size);
}

static PyMethodDef buf_methods[] = {
{ "dump", (PyCFunction)buf_dump, METH_VARARGS, },
{ "dump_pool", (PyCFunction)buf_dump_pool, METH_O, },
{ "dump_raw", (PyCFunction)buf_dump_raw, METH_O, },
{ NULL, NULL }};

static PyTypeObject buf_type = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "buf",
    .tp_basicsize = sizeof(struct buf),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_base = &mmap_type,
    .tp_new = (newfunc)buf_new,
    .tp_dealloc = (destructor)buf_dealloc,
    .tp_methods = buf_methods,
};

/* chunk type */

struct chunk {
    PyObject_HEAD
    const uint8_t *buf;
    size_t size;
    PyObject *base;
};

static struct chunk*
chunk_new(PyTypeObject *type, PyObject *args)
{
    PyObject *base;
    size_t offset = 0, want = -1;
    void *buf;
    size_t size;
    struct chunk *self;

    if (!PyArg_ParseTuple(args, "O|nn:chunk", &base, &offset, &want) ||
        PyObject_AsReadBuffer(base, (const void**)&buf, &size)) goto err;
    if (offset > size) goto err_size;
    buf += offset;
    size -= offset;
    if (size > want) size = want;

    self = PyObject_NEW(struct chunk, type);
    if (!self) goto err;
    self->buf = buf;
    self->size = size;
    self->base = base; Py_INCREF(base);
    return self;

 err_size:
    PyErr_SetString(PyExc_ValueError, "short buffer");
 err:
    return NULL;
}

static void
chunk_dealloc(struct chunk *self)
{
    Py_DECREF(self->base);
    PyObject_DEL(self);
}

static PyObject*
chunk_repr(struct chunk *self)
{
    PyObject *base = Py_TYPE(self->base)->tp_repr(self->base);
    PyObject *ret = PyString_FromFormat(
        "<%s %p %d of %s>", Py_TYPE(self)->tp_name, self->buf, self->size,
        PyString_AS_STRING(base));
    Py_DECREF(base);
    return ret;
}

static int
chunk_compare(struct chunk *a, struct chunk *b)
{
    const uint8_t *ap = a->buf;
    const uint8_t *bp = b->buf;
    int cmp;

    if (ap == bp)
        return 0;
    if (a->base == b->base)
        cmp = ap - bp;
    else {
        const uint8_t *aend = ap + a->size;
        const uint8_t *bend = bp + b->size;
        do {
            if (bp == bend)
                return ap == aend ? 0 : +1;
            if (ap == aend)
                return -1;
            cmp = *ap++ - *bp++;
        } while (cmp == 0);
    }
    return cmp > 0 ? +1 : -1;
}

static long
chunk_hash(struct chunk *self)
{
    return (long)self->buf;
}

/* chunk methods */

static PyObject*
chunk_load(struct chunk *self, PyObject *arg)
{
    uint8_t c;
    size_t n = 1, i, j, s;
    PyObject *ret = NULL;
    PyObject **rets = &ret;
    PyObject **args = &arg;
    const uint8_t *buf = self->buf;
    const uint8_t *end = buf + self->size;

    if (PyTuple_CheckExact(arg)) {
        n = Py_SIZE(arg);
        ret = PyTuple_New(n);
        if (!ret) return NULL;
        rets = &PyTuple_GET_ITEM(ret, 0);
        args = &PyTuple_GET_ITEM(arg, 0);
    }

    for (i = 0; i < n; i++) {
        PyObject *item = args[i];
        if (PyInt_CheckExact(item)) {
            for (s = c = *buf++; c & 0x80; s += c = *buf++)
                s = s - 0x7f << 7;
            item = PyInt_FromLong(s);
            if (!item) goto err;
        } else
        if (PyString_CheckExact(item)) {
            struct chunk *chunk = PyObject_NEW(struct chunk, self->ob_type);
            if (!chunk) goto err;
            for (s = c = *buf++; c & 0x80; s += c = *buf++)
                s = s - 0x7f << 7;
            chunk->buf = buf; buf += s;
            chunk->size = s;
            chunk->base = (PyObject*)self; Py_INCREF(self);
            item = (PyObject*)chunk;
        } else
        if (PyList_CheckExact(item) && Py_SIZE(item) == 1) {
            PyObject *arg = PyList_GET_ITEM(item, 0);
            for (s = c = *buf++; c & 0x80; s += c = *buf++)
                s = s - 0x7f << 7;
            item = PyList_New(s);
            if (!item) goto err;
            self->buf = buf;
            self->size = end - buf;
            for (j = 0; j < s; j++) {
                PyObject *ret = chunk_load(self, arg);
                if (!ret) {
                    Py_DECREF(item);
                    goto err;
                }
                PyList_SET_ITEM(item, j, ret);
            }
            buf = self->buf;
        } else
        if (PyTuple_CheckExact(item) && i && PyInt_CheckExact(rets[i - 1])) {
            s = PyInt_AS_LONG(rets[i - 1]); Py_DECREF(rets[i - 1]);
            rets[i - 1] = PyInt_FromLong(s >> 1);
            if (s & 1) {
                self->buf = buf;
                self->size = end - buf;
                item = chunk_load(self, item);
                if (!item) goto err;
                buf = self->buf;
            } else {
                item = Py_None;
                Py_INCREF(item);
            }
        } else
        {
            PyErr_Format(PyExc_TypeError, "cannot load %s", Py_TYPE(item)->tp_name);
            goto err;
        }
        rets[i] = item;
        if (buf > end) {
            PyErr_SetString(PyExc_ValueError, "short buffer");
            goto err;
        }
    }
    self->buf = buf;
    self->size = end - buf;
    return ret;

 err:
    Py_XDECREF(ret);
    return NULL;
}

static PyObject*
chunk_load_raw(struct chunk *self, PyObject *arg)
{
    size_t size;
    PyObject *ret;

    size = PyInt_AsLong(arg);
    if (size == -1) goto err;
    if (size > self->size) size = self->size;
    ret = PyString_FromStringAndSize(self->buf, size);
    if (!ret) goto err;
    self->buf += size;
    self->size -= size;
    return ret;

 err:
    return NULL;
}

static PyObject*
chunk_load_cstr(struct chunk *self)
{
    const uint8_t *buf = memchr(self->buf, 0, self->size);
    if (buf) {
        struct chunk *ret = PyObject_NEW(struct chunk, Py_TYPE(self));
        if (!ret) return NULL;
        ret->buf = self->buf;
        ret->size = buf - self->buf;
        ret->base = (PyObject*)self; Py_INCREF(self);
        self->buf = ++buf;
        self->size -= buf - ret->buf;
        return (PyObject*)ret;
    }
    Py_RETURN_NONE;
}

static PyObject*
chunk_startswith(struct chunk *self, PyObject *arg)
{
    const void *buf;
    size_t size;

    if (PyObject_AsReadBuffer(arg, &buf, &size))
        return NULL;
    if (self->size >= size && !memcmp(self->buf, buf, size))
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static struct pool* chunk_load_pool();
static PyMethodDef chunk_methods[] = {
{ "load", (PyCFunction)chunk_load, METH_VARARGS, },
{ "load_pool", (PyCFunction)chunk_load_pool, METH_NOARGS, },
{ "load_raw", (PyCFunction)chunk_load_raw, METH_O, },
{ "load_cstr", (PyCFunction)chunk_load_cstr, METH_NOARGS, },
{ "startswith", (PyCFunction)chunk_startswith, METH_O, },
{ NULL, NULL }};

static PyTypeObject chunk_type = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "chunk",
    .tp_basicsize = sizeof(struct chunk),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_base = &mmap_type,
    .tp_new = (newfunc)chunk_new,
    .tp_dealloc = (destructor)chunk_dealloc,
    .tp_repr = (reprfunc)chunk_repr,
    .tp_compare = (cmpfunc)chunk_compare,
    .tp_hash = (hashfunc)chunk_hash,
    .tp_methods = chunk_methods,
};

/* pool type */

struct pool {
    PyObject_VAR_HEAD
    PyObject *base;
    const uint8_t *buf;
    size_t item[1];
};

static void
pool_dealloc(struct pool *self)
{
    Py_DECREF(self->base);
    PyObject_DEL(self);
}

static Py_ssize_t
pool_length(struct pool *self)
{
    return Py_SIZE(self);
}

static PyObject*
pool_item(struct pool *self, Py_ssize_t i)
{
    struct chunk *chunk;

    if (i < 0 || i >= Py_SIZE(self))
        return PyErr_SetString(PyExc_IndexError, "invalid"), NULL;
    chunk = PyObject_NEW(struct chunk, &chunk_type);
    if (!chunk) return NULL;
    chunk->buf = self->buf + self->item[i];
    chunk->size = self->item[i + 1] - self->item[i];
    chunk->base = (PyObject*)self; Py_INCREF(self);
    return (PyObject*)chunk;
}

static PySequenceMethods pool_as_sequence = {
    .sq_length = (lenfunc)pool_length,
    .sq_item = (ssizeargfunc)pool_item,
};

/* pool methods */

static PyObject*
pool_find(struct pool *self, PyObject *arg)
{
    const uint8_t *buf;
    size_t size;
    unsigned low = 0, high = Py_SIZE(self);

    if (PyObject_AsReadBuffer(arg, (const void**)&buf, &size))
        return NULL;
    while (low < high) {
        unsigned i = 0, mid = (low + high)/2;
        const uint8_t *item = self->buf + self->item[mid];
        unsigned item_size = self->item[mid + 1] - self->item[mid];
        while (1) {
            if (i == size) goto ge;
            if (i == item_size) goto lt;
            if (item[i] > buf[i]) goto ge;
            if (item[i] < buf[i]) goto lt;
            i++;
        }
        ge: high = mid; continue;
        lt: low = mid + 1; continue;
    }
    return PyInt_FromLong(low);
}

static PyMethodDef pool_methods[] = {
{ "find", (PyCFunction)pool_find, METH_O, },
{ NULL, NULL }};

static PyObject*
pool_repr(struct pool *self)
{
    PyObject *ret = PyString_FromFormat("<%s %p len %d>",
        Py_TYPE(self)->tp_name, self->buf, Py_SIZE(self));
    return ret;
}

static PyTypeObject pool_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pool",
    .tp_basicsize = sizeof(struct pool),
    .tp_itemsize = sizeof(size_t),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)pool_dealloc,
    .tp_repr = (reprfunc)pool_repr,
    .tp_as_sequence = &pool_as_sequence,
    .tp_methods = pool_methods,
};

/* read pool from chunk */

static struct pool*
chunk_load_pool(struct chunk *self)
{
    const uint8_t *buf = self->buf;
    const uint8_t *end = buf + self->size;
    size_t c, s, i, sum;
    struct pool *pool;

    for (s = c = *buf++; c & 0x80; s += c = *buf++)
        s = s - 0x7f << 7;
    pool = PyObject_NEW_VAR(struct pool, &pool_type, s);
    if (!pool) goto err;
    for (i = sum = 0; pool->item[i] = sum, i < Py_SIZE(pool); i++, sum += s)
        for (s = c = *buf++; c & 0x80; s += c = *buf++)
            s = s - 0x7f << 7;
    if (buf > end) goto err_size;
    pool->buf = buf;
    if ((size_t)(buf += sum) < sum) goto err_size;
    if (buf > end) goto err_size;
    self->buf = buf;
    self->size = end - buf;
    pool->base = (PyObject*)self; Py_INCREF(self);
    return pool;

 err_size:
    PyErr_SetString(PyExc_ValueError, "short buffer");
 err_del:
    PyObject_DEL(pool);
 err:
    return NULL;
}

/* module */

static PyObject *module;
static void add_type(PyTypeObject *type)
{
    if (!PyType_Ready(type)) {
        Py_INCREF(type);
        PyModule_AddObject(module, type->tp_name, (PyObject*)type);
    }
}

PyMODINIT_FUNC
initstrpool(void)
{
    module = Py_InitModule("strpool", NULL);
    if (!module) return;

    add_type(&mmap_type);
    add_type(&buf_type);
    add_type(&chunk_type);
    add_type(&pool_type);
}
