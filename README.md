Python data serialization library

pros:

- fast
- untyped
- quite compact
- supports random access
- supports optional elements

cons:

- only ints, strings, and non-circular lists
- loader must supply the type information

```
integers are var-sized (0, 128, 16512)     => \x00\x80\x00\x80\x80\x00
lists get size prefix  ([(5, 6)],)         => \x01\x05\x06
strings get prefix too ([(5, 'abc')],)     => \x01\x05\x03abc
None & () are special  (3, 1)              => \x03\x01
6 = 3*2 + 0            (3, None)           => \x06
7 = 3*2 + 1            (3, ('abc',))       => \x07\x03abc
dump vs                ([('a',), ('b',)],) => \x02\x01a\x01b
dump_pool              ([('a',), ('b',)],) => \x02\x02\x02\x01a\x01b
dump_pool              (['a', 'b'],)       => \x02\x01\x01ab
```
