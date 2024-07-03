teepot
======

`teepot` copies data from its input to many outputs, with buffering.

`teepot` duplicates data from its input to each of its outputs, in a similar
manner to `tee`,  but with additional buffering to avoid some of the
deadlock problems that can occur when writing to other processes via pipes.
It also handles piped outputs that get shut down before all of the data has
been sent (for example when writing to `head`).

If the outputs do not read at the same rate, data will be buffered up to
a given limit.  When this limit is reached, `teepot` will try to run at the
speed of the slowest reader.  If the slowest reader fails to read anything
for too long, `teepot` will use the filesystem to provide extra buffering.
If the input is a regular file it will do this by simply re-reading the input,
otherwise the data will be saved to temporary files.

### Building teepot

If building from a git checkout, you first need to generate the `configure`
script (this is included in the release tarballs):

```sh
autoreconf -i
```

Then the package can be built using:

```sh
./configure
make
```

To install it, use:

```sh
make install
```

Run `./configure --help` to show build options.  In particular, `--prefix` can
be used to set an alternate install location, which is useful if you want
to install it without needing root privileges.  For example:

```sh
./configure --prefix=$HOME/opt/teepot
make
make install
```

will install the program to `opt/teepot/bin` in your home directory.

### License

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
[GNU General Public License](LICENSE) for more details.
