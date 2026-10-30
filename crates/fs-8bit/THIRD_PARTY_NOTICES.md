# Third-party notices

`fs-8bit` is MIT-licensed (see `LICENSE`). Parts of it were written by following MAME source files released under the BSD 3-Clause license. Their notices are reproduced below as that license requires.

| Module | MAME source followed | MAME copyright holder |
|---|---|---|
| `oric` (filename preparation, block allocation, name validation, `format` layout) | `src/lib/formats/fs_oric_jasmin.cpp` | Olivier Galibert |
| `dragondos` (on-disk layout reference) | `src/tools/imgtool/modules/dgndos.cpp` | tim lindner |

Other MAME files mentioned in this crate's documentation (`imgtool` modules used to cross-check output, `bk0010_dsk.cpp`) were used as test oracles or for layout facts only.

## BSD 3-Clause License (MAME files above)

Copyright the MAME copyright holders named above.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
