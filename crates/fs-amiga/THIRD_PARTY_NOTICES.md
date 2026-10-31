# Third-party notices

`fs-amiga` is MIT licensed (see `LICENSE`), except as noted here.

## pfs3aio (PFS3)

The PFS3 driver (`src/pfs3.rs`, `src/pfs3_fsck.rs`, `src/pfs3_clone.rs`) reimplements on-disk behaviour of pfs3aio and cites its sources by file and line. No pfs3aio code was translated, but as a precaution this crate is distributed under `MIT AND BSD-4-Clause` and carries pfs3aio's licence:

This product includes software developed by Michiel Pelt.

```
Copyright (c) 2011, Michiel Pelt
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. All advertising materials mentioning features or use of this software
   must display the following acknowledgement:
   This product includes software developed by Michiel Pelt.
4. Neither the name of the author nor the
   names of its contributors may be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY MICHIEL PELT ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL MICHIEL PELT BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.```

## Smart File System (SFS)

The SFS driver (`src/sfs.rs`, `src/sfs_fsck.rs`) follows the on-disk format of the SmartFilesystem sources (`filesystemmain.c`, `nodes.c`, `SFScheck.c`), which were released as freeware without licence terms. Only format facts are used; no code was copied.

## AFFS

The AFFS driver follows the AmigaDOS on-disk format. ADFlib (GPL-2.0-or-later / LGPL-2.1) and amitools (GPL-2.0) were used as cross-checks only; no code was copied.
