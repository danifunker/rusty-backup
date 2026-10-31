# Third-party notices

`dsk-ghost` is MIT licensed (see `LICENSE`). Parts of it follow other sources:

## nyarime/gho (Fast-LZ)

The GHO Fast-LZ decoder in `src/gho.rs` (the compression byte `0x02` path, including `fast_lz_hash`) is ported from the clean-room reference implementation [nyarime/gho](https://github.com/nyarime/gho), under the MIT licence:

```
MIT License

Copyright (c) 2026 Nyarime

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Ghost password encryption

`src/gho_crypto.rs` was reverse-engineered from the behaviour of the `ghostexp.exe` binary. No source code was available or used.
