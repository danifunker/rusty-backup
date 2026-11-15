// What the app needs that Mac OS X 10.7 does not export; check-lion-imports.sh and a
// DYLD_BIND_AT_LAUNCH=1 launch on Lion prove nothing else is missing.
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <sstream>

// The SDK's <sstream> declares these extern unconditionally; Lion's libc++ has no vtables for them.
template class std::basic_stringbuf<char>;
template class std::basic_ostringstream<char>;

// getrandom 0.3 (via zip) calls getentropy, which arrived in 10.12; arc4random_buf is on 10.7.
extern "C" int getentropy(void *buf, size_t len) {
    if (len > 256) { errno = EIO; return -1; }
    arc4random_buf(buf, len);
    return 0;
}
