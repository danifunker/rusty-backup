/* Print Apple RelString(a, b, caseSensitive=false, diacSensitive=true) for every pair of
   one-byte Pascal strings: 256 lines of 256 digits, 0 = a < b, 1 = equal, 2 = a > b.
   Classic HFS orders catalog names with exactly this comparison. macOS only. */
#include <CoreServices/CoreServices.h>
#include <stdio.h>

extern short RelString(const unsigned char *, const unsigned char *, Boolean, Boolean);

int main(void) {
    unsigned char a[2] = {1, 0}, b[2] = {1, 0};
    for (int i = 0; i < 256; i++) {
        for (int j = 0; j < 256; j++) {
            a[1] = (unsigned char)i;
            b[1] = (unsigned char)j;
            putchar('1' + RelString(a, b, false, true));
        }
        putchar('\n');
    }
    return 0;
}
