// Gate 0 of docs/cocoa_vintage_shell_plan.md: lists every AXIdentifier in an app's windows.
// Objective-C 1.0 so it runs from 10.7 up, where the Swift driver cannot; the seed of its port.
//
//   ax_probe <pid>            -> one "role<TAB>identifier<TAB>value" line per element with an identifier
//   ax_probe --window <pid>   -> the CGWindowID of the app's front window, for `screencapture -l`

#import <Foundation/Foundation.h>
#import <ApplicationServices/ApplicationServices.h>

static CFTypeRef ax_attr(AXUIElementRef e, CFStringRef name)
{
    CFTypeRef v = NULL;
    return AXUIElementCopyAttributeValue(e, name, &v) == kAXErrorSuccess ? v : NULL;
}

static NSString *ax_str(AXUIElementRef e, CFStringRef name)
{
    CFTypeRef v = ax_attr(e, name);
    if (v == NULL) return @"";
    NSString *s = [[(id)v description] retain];
    CFRelease(v);
    return [s autorelease];
}

static int walk(AXUIElementRef e, int depth)
{
    int found = 0;
    NSString *ident = ax_str(e, CFSTR("AXIdentifier"));
    if ([ident length] > 0) {
        printf("%s\t%s\t%s\n", [ax_str(e, kAXRoleAttribute) UTF8String], [ident UTF8String],
               [ax_str(e, kAXValueAttribute) UTF8String]);
        found++;
    }
    if (depth > 60) return found;
    CFTypeRef kids = ax_attr(e, kAXChildrenAttribute);
    if (kids == NULL) return found;
    CFIndex i, n = CFArrayGetCount((CFArrayRef)kids);
    for (i = 0; i < n; i++)
        found += walk((AXUIElementRef)CFArrayGetValueAtIndex((CFArrayRef)kids, i), depth + 1);
    CFRelease(kids);
    return found;
}

static int window_id(pid_t pid)
{
    CFArrayRef list = CGWindowListCopyWindowInfo(kCGWindowListOptionOnScreenOnly, kCGNullWindowID);
    int wid = -1;
    CFIndex i, n = list ? CFArrayGetCount(list) : 0;
    for (i = 0; i < n && wid < 0; i++) {
        NSDictionary *w = (NSDictionary *)CFArrayGetValueAtIndex(list, i);
        if ([[w objectForKey:(id)kCGWindowOwnerPID] intValue] == pid
            && [[w objectForKey:(id)kCGWindowLayer] intValue] == 0)
            wid = [[w objectForKey:(id)kCGWindowNumber] intValue];
    }
    if (list) CFRelease(list);
    return wid;
}

int main(int argc, const char **argv)
{
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    if (argc == 3 && strcmp(argv[1], "--window") == 0) {
        int wid = window_id((pid_t)atoi(argv[2]));
        if (wid >= 0) printf("%d\n", wid);
        [pool release];
        return wid >= 0 ? 0 : 1;
    }
    if (argc != 2) {
        fprintf(stderr, "usage: ax_probe [--window] <pid>\n");
        return 2;
    }
    if (!AXAPIEnabled() && !AXIsProcessTrusted()) {
        fprintf(stderr, "ax_probe: accessibility is off (10.7: System Preferences > Universal Access)\n");
        return 3;
    }
    AXUIElementRef app = AXUIElementCreateApplication((pid_t)atoi(argv[1]));
    CFTypeRef windows = ax_attr(app, kAXWindowsAttribute);
    int found = 0;
    if (windows != NULL) {
        CFIndex i, n = CFArrayGetCount((CFArrayRef)windows);
        for (i = 0; i < n; i++)
            found += walk((AXUIElementRef)CFArrayGetValueAtIndex((CFArrayRef)windows, i), 0);
        CFRelease(windows);
    }
    CFRelease(app);
    fprintf(stderr, "ax_probe: %d identified elements\n", found);
    [pool release];
    return found > 0 ? 0 : 1;
}
