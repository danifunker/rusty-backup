// Gate 0 of docs/cocoa_vintage_shell_plan.md: lists every AXIdentifier in an app's windows.
// Objective-C 1.0 so it runs from 10.7 up, where the Swift driver cannot; the seed of its port.
//
//   ax_probe <pid>            -> one "role<TAB>identifier<TAB>value" line per element with an identifier
//   ax_probe --window <pid>   -> the CGWindowID of the app's front window, for `screencapture -l`
//   ax_probe <pid> press <id> | set <id> <value> | texts   -> act on a control, or list all text

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

// The first element in `e`'s tree whose AXIdentifier is `want`; the caller releases it.
static AXUIElementRef find(AXUIElementRef e, NSString *want, int depth)
{
    if ([ax_str(e, CFSTR("AXIdentifier")) isEqualToString:want]) {
        CFRetain(e);
        return e;
    }
    if (depth > 60) return NULL;
    CFTypeRef kids = ax_attr(e, kAXChildrenAttribute);
    if (kids == NULL) return NULL;
    AXUIElementRef hit = NULL;
    CFIndex i, n = CFArrayGetCount((CFArrayRef)kids);
    for (i = 0; i < n && hit == NULL; i++)
        hit = find((AXUIElementRef)CFArrayGetValueAtIndex((CFArrayRef)kids, i), want, depth + 1);
    CFRelease(kids);
    return hit;
}

// Every static text's value, in tree order.
static void texts(AXUIElementRef e, int depth)
{
    if ([ax_str(e, kAXRoleAttribute) isEqualToString:(NSString *)kAXStaticTextRole])
        printf("%s\n", [ax_str(e, kAXValueAttribute) UTF8String]);
    if (depth > 60) return;
    CFTypeRef kids = ax_attr(e, kAXChildrenAttribute);
    if (kids == NULL) return;
    CFIndex i, n = CFArrayGetCount((CFArrayRef)kids);
    for (i = 0; i < n; i++)
        texts((AXUIElementRef)CFArrayGetValueAtIndex((CFArrayRef)kids, i), depth + 1);
    CFRelease(kids);
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
    if (argc < 2) {
        fprintf(stderr, "usage: ax_probe [--window] <pid> [press <id> | set <id> <value> | texts]\n");
        return 2;
    }
    if (!AXAPIEnabled() && !AXIsProcessTrusted()) {
        fprintf(stderr, "ax_probe: accessibility is off (10.7: System Preferences > Universal Access)\n");
        return 3;
    }
    AXUIElementRef app = AXUIElementCreateApplication((pid_t)atoi(argv[1]));
    if (argc >= 3) {
        NSString *op = [NSString stringWithUTF8String:argv[2]];
        CFTypeRef wins = ax_attr(app, kAXWindowsAttribute);
        AXUIElementRef win = (wins && CFArrayGetCount((CFArrayRef)wins) > 0)
            ? (AXUIElementRef)CFArrayGetValueAtIndex((CFArrayRef)wins, 0) : NULL;
        int rc = 1;
        if (win != NULL && [op isEqualToString:@"texts"]) {
            texts(win, 0);
            rc = 0;
        } else if (win != NULL && argc >= 4) {
            AXUIElementRef el = find(win, [NSString stringWithUTF8String:argv[3]], 0);
            if (el == NULL) {
                fprintf(stderr, "ax_probe: no element %s\n", argv[3]);
            } else if ([op isEqualToString:@"press"]) {
                rc = AXUIElementPerformAction(el, kAXPressAction) == kAXErrorSuccess ? 0 : 1;
            } else if ([op isEqualToString:@"set"] && argc >= 5) {
                NSString *v = [NSString stringWithUTF8String:argv[4]];
                rc = AXUIElementSetAttributeValue(el, kAXValueAttribute, (CFTypeRef)v) == kAXErrorSuccess ? 0 : 1;
            }
            if (el != NULL) CFRelease(el);
        }
        if (wins) CFRelease(wins);
        CFRelease(app);
        [pool release];
        return rc;
    }
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
