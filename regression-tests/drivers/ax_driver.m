// The vintage Cocoa shell's flow driver: `ax_driver.swift` in Objective-C, so it runs from 10.7 up
// (Swift needs 10.9). One JSON op per line on stdin, one JSON answer per line on stdout (FLOWS.md).
//
// Built by runner/src/ax.rs with clang and run as `ax_driver_objc <shell binary> <work dir>`. It
// starts the shell with a scratch config and a picks file, answers {"ready": true}, then reads ops
// and answers {"ok": true} or {"ok": false, "expected": ..., "observed": ...}. Controls are found
// by AXIdentifier, else by title, description or value; acting goes through AX actions and
// attributes, and only `key` / `type` post keyboard events to the shell.

#import <Foundation/Foundation.h>
#import <ApplicationServices/ApplicationServices.h>

static AXUIElementRef app;
static NSTask *child;
static NSString *picks;
static NSString *shot_dir;
static AXUIElementRef current;

@interface Fail : NSObject
{
@public
    NSString *expected;
    NSString *observed;
}
@end

@implementation Fail
@end

static Fail *fail(NSString *expected, NSString *observed)
{
    Fail *f = [[[Fail alloc] init] autorelease];
    f->expected = [expected retain];
    f->observed = [observed retain];
    return f;
}

static id attr(AXUIElementRef e, NSString *name)
{
    CFTypeRef v = NULL;
    if (AXUIElementCopyAttributeValue(e, (CFStringRef)name, &v) != kAXErrorSuccess || v == NULL)
        return nil;
    return [(id)v autorelease];
}

static NSString *str(AXUIElementRef e, NSString *name)
{
    id v = attr(e, name);
    if ([v isKindOfClass:[NSString class]])
        return v;
    if ([v isKindOfClass:[NSNumber class]])
        return [v stringValue];
    return @"";
}

static NSArray *children(AXUIElementRef e)
{
    id kids = attr(e, (NSString *)kAXChildrenAttribute);
    return [kids isKindOfClass:[NSArray class]] ? kids : [NSArray array];
}

static void walk(AXUIElementRef e, int depth, NSMutableArray *out)
{
    if (depth > 60)
        return;
    [out addObject:(id)e];
    NSArray *kids = children(e);
    NSUInteger i;
    for (i = 0; i < [kids count]; i++)
        walk((AXUIElementRef)[kids objectAtIndex:i], depth + 1, out);
}

// Every element of the app's windows, in tree order; the menu bar is left out, an open menu is not.
static NSArray *elements(void)
{
    NSMutableArray *out = [NSMutableArray array];
    NSArray *top = children(app);
    NSUInteger i;
    for (i = 0; i < [top count]; i++) {
        AXUIElementRef c = (AXUIElementRef)[top objectAtIndex:i];
        if (![str(c, (NSString *)kAXRoleAttribute) isEqualToString:(NSString *)kAXMenuBarRole])
            walk(c, 0, out);
    }
    return out;
}

static NSString *ident(AXUIElementRef e)
{
    return str(e, @"AXIdentifier");
}

static NSArray *names(AXUIElementRef e)
{
    NSMutableArray *out = [NSMutableArray array];
    NSArray *keys = [NSArray arrayWithObjects:(NSString *)kAXTitleAttribute, (NSString *)kAXDescriptionAttribute,
                     (NSString *)kAXValueAttribute, @"AXLabelValue", nil];
    NSUInteger i;
    for (i = 0; i < [keys count]; i++) {
        NSString *s = str(e, [keys objectAtIndex:i]);
        if ([s length] > 0)
            [out addObject:s];
    }
    return out;
}

static NSString *visible_text(void)
{
    NSMutableArray *lines = [NSMutableArray array];
    NSArray *all = elements();
    NSUInteger i;
    for (i = 0; i < [all count]; i++) {
        AXUIElementRef e = (AXUIElementRef)[all objectAtIndex:i];
        NSMutableArray *parts = [NSMutableArray array];
        NSString *idv = ident(e);
        if ([idv length] > 0)
            [parts addObject:[NSString stringWithFormat:@"[%@]", idv]];
        NSArray *ns = [[[NSSet setWithArray:names(e)] allObjects] sortedArrayUsingSelector:@selector(compare:)];
        [parts addObjectsFromArray:ns];
        if ([parts count] == 0)
            continue;
        NSString *line = [parts componentsJoinedByString:@" "];
        if ([line length] > 300)
            line = [line substringToIndex:300];
        [lines addObject:line];
    }
    return [lines componentsJoinedByString:@"\n"];
}

static BOOL any_name(AXUIElementRef e, NSString *want, BOOL part)
{
    NSArray *ns = names(e);
    NSUInteger i;
    for (i = 0; i < [ns count]; i++) {
        NSString *n = [ns objectAtIndex:i];
        if (part ? [n rangeOfString:want].location != NSNotFound : [n isEqualToString:want])
            return YES;
    }
    return NO;
}

// By AXIdentifier first, then by an element's title, description or value (exact, then a part).
static AXUIElementRef find(NSString *id_or_name, NSString *label, NSUInteger nth, double secs, Fail **err)
{
    NSDate *end = [NSDate dateWithTimeIntervalSinceNow:secs];
    for (;;) {
        NSArray *all = elements();
        NSMutableArray *hits = [NSMutableArray array];
        NSUInteger i;
        for (i = 0; i < [all count]; i++)
            if ([ident((AXUIElementRef)[all objectAtIndex:i]) isEqualToString:id_or_name])
                [hits addObject:[all objectAtIndex:i]];
        if ([hits count] == 0) {
            NSArray *wants = label ? [NSArray arrayWithObjects:label, id_or_name, nil] : [NSArray arrayWithObject:id_or_name];
            NSUInteger w;
            for (w = 0; w < [wants count] && [hits count] == 0; w++) {
                NSString *want = [wants objectAtIndex:w];
                if ([want length] == 0)
                    continue;
                int pass;
                for (pass = 0; pass < 2 && [hits count] == 0; pass++)
                    for (i = 0; i < [all count]; i++)
                        if (any_name((AXUIElementRef)[all objectAtIndex:i], want, pass == 1))
                            [hits addObject:[all objectAtIndex:i]];
            }
        }
        if ([hits count] > nth)
            return (AXUIElementRef)[hits objectAtIndex:nth];
        if ([end timeIntervalSinceNow] <= 0) {
            NSString *what = [NSString stringWithFormat:@"a control '%@'%@", id_or_name,
                              label ? [NSString stringWithFormat:@" ('%@')", label] : @""];
            *err = fail(what, visible_text());
            return NULL;
        }
        [NSThread sleepForTimeInterval:0.2];
    }
}

// The table row `e` sits in, when it is (or is inside) one.
static AXUIElementRef row_of(AXUIElementRef e)
{
    AXUIElementRef cur = e;
    int i;
    for (i = 0; i < 6 && cur != NULL; i++) {
        if ([str(cur, (NSString *)kAXRoleAttribute) isEqualToString:(NSString *)kAXRowRole])
            return cur;
        cur = (AXUIElementRef)attr(cur, (NSString *)kAXParentAttribute);
    }
    return NULL;
}

static NSArray *actions(AXUIElementRef e)
{
    CFArrayRef names_ = NULL;
    if (AXUIElementCopyActionNames(e, &names_) != kAXErrorSuccess || names_ == NULL)
        return [NSArray array];
    return [(NSArray *)names_ autorelease];
}

static BOOL activate(AXUIElementRef e, Fail **err)
{
    if ([actions(e) containsObject:(NSString *)kAXPressAction]) {
        AXUIElementPerformAction(e, kAXPressAction);
        [NSThread sleepForTimeInterval:0.2];
        return YES;
    }
    if (AXUIElementSetAttributeValue(e, kAXSelectedAttribute, kCFBooleanTrue) == kAXErrorSuccess) {
        [NSThread sleepForTimeInterval:0.2];
        return YES;
    }
    // A wrapper that carries the id (a toolbar item's group) passes the action to its one child.
    NSArray *kids = children(e);
    if ([kids count] == 1)
        return activate((AXUIElementRef)[kids objectAtIndex:0], err);
    NSString *who = [names(e) count] ? [names(e) objectAtIndex:0] : ident(e);
    *err = fail([NSString stringWithFormat:@"'%@' to take an action", who], visible_text());
    return NO;
}

static BOOL choose(NSString *value, Fail **err)
{
    if (current == NULL) {
        *err = fail(@"a focused control to choose on", @"");
        return NO;
    }
    AXUIElementRef e = current;
    NSString *want = [value lowercaseString];
    NSString *role = str(e, (NSString *)kAXRoleAttribute);
    if (([role isEqualToString:(NSString *)kAXCheckBoxRole] || [role isEqualToString:(NSString *)kAXRadioButtonRole])
        && ([want isEqualToString:@"on"] || [want isEqualToString:@"off"])) {
        BOOL on = [str(e, (NSString *)kAXValueAttribute) isEqualToString:@"1"];
        if (on != [want isEqualToString:@"on"])
            return activate(e, err);
        return YES;
    }
    // A pop-up button opens its menu; the item is pressed by its title.
    AXUIElementPerformAction(e, kAXPressAction);
    NSDate *end = [NSDate dateWithTimeIntervalSinceNow:3];
    while ([end timeIntervalSinceNow] > 0) {
        NSArray *all = elements();
        NSUInteger i;
        for (i = 0; i < [all count]; i++) {
            AXUIElementRef it = (AXUIElementRef)[all objectAtIndex:i];
            if (![str(it, (NSString *)kAXRoleAttribute) isEqualToString:(NSString *)kAXMenuItemRole])
                continue;
            NSString *title = str(it, (NSString *)kAXTitleAttribute);
            NSString *t = [title lowercaseString];
            if ([t isEqualToString:want] || ([t length] > 0 && [t rangeOfString:want].location != NSNotFound)) {
                AXUIElementPerformAction(it, kAXPressAction);
                // A slow runner commits the pick after the menu closes: wait for the pop-up to show it.
                NSDate *shown = [NSDate dateWithTimeIntervalSinceNow:5];
                while ([shown timeIntervalSinceNow] > 0
                       && ![str(e, (NSString *)kAXValueAttribute) isEqualToString:title]
                       && ![str(e, (NSString *)kAXTitleAttribute) isEqualToString:title])
                    [NSThread sleepForTimeInterval:0.1];
                [NSThread sleepForTimeInterval:0.2];
                return YES;
            }
        }
        [NSThread sleepForTimeInterval:0.2];
    }
    AXUIElementPerformAction(e, kAXCancelAction);
    NSString *who = [names(e) count] ? [names(e) objectAtIndex:0] : ident(e);
    *err = fail([NSString stringWithFormat:@"a choice '%@' on '%@'", value, who], visible_text());
    return NO;
}

static BOOL fill(NSString *text, Fail **err)
{
    if (current == NULL) {
        *err = fail(@"a focused field to fill", @"");
        return NO;
    }
    if (AXUIElementSetAttributeValue(current, kAXValueAttribute, (CFStringRef)text) != kAXErrorSuccess) {
        NSString *who = [names(current) count] ? [names(current) objectAtIndex:0] : ident(current);
        *err = fail([NSString stringWithFormat:@"'%@' to take text", who], visible_text());
        return NO;
    }
    [NSThread sleepForTimeInterval:0.2];
    return YES;
}

static BOOL shows(NSString *text)
{
    NSArray *wants = [text componentsSeparatedByString:@" || "];
    NSArray *all = elements();
    NSUInteger i, w;
    for (i = 0; i < [all count]; i++)
        for (w = 0; w < [wants count]; w++)
            if (any_name((AXUIElementRef)[all objectAtIndex:i], [wants objectAtIndex:w], YES))
                return YES;
    return NO;
}

static BOOL wait_until(double secs, BOOL want_shown, NSString *text)
{
    NSDate *end = [NSDate dateWithTimeIntervalSinceNow:secs];
    while ([end timeIntervalSinceNow] > 0) {
        if (shows(text) == want_shown)
            return YES;
        [NSThread sleepForTimeInterval:0.3];
    }
    return shows(text) == want_shown;
}

static int key_code(NSString *k)
{
    NSDictionary *codes = [NSDictionary dictionaryWithObjectsAndKeys:
        @36, @"Enter", @48, @"Tab", @49, @"Space", @51, @"Backspace", @53, @"Esc", @53, @"Escape",
        @117, @"Delete", @115, @"Home", @119, @"End", @123, @"Left", @124, @"Right", @125, @"Down",
        @126, @"Up", @122, @"F1", @120, @"F2", @99, @"F3", @118, @"F4", @96, @"F5", @97, @"F6",
        @98, @"F7", @100, @"F8", @101, @"F9", @109, @"F10", @103, @"F11", @111, @"F12", nil];
    NSNumber *n = [codes objectForKey:k];
    return n ? [n intValue] : -1;
}

static void post(CGKeyCode code, CGEventFlags flags, NSString *unicode)
{
    pid_t pid = [child processIdentifier];
    int down;
    for (down = 1; down >= 0; down--) {
        CGEventRef ev = CGEventCreateKeyboardEvent(NULL, code, down ? true : false);
        if (ev == NULL)
            continue;
        CGEventSetFlags(ev, flags);
        if (unicode != nil) {
            UniChar chars[8];
            NSUInteger len = [unicode length] < 8 ? [unicode length] : 8;
            [unicode getCharacters:chars range:NSMakeRange(0, len)];
            CGEventKeyboardSetUnicodeString(ev, len, chars);
        }
        if (@available(macOS 10.11, *)) {
            CGEventPostToPid(pid, ev);
        } else {
            ProcessSerialNumber psn;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            if (GetProcessForPID(pid, &psn) == noErr)
                CGEventPostToPSN(&psn, ev);
#pragma clang diagnostic pop
        }
        CFRelease(ev);
    }
    [NSThread sleepForTimeInterval:0.15];
}

static BOOL press(NSString *name, Fail **err)
{
    NSString *key = name;
    CGEventFlags flags = 0;
    NSRange plus;
    while ([key length] > 1 && (plus = [key rangeOfString:@"+"]).location != NSNotFound && plus.location > 0) {
        NSString *mod = [key substringToIndex:plus.location];
        if ([mod isEqualToString:@"Ctrl"])
            flags |= kCGEventFlagMaskControl;
        else if ([mod isEqualToString:@"Shift"])
            flags |= kCGEventFlagMaskShift;
        else if ([mod isEqualToString:@"Alt"])
            flags |= kCGEventFlagMaskAlternate;
        else if ([mod isEqualToString:@"Cmd"])
            flags |= kCGEventFlagMaskCommand;
        else {
            *err = fail([NSString stringWithFormat:@"a known modifier in '%@'", name], @"");
            return NO;
        }
        key = [key substringFromIndex:plus.location + 1];
    }
    int code = key_code(key);
    if (code >= 0) {
        post((CGKeyCode)code, flags, nil);
    } else if ([key length] == 1) {
        post(0, flags, key);
    } else {
        *err = fail([NSString stringWithFormat:@"a known key, not '%@'", name], @"");
        return NO;
    }
    return YES;
}

// Capture the shell's own on-screen windows, front to back: `<name>.png`, then `<name>-2.png`...
static NSDictionary *shot(NSString *name)
{
    Fail *ignored = nil;
    AXUIElementRef clear = find(@"log.clear", nil, 0, 0, &ignored);
    if (clear != NULL)
        activate(clear, &ignored);
    [NSThread sleepForTimeInterval:0.4];
    pid_t pid = [child processIdentifier];
    NSArray *info = [(NSArray *)CGWindowListCopyWindowInfo(kCGWindowListOptionOnScreenOnly | kCGWindowListExcludeDesktopElements, kCGNullWindowID) autorelease];
    NSMutableArray *saved = [NSMutableArray array];
    [[NSFileManager defaultManager] createDirectoryAtPath:shot_dir withIntermediateDirectories:YES attributes:nil error:NULL];
    NSUInteger i, n = 0;
    for (i = 0; i < [info count]; i++) {
        NSDictionary *w = [info objectAtIndex:i];
        NSDictionary *b = [w objectForKey:(NSString *)kCGWindowBounds];
        if ([[w objectForKey:(NSString *)kCGWindowOwnerPID] intValue] != pid
            || [[w objectForKey:(NSString *)kCGWindowLayer] intValue] >= 25
            || [[b objectForKey:@"Width"] doubleValue] <= 80 || [[b objectForKey:@"Height"] doubleValue] <= 60)
            continue;
        NSString *file = [shot_dir stringByAppendingPathComponent:
            n == 0 ? [name stringByAppendingString:@".png"] : [NSString stringWithFormat:@"%@-%lu.png", name, (unsigned long)n + 1]];
        n++;
        NSTask *t = [[[NSTask alloc] init] autorelease];
        [t setLaunchPath:@"/usr/sbin/screencapture"];
        [t setArguments:[NSArray arrayWithObjects:@"-x", @"-o",
                         [NSString stringWithFormat:@"-l%@", [w objectForKey:(NSString *)kCGWindowNumber]], file, nil]];
        [t launch];
        [t waitUntilExit];
        NSDictionary *a = [[NSFileManager defaultManager] attributesOfItemAtPath:file error:NULL];
        if ([t terminationStatus] == 0 && [[a objectForKey:NSFileSize] longLongValue] > 0)
            [saved addObject:file];
    }
    return [NSDictionary dictionaryWithObjectsAndKeys:@YES, @"ok", saved, @"shots", nil];
}

static NSDictionary *ok(void)
{
    return [NSDictionary dictionaryWithObject:@YES forKey:@"ok"];
}

static NSDictionary *run(NSDictionary *op, Fail **err)
{
    NSString *kind = [op objectForKey:@"op"] ?: @"";
    NSString *arg = [op objectForKey:@"arg"] ?: @"";
    NSString *idv = [op objectForKey:@"id"] ?: arg;
    NSUInteger nth = [[op objectForKey:@"nth"] unsignedIntegerValue];
    double limit = ([op objectForKey:@"timeout_ms"] ? [[op objectForKey:@"timeout_ms"] doubleValue] : 30000) / 1000;
    if ([kind isEqualToString:@"tab"]) {
        // A screen is an item on the screen toolbar, found by its label.
        AXUIElementRef e = find(arg, arg, 0, 10, err);
        return (e && activate(e, err)) ? ok() : nil;
    }
    if ([kind isEqualToString:@"focus"]) {
        AXUIElementRef e = find(idv, arg, nth, 10, err);
        if (e == NULL)
            return nil;
        if (current)
            CFRelease(current);
        current = (AXUIElementRef)CFRetain(e);
        AXUIElementRef row = row_of(e);
        NSString *role = str(e, (NSString *)kAXRoleAttribute);
        if (row != NULL) {
            // A name in a list: select its row, and give the list the keys that follow.
            AXUIElementSetAttributeValue(row, kAXSelectedAttribute, kCFBooleanTrue);
            AXUIElementRef table = (AXUIElementRef)attr(row, (NSString *)kAXParentAttribute);
            if (table)
                AXUIElementSetAttributeValue(table, kAXFocusedAttribute, kCFBooleanTrue);
        } else if ([role isEqualToString:(NSString *)kAXTextFieldRole] || [role isEqualToString:(NSString *)kAXTextAreaRole]) {
            // A field takes its value whole through `fill`; focusing it would route that to the field editor.
        } else if (AXUIElementSetAttributeValue(e, kAXSelectedAttribute, kCFBooleanTrue) != kAXErrorSuccess) {
            AXUIElementSetAttributeValue(e, kAXFocusedAttribute, kCFBooleanTrue);
        }
        [NSThread sleepForTimeInterval:0.2];
        return ok();
    }
    if ([kind isEqualToString:@"choose"])
        return choose(arg, err) ? ok() : nil;
    if ([kind isEqualToString:@"activate"]) {
        AXUIElementRef e = find(idv, arg, nth, 10, err);
        return (e && activate(e, err)) ? ok() : nil;
    }
    if ([kind isEqualToString:@"pick"]) {
        [[NSString stringWithFormat:@"*\t%@\n", arg] writeToFile:picks atomically:YES encoding:NSUTF8StringEncoding error:NULL];
        return ok();
    }
    if ([kind isEqualToString:@"key"])
        return press(arg, err) ? ok() : nil;
    if ([kind isEqualToString:@"keys"]) {
        NSArray *ks = [arg componentsSeparatedByString:@" "];
        NSUInteger i;
        for (i = 0; i < [ks count]; i++)
            if ([[ks objectAtIndex:i] length] && !press([ks objectAtIndex:i], err))
                return nil;
        return ok();
    }
    if ([kind isEqualToString:@"type"]) {
        NSUInteger i;
        for (i = 0; i < [arg length]; i++)
            if (!press([arg substringWithRange:NSMakeRange(i, 1)], err))
                return nil;
        return ok();
    }
    if ([kind isEqualToString:@"fill"])
        return fill(arg, err) ? ok() : nil;
    if ([kind isEqualToString:@"wait"] || [kind isEqualToString:@"expect"]) {
        double secs = [kind isEqualToString:@"wait"] ? limit : 2;
        if (!wait_until(secs, YES, arg)) {
            *err = fail([NSString stringWithFormat:@"the window shows '%@'", arg], visible_text());
            return nil;
        }
        return ok();
    }
    if ([kind isEqualToString:@"wait_gone"]) {
        if (!wait_until(limit, NO, arg)) {
            *err = fail([NSString stringWithFormat:@"the window without '%@'", arg], visible_text());
            return nil;
        }
        return ok();
    }
    if ([kind isEqualToString:@"expect_not"]) {
        [NSThread sleepForTimeInterval:0.5];
        if (shows(arg)) {
            *err = fail([NSString stringWithFormat:@"the window without '%@'", arg], visible_text());
            return nil;
        }
        return ok();
    }
    if ([kind isEqualToString:@"text"])
        return [NSDictionary dictionaryWithObjectsAndKeys:@YES, @"ok", visible_text(), @"text", nil];
    if ([kind isEqualToString:@"shot"])
        return shot(arg);
    *err = fail([NSString stringWithFormat:@"a known op, not '%@'", kind], @"");
    return nil;
}

static void reply(NSDictionary *obj)
{
    NSData *d = [NSJSONSerialization dataWithJSONObject:obj options:0 error:NULL];
    if (d == nil)
        return;
    fwrite([d bytes], 1, [d length], stdout);
    fputc('\n', stdout);
    fflush(stdout);
}

static BOOL trusted(void)
{
    if (@available(macOS 10.9, *))
        return AXIsProcessTrusted();
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
    return AXAPIEnabled();
#pragma clang diagnostic pop
}

int main(int argc, const char **argv)
{
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    if (argc < 3) {
        reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ready", @"usage: ax_driver_objc <shell> <work dir>", @"why", nil]);
        return 1;
    }
    NSString *binary = [NSString stringWithUTF8String:argv[1]];
    NSString *work = [NSString stringWithUTF8String:argv[2]];
    if (!trusted()) {
        reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ready",
               @"this process has no Accessibility permission (10.7 to 10.8: Universal Access; later: Privacy & Security)", @"why", nil]);
        return 1;
    }
    NSFileManager *fm = [NSFileManager defaultManager];
    NSDictionary *env0 = [[NSProcessInfo processInfo] environment];
    // A tour run collects every case's shots in one folder; otherwise they stay with the case.
    shot_dir = [[env0 objectForKey:@"RB_SHOTS_DIR"] ?: [work stringByAppendingPathComponent:@"shots"] retain];
    NSString *home = [work stringByAppendingPathComponent:@"home"];
    NSString *config = [home stringByAppendingPathComponent:@".config/rusty-backup"];
    [fm createDirectoryAtPath:config withIntermediateDirectories:YES attributes:nil error:NULL];
    [@"{\"update_check\":{\"enabled\":false,\"repository_url\":\"\"}}" writeToFile:[config stringByAppendingPathComponent:@"config.json"]
                                                                          atomically:YES encoding:NSUTF8StringEncoding error:NULL];
    picks = [[work stringByAppendingPathComponent:@"picks.tsv"] retain];
    [fm createFileAtPath:picks contents:[NSData data] attributes:nil];
    NSMutableDictionary *env = [NSMutableDictionary dictionaryWithDictionary:env0];
    [env setObject:home forKey:@"HOME"];
    [env setObject:config forKey:@"RUSTY_BACKUP_CONFIG_DIR"];
    [env setObject:picks forKey:@"RUSTY_BACKUP_TEST_PICKS"];
    NSString *log_path = [work stringByAppendingPathComponent:@"cocoa-app.log"];
    [fm createFileAtPath:log_path contents:[NSData data] attributes:nil];
    NSFileHandle *log = [NSFileHandle fileHandleForWritingAtPath:log_path];
    child = [[NSTask alloc] init];
    [child setLaunchPath:binary];
    [child setEnvironment:env];
    [child setCurrentDirectoryPath:work];
    [child setStandardOutput:log];
    [child setStandardError:log];
    [child setStandardInput:[NSFileHandle fileHandleWithNullDevice]];
    @try {
        [child launch];
    } @catch (NSException *e) {
        reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ready",
               [NSString stringWithFormat:@"the Cocoa shell did not start: %@", [e reason]], @"why", nil]);
        return 1;
    }
    app = AXUIElementCreateApplication([child processIdentifier]);
    NSDate *end = [NSDate dateWithTimeIntervalSinceNow:60];
    BOOL up = NO;
    while (!up && [end timeIntervalSinceNow] > 0) {
        up = [attr(app, (NSString *)kAXWindowsAttribute) count] > 0;
        if (!up)
            [NSThread sleepForTimeInterval:0.3];
    }
    if (!up) {
        reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ready", @"the Cocoa shell's window never appeared", @"why", nil]);
        [child terminate];
        return 1;
    }
    reply([NSDictionary dictionaryWithObject:@YES forKey:@"ready"]);
    char buf[65536];
    while (fgets(buf, sizeof buf, stdin) != NULL) {
        NSAutoreleasePool *inner = [[NSAutoreleasePool alloc] init];
        NSString *line = [[NSString stringWithUTF8String:buf] stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]];
        if ([line length] > 0) {
            id op = [NSJSONSerialization JSONObjectWithData:[line dataUsingEncoding:NSUTF8StringEncoding] options:0 error:NULL];
            Fail *err = nil;
            NSDictionary *answer = [op isKindOfClass:[NSDictionary class]] ? run(op, &err) : nil;
            if (![op isKindOfClass:[NSDictionary class]])
                err = fail(@"a JSON object", line);
            if (answer != nil)
                reply(answer);
            else
                reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ok",
                       err ? err->expected : @"the op to complete", @"expected",
                       err ? err->observed : visible_text(), @"observed", nil]);
            if (![child isRunning]) {
                reply([NSDictionary dictionaryWithObjectsAndKeys:@NO, @"ok", @"the Cocoa shell running", @"expected",
                       @"it exited", @"observed", nil]);
                [inner release];
                break;
            }
        }
        [inner release];
    }
    if ([child isRunning])
        [child terminate];
    [pool release];
    return 0;
}
