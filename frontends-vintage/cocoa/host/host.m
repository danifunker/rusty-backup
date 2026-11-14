// The widget factory behind frontends-vintage/cocoa/rust/src/host.rs: plain Cocoa from 10.4 up.
// Objective-C 1.0, manual retain/release, no blocks. Rust owns every rule; this only makes views.

#import <Cocoa/Cocoa.h>
#include <string.h>

#ifndef NSINTEGER_DEFINED
typedef int NSInteger;  // 10.4's SDK predates NSInteger
#endif

extern void rb_event(uint32_t s, uint32_t c, uint32_t kind);

enum { RB_ACT = 0, RB_EDITED = 1, RB_VALUE_SET = 2 };

// A field's text is being read: the commit that reading makes reports nothing.
static BOOL rb_reading = NO;

@interface RBFlipped : NSView
@end

@implementation RBFlipped
- (BOOL)isFlipped
{
    return YES;
}
@end

// One surface's action target and text delegate.
@interface RBTarget : NSObject
{
    uint32_t sid;
}
- (id)initWithSurface:(uint32_t)s;
@end

@implementation RBTarget
- (id)initWithSurface:(uint32_t)s
{
    if ((self = [super init]) != nil)
        sid = s;
    return self;
}

- (void)fire:(id)sender
{
    NSInteger tag = [sender tag];
    if (tag >= 0)
        rb_event(sid, (uint32_t)tag, RB_ACT);
}

- (void)controlTextDidChange:(NSNotification *)note
{
    rb_event(sid, (uint32_t)[[note object] tag], RB_EDITED);
}
@end

// A field's cell that reports a value set from outside (accessibility), as typing reports.
@interface RBFieldCell : NSTextFieldCell
{
    uint32_t sid;
    uint32_t ctl;
    BOOL live;
}
- (void)setSurface:(uint32_t)s control:(uint32_t)c;
@end

@implementation RBFieldCell
- (void)setSurface:(uint32_t)s control:(uint32_t)c
{
    sid = s;
    ctl = c;
    live = YES;
}

- (void)setObjectValue:(id)value
{
    [super setObjectValue:value];
    if (live && !rb_reading)
        rb_event(sid, ctl, RB_VALUE_SET);
}
@end

@interface RBSurface : NSObject
{
@public
    NSWindow *window;
    NSView *body;
    NSMutableDictionary *views;
    RBTarget *target;
}
@end

@implementation RBSurface
- (void)dealloc
{
    [window release];
    [body release];
    [views release];
    [target release];
    [super dealloc];
}
@end

static NSMutableArray *rb_surfaces = nil;

static RBSurface *surf(uint32_t s)
{
    if (rb_surfaces == nil || s >= [rb_surfaces count])
        return nil;
    id x = [rb_surfaces objectAtIndex:s];
    return x == [NSNull null] ? nil : (RBSurface *)x;
}

static NSNumber *key(uint32_t c)
{
    return [NSNumber numberWithUnsignedInt:c];
}

static id view(uint32_t s, uint32_t c)
{
    return [surf(s)->views objectForKey:key(c)];
}

static NSString *str(const char *s)
{
    NSString *n = s ? [NSString stringWithUTF8String:s] : nil;
    return n ? n : @"";
}

static char *dup_str(NSString *s)
{
    const char *u = [s UTF8String];
    return strdup(u ? u : "");
}

// The controller id as the accessibility identifier, where AX has identifiers (10.7 on).
static void set_id(NSView *v, const char *ax_id)
{
    if (ax_id && *ax_id && [v respondsToSelector:@selector(setIdentifier:)])
        [v performSelector:@selector(setIdentifier:) withObject:str(ax_id)];
}

// Put `v` on surface `s` as control `c`; a control that acts gets the surface's target.
static void adopt(uint32_t s, uint32_t c, NSView *v, BOOL acts)
{
    RBSurface *x = surf(s);
    if ([v isKindOfClass:[NSControl class]]) {
        NSControl *ctl = (NSControl *)v;
        [ctl setTag:c];
        if (acts) {
            [ctl setTarget:x->target];
            [ctl setAction:@selector(fire:)];
        }
    }
    [[x->views objectForKey:key(c)] removeFromSuperview];
    [x->body addSubview:v];
    [x->views setObject:v forKey:key(c)];
}

uint32_t rb_host_window(const char *title, int32_t w, int32_t h)
{
    if (rb_surfaces == nil)
        rb_surfaces = [[NSMutableArray alloc] init];
    uint32_t s = (uint32_t)[rb_surfaces count];
    RBSurface *x = [[[RBSurface alloc] init] autorelease];
    x->window = [[NSWindow alloc] initWithContentRect:NSMakeRect(0, 0, w, h)
                                            styleMask:(NSTitledWindowMask | NSClosableWindowMask | NSMiniaturizableWindowMask)
                                              backing:NSBackingStoreBuffered
                                                defer:NO];
    [x->window setReleasedWhenClosed:NO];
    [x->window setTitle:str(title)];
    x->body = [[RBFlipped alloc] initWithFrame:NSMakeRect(0, 0, w, h)];
    [x->window setContentView:x->body];
    x->views = [[NSMutableDictionary alloc] init];
    x->target = [[RBTarget alloc] initWithSurface:s];
    [rb_surfaces addObject:x];
    return s;
}

void rb_host_window_show(uint32_t s)
{
    NSWindow *w = surf(s)->window;
    [w center];
    [w makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    // Test rigs over ssh cannot list windows on 10.7; this is the id `screencapture -l` takes.
    NSLog(@"rusty-backup: window %ld", (long)[w windowNumber]);
}

void rb_host_window_close(uint32_t s)
{
    [surf(s)->window close];
}

void rb_host_area(uint32_t s, int32_t *out)
{
    NSRect b = [surf(s)->body bounds];
    out[0] = 0;
    out[1] = 0;
    out[2] = (int32_t)b.size.width;
    out[3] = (int32_t)b.size.height;
}

void rb_host_release(uint32_t s)
{
    RBSurface *x = surf(s);
    if (x != nil)
        [x->views removeAllObjects];
}

static NSButton *new_button(const char *title, NSButtonType type)
{
    NSButton *b = [[[NSButton alloc] initWithFrame:NSZeroRect] autorelease];
    [b setButtonType:type];
    if (type == NSMomentaryPushInButton)
        [b setBezelStyle:NSRoundedBezelStyle];
    [b setTitle:str(title)];
    return b;
}

void rb_host_button(uint32_t s, uint32_t c, const char *title, const char *ax_id)
{
    NSButton *b = new_button(title, NSMomentaryPushInButton);
    set_id(b, ax_id);
    adopt(s, c, b, YES);
}

void rb_host_check(uint32_t s, uint32_t c, const char *title, const char *ax_id)
{
    NSButton *b = new_button(title, NSSwitchButton);
    set_id(b, ax_id);
    adopt(s, c, b, YES);
}

// Radios do not group themselves before 10.8; the screen sets every state from its controller.
void rb_host_radio(uint32_t s, uint32_t c, uint32_t group, const char *title, const char *ax_id)
{
    NSButton *b = new_button(title, NSRadioButton);
    set_id(b, ax_id);
    adopt(s, c, b, YES);
}

void rb_host_label(uint32_t s, uint32_t c, const char *text, int wrap)
{
    NSTextField *l = [[[NSTextField alloc] initWithFrame:NSZeroRect] autorelease];
    [l setEditable:NO];
    [l setSelectable:NO];
    [l setBezeled:NO];
    [l setDrawsBackground:NO];
    [l setStringValue:str(text)];
    [[l cell] setWraps:(wrap ? YES : NO)];
    [[l cell] setLineBreakMode:(wrap ? NSLineBreakByWordWrapping : NSLineBreakByTruncatingTail)];
    adopt(s, c, l, NO);
}

void rb_host_popup(uint32_t s, uint32_t c, const char *const *items, uint32_t n, const char *ax_id, int pulls_down)
{
    NSPopUpButton *p = [[[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:(pulls_down ? YES : NO)] autorelease];
    [p setAutoenablesItems:NO];
    uint32_t i;
    for (i = 0; i < n; i++)
        [p addItemWithTitle:str(items[i])];
    set_id(p, ax_id);
    adopt(s, c, p, pulls_down ? NO : YES);
}

void rb_host_set_pulldown(uint32_t s, uint32_t c, const char *title, const char *const *labels,
                          const int32_t *ctls, const uint8_t *on, uint32_t n)
{
    NSPopUpButton *p = view(s, c);
    [p removeAllItems];
    [p addItemWithTitle:str(title)];
    uint32_t i;
    for (i = 0; i < n; i++) {
        [p addItemWithTitle:str(labels[i])];
        NSMenuItem *item = [p lastItem];
        if (ctls[i] >= 0) {
            [item setTag:ctls[i]];
            [item setTarget:surf(s)->target];
            [item setAction:@selector(fire:)];
        }
        [item setEnabled:(ctls[i] >= 0 && on[i]) ? YES : NO];
    }
}

void rb_host_field(uint32_t s, uint32_t c, const char *ax_id, const char *caption, int secret)
{
    NSTextField *f;
    if (secret) {
        f = [[[NSSecureTextField alloc] initWithFrame:NSMakeRect(0, 0, 200, 22)] autorelease];
    } else {
        f = [[[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 200, 22)] autorelease];
        RBFieldCell *cell = [[[RBFieldCell alloc] initTextCell:@""] autorelease];
        [cell setEditable:YES];
        [cell setBezeled:YES];
        [cell setScrollable:YES];
        [f setCell:cell];
        [cell setSurface:s control:c];
    }
    set_id(f, ax_id);
    [[f cell] accessibilitySetOverrideValue:str(caption) forAttribute:NSAccessibilityDescriptionAttribute];
    [f setDelegate:(id)surf(s)->target];
    adopt(s, c, f, NO);
}

void rb_host_progress(uint32_t s, uint32_t c)
{
    NSProgressIndicator *p = [[[NSProgressIndicator alloc] initWithFrame:NSZeroRect] autorelease];
    [p setStyle:NSProgressIndicatorBarStyle];
    [p setIndeterminate:NO];
    [p setMinValue:0.0];
    [p setMaxValue:100.0];
    adopt(s, c, p, NO);
}

void rb_host_remove(uint32_t s, uint32_t c)
{
    [view(s, c) removeFromSuperview];
    [surf(s)->views removeObjectForKey:key(c)];
}

int rb_host_shown(uint32_t s, uint32_t c)
{
    return [view(s, c) isHidden] ? 0 : 1;
}

void rb_host_show(uint32_t s, uint32_t c, int on)
{
    NSView *v = view(s, c);
    if ([v isHidden] == (on ? YES : NO))
        [v setHidden:(on ? NO : YES)];
}

void rb_host_enable(uint32_t s, uint32_t c, int on)
{
    id v = view(s, c);
    if ([v isKindOfClass:[NSControl class]] && [v isEnabled] != (on ? YES : NO))
        [v setEnabled:(on ? YES : NO)];
}

int rb_host_checked(uint32_t s, uint32_t c)
{
    return [view(s, c) state] == NSOnState ? 1 : 0;
}

void rb_host_set_check(uint32_t s, uint32_t c, int on)
{
    NSButton *b = view(s, c);
    NSInteger want = on ? NSOnState : NSOffState;
    if ([b state] != want)
        [b setState:want];
}

void rb_host_set_title(uint32_t s, uint32_t c, const char *title)
{
    NSButton *b = view(s, c);
    NSString *t = str(title);
    if (![[b title] isEqualToString:t])
        [b setTitle:t];
}

// Only when it changed: a blind set moves the caret mid-typing.
void rb_host_set_text(uint32_t s, uint32_t c, const char *text)
{
    NSTextField *f = view(s, c);
    NSString *t = str(text);
    if (![[f stringValue] isEqualToString:t])
        [f setStringValue:t];
}

void rb_host_set_tone(uint32_t s, uint32_t c, int tone)
{
    NSColor *color;
    switch (tone) {
    case 1: color = [NSColor orangeColor]; break;
    case 2: color = [NSColor redColor]; break;
    case 3: color = [NSColor colorWithCalibratedRed:0.0 green:0.55 blue:0.0 alpha:1.0]; break;
    default: color = [NSColor controlTextColor]; break;
    }
    [view(s, c) setTextColor:color];
}

char *rb_host_text(uint32_t s, uint32_t c)
{
    BOOL was = rb_reading;
    rb_reading = YES;
    NSString *t = [view(s, c) stringValue];
    rb_reading = was;
    return dup_str(t);
}

int rb_host_typing(uint32_t s, uint32_t c)
{
    return [view(s, c) currentEditor] != nil ? 1 : 0;
}

int32_t rb_host_picked(uint32_t s, uint32_t c)
{
    return (int32_t)[view(s, c) indexOfSelectedItem];
}

void rb_host_select(uint32_t s, uint32_t c, uint32_t i)
{
    [view(s, c) selectItemAtIndex:i];
}

uint32_t rb_host_item_count(uint32_t s, uint32_t c)
{
    return (uint32_t)[view(s, c) numberOfItems];
}

char *rb_host_item(uint32_t s, uint32_t c, uint32_t i)
{
    return dup_str([view(s, c) itemTitleAtIndex:i]);
}

void rb_host_set_items(uint32_t s, uint32_t c, const char *const *items, uint32_t n)
{
    NSPopUpButton *p = view(s, c);
    [p removeAllItems];
    uint32_t i;
    for (i = 0; i < n; i++)
        [p addItemWithTitle:str(items[i])];
}

void rb_host_move(uint32_t s, uint32_t c, int32_t x, int32_t y, int32_t w, int32_t h)
{
    NSView *v = view(s, c);
    NSRect r = NSMakeRect(x, y, w > 0 ? w : 0, h > 0 ? h : 0);
    if (!NSEqualRects([v frame], r))
        [v setFrame:r];
}
