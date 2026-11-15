// The widget factory behind frontends-vintage/cocoa/rust/src/host.rs: plain Cocoa from 10.4 up.
// Objective-C 1.0, manual retain/release, no blocks. Rust owns every rule; this only makes views.

#import <Cocoa/Cocoa.h>
#include <string.h>

#include <objc/objc-runtime.h>

// 10.4's SDK predates NSInteger, NSUInteger and CGFloat.
#ifndef NSINTEGER_DEFINED
typedef int NSInteger;
typedef unsigned int NSUInteger;
#endif
#ifndef CGFLOAT_DEFINED
typedef float CGFloat;
#endif

// A selector newer than the SDK in use, sent with one BOOL: the caller checks it responds.
static void rb_send_bool(id target, SEL sel, BOOL on)
{
    ((void (*)(id, SEL, BOOL))objc_msgSend)(target, sel, on);
}

extern void rb_event(uint32_t s, uint32_t c, uint32_t kind);
extern void rb_window_event(uint32_t s, uint32_t kind);
extern void rb_later(void);
extern void rb_tick(void);
extern void rb_timed(void);
extern void rb_canvas_draw(uint32_t s, uint32_t c, double w, double h, int dark);
extern void rb_dropped(uint32_t s, const char *paths, double x, double y);
extern void rb_command(const char *id);
extern void rb_rows_event(uint32_t s, uint32_t c, uint32_t kind, uint32_t a, uint32_t b, uint32_t mods, double x, double y);

enum { RB_ACT = 0, RB_EDITED = 1, RB_VALUE_SET = 2, RB_SELECTED = 3, RB_ACTIVATED = 4, RB_TOGGLED = 5 };

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

// A window's content: flipped, and taking file drops once the window's surface asks for them.
@interface RBDropView : RBFlipped
{
@public
    uint32_t sid;
    BOOL drops;
}
@end

@implementation RBDropView
- (NSDragOperation)draggingEntered:(id)info
{
    NSArray *files = [[info draggingPasteboard] propertyListForType:NSFilenamesPboardType];
    return (drops && [files count] > 0) ? NSDragOperationCopy : NSDragOperationNone;
}

- (BOOL)performDragOperation:(id)info
{
    NSArray *files = [[info draggingPasteboard] propertyListForType:NSFilenamesPboardType];
    if (!drops || [files count] == 0)
        return NO;
    NSPoint at = [info draggingLocation];
    NSString *joined = [files componentsJoinedByString:@"\n"];
    rb_dropped(sid, [joined UTF8String], at.x, at.y);
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

// A text area's edits: an NSTextView has no tag, so the area's control is looked up.
- (void)textDidChange:(NSNotification *)note
{
    extern int32_t rb_text_area_ctl(uint32_t s, id view);
    int32_t c = rb_text_area_ctl(sid, [note object]);
    if (c >= 0)
        rb_event(sid, (uint32_t)c, RB_EDITED);
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

// A window's close request and resizes, reported to Rust, which decides.
@interface RBWindowDelegate : NSObject
{
    uint32_t sid;
}
- (id)initWithSurface:(uint32_t)s;
@end

@implementation RBWindowDelegate
- (id)initWithSurface:(uint32_t)s
{
    if ((self = [super init]) != nil)
        sid = s;
    return self;
}

- (BOOL)windowShouldClose:(id)sender
{
    rb_window_event(sid, 0);
    return NO;
}

- (void)windowDidResize:(NSNotification *)note
{
    rb_window_event(sid, 1);
}
@end

// A list's table: Return activates the selected row, as a double-click does.
@interface RBTableView : NSTableView
@end

@implementation RBTableView
- (void)keyDown:(NSEvent *)event
{
    NSString *k = [event charactersIgnoringModifiers];
    if ([k length] == 1 && ([k characterAtIndex:0] == '\r' || [k characterAtIndex:0] == 3)) {
        [[self target] performSelector:[self doubleAction] withObject:self];
        return;
    }
    [super keyDown:event];
}
@end

// A list's rows, ticks and selection reports; rows are the controller's, kept for drawing.
@interface RBList : NSObject
{
@public
    uint32_t sid;
    uint32_t ctl;
    NSTableView *table;
    NSScrollView *scroll;
    NSArray *rows;
    NSMutableArray *checks;
    NSArray *right;
    BOOL quiet;
    BOOL has_toggle;
    int32_t toggle_row;
    BOOL toggle_on;
}
@end

@implementation RBList
- (void)dealloc
{
    [table setDataSource:nil];
    [table setDelegate:nil];
    [table release];
    [scroll release];
    [rows release];
    [checks release];
    [right release];
    [super dealloc];
}

- (NSInteger)numberOfRowsInTableView:(NSTableView *)view
{
    return (NSInteger)[rows count];
}

- (id)tableView:(NSTableView *)view objectValueForTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    NSString *ident = [col identifier];
    if ([ident isEqualToString:@"check"])
        return [NSNumber numberWithBool:(row < (NSInteger)[checks count] && [[checks objectAtIndex:row] boolValue])];
    NSArray *cells = [rows objectAtIndex:row];
    int i = [ident intValue];
    return i < (int)[cells count] ? [cells objectAtIndex:i] : @"";
}

- (void)tableView:(NSTableView *)view setObjectValue:(id)value forTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    if (![[col identifier] isEqualToString:@"check"])
        return;
    BOOL on = [value boolValue];
    while ((NSInteger)[checks count] <= row)
        [checks addObject:[NSNumber numberWithBool:NO]];
    [checks replaceObjectAtIndex:row withObject:[NSNumber numberWithBool:on]];
    has_toggle = YES;
    toggle_row = (int32_t)row;
    toggle_on = on;
    rb_event(sid, ctl, RB_TOGGLED);
}

- (void)tableView:(NSTableView *)view willDisplayCell:(id)cell forTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    NSString *ident = [col identifier];
    if ([ident isEqualToString:@"check"] || ![cell respondsToSelector:@selector(setAlignment:)])
        return;
    int i = [ident intValue];
    BOOL r = i < (int)[right count] && [[right objectAtIndex:i] boolValue];
    [cell setAlignment:(r ? NSRightTextAlignment : NSLeftTextAlignment)];
}

- (void)tableViewSelectionDidChange:(NSNotification *)note
{
    if (!quiet)
        rb_event(sid, ctl, RB_SELECTED);
}

- (void)activate:(id)sender
{
    rb_event(sid, ctl, RB_ACTIVATED);
}
@end

// A view Rust paints: drawRect: runs the surface's painter, a click is remembered and reported.
@interface RBCanvas : NSView
{
@public
    uint32_t sid;
    uint32_t ctl;
    BOOL has_click;
    double click[4];
}
@end

// Whether `v` draws in a dark appearance; only 10.14 and later have one.
static BOOL rb_dark(NSView *v)
{
    if (![v respondsToSelector:@selector(effectiveAppearance)])
        return NO;
    id look = [v performSelector:@selector(effectiveAppearance)];
    NSString *name = [look respondsToSelector:@selector(name)] ? [look performSelector:@selector(name)] : nil;
    return name != nil && [name rangeOfString:@"Dark"].location != NSNotFound;
}

static BOOL rb_painting_dark = NO;

@implementation RBCanvas
- (BOOL)isFlipped
{
    return YES;
}

- (void)drawRect:(NSRect)dirty
{
    NSRect b = [self bounds];
    rb_painting_dark = rb_dark(self);
    rb_canvas_draw(sid, ctl, b.size.width, b.size.height, rb_painting_dark ? 1 : 0);
}

- (void)mouseDown:(NSEvent *)event
{
    NSPoint at = [self convertPoint:[event locationInWindow] fromView:nil];
    NSRect b = [self bounds];
    click[0] = at.x;
    click[1] = at.y;
    click[2] = b.size.width;
    click[3] = b.size.height;
    has_click = YES;
    rb_event(sid, ctl, RB_ACT);
}
@end

@interface RBSurface : NSObject
{
@public
    NSWindow *window;
    RBWindowDelegate *delegate;
    NSView *body;
    NSMutableDictionary *views;
    NSMutableDictionary *lists;
    NSMutableDictionary *texts;
    NSMutableDictionary *rowsv;
    RBTarget *target;
}
@end

@implementation RBSurface
- (void)dealloc
{
    [window setDelegate:nil];
    [window release];
    [delegate release];
    [body release];
    [views release];
    [lists release];
    [texts release];
    [rowsv release];
    [target release];
    [super dealloc];
}
@end

// Runs `rb_later` on the main thread, from any thread.
@interface RBPoster : NSObject
@end

@implementation RBPoster
- (void)run
{
    rb_later();
}

- (void)tick
{
    rb_tick();
}

- (void)timed
{
    rb_timed();
}
@end

static RBPoster *rb_poster = nil;
static RBPoster *poster(void);

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
    poster();
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
    RBDropView *content = [[RBDropView alloc] initWithFrame:NSMakeRect(0, 0, w, h)];
    content->sid = s;
    x->body = content;
    [x->body setAutoresizingMask:(NSViewWidthSizable | NSViewHeightSizable)];
    [x->window setContentView:x->body];
    x->views = [[NSMutableDictionary alloc] init];
    x->lists = [[NSMutableDictionary alloc] init];
    x->texts = [[NSMutableDictionary alloc] init];
    x->rowsv = [[NSMutableDictionary alloc] init];
    x->target = [[RBTarget alloc] initWithSurface:s];
    x->delegate = [[RBWindowDelegate alloc] initWithSurface:s];
    [x->window setDelegate:(id)x->delegate];
    [x->window center];
    [rb_surfaces addObject:x];
    return s;
}

void rb_host_window_show(uint32_t s)
{
    NSWindow *w = surf(s)->window;
    [w makeKeyAndOrderFront:nil];
    [w makeFirstResponder:nil];
    [NSApp activateIgnoringOtherApps:YES];
    // Test rigs over ssh cannot list windows on 10.7; this is the id `screencapture -l` takes.
    NSLog(@"rusty-backup: window %ld", (long)[w windowNumber]);
}

void rb_host_window_hide(uint32_t s)
{
    [surf(s)->window orderOut:nil];
}

void rb_host_run_modal(uint32_t s)
{
    [NSApp runModalForWindow:surf(s)->window];
}

void rb_host_stop_modal(void)
{
    [NSApp stopModal];
}

// Made at launch on the main thread, so a worker's wake never races to make it.
static RBPoster *poster(void)
{
    if (rb_poster == nil)
        rb_poster = [[RBPoster alloc] init];
    return rb_poster;
}

void rb_host_post(void)
{
    [poster() performSelectorOnMainThread:@selector(run) withObject:nil waitUntilDone:NO];
}

void rb_host_post_after(uint64_t ms)
{
    [poster() performSelector:@selector(timed) withObject:nil afterDelay:ms / 1000.0];
}

// Safe from any thread: a job's wake arrives on its worker.
void rb_host_wake(void)
{
    [rb_poster performSelectorOnMainThread:@selector(tick) withObject:nil waitUntilDone:NO];
}

void rb_host_area(uint32_t s, int32_t *out)
{
    NSRect b = [surf(s)->body bounds];
    out[0] = 0;
    out[1] = 0;
    out[2] = (int32_t)b.size.width;
    out[3] = (int32_t)b.size.height;
}

// A window's surface takes its window down with it.
void rb_host_release(uint32_t s)
{
    RBSurface *x = surf(s);
    if (x == nil)
        return;
    if (x->window != nil) {
        [x->window setDelegate:nil];
        [x->window orderOut:nil];
    }
    [rb_surfaces replaceObjectAtIndex:s withObject:[NSNull null]];
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
    // A push button's bezel (and its shadow) is taller than a layout row: centre it there, unclipped.
    if ([v isKindOfClass:[NSButton class]] && ![v isKindOfClass:[NSPopUpButton class]]
        && [(NSButton *)v bezelStyle] == NSRoundedBezelStyle) {
        double natural = [[(NSButton *)v cell] cellSize].height;
        if (natural > r.size.height) {
            r.origin.y -= floor((natural - r.size.height) / 2);
            r.size.height = natural;
        }
    }
    if (!NSEqualRects([v frame], r))
        [v setFrame:r];
}

void rb_host_set_progress(uint32_t s, uint32_t c, int running, int32_t pct)
{
    NSProgressIndicator *p = view(s, c);
    if ([p isHidden] == (running ? YES : NO))
        [p setHidden:(running ? NO : YES)];
    BOOL sweep = running && pct < 0;
    if ([p isIndeterminate] != sweep)
        [p setIndeterminate:sweep];
    if (sweep)
        [p startAnimation:nil];
    else
        [p setDoubleValue:(pct < 0 ? 0.0 : (double)pct)];
}

static RBList *list_of(uint32_t s, uint32_t c)
{
    return [surf(s)->lists objectForKey:key(c)];
}

void rb_host_table(uint32_t s, uint32_t c, const char *ax_id, const char *const *titles,
                   const double *widths, const uint8_t *right, uint32_t n, int checks)
{
    RBSurface *x = surf(s);
    RBList *l = [[[RBList alloc] init] autorelease];
    l->sid = s;
    l->ctl = c;
    l->rows = [[NSArray alloc] init];
    l->checks = [[NSMutableArray alloc] init];
    NSMutableArray *r = [NSMutableArray array];
    l->table = [[RBTableView alloc] initWithFrame:NSZeroRect];
    if (checks) {
        NSTableColumn *col = [[[NSTableColumn alloc] initWithIdentifier:@"check"] autorelease];
        NSButtonCell *cell = [[[NSButtonCell alloc] init] autorelease];
        [cell setButtonType:NSSwitchButton];
        [cell setTitle:@""];
        [col setDataCell:cell];
        [col setWidth:24];
        [[col headerCell] setStringValue:@""];
        [l->table addTableColumn:col];
    }
    uint32_t i;
    for (i = 0; i < n; i++) {
        NSTableColumn *col = [[[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%u", i]] autorelease];
        [[col headerCell] setStringValue:str(titles[i])];
        [col setWidth:widths[i]];
        [col setEditable:NO];
        [l->table addTableColumn:col];
        [r addObject:[NSNumber numberWithBool:(right[i] ? YES : NO)]];
    }
    l->right = [r retain];
    BOOL titled = NO;
    for (i = 0; i < n; i++)
        titled = titled || (titles[i] && *titles[i]);
    if (!titled)
        [l->table setHeaderView:nil];
    [l->table setUsesAlternatingRowBackgroundColors:YES];
    [l->table setAllowsMultipleSelection:NO];
    [l->table setAllowsEmptySelection:YES];
    [l->table setDataSource:(id)l];
    [l->table setDelegate:(id)l];
    [l->table setTarget:l];
    [l->table setDoubleAction:@selector(activate:)];
    set_id(l->table, ax_id);
    l->scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    [l->scroll setHasVerticalScroller:YES];
    [l->scroll setHasHorizontalScroller:YES];
    [l->scroll setAutohidesScrollers:YES];
    [l->scroll setBorderType:NSBezelBorder];
    [l->scroll setDocumentView:l->table];
    [x->lists setObject:l forKey:key(c)];
    adopt(s, c, l->scroll, NO);
}

static void quietly_select(RBList *l, NSInteger i)
{
    l->quiet = YES;
    [l->table selectRowIndexes:[NSIndexSet indexSetWithIndex:i] byExtendingSelection:NO];
    [l->table scrollRowToVisible:i];
    l->quiet = NO;
}

// Only a change reloads, so the selection and scroll survive a refresh.
void rb_host_set_rows(uint32_t s, uint32_t c, const char *const *cells, uint32_t nrows, uint32_t ncols)
{
    RBList *l = list_of(s, c);
    NSMutableArray *rows = [NSMutableArray arrayWithCapacity:nrows];
    uint32_t r, k;
    for (r = 0; r < nrows; r++) {
        NSMutableArray *row = [NSMutableArray arrayWithCapacity:ncols];
        for (k = 0; k < ncols; k++)
            [row addObject:str(cells[r * ncols + k])];
        [rows addObject:row];
    }
    if ([rows isEqualToArray:l->rows])
        return;
    NSInteger sel = [l->table selectedRow];
    [l->rows release];
    l->rows = [rows retain];
    l->quiet = YES;
    [l->table reloadData];
    l->quiet = NO;
    if (sel >= 0 && sel < (NSInteger)nrows)
        quietly_select(l, sel);
}

void rb_host_set_checks(uint32_t s, uint32_t c, const uint8_t *checks, uint32_t n)
{
    RBList *l = list_of(s, c);
    NSMutableArray *v = [NSMutableArray arrayWithCapacity:n];
    uint32_t i;
    for (i = 0; i < n; i++)
        [v addObject:[NSNumber numberWithBool:(checks[i] ? YES : NO)]];
    if ([v isEqualToArray:l->checks])
        return;
    [l->checks setArray:v];
    l->quiet = YES;
    [l->table reloadData];
    l->quiet = NO;
}

int32_t rb_host_selected_row(uint32_t s, uint32_t c)
{
    return (int32_t)[list_of(s, c)->table selectedRow];
}

void rb_host_select_row(uint32_t s, uint32_t c, uint32_t i)
{
    RBList *l = list_of(s, c);
    if ([l->table selectedRow] != (NSInteger)i)
        quietly_select(l, i);
}

int rb_host_toggled(uint32_t s, uint32_t c, int32_t *row, int *on)
{
    RBList *l = list_of(s, c);
    if (!l->has_toggle)
        return 0;
    *row = l->toggle_row;
    *on = l->toggle_on ? 1 : 0;
    return 1;
}

void rb_host_key_equivalent(uint32_t s, uint32_t c, const char *k)
{
    [(NSButton *)view(s, c) setKeyEquivalent:str(k)];
}

void rb_host_open_url(const char *url)
{
    NSURL *u = [NSURL URLWithString:str(url)];
    if (u != nil)
        [[NSWorkspace sharedWorkspace] openURL:u];
}

void rb_host_copy_text(const char *text)
{
    NSPasteboard *pb = [NSPasteboard generalPasteboard];
    [pb declareTypes:[NSArray arrayWithObject:NSStringPboardType] owner:nil];
    [pb setString:str(text) forType:NSStringPboardType];
}

void rb_host_quit(void)
{
    [NSApp terminate:nil];
}

// The open or save panel through the 10.4 API, which later systems still run.
char *rb_host_pick(const char *title, int save, int folder, const char *file_name,
                   const char *const *exts, uint32_t n, const char *start)
{
    NSString *dir = (start && *start) ? str(start) : nil;
    if (save) {
        NSSavePanel *panel = [NSSavePanel savePanel];
        [panel setMessage:str(title)];
        [panel setCanCreateDirectories:YES];
        if ([panel runModalForDirectory:dir file:str(file_name)] != NSOKButton)
            return NULL;
        return dup_str([panel filename]);
    }
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    [panel setMessage:str(title)];
    [panel setCanChooseFiles:(folder ? NO : YES)];
    [panel setCanChooseDirectories:(folder ? YES : NO)];
    [panel setCanCreateDirectories:(folder ? YES : NO)];
    [panel setAllowsMultipleSelection:NO];
    NSMutableArray *types = nil;
    if (!folder && n > 0) {
        types = [NSMutableArray arrayWithCapacity:n];
        uint32_t i;
        for (i = 0; i < n; i++)
            [types addObject:str(exts[i])];
    }
    if ([panel runModalForDirectory:dir file:nil types:types] != NSOKButton)
        return NULL;
    return dup_str([panel filename]);
}

// The index of the button pressed; `escape` (or -1) is the button Esc presses.
int32_t rb_host_alert(const char *title, const char *message, const char *const *buttons,
                      uint32_t n, int critical, int32_t escape)
{
    NSAlert *a = [[[NSAlert alloc] init] autorelease];
    [a setMessageText:str(title)];
    [a setInformativeText:str(message)];
    uint32_t i;
    for (i = 0; i < n; i++)
        [a addButtonWithTitle:str(buttons[i])];
    if (critical)
        [a setAlertStyle:NSCriticalAlertStyle];
    if (escape > 0 && escape < (int32_t)n)
        [[[a buttons] objectAtIndex:escape] setKeyEquivalent:@"\033"];
    NSInteger r = [a runModal] - NSAlertFirstButtonReturn;
    return (r >= 0 && r < (NSInteger)n) ? (int32_t)r : -1;
}

void rb_host_canvas(uint32_t s, uint32_t c, const char *ax_id)
{
    RBCanvas *v = [[[RBCanvas alloc] initWithFrame:NSZeroRect] autorelease];
    v->sid = s;
    v->ctl = c;
    set_id(v, ax_id);
    adopt(s, c, v, NO);
}

void rb_host_redraw(uint32_t s, uint32_t c)
{
    [view(s, c) setNeedsDisplay:YES];
}

int rb_host_clicked(uint32_t s, uint32_t c, double *out)
{
    RBCanvas *v = view(s, c);
    if (![v isKindOfClass:[RBCanvas class]] || !v->has_click)
        return 0;
    int i;
    for (i = 0; i < 4; i++)
        out[i] = v->click[i];
    return 1;
}

static NSColor *rb_ink(int ink, uint32_t rgb)
{
    switch (ink) {
    case 1: return [NSColor controlTextColor];
    case 2: return [NSColor colorWithCalibratedWhite:0.5 alpha:0.6];
    case 3: return rb_painting_dark ? [NSColor whiteColor] : [NSColor blackColor];
    default:
        return [NSColor colorWithCalibratedRed:((rgb >> 16) & 0xff) / 255.0
                                         green:((rgb >> 8) & 0xff) / 255.0
                                          blue:(rgb & 0xff) / 255.0
                                         alpha:1.0];
    }
}

void rb_pen_fill(double x, double y, double w, double h, int ink, uint32_t rgb)
{
    [rb_ink(ink, rgb) set];
    [NSBezierPath fillRect:NSMakeRect(x, y, w, h)];
}

void rb_pen_line(double x0, double y0, double x1, double y1, int ink, uint32_t rgb)
{
    [rb_ink(ink, rgb) set];
    [NSBezierPath strokeLineFromPoint:NSMakePoint(x0, y0) toPoint:NSMakePoint(x1, y1)];
}

void rb_pen_frame(double x, double y, double w, double h, double width, int ink, uint32_t rgb)
{
    [rb_ink(ink, rgb) set];
    NSBezierPath *p = [NSBezierPath bezierPathWithRect:NSMakeRect(x, y, w, h)];
    [p setLineWidth:width];
    [p stroke];
}

// The small system font in `color`, aligned, cut with an ellipsis where it does not fit.
static NSDictionary *rb_text_attrs(NSColor *color, int center)
{
    NSMutableParagraphStyle *style = [[[NSMutableParagraphStyle alloc] init] autorelease];
    [style setAlignment:(center ? NSCenterTextAlignment : NSLeftTextAlignment)];
    [style setLineBreakMode:NSLineBreakByTruncatingTail];
    return [NSDictionary dictionaryWithObjectsAndKeys:
        [NSFont systemFontOfSize:11.0], NSFontAttributeName,
        color, NSForegroundColorAttributeName,
        style, NSParagraphStyleAttributeName, nil];
}

void rb_pen_text(double x, double y, double w, double h, const char *text, int ink, uint32_t rgb, int center)
{
    [str(text) drawInRect:NSMakeRect(x, y, w, h) withAttributes:rb_text_attrs(rb_ink(ink, rgb), center)];
}

double rb_pen_text_width(const char *text)
{
    return [str(text) sizeWithAttributes:rb_text_attrs([NSColor blackColor], 0)].width;
}

static NSFont *rb_log_font(void)
{
    NSFont *f = [NSFont userFixedPitchFontOfSize:11.0];
    return f != nil ? f : [NSFont systemFontOfSize:11.0];
}

void rb_host_text_view(uint32_t s, uint32_t c, const char *ax_id)
{
    NSRect frame = NSMakeRect(0, 0, 400, 120);
    NSScrollView *scroll = [[[NSScrollView alloc] initWithFrame:frame] autorelease];
    [scroll setHasVerticalScroller:YES];
    [scroll setBorderType:NSBezelBorder];
    NSTextView *t = [[[NSTextView alloc] initWithFrame:frame] autorelease];
    [t setEditable:NO];
    [t setSelectable:YES];
    [t setVerticallyResizable:YES];
    [t setHorizontallyResizable:NO];
    [t setAutoresizingMask:NSViewWidthSizable];
    [[t textContainer] setWidthTracksTextView:YES];
    [t setFont:rb_log_font()];
    set_id(t, ax_id);
    [scroll setDocumentView:t];
    [surf(s)->texts setObject:t forKey:key(c)];
    adopt(s, c, scroll, NO);
}

void rb_host_set_text_view(uint32_t s, uint32_t c, const char *text)
{
    NSTextView *t = [surf(s)->texts objectForKey:key(c)];
    NSString *v = str(text);
    if (![[t string] isEqualToString:v]) {
        [t setString:v];
        [t setFont:rb_log_font()];
    }
}

void rb_host_append_text_view(uint32_t s, uint32_t c, const char *text, int follow)
{
    NSTextView *t = [surf(s)->texts objectForKey:key(c)];
    NSDictionary *attrs = [NSDictionary dictionaryWithObjectsAndKeys:
        rb_log_font(), NSFontAttributeName,
        [NSColor textColor], NSForegroundColorAttributeName, nil];
    NSAttributedString *a = [[[NSAttributedString alloc] initWithString:str(text) attributes:attrs] autorelease];
    [[t textStorage] appendAttributedString:a];
    if (follow)
        [t scrollRangeToVisible:NSMakeRange([[t string] length], 0)];
}

void rb_host_tooltip(uint32_t s, uint32_t c, const char *text)
{
    [(NSView *)view(s, c) setToolTip:str(text)];
}

int rb_host_page_viewport(uint32_t s, int32_t *out)
{
    NSScrollView *scroll = [surf(s)->body enclosingScrollView];
    if (scroll == nil)
        return 0;
    NSSize v = [scroll contentSize];
    out[0] = (int32_t)v.width;
    out[1] = (int32_t)v.height;
    return 1;
}

void rb_host_set_surface_size(uint32_t s, int32_t w, int32_t h)
{
    NSView *body = surf(s)->body;
    NSSize size = NSMakeSize(w, h);
    if (!NSEqualSizes([body frame].size, size))
        [body setFrameSize:size];
}

// Hidden itself, or inside a hidden view (an off-screen tab's page).
int rb_host_on_screen(uint32_t s)
{
    NSView *v = surf(s)->body;
    for (; v != nil; v = [v superview])
        if ([v isHidden])
            return 0;
    return 1;
}

// ---- Pane lists: a cell-based table (10.4 has no other) with an indented, iconned name cell.

enum { RB_ROWS_SELECT = 0, RB_ROWS_TICK, RB_ROWS_ICON, RB_ROWS_ACTIVATE, RB_ROWS_KEY, RB_ROWS_HEADER, RB_ROWS_MENU, RB_ROWS_FOCUSED };

// The Tabler row icons bundled in Resources, by the code rows.rs sends.
static NSImage *rb_row_icon(int code)
{
    static NSMutableDictionary *cache = nil;
    static const char *names[] = { NULL, "file", "file-plus", "file", "file-minus", "folder",
                                   "folder-plus", "folder", "folder-minus", "folder", "folder-open" };
    if (code <= 0 || code > 10)
        return nil;
    if (cache == nil)
        cache = [[NSMutableDictionary alloc] init];
    NSNumber *k = [NSNumber numberWithInt:code];
    NSImage *img = [cache objectForKey:k];
    if (img == nil) {
        NSString *name = [NSString stringWithFormat:@"%s-32", names[code]];
        NSString *path = [[NSBundle mainBundle] pathForResource:name ofType:@"png"];
        img = path ? [[[NSImage alloc] initWithContentsOfFile:path] autorelease] : nil;
        if (img == nil)
            return nil;
        [img setSize:NSMakeSize(16, 16)];
        if ([img respondsToSelector:@selector(setTemplate:)])
            rb_send_bool(img, @selector(setTemplate:), YES);
        [cache setObject:img forKey:k];
    }
    return img;
}

static uint32_t rb_mods(NSUInteger f)
{
    return ((f & NSCommandKeyMask) ? 1 : 0) | ((f & NSShiftKeyMask) ? 2 : 0)
         | ((f & NSControlKeyMask) ? 4 : 0) | ((f & NSAlternateKeyMask) ? 8 : 0);
}

// The name cell: indented by depth, the row icon, then the name.
@interface RBNameCell : NSTextFieldCell
{
@public
    int depth;
    int icon;
}
@end

@implementation RBNameCell
- (void)drawWithFrame:(NSRect)frame inView:(NSView *)view
{
    NSRect r = frame;
    r.origin.x += 2 + 16 * depth;
    r.size.width -= 2 + 16 * depth;
    NSImage *img = rb_row_icon(icon);
    if (img != nil) {
        NSRect at = NSMakeRect(r.origin.x, r.origin.y + (r.size.height - 16) / 2, 16, 16);
        SEL modern = @selector(drawInRect:fromRect:operation:fraction:respectFlipped:hints:);
        if ([img respondsToSelector:modern]) {
            NSMethodSignature *sig = [img methodSignatureForSelector:modern];
            NSInvocation *inv = [NSInvocation invocationWithMethodSignature:sig];
            NSRect zero = NSZeroRect;
            NSCompositingOperation op = NSCompositeSourceOver;
            CGFloat fraction = 1.0;
            BOOL flip = YES;
            id hints = nil;
            [inv setSelector:modern];
            [inv setArgument:&at atIndex:2];
            [inv setArgument:&zero atIndex:3];
            [inv setArgument:&op atIndex:4];
            [inv setArgument:&fraction atIndex:5];
            [inv setArgument:&flip atIndex:6];
            [inv setArgument:&hints atIndex:7];
            [inv invokeWithTarget:img];
        } else {
            [img setFlipped:[view isFlipped]];
            [img drawInRect:at fromRect:NSZeroRect operation:NSCompositeSourceOver fraction:1.0];
        }
        r.origin.x += 20;
        r.size.width -= 20;
    }
    [super drawWithFrame:r inView:view];
}
@end

@class RBRows;

// A table that hands its owner the keys it does not use, right-clicks and icon clicks.
@interface RBRowsTable : NSTableView
{
@public
    RBRows *owner;
}
@end

@interface RBRows : NSObject
{
@public
    uint32_t sid;
    uint32_t ctl;
    RBRowsTable *table;
    NSScrollView *scroll;
    NSTableColumn *tick_col;
    NSTableColumn *name_col;
    NSArray *right;
    NSMutableArray *cells;
    NSMutableArray *depths;
    NSMutableArray *icons;
    NSMutableArray *ticks;
    BOOL quiet;
}
@end

@implementation RBRowsTable
- (void)keyDown:(NSEvent *)e
{
    unsigned short code = [e keyCode];
    // Arrows, Page Up / Down, Home and End move the table's own selection.
    if (code == 125 || code == 126 || code == 116 || code == 121 || code == 115 || code == 119) {
        [super keyDown:e];
        return;
    }
    NSString *ch = [e charactersIgnoringModifiers];
    unichar u = [ch length] > 0 ? [ch characterAtIndex:0] : 0;
    rb_rows_event(owner->sid, owner->ctl, RB_ROWS_KEY, code, u, rb_mods([e modifierFlags]), 0, 0);
}

- (BOOL)becomeFirstResponder
{
    BOOL took = [super becomeFirstResponder];
    if (took)
        rb_rows_event(owner->sid, owner->ctl, RB_ROWS_FOCUSED, 0, 0, 0, 0, 0);
    return took;
}

- (void)rightMouseDown:(NSEvent *)e
{
    NSPoint at = [self convertPoint:[e locationInWindow] fromView:nil];
    NSInteger row = [self rowAtPoint:at];
    if (row >= 0)
        rb_rows_event(owner->sid, owner->ctl, RB_ROWS_MENU, (uint32_t)row, 0, 0, at.x, at.y);
}

// A click on a row's icon opens or shuts it, as the icon button does on AppKit's list.
- (void)mouseDown:(NSEvent *)e
{
    NSPoint at = [self convertPoint:[e locationInWindow] fromView:nil];
    NSInteger row = [self rowAtPoint:at];
    NSInteger col = [self columnAtPoint:at];
    if (row >= 0 && col >= 0 && [[self tableColumns] objectAtIndex:col] == owner->name_col
        && row < (NSInteger)[owner->icons count] && [[owner->icons objectAtIndex:row] intValue] != 0) {
        NSRect cell = [self frameOfCellAtColumn:col row:row];
        double x0 = cell.origin.x + 2 + 16 * [[owner->depths objectAtIndex:row] intValue];
        if (at.x >= x0 && at.x < x0 + 18) {
            rb_rows_event(owner->sid, owner->ctl, RB_ROWS_ICON, (uint32_t)row, 0, 0, 0, 0);
            return;
        }
    }
    [super mouseDown:e];
}
@end

@implementation RBRows
- (void)dealloc
{
    [table setDataSource:nil];
    [table setDelegate:nil];
    [table release];
    [scroll release];
    [tick_col release];
    [name_col release];
    [right release];
    [cells release];
    [depths release];
    [icons release];
    [ticks release];
    [super dealloc];
}

- (NSInteger)numberOfRowsInTableView:(NSTableView *)view
{
    return (NSInteger)[cells count];
}

- (id)tableView:(NSTableView *)view objectValueForTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    if (col == tick_col)
        return [ticks objectAtIndex:row];
    NSArray *line = [cells objectAtIndex:row];
    int i = [[col identifier] intValue];
    return i < (int)[line count] ? [line objectAtIndex:i] : @"";
}

- (void)tableView:(NSTableView *)view setObjectValue:(id)value forTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    if (col == tick_col)
        rb_rows_event(sid, ctl, RB_ROWS_TICK, (uint32_t)row, 0, 0, 0, 0);
}

- (void)tableView:(NSTableView *)view willDisplayCell:(id)cell forTableColumn:(NSTableColumn *)col row:(NSInteger)row
{
    if (col == tick_col)
        return;
    if (col == name_col && [cell isKindOfClass:[RBNameCell class]]) {
        ((RBNameCell *)cell)->depth = [[depths objectAtIndex:row] intValue];
        ((RBNameCell *)cell)->icon = [[icons objectAtIndex:row] intValue];
    }
    int i = [[col identifier] intValue];
    BOOL r = i < (int)[right count] && [[right objectAtIndex:i] boolValue];
    [cell setAlignment:(r ? NSRightTextAlignment : NSLeftTextAlignment)];
}

- (void)tableViewSelectionDidChange:(NSNotification *)note
{
    if (quiet)
        return;
    NSInteger row = [table clickedRow];
    if (row < 0)
        row = [table selectedRow];
    if (row < 0)
        return;
    NSEvent *e = [NSApp currentEvent];
    rb_rows_event(sid, ctl, RB_ROWS_SELECT, (uint32_t)row, 0, e ? rb_mods([e modifierFlags]) : 0, 0, 0);
}

- (void)tableView:(NSTableView *)view didClickTableColumn:(NSTableColumn *)col
{
    if (col != tick_col)
        rb_rows_event(sid, ctl, RB_ROWS_HEADER, (uint32_t)[[col identifier] intValue], 0, 0, 0, 0);
}

- (void)activate:(id)sender
{
    rb_rows_event(sid, ctl, RB_ROWS_ACTIVATE, 0, 0, 0, 0, 0);
}
@end

static RBRows *rows_of(uint32_t s, uint32_t c)
{
    return [surf(s)->rowsv objectForKey:key(c)];
}

void rb_host_rows(uint32_t s, uint32_t c, const char *ax_id, const char *const *titles,
                  const double *widths, const uint8_t *right, uint32_t n, int header, int multiple, int stripes)
{
    RBRows *r = [[[RBRows alloc] init] autorelease];
    r->sid = s;
    r->ctl = c;
    r->cells = [[NSMutableArray alloc] init];
    r->depths = [[NSMutableArray alloc] init];
    r->icons = [[NSMutableArray alloc] init];
    r->ticks = [[NSMutableArray alloc] init];
    r->table = [[RBRowsTable alloc] initWithFrame:NSZeroRect];
    r->table->owner = r;
    NSMutableArray *ra = [NSMutableArray array];
    uint32_t i;
    for (i = 0; i < n; i++) {
        NSTableColumn *col = [[[NSTableColumn alloc] initWithIdentifier:[NSString stringWithFormat:@"%u", i]] autorelease];
        [[col headerCell] setStringValue:str(titles[i])];
        [col setWidth:widths[i]];
        [col setEditable:NO];
        if (i == 0) {
            RBNameCell *cell = [[[RBNameCell alloc] initTextCell:@""] autorelease];
            [cell setLineBreakMode:NSLineBreakByTruncatingTail];
            [col setDataCell:cell];
            r->name_col = [col retain];
        }
        [r->table addTableColumn:col];
        [ra addObject:[NSNumber numberWithBool:(right[i] ? YES : NO)]];
    }
    r->right = [ra retain];
    NSTableColumn *tc = [[NSTableColumn alloc] initWithIdentifier:@"check"];
    NSButtonCell *bc = [[[NSButtonCell alloc] init] autorelease];
    [bc setButtonType:NSSwitchButton];
    [bc setTitle:@""];
    [tc setDataCell:bc];
    [tc setWidth:22];
    [[tc headerCell] setStringValue:@""];
    r->tick_col = tc;
    if (!header)
        [r->table setHeaderView:nil];
    [r->table setUsesAlternatingRowBackgroundColors:(stripes ? YES : NO)];
    [r->table setAllowsMultipleSelection:(multiple ? YES : NO)];
    [r->table setAllowsEmptySelection:YES];
    [r->table setDataSource:(id)r];
    [r->table setDelegate:(id)r];
    [r->table setTarget:r];
    [r->table setDoubleAction:@selector(activate:)];
    set_id(r->table, ax_id);
    r->scroll = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    [r->scroll setHasVerticalScroller:YES];
    [r->scroll setHasHorizontalScroller:YES];
    [r->scroll setAutohidesScrollers:YES];
    [r->scroll setBorderType:NSBezelBorder];
    [r->scroll setDocumentView:r->table];
    [surf(s)->rowsv setObject:r forKey:key(c)];
    adopt(s, c, r->scroll, NO);
}

void rb_host_set_row_lines(uint32_t s, uint32_t c, const char *const *cells, uint32_t nrows, uint32_t ncols,
                           const uint32_t *depth, const uint8_t *icon, const int8_t *tick)
{
    RBRows *r = rows_of(s, c);
    NSMutableArray *rows = [NSMutableArray arrayWithCapacity:nrows];
    NSMutableArray *ds = [NSMutableArray arrayWithCapacity:nrows];
    NSMutableArray *is = [NSMutableArray arrayWithCapacity:nrows];
    NSMutableArray *ts = [NSMutableArray arrayWithCapacity:nrows];
    BOOL any_tick = NO;
    uint32_t i, k;
    for (i = 0; i < nrows; i++) {
        NSMutableArray *line = [NSMutableArray arrayWithCapacity:ncols];
        for (k = 0; k < ncols; k++)
            [line addObject:str(cells[i * ncols + k])];
        [rows addObject:line];
        [ds addObject:[NSNumber numberWithInt:depth[i]]];
        [is addObject:[NSNumber numberWithInt:icon[i]]];
        [ts addObject:[NSNumber numberWithBool:(tick[i] > 0)]];
        any_tick = any_tick || tick[i] >= 0;
    }
    if ([rows isEqualToArray:r->cells] && [ds isEqualToArray:r->depths] && [is isEqualToArray:r->icons]
        && [ts isEqualToArray:r->ticks])
        return;
    [r->cells setArray:rows];
    [r->depths setArray:ds];
    [r->icons setArray:is];
    [r->ticks setArray:ts];
    BOOL has = [[r->table tableColumns] indexOfObject:r->tick_col] != NSNotFound;
    if (any_tick && !has) {
        [r->table addTableColumn:r->tick_col];
        [r->table moveColumn:[[r->table tableColumns] count] - 1 toColumn:0];
    } else if (!any_tick && has) {
        [r->table removeTableColumn:r->tick_col];
    }
    r->quiet = YES;
    [r->table reloadData];
    r->quiet = NO;
}

void rb_host_hide_column(uint32_t s, uint32_t c, uint32_t col, int hidden)
{
    RBRows *r = rows_of(s, c);
    NSTableColumn *tc = [r->table tableColumnWithIdentifier:[NSString stringWithFormat:@"%u", col]];
    if (tc == nil)
        return;
    if ([tc respondsToSelector:@selector(setHidden:)]) {
        BOOL h = hidden ? YES : NO;
        NSMethodSignature *sig = [tc methodSignatureForSelector:@selector(setHidden:)];
        NSInvocation *inv = [NSInvocation invocationWithMethodSignature:sig];
        [inv setSelector:@selector(setHidden:)];
        [inv setArgument:&h atIndex:2];
        [inv invokeWithTarget:tc];
    } else {
        [tc setWidth:(hidden ? 0 : 80)];
    }
}

void rb_host_select_rows(uint32_t s, uint32_t c, const uint32_t *rows, uint32_t n, int32_t visible)
{
    RBRows *r = rows_of(s, c);
    NSMutableIndexSet *set = [NSMutableIndexSet indexSet];
    uint32_t i;
    for (i = 0; i < n; i++)
        if (rows[i] < [r->cells count])
            [set addIndex:rows[i]];
    r->quiet = YES;
    if (![[r->table selectedRowIndexes] isEqualToIndexSet:set])
        [r->table selectRowIndexes:set byExtendingSelection:NO];
    if (visible >= 0 && visible < (int32_t)[r->cells count])
        [r->table scrollRowToVisible:visible];
    r->quiet = NO;
}

void rb_host_focus(uint32_t s, uint32_t c)
{
    RBRows *r = rows_of(s, c);
    NSView *v = r != nil ? (NSView *)r->table : (NSView *)view(s, c);
    [[v window] makeFirstResponder:v];
}

int32_t rb_host_page_rows(uint32_t s, uint32_t c)
{
    RBRows *r = rows_of(s, c);
    double visible = [r->scroll contentSize].height;
    double row = [r->table rowHeight] + [r->table intercellSpacing].height;
    int32_t n = (int32_t)(visible / (row > 1 ? row : 1));
    return n > 1 ? n : 1;
}

void rb_host_control_size(uint32_t s, uint32_t c, double *out)
{
    NSRect f = [(NSView *)view(s, c) frame];
    out[0] = f.size.width;
    out[1] = f.size.height;
}

// The menu's pick, kept by its items' target until the menu closes.
@interface RBMenuTarget : NSObject
{
@public
    NSInteger picked;
}
@end

@implementation RBMenuTarget
- (void)pick:(id)sender
{
    picked = [sender tag];
}
@end

int32_t rb_host_menu_at(uint32_t s, uint32_t c, const char *const *labels, const uint8_t *kinds, uint32_t n,
                        double x, double y)
{
    RBMenuTarget *t = [[[RBMenuTarget alloc] init] autorelease];
    t->picked = -1;
    NSMenu *menu = [[[NSMenu alloc] initWithTitle:@""] autorelease];
    [menu setAutoenablesItems:NO];
    NSMenu *into = menu;
    NSInteger next = 0;
    uint32_t i;
    for (i = 0; i < n; i++) {
        if (kinds[i] == 3) {
            NSMenu *sub = [[[NSMenu alloc] initWithTitle:str(labels[i])] autorelease];
            [sub setAutoenablesItems:NO];
            NSMenuItem *holder = [[[NSMenuItem alloc] initWithTitle:str(labels[i]) action:NULL keyEquivalent:@""] autorelease];
            [holder setSubmenu:sub];
            [menu addItem:holder];
            into = sub;
            continue;
        }
        if (kinds[i] == 5) {
            into = menu;
            continue;
        }
        NSMenuItem *it = [[[NSMenuItem alloc] initWithTitle:str(labels[i]) action:@selector(pick:) keyEquivalent:@""] autorelease];
        [it setTarget:t];
        [it setTag:next++];
        [it setEnabled:(kinds[i] != 1)];
        if (kinds[i] == 2)
            [it setState:NSOnState];
        [into addItem:it];
    }
    RBRows *r = rows_of(s, c);
    NSView *v = r != nil ? (NSView *)r->table : (NSView *)view(s, c);
    SEL modern = @selector(popUpMenuPositioningItem:atLocation:inView:);
    if ([menu respondsToSelector:modern]) {
        NSMethodSignature *sig = [menu methodSignatureForSelector:modern];
        NSInvocation *inv = [NSInvocation invocationWithMethodSignature:sig];
        id item = nil;
        NSPoint at = NSMakePoint(x, y);
        [inv setSelector:modern];
        [inv setArgument:&item atIndex:2];
        [inv setArgument:&at atIndex:3];
        [inv setArgument:&v atIndex:4];
        [inv invokeWithTarget:menu];
    } else {
        NSEvent *e = [NSApp currentEvent];
        if (e != nil)
            [NSMenu popUpContextMenu:menu withEvent:e forView:v];
    }
    return (int32_t)t->picked;
}

void rb_host_wrap_text_view(uint32_t s, uint32_t c, int wrap)
{
    NSTextView *t = [surf(s)->texts objectForKey:key(c)];
    NSTextContainer *tc = [t textContainer];
    NSSize wide = NSMakeSize(1.0e7, 1.0e7);
    [t setHorizontallyResizable:(wrap ? NO : YES)];
    [t setMaxSize:wide];
    [tc setWidthTracksTextView:(wrap ? YES : NO)];
    [tc setContainerSize:NSMakeSize(wrap ? [t frame].size.width : wide.width, wide.height)];
}

int rb_host_window_frame(uint32_t s, double *out)
{
    NSWindow *w = surf(s)->window;
    if (w == nil)
        return 0;
    NSRect f = [w frame];
    out[0] = f.origin.x;
    out[1] = f.origin.y;
    out[2] = f.size.width;
    out[3] = f.size.height;
    return 1;
}

void rb_host_set_window_frame(uint32_t s, double x, double y, double w, double h)
{
    [surf(s)->window setFrame:NSMakeRect(x, y, w, h) display:YES];
}

// Several files, joined by newlines; empty when cancelled.
char *rb_host_pick_files(const char *title)
{
    NSOpenPanel *panel = [NSOpenPanel openPanel];
    [panel setMessage:str(title)];
    [panel setCanChooseFiles:YES];
    [panel setCanChooseDirectories:NO];
    [panel setAllowsMultipleSelection:YES];
    if ([panel runModalForDirectory:nil file:nil types:nil] != NSOKButton)
        return dup_str(@"");
    return dup_str([[panel filenames] componentsJoinedByString:@"\n"]);
}

// The control a text view is, on surface `s`; -1 if it is none of them.
int32_t rb_text_area_ctl(uint32_t s, id v)
{
    NSDictionary *texts = surf(s)->texts;
    NSEnumerator *e = [texts keyEnumerator];
    NSNumber *k;
    while ((k = [e nextObject]) != nil)
        if ([texts objectForKey:k] == v)
            return [k intValue];
    return -1;
}

void rb_host_text_area(uint32_t s, uint32_t c, const char *ax_id, const char *caption)
{
    NSRect frame = NSMakeRect(0, 0, 400, 160);
    NSScrollView *scroll = [[[NSScrollView alloc] initWithFrame:frame] autorelease];
    [scroll setHasVerticalScroller:YES];
    [scroll setBorderType:NSBezelBorder];
    NSTextView *t = [[[NSTextView alloc] initWithFrame:frame] autorelease];
    [t setEditable:YES];
    [t setRichText:NO];
    [t setVerticallyResizable:YES];
    [t setAutoresizingMask:NSViewWidthSizable];
    [t setFont:rb_log_font()];
    set_id(t, ax_id);
    [t accessibilitySetOverrideValue:str(caption) forAttribute:NSAccessibilityDescriptionAttribute];
    [t setDelegate:(id)surf(s)->target];
    [scroll setDocumentView:t];
    [surf(s)->texts setObject:t forKey:key(c)];
    adopt(s, c, scroll, NO);
}

char *rb_host_text_view_text(uint32_t s, uint32_t c)
{
    NSTextView *t = [surf(s)->texts objectForKey:key(c)];
    return dup_str(t != nil ? [t string] : @"");
}

void rb_host_resize_window(uint32_t s, int32_t w, int32_t h)
{
    NSWindow *win = surf(s)->window;
    [win setContentSize:NSMakeSize(w, h)];
    [win center];
}

// A surface on `body`, with no window of its own: a page or a pane.
static uint32_t new_surface(NSView *body)
{
    uint32_t s = (uint32_t)[rb_surfaces count];
    RBSurface *x = [[[RBSurface alloc] init] autorelease];
    x->body = [body retain];
    x->views = [[NSMutableDictionary alloc] init];
    x->lists = [[NSMutableDictionary alloc] init];
    x->texts = [[NSMutableDictionary alloc] init];
    x->rowsv = [[NSMutableDictionary alloc] init];
    x->target = [[RBTarget alloc] initWithSurface:s];
    [rb_surfaces addObject:x];
    return s;
}

void rb_host_set_min_size(uint32_t s, int32_t w, int32_t h)
{
    [surf(s)->window setContentMinSize:NSMakeSize(w, h)];
}

uint32_t rb_host_page(uint32_t s, uint32_t c)
{
    NSScrollView *scroll = [[[NSScrollView alloc] initWithFrame:NSZeroRect] autorelease];
    [scroll setHasVerticalScroller:YES];
    [scroll setAutohidesScrollers:YES];
    [scroll setDrawsBackground:NO];
    RBFlipped *body = [[[RBFlipped alloc] initWithFrame:NSZeroRect] autorelease];
    [scroll setDocumentView:body];
    adopt(s, c, scroll, NO);
    return new_surface(body);
}

uint32_t rb_host_pane(uint32_t s, uint32_t c)
{
    RBFlipped *body = [[[RBFlipped alloc] initWithFrame:NSZeroRect] autorelease];
    adopt(s, c, body, NO);
    return new_surface(body);
}

void rb_host_accept_drops(uint32_t s)
{
    NSView *v = surf(s)->body;
    if ([v isKindOfClass:[RBDropView class]]) {
        ((RBDropView *)v)->drops = YES;
        [v registerForDraggedTypes:[NSArray arrayWithObject:NSFilenamesPboardType]];
    }
}

// `(x, y)` is a window point, as a drop reports it.
int rb_host_hit(uint32_t s, uint32_t c, double x, double y)
{
    NSView *v = view(s, c);
    NSPoint p = [v convertPoint:NSMakePoint(x, y) fromView:nil];
    NSRect b = [v bounds];
    return (p.x >= 0 && p.y >= 0 && p.x <= b.size.width && p.y <= b.size.height) ? 1 : 0;
}

// ---- The toolbar: the screens as selectable items, Commander at the end.

@interface RBToolbar : NSObject
{
@public
    NSArray *ids;
    NSArray *labels;
    NSArray *icons;
    NSString *extra;
}
@end

@implementation RBToolbar
- (void)dealloc
{
    [ids release];
    [labels release];
    [icons release];
    [extra release];
    [super dealloc];
}

- (void)tool:(id)sender
{
    rb_command([[sender itemIdentifier] UTF8String]);
}

- (NSToolbarItem *)toolbar:(NSToolbar *)toolbar itemForItemIdentifier:(NSString *)ident willBeInsertedIntoToolbar:(BOOL)insert
{
    NSUInteger i = [ids indexOfObject:ident];
    NSString *label = i != NSNotFound ? [labels objectAtIndex:i] : @"Commander";
    NSString *icon = i != NSNotFound ? [icons objectAtIndex:i] : @"commander";
    if (i == NSNotFound && ![ident isEqualToString:extra])
        return nil;
    NSToolbarItem *item = [[[NSToolbarItem alloc] initWithItemIdentifier:ident] autorelease];
    [item setLabel:label];
    [item setPaletteLabel:label];
    [item setToolTip:label];
    NSString *path = [[NSBundle mainBundle] pathForResource:[icon stringByAppendingString:@"-48"] ofType:@"png"];
    NSImage *img = path ? [[[NSImage alloc] initWithContentsOfFile:path] autorelease] : nil;
    if (img != nil) {
        [img setSize:NSMakeSize(24, 24)];
        if ([img respondsToSelector:@selector(setTemplate:)])
            rb_send_bool(img, @selector(setTemplate:), YES);
        [item setImage:img];
    }
    [item setTarget:self];
    [item setAction:@selector(tool:)];
    return item;
}

- (NSArray *)toolbarDefaultItemIdentifiers:(NSToolbar *)toolbar
{
    NSMutableArray *all = [NSMutableArray arrayWithArray:ids];
    [all addObject:NSToolbarFlexibleSpaceItemIdentifier];
    [all addObject:extra];
    return all;
}

- (NSArray *)toolbarAllowedItemIdentifiers:(NSToolbar *)toolbar
{
    return [self toolbarDefaultItemIdentifiers:toolbar];
}

- (NSArray *)toolbarSelectableItemIdentifiers:(NSToolbar *)toolbar
{
    return ids;
}
@end

static RBToolbar *rb_toolbar = nil;

void rb_host_toolbar(uint32_t s, const char *const *ids, const char *const *labels, const char *const *icons,
                     uint32_t n, const char *extra)
{
    NSMutableArray *a = [NSMutableArray array], *b = [NSMutableArray array], *c = [NSMutableArray array];
    uint32_t i;
    for (i = 0; i < n; i++) {
        [a addObject:str(ids[i])];
        [b addObject:str(labels[i])];
        [c addObject:str(icons[i])];
    }
    rb_toolbar = [[RBToolbar alloc] init];
    rb_toolbar->ids = [a retain];
    rb_toolbar->labels = [b retain];
    rb_toolbar->icons = [c retain];
    rb_toolbar->extra = [str(extra) retain];
    NSToolbar *t = [[[NSToolbar alloc] initWithIdentifier:@"app.screens"] autorelease];
    [t setDelegate:(id)rb_toolbar];
    [t setDisplayMode:NSToolbarDisplayModeIconAndLabel];
    [t setAllowsUserCustomization:NO];
    [surf(s)->window setToolbar:t];
}

void rb_host_select_tool(uint32_t s, const char *ident)
{
    NSToolbar *t = [surf(s)->window toolbar];
    NSString *i = str(ident);
    if (![[t selectedItemIdentifier] isEqualToString:i])
        [t setSelectedItemIdentifier:i];
}

void rb_host_show_toolbar(uint32_t s, int on)
{
    [[surf(s)->window toolbar] setVisible:(on ? YES : NO)];
}

// ---- The menu bar Rust describes: each item sends its controller id.

@interface RBCommand : NSObject
@end

@implementation RBCommand
- (void)run:(id)sender
{
    rb_command([[sender representedObject] UTF8String]);
}
@end

static RBCommand *rb_commands = nil;

void rb_host_menu_add(const char *menu, const char *label, const char *ident, const char *keyeq)
{
    if (rb_commands == nil)
        rb_commands = [[RBCommand alloc] init];
    NSMenu *bar = [NSApp mainMenu];
    NSString *title = str(menu);
    NSMenu *into = nil;
    // The app menu is the first; the others are found or made by title.
    if ([title isEqualToString:@"Rusty Backup"]) {
        into = [[bar itemAtIndex:0] submenu];
    } else {
        NSInteger i;
        for (i = 1; i < [bar numberOfItems]; i++)
            if ([[[bar itemAtIndex:i] title] isEqualToString:title])
                into = [[bar itemAtIndex:i] submenu];
        if (into == nil) {
            into = [[[NSMenu alloc] initWithTitle:title] autorelease];
            [into setAutoenablesItems:NO];
            NSMenuItem *holder = [[[NSMenuItem alloc] initWithTitle:title action:NULL keyEquivalent:@""] autorelease];
            [holder setSubmenu:into];
            [bar addItem:holder];
        }
    }
    [into setAutoenablesItems:NO];
    NSMenuItem *it = [[[NSMenuItem alloc] initWithTitle:str(label) action:@selector(run:) keyEquivalent:str(keyeq)] autorelease];
    [it setTarget:rb_commands];
    [it setRepresentedObject:str(ident)];
    // The app menu ends with Quit; its own items go before it.
    if (into == [[bar itemAtIndex:0] submenu] && [into numberOfItems] > 0)
        [into insertItem:it atIndex:[into numberOfItems] - 1];
    else
        [into addItem:it];
}

static void rb_enable_in(NSMenu *menu, NSString *ident, BOOL on)
{
    NSInteger i;
    for (i = 0; i < [menu numberOfItems]; i++) {
        NSMenuItem *it = [menu itemAtIndex:i];
        if ([[it representedObject] isEqual:ident])
            [it setEnabled:on];
        if ([it submenu] != nil)
            rb_enable_in([it submenu], ident, on);
    }
}

void rb_host_menu_enable(const char *ident, int on)
{
    rb_enable_in([NSApp mainMenu], str(ident), on ? YES : NO);
}
