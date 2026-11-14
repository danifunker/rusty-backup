// The widget factory behind frontends-vintage/cocoa/rust/src/host.rs: plain Cocoa from 10.4 up.
// Objective-C 1.0, manual retain/release, no blocks. Rust owns every rule; this only makes views.

#import <Cocoa/Cocoa.h>
#include <string.h>

#ifndef NSINTEGER_DEFINED
typedef int NSInteger;  // 10.4's SDK predates NSInteger
#endif

extern void rb_event(uint32_t s, uint32_t c, uint32_t kind);
extern void rb_window_event(uint32_t s, uint32_t kind);
extern void rb_later(void);
extern void rb_tick(void);

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

@interface RBSurface : NSObject
{
@public
    NSWindow *window;
    RBWindowDelegate *delegate;
    NSView *body;
    NSMutableDictionary *views;
    NSMutableDictionary *lists;
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
    x->body = [[RBFlipped alloc] initWithFrame:NSMakeRect(0, 0, w, h)];
    [x->body setAutoresizingMask:(NSViewWidthSizable | NSViewHeightSizable)];
    [x->window setContentView:x->body];
    x->views = [[NSMutableDictionary alloc] init];
    x->lists = [[NSMutableDictionary alloc] init];
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
