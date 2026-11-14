// The vintage Cocoa host (docs/cocoa_vintage_shell_plan.md): Objective-C 1.0, manual retain/release.
// Gate 0: one window showing what the Rust half reports. No screen logic lives here.

#import <Cocoa/Cocoa.h>

extern const char *rb_version(void);
extern const char *rb_summary(void);

static NSTextField *rb_label(NSString *text, NSRect frame, NSString *ax_id, CGFloat size)
{
    NSTextField *label = [[[NSTextField alloc] initWithFrame:frame] autorelease];
    [label setStringValue:text];
    [label setEditable:NO];
    [label setSelectable:NO];
    [label setBezeled:NO];
    [label setDrawsBackground:NO];
    [label setFont:[NSFont systemFontOfSize:size]];
    // NSUserInterfaceItemIdentification (10.7): what AX reports as AXIdentifier.
    if ([label respondsToSelector:@selector(setIdentifier:)])
        [label performSelector:@selector(setIdentifier:) withObject:ax_id];
    return label;
}

@interface RBAppDelegate : NSObject
{
    NSWindow *window;
}
@end

@implementation RBAppDelegate

- (void)applicationDidFinishLaunching:(NSNotification *)note
{
    NSRect frame = NSMakeRect(0, 0, 420, 140);
    window = [[NSWindow alloc] initWithContentRect:frame
                                         styleMask:(NSTitledWindowMask | NSClosableWindowMask | NSMiniaturizableWindowMask)
                                           backing:NSBackingStoreBuffered
                                             defer:NO];
    [window setTitle:@"Rusty Backup"];
    NSView *content = [window contentView];
    NSString *version = [NSString stringWithUTF8String:rb_version()];
    NSString *summary = [NSString stringWithUTF8String:rb_summary()];
    [content addSubview:rb_label(@"Rusty Backup", NSMakeRect(20, 90, 380, 28), @"gate0.title", 20)];
    [content addSubview:rb_label([@"Version " stringByAppendingString:version], NSMakeRect(20, 62, 380, 20), @"gate0.version", 13)];
    [content addSubview:rb_label(summary, NSMakeRect(20, 20, 380, 36), @"gate0.summary", 11)];
    [window center];
    [window makeKeyAndOrderFront:nil];
    // Test rigs over ssh cannot list windows on 10.7; this is the id `screencapture -l` takes.
    NSLog(@"rusty-backup: window %ld", (long)[window windowNumber]);
    [NSApp activateIgnoringOtherApps:YES];
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication *)app
{
    return YES;
}

- (void)dealloc
{
    [window release];
    [super dealloc];
}

@end

static void rb_install_menu(void)
{
    NSMenu *bar = [[[NSMenu alloc] initWithTitle:@""] autorelease];
    NSMenuItem *app_item = [[[NSMenuItem alloc] initWithTitle:@"" action:NULL keyEquivalent:@""] autorelease];
    NSMenu *app_menu = [[[NSMenu alloc] initWithTitle:@"Rusty Backup"] autorelease];
    [app_menu addItemWithTitle:@"Quit Rusty Backup" action:@selector(terminate:) keyEquivalent:@"q"];
    [app_item setSubmenu:app_menu];
    [bar addItem:app_item];
    [NSApp setMainMenu:bar];
}

int main(int argc, const char **argv)
{
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    [NSApplication sharedApplication];
    [NSApp setActivationPolicy:NSApplicationActivationPolicyRegular];
    rb_install_menu();
    RBAppDelegate *delegate = [[RBAppDelegate alloc] init];
    [NSApp setDelegate:(id)delegate];  // 10.4's SDK has no formal NSApplicationDelegate protocol
    [NSApp run];
    [delegate release];
    [pool release];
    return 0;
}
