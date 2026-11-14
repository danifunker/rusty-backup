// The vintage Cocoa host (docs/cocoa_vintage_shell_plan.md): Objective-C 1.0, manual retain/release.
// The app's lifecycle only; the windows are Rust's (rust/src), built through host.m.

#import <Cocoa/Cocoa.h>

extern void rb_start(void);

@interface RBAppDelegate : NSObject
@end

@implementation RBAppDelegate

- (void)applicationDidFinishLaunching:(NSNotification *)note
{
    rb_start();
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication *)app
{
    return YES;
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
