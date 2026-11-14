// The vintage Cocoa host (docs/cocoa_vintage_shell_plan.md): Objective-C 1.0, manual retain/release.
// The app's lifecycle and its Quit; the windows and the rest of the menu bar are Rust's.

#import <Cocoa/Cocoa.h>
#include <objc/objc-runtime.h>

#ifndef NSINTEGER_DEFINED
typedef int NSInteger;  // 10.4's SDK predates NSInteger
#endif

extern void rb_start(void);
extern void rb_open_files(const char *paths);

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

// Files the Finder hands over (a double-click, a drop on the Dock icon).
- (void)application:(NSApplication *)app openFiles:(NSArray *)files
{
    rb_open_files([[files componentsJoinedByString:@"\n"] UTF8String]);
    [app replyToOpenOrPrint:NSApplicationDelegateReplySuccess];
}

@end

static void rb_install_menu(void)
{
    NSMenu *bar = [[[NSMenu alloc] initWithTitle:@""] autorelease];
    NSMenuItem *app_item = [[[NSMenuItem alloc] initWithTitle:@"" action:NULL keyEquivalent:@""] autorelease];
    NSMenu *app_menu = [[[NSMenu alloc] initWithTitle:@"Rusty Backup"] autorelease];
    [app_menu setAutoenablesItems:NO];
    [app_menu addItemWithTitle:@"Quit Rusty Backup" action:@selector(terminate:) keyEquivalent:@"q"];
    [app_item setSubmenu:app_menu];
    [bar addItem:app_item];
    [NSApp setMainMenu:bar];
}

// On PowerPC, Rust owns `main` (ppc_main.rs) and calls this.
#ifdef RB_RUST_MAIN
int rb_objc_main(int argc, const char **argv)
#else
int main(int argc, const char **argv)
#endif
{
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    [NSApplication sharedApplication];
    // 10.6 and later: a regular app. Earlier systems take a bundled app as one already.
    if ([NSApp respondsToSelector:@selector(setActivationPolicy:)])
        ((BOOL (*)(id, SEL, NSInteger))objc_msgSend)(NSApp, @selector(setActivationPolicy:), 0);
    rb_install_menu();
    RBAppDelegate *delegate = [[RBAppDelegate alloc] init];
    [NSApp setDelegate:(id)delegate];  // 10.4's SDK has no formal NSApplicationDelegate protocol
    [NSApp run];
    [delegate release];
    [pool release];
    return 0;
}
