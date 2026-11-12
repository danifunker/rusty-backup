// The AppKit shell's flow driver: AX over ApplicationServices, one JSON op per line (FLOWS.md).
//
// Built by runner/src/ax.rs with `swiftc` and run as `ax_driver <rusty-backup-appkit> <work dir>`.
// It starts the shell with a scratch config and a picks file, answers {"ready": true}, then reads
// ops from stdin and answers {"ok": true} or {"ok": false, "expected": ..., "observed": ...}.
//
// Controls are found by AXIdentifier, which the shell sets to the controller id, so this driver
// needs no labels.toml section. A name that is no id (a toolbar screen, a message) is matched
// against titles, descriptions and values. Acting goes through AX actions and attributes, never
// a synthetic click; only `key` / `type` post keyboard events to the app.
//
// The process running this needs the Accessibility permission (docs/appkit_shell_plan.md Gate 0).

import ApplicationServices
import Foundation

struct Fail: Error {
    let expected: String
    let observed: String
}

var app: AXUIElement!
var child: Process!
var picks = ""
var current: AXUIElement?

func attr(_ e: AXUIElement, _ name: String) -> AnyObject? {
    var v: AnyObject?
    return AXUIElementCopyAttributeValue(e, name as CFString, &v) == .success ? v : nil
}

func str(_ e: AXUIElement, _ name: String) -> String {
    if let s = attr(e, name) as? String { return s }
    if let n = attr(e, name) as? NSNumber { return n.stringValue }
    return ""
}

func children(_ e: AXUIElement) -> [AXUIElement] {
    (attr(e, kAXChildrenAttribute) as? [AXUIElement]) ?? []
}

/// Every element of the app's windows, in tree order; the menu bar is left out, an open menu is not.
func elements() -> [AXUIElement] {
    var out: [AXUIElement] = []
    func walk(_ e: AXUIElement, _ depth: Int) {
        if depth > 60 { return }
        out.append(e)
        for c in children(e) { walk(c, depth + 1) }
    }
    for c in children(app) where str(c, kAXRoleAttribute) != (kAXMenuBarRole as String) {
        walk(c, 0)
    }
    return out
}

func ident(_ e: AXUIElement) -> String { str(e, kAXIdentifierAttribute) }

func names(_ e: AXUIElement) -> [String] {
    [kAXTitleAttribute, kAXDescriptionAttribute, kAXValueAttribute, kAXLabelValueAttribute]
        .map { str(e, $0 as String) }
        .filter { !$0.isEmpty }
}

func visibleText() -> String {
    var lines: [String] = []
    for e in elements() {
        let id = ident(e)
        var parts: [String] = []
        if !id.isEmpty { parts.append("[\(id)]") }
        parts.append(contentsOf: Array(Set(names(e))).sorted())
        if parts.isEmpty { continue }
        lines.append(String(parts.joined(separator: " ").prefix(300)))
    }
    return lines.joined(separator: "\n")
}

/// By AXIdentifier first, then by an element's title, description or value (exact, then a part).
func find(_ idOrName: String, _ label: String?, nth: Int = 0, secs: Double = 10) throws -> AXUIElement {
    let end = Date().addingTimeInterval(secs)
    while true {
        let all = elements()
        var hits = all.filter { ident($0) == idOrName }
        if hits.isEmpty {
            for want in [label, idOrName].compactMap({ $0 }) where !want.isEmpty {
                hits = all.filter { names($0).contains(want) }
                if hits.isEmpty { hits = all.filter { names($0).contains { $0.contains(want) } } }
                if !hits.isEmpty { break }
            }
        }
        if hits.count > nth { return hits[nth] }
        if Date() >= end {
            let what = "a control '\(idOrName)'" + (label.map { " ('\($0)')" } ?? "")
            throw Fail(expected: what, observed: visibleText())
        }
        Thread.sleep(forTimeInterval: 0.2)
    }
}

/// The table row `e` sits in, when it is (or is inside) one.
func rowOf(_ e: AXUIElement) -> AXUIElement? {
    var cur: AXUIElement? = e
    for _ in 0..<6 {
        guard let c = cur else { return nil }
        if str(c, kAXRoleAttribute) == (kAXRowRole as String) { return c }
        cur = attr(c, kAXParentAttribute).map { $0 as! AXUIElement }
    }
    return nil
}

func actions(_ e: AXUIElement) -> [String] {
    var names: CFArray?
    guard AXUIElementCopyActionNames(e, &names) == .success else { return [] }
    return (names as? [String]) ?? []
}

func activate(_ e: AXUIElement) throws {
    if actions(e).contains(kAXPressAction as String) {
        AXUIElementPerformAction(e, kAXPressAction as CFString)
        Thread.sleep(forTimeInterval: 0.2)
        return
    }
    if AXUIElementSetAttributeValue(e, kAXSelectedAttribute as CFString, kCFBooleanTrue) == .success {
        Thread.sleep(forTimeInterval: 0.2)
        return
    }
    // A wrapper that carries the id (a toolbar item's group) passes the action to its one child.
    let kids = children(e)
    if kids.count == 1 { return try activate(kids[0]) }
    throw Fail(expected: "'\(names(e).first ?? ident(e))' to take an action", observed: visibleText())
}

func choose(_ value: String) throws {
    guard let e = current else { throw Fail(expected: "a focused control to choose on", observed: "") }
    let want = value.lowercased()
    let role = str(e, kAXRoleAttribute)
    if role == (kAXCheckBoxRole as String) || role == (kAXRadioButtonRole as String), want == "on" || want == "off" {
        let on = str(e, kAXValueAttribute) == "1"
        if on != (want == "on") { try activate(e) }
        return
    }
    // A pop-up button opens its menu; the item is pressed by its title.
    AXUIElementPerformAction(e, kAXPressAction as CFString)
    let end = Date().addingTimeInterval(3)
    while Date() < end {
        for it in elements() where str(it, kAXRoleAttribute) == (kAXMenuItemRole as String) {
            let t = str(it, kAXTitleAttribute).lowercased()
            if t == want || (!t.isEmpty && t.contains(want)) {
                AXUIElementPerformAction(it, kAXPressAction as CFString)
                Thread.sleep(forTimeInterval: 0.2)
                return
            }
        }
        Thread.sleep(forTimeInterval: 0.2)
    }
    AXUIElementPerformAction(e, kAXCancelAction as CFString)
    throw Fail(expected: "a choice '\(value)' on '\(names(e).first ?? ident(e))'", observed: visibleText())
}

func fill(_ text: String) throws {
    guard let e = current else { throw Fail(expected: "a focused field to fill", observed: "") }
    guard AXUIElementSetAttributeValue(e, kAXValueAttribute as CFString, text as CFString) == .success else {
        throw Fail(expected: "'\(names(e).first ?? ident(e))' to take text", observed: visibleText())
    }
    Thread.sleep(forTimeInterval: 0.2)
}

func shows(_ text: String) -> Bool {
    let wants = text.components(separatedBy: " || ")
    return elements().contains { e in names(e).contains { n in wants.contains { n.contains($0) } } }
}

func waitUntil(_ secs: Double, _ pred: () -> Bool) -> Bool {
    let end = Date().addingTimeInterval(secs)
    while Date() < end {
        if pred() { return true }
        Thread.sleep(forTimeInterval: 0.3)
    }
    return pred()
}

let keyCodes: [String: CGKeyCode] = [
    "Enter": 36, "Tab": 48, "Space": 49, "Backspace": 51, "Esc": 53, "Escape": 53,
    "Delete": 117, "Home": 115, "End": 119, "Left": 123, "Right": 124, "Down": 125, "Up": 126,
    "F1": 122, "F2": 120, "F3": 99, "F4": 118, "F5": 96, "F6": 97, "F7": 98, "F8": 100,
    "F9": 101, "F10": 109, "F11": 103, "F12": 111,
]

func post(_ code: CGKeyCode, _ flags: CGEventFlags, unicode: String? = nil) {
    let pid = child.processIdentifier
    for down in [true, false] {
        guard let ev = CGEvent(keyboardEventSource: nil, virtualKey: code, keyDown: down) else { continue }
        ev.flags = flags
        if let u = unicode {
            let chars = Array(u.utf16)
            ev.keyboardSetUnicodeString(stringLength: chars.count, unicodeString: chars)
        }
        ev.postToPid(pid)
    }
    Thread.sleep(forTimeInterval: 0.15)
}

func press(_ name: String) throws {
    var key = name
    var flags: CGEventFlags = []
    while key.count > 1, let plus = key.firstIndex(of: "+") {
        switch key[..<plus] {
        case "Ctrl": flags.insert(.maskControl)
        case "Shift": flags.insert(.maskShift)
        case "Alt": flags.insert(.maskAlternate)
        case "Cmd": flags.insert(.maskCommand)
        default: throw Fail(expected: "a known modifier in '\(name)'", observed: "")
        }
        key = String(key[key.index(after: plus)...])
    }
    if let code = keyCodes[key] {
        post(code, flags)
    } else if key.count == 1 {
        post(0, flags, unicode: key)
    } else {
        throw Fail(expected: "a known key, not '\(name)'", observed: "")
    }
}

func run(_ op: [String: Any]) throws -> [String: Any] {
    let kind = op["op"] as? String ?? ""
    let arg = op["arg"] as? String ?? ""
    let id = op["id"] as? String ?? arg
    let nth = op["nth"] as? Int ?? 0
    let limit = (op["timeout_ms"] as? Double ?? 30000) / 1000
    switch kind {
    case "tab":
        // A screen is an item on the screen toolbar (D9), found by its label.
        try activate(try find(arg, arg))
    case "focus":
        let e = try find(id, arg, nth: nth)
        current = e
        if let row = rowOf(e) {
            // A name in a list: select its row, and give the list the keys that follow.
            AXUIElementSetAttributeValue(row, kAXSelectedAttribute as CFString, kCFBooleanTrue)
            if let table = attr(row, kAXParentAttribute) {
                AXUIElementSetAttributeValue(table as! AXUIElement, kAXFocusedAttribute as CFString, kCFBooleanTrue)
            }
        } else if [kAXTextFieldRole as String, kAXTextAreaRole as String].contains(str(e, kAXRoleAttribute)) {
            // A field takes its value whole through `fill`; focusing it would route that to the field editor.
        } else if AXUIElementSetAttributeValue(e, kAXSelectedAttribute as CFString, kCFBooleanTrue) != .success {
            AXUIElementSetAttributeValue(e, kAXFocusedAttribute as CFString, kCFBooleanTrue)
        }
        Thread.sleep(forTimeInterval: 0.2)
    case "choose": try choose(arg)
    case "activate": try activate(try find(id, arg, nth: nth))
    case "pick": try "*\t\(arg)\n".write(toFile: picks, atomically: true, encoding: .utf8)
    case "key": try press(arg)
    case "keys": for k in arg.split(separator: " ") { try press(String(k)) }
    case "type": for ch in arg { try press(String(ch)) }
    case "fill": try fill(arg)
    case "wait":
        if !waitUntil(limit, { shows(arg) }) { throw Fail(expected: "the window shows '\(arg)'", observed: visibleText()) }
    case "wait_gone":
        if !waitUntil(limit, { !shows(arg) }) { throw Fail(expected: "the window without '\(arg)'", observed: visibleText()) }
    case "expect":
        if !waitUntil(2, { shows(arg) }) { throw Fail(expected: "the window shows '\(arg)'", observed: visibleText()) }
    case "expect_not":
        Thread.sleep(forTimeInterval: 0.5)
        if shows(arg) { throw Fail(expected: "the window without '\(arg)'", observed: visibleText()) }
    case "text": return ["ok": true, "text": visibleText()]
    default: throw Fail(expected: "a known op, not '\(kind)'", observed: "")
    }
    return ["ok": true]
}

func reply(_ obj: [String: Any]) {
    if let data = try? JSONSerialization.data(withJSONObject: obj), let s = String(data: data, encoding: .utf8) {
        print(s)
        fflush(stdout)
    }
}

func main() {
    let argv = CommandLine.arguments
    guard argv.count >= 3 else {
        reply(["ready": false, "why": "usage: ax_driver <rusty-backup-appkit> <work dir>"])
        return
    }
    let (binary, work) = (argv[1], argv[2])
    guard AXIsProcessTrusted() else {
        reply(["ready": false, "why": "this process has no Accessibility permission (System Settings > Privacy & Security > Accessibility)"])
        return
    }
    let fm = FileManager.default
    let home = (work as NSString).appendingPathComponent("home")
    let config = (home as NSString).appendingPathComponent(".config/rusty-backup")
    try? fm.createDirectory(atPath: config, withIntermediateDirectories: true)
    try? "{\"update_check\":{\"enabled\":false,\"repository_url\":\"\"}}"
        .write(toFile: (config as NSString).appendingPathComponent("config.json"), atomically: true, encoding: .utf8)
    picks = (work as NSString).appendingPathComponent("picks.tsv")
    fm.createFile(atPath: picks, contents: Data())
    var env = ProcessInfo.processInfo.environment
    env["HOME"] = home
    env["RUSTY_BACKUP_CONFIG_DIR"] = config
    env["RUSTY_BACKUP_TEST_PICKS"] = picks
    child = Process()
    child.executableURL = URL(fileURLWithPath: binary)
    child.environment = env
    child.currentDirectoryURL = URL(fileURLWithPath: work)
    let logPath = (work as NSString).appendingPathComponent("appkit-app.log")
    fm.createFile(atPath: logPath, contents: Data())
    let log = FileHandle(forWritingAtPath: logPath)
    child.standardOutput = log
    child.standardError = log
    child.standardInput = FileHandle.nullDevice
    do { try child.run() } catch {
        reply(["ready": false, "why": "the AppKit shell did not start: \(error)"])
        return
    }
    defer { if child.isRunning { child.terminate() } }
    app = AXUIElementCreateApplication(child.processIdentifier)
    let up = waitUntil(60) { !((attr(app, kAXWindowsAttribute) as? [AXUIElement]) ?? []).isEmpty }
    if !up {
        reply(["ready": false, "why": "the AppKit shell's window never appeared"])
        return
    }
    reply(["ready": true])
    while let line = readLine() {
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        if trimmed.isEmpty { continue }
        do {
            guard let data = trimmed.data(using: .utf8),
                  let op = try JSONSerialization.jsonObject(with: data) as? [String: Any]
            else { throw Fail(expected: "a JSON object", observed: trimmed) }
            reply(try run(op))
        } catch let f as Fail {
            reply(["ok": false, "expected": f.expected, "observed": f.observed])
        } catch {
            reply(["ok": false, "expected": "the op to complete", "observed": "\(error)\n\(visibleText())"])
        }
        if !child.isRunning {
            reply(["ok": false, "expected": "the AppKit shell running", "observed": "it exited"])
            return
        }
    }
}

main()
