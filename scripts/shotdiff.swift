// The share of pixels that differ between two screenshots, for scripts/appkit-tour.sh.
//
//   shotdiff a.png b.png   prints the share (0.0 to 1.0), or 1.0 when the sizes differ
//
// A pixel differs when any channel moves by more than 24 of 255, so antialiasing noise is not a
// change; the tour script decides what share counts as a change.

import CoreGraphics
import Foundation
import ImageIO

func pixels(_ path: String) -> (Int, Int, [UInt8])? {
    guard let src = CGImageSourceCreateWithURL(URL(fileURLWithPath: path) as CFURL, nil),
          let img = CGImageSourceCreateImageAtIndex(src, 0, nil) else { return nil }
    let (w, h) = (img.width, img.height)
    var buf = [UInt8](repeating: 0, count: w * h * 4)
    let ok = buf.withUnsafeMutableBytes { raw -> Bool in
        guard let ctx = CGContext(data: raw.baseAddress, width: w, height: h, bitsPerComponent: 8,
                                  bytesPerRow: w * 4, space: CGColorSpaceCreateDeviceRGB(),
                                  bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue) else { return false }
        ctx.draw(img, in: CGRect(x: 0, y: 0, width: w, height: h))
        return true
    }
    return ok ? (w, h, buf) : nil
}

let args = CommandLine.arguments
guard args.count == 3, let a = pixels(args[1]), let b = pixels(args[2]) else {
    print("1.0")
    exit(0)
}
guard a.0 == b.0 && a.1 == b.1 else {
    print("1.0")
    exit(0)
}
var differ = 0
let n = a.0 * a.1
for i in 0..<n {
    let o = i * 4
    for c in 0..<3 where abs(Int(a.2[o + c]) - Int(b.2[o + c])) > 24 {
        differ += 1
        break
    }
}
print(String(format: "%.5f", Double(differ) / Double(max(n, 1))))
