  Three new steps were added to the build-macos job, all conditional on the secrets being present:

  1. Import signing certificate — Decodes the base64 .p12 from MACOS_CERTIFICATE, imports it into a temporary keychain on the runner
  2. Code signing (in the existing app bundle step) — Uses Developer ID Application identity when secrets are set, falls back to ad-hoc (--sign -) when they're not
  3. Notarize DMG — Submits to Apple's notary service and staples the ticket
  4. Cleanup keychain — Always runs to delete the temporary keychain (even on failure)

  Setup steps for you

  1. Create the right certificate

  Make sure you have a "Developer ID Application" certificate (not "Apple Distribution"). Create it at developer.apple.com/account/resources/certificates > "+" > "Developer ID
  Application".

  2. Export as .p12

  In Keychain Access: right-click the cert > Export Items > choose .p12 format > set a password.

  3. Create an App-Specific Password

  Go to appleid.apple.com > Sign-In and Security > App-Specific Passwords > Generate one for "GitHub CI".

  4. Set GitHub Secrets

  In your repo: Settings > Secrets and variables > Actions > New repository secret:

  ┌─────────────────────────┬────────────────────────────────────────────────────┐
  │         Secret          │                       Value                        │
  ├─────────────────────────┼────────────────────────────────────────────────────┤
  │ MACOS_CERTIFICATE       │ base64 -i YourCert.p12 | pbcopy then paste         │
  ├─────────────────────────┼────────────────────────────────────────────────────┤
  │ MACOS_CERTIFICATE_PWD   │ The password you set during .p12 export            │
  ├─────────────────────────┼────────────────────────────────────────────────────┤
  │ MACOS_TEAM_ID           │ Your 10-character Team ID from developer.apple.com │
  ├─────────────────────────┼────────────────────────────────────────────────────┤
  │ MACOS_NOTARIZE_APPLE_ID │ Your Apple ID email                                │
  ├─────────────────────────┼────────────────────────────────────────────────────┤
  │ MACOS_NOTARIZE_PASSWORD │ The app-specific password from step 3              │
  └─────────────────────────┴────────────────────────────────────────────────────┘

  Fork-friendly

  When these secrets aren't set, the build falls back to ad-hoc signing (the previous behavior), so forks build without issues.

  One thing to verify

  The codesign identity string format is "Developer ID Application: (TEAM_ID)". If your certificate shows a different name format (e.g., includes your full name), you may need to
   adjust line 214 in the workflow. You can check by running security find-identity -v -p codesigning locally after importing the cert.
