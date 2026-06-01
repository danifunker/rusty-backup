; Inno Setup script for Rusty Backup (Windows, per-user install).
;
; Produces a per-user Setup.exe that installs into %LocalAppData% (no admin),
; which is what lets the in-app self-updater (self_replace) overwrite the
; running exe later. See docs/windows_self_update.md.
;
; Build:
;   iscc /DMyAppVersion=2026-06-01-14-30 /DSourceDir=path\to\build installer\rusty-backup.iss
;
; CI wiring (compile per arch, attach Setup.exe to the release) is Phase 7 and
; is intentionally not done yet.

#ifndef MyAppVersion
  #define MyAppVersion "0.0.0-dev"
#endif

; Directory containing the built rusty-backup.exe + rb-cli.exe + icon.ico.
#ifndef SourceDir
  #define SourceDir "..\target\release"
#endif

; Directory containing icon.ico (assets are not under the build dir).
#ifndef AssetsDir
  #define AssetsDir "..\assets\icons"
#endif

#define MyAppName "Rusty Backup"
#define MyAppPublisher "Dani"
#define MyAppURL "https://github.com/danifunker/rusty-backup"
#define MyAppExeName "rusty-backup.exe"
#define MyCliExeName "rb-cli.exe"

[Setup]
; Stable AppId so upgrades replace in place and the ARP entry is reused. Do
; not change this GUID across releases.
AppId={{8F3A1C2E-5B7D-4E9A-9C1F-2D6B8A4E7F10}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}/releases
; Per-user install: no elevation, installs under %LocalAppData% so the in-app
; updater can overwrite files without admin.
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
DefaultDirName={localappdata}\Programs\Rusty Backup
DisableProgramGroupPage=yes
DefaultGroupName={#MyAppName}
; Let the user pick a different install location.
DisableDirPage=no
AllowNoIcons=yes
UninstallDisplayIcon={app}\{#MyAppExeName}
OutputBaseFilename=Rusty-Backup-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
; Required so the PATH change is broadcast (WM_SETTINGCHANGE) without a reboot.
ChangesEnvironment=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked
Name: "addtopath"; Description: "Add &rb-cli (command-line tool) to PATH"; GroupDescription: "Command line:"
Name: "associate"; Description: "&Associate disk image files with Rusty Backup"; GroupDescription: "File associations:"

[Files]
Source: "{#SourceDir}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
; rb-cli goes in bin\ so only the CLI lands on PATH (not the GUI exe).
Source: "{#SourceDir}\{#MyCliExeName}"; DestDir: "{app}\bin"; Flags: ignoreversion skipifsourcedoesntexist
Source: "{#AssetsDir}\icon.ico"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{userdesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
; Register file associations from the app itself (single source of truth in
; model::file_types), only if the user kept the task checked.
Filename: "{app}\{#MyAppExeName}"; Parameters: "--register-file-associations"; Tasks: associate; Flags: runhidden nowait
Filename: "{app}\{#MyAppExeName}"; Description: "Launch {#MyAppName}"; Flags: nowait postinstall skipifsilent

[UninstallRun]
; Clean up associations before the exe is removed.
Filename: "{app}\{#MyAppExeName}"; Parameters: "--unregister-file-associations"; Flags: runhidden; RunOnceId: "UnregisterAssoc"

[Code]
const
  EnvironmentKey = 'Environment';

function BinPath(): String;
begin
  Result := ExpandConstant('{app}\bin');
end;

// True if Path does not already contain our bin dir.
function NeedsAddPath(): Boolean;
var
  OrigPath: String;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvironmentKey, 'Path', OrigPath) then
  begin
    Result := True;
    exit;
  end;
  // Wrap in semicolons so we match whole entries, case-insensitively.
  Result := Pos(';' + Lowercase(BinPath()) + ';', ';' + Lowercase(OrigPath) + ';') = 0;
end;

procedure AddToPath();
var
  OrigPath: String;
  NewPath: String;
begin
  if not (WizardIsTaskSelected('addtopath') and NeedsAddPath()) then
    exit;
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvironmentKey, 'Path', OrigPath) then
    OrigPath := '';
  if (OrigPath <> '') and (OrigPath[Length(OrigPath)] <> ';') then
    NewPath := OrigPath + ';' + BinPath()
  else
    NewPath := OrigPath + BinPath();
  RegWriteExpandStringValue(HKEY_CURRENT_USER, EnvironmentKey, 'Path', NewPath);
end;

procedure RemoveFromPath();
var
  OrigPath: String;
  Needle: String;
  P: Integer;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvironmentKey, 'Path', OrigPath) then
    exit;
  // Match ';bin' or 'bin;' or 'bin' as a whole entry.
  Needle := ';' + Lowercase(OrigPath) + ';';
  P := Pos(';' + Lowercase(BinPath()) + ';', Needle);
  if P = 0 then
    exit;
  // Rebuild without our entry (operate on the original-case string).
  Needle := ';' + OrigPath + ';';
  Delete(Needle, P, Length(BinPath()) + 1);
  // Strip the wrapping semicolons we added.
  if (Length(Needle) >= 2) and (Needle[1] = ';') then
    Delete(Needle, 1, 1);
  if (Length(Needle) >= 1) and (Needle[Length(Needle)] = ';') then
    Delete(Needle, Length(Needle), 1);
  RegWriteExpandStringValue(HKEY_CURRENT_USER, EnvironmentKey, 'Path', Needle);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
    AddToPath();
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
    RemoveFromPath();
end;
