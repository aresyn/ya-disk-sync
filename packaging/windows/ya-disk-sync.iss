#define AppName "ya-disk-sync"
#define AppVersion "0.1.1"
#ifndef PayloadDir
#define PayloadDir "..\..\dist\windows-payload"
#endif
#ifndef OutputDir
#define OutputDir "..\..\dist"
#endif

[Setup]
AppId={{D4EAB5A8-6883-4C26-A88A-8B0FD3C4C796}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher=aresyn
AppPublisherURL=https://github.com/aresyn/ya-disk-sync
AppSupportURL=https://github.com/aresyn/ya-disk-sync/issues
AppUpdatesURL=https://github.com/aresyn/ya-disk-sync/releases
DefaultDirName={autopf}\YaDiskSync
DefaultGroupName=YaDiskSync
DisableProgramGroupPage=yes
OutputDir={#OutputDir}
OutputBaseFilename=ya-disk-sync-{#AppVersion}-windows-x86_64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=admin
UninstallDisplayName=ya-disk-sync
UninstallDisplayIcon={app}\ya-disk-sync.exe

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"

[Tasks]
Name: "startservice"; Description: "Запустить службу после установки"; GroupDescription: "После установки:"; Flags: checkedonce

[Files]
Source: "{#PayloadDir}\ya-disk-sync.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PayloadDir}\README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PayloadDir}\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PayloadDir}\docs\*"; DestDir: "{app}\docs"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#PayloadDir}\assets\*"; DestDir: "{app}\assets"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#PayloadDir}\scripts\install-windows-service.ps1"; DestDir: "{app}\scripts"; Flags: ignoreversion

[Dirs]
Name: "{code:GetConfigDir}"
Name: "{code:GetStateDir}"
Name: "{code:GetLogsDir}"
Name: "{code:GetStagingDir}"

[Icons]
Name: "{group}\Открыть Web UI"; Filename: "{app}\ya-disk-sync.exe"; Parameters: "--config ""{code:GetConfigPath}"" web open"; WorkingDir: "{app}"
Name: "{group}\Status"; Filename: "{app}\ya-disk-sync.exe"; Parameters: "--config ""{code:GetConfigPath}"" status"; WorkingDir: "{app}"
Name: "{group}\Logs"; Filename: "{app}\ya-disk-sync.exe"; Parameters: "--config ""{code:GetConfigPath}"" logs tail --lines 100"; WorkingDir: "{app}"
Name: "{group}\Config folder"; Filename: "{code:GetConfigDir}"
Name: "{group}\Uninstall"; Filename: "{uninstallexe}"

[UninstallRun]
Filename: "{cmd}"; Parameters: "/C """"{app}\ya-disk-sync.exe"" service stop >NUL 2>NUL || exit /B 0"""; Flags: runhidden waituntilterminated skipifdoesntexist
Filename: "{cmd}"; Parameters: "/C """"{app}\ya-disk-sync.exe"" service uninstall >NUL 2>NUL || exit /B 0"""; Flags: runhidden waituntilterminated skipifdoesntexist

[Code]
function GetDataDir(Param: String): String;
begin
  Result := ExpandConstant('{param:YDSDATA|}');
  if Result = '' then
    Result := ExpandConstant('{commonappdata}\YaDiskSync');
end;

function GetConfigDir(Param: String): String;
begin
  Result := GetDataDir('') + '\config';
end;

function GetStateDir(Param: String): String;
begin
  Result := GetDataDir('') + '\state';
end;

function GetLogsDir(Param: String): String;
begin
  Result := GetDataDir('') + '\logs';
end;

function GetStagingDir(Param: String): String;
begin
  Result := GetDataDir('') + '\staging';
end;

function GetConfigPath(Param: String): String;
begin
  Result := GetConfigDir('') + '\config.json';
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  ResultCode: Integer;
begin
  Result := '';
  if FileExists(ExpandConstant('{app}\ya-disk-sync.exe')) then
    Exec(ExpandConstant('{app}\ya-disk-sync.exe'), 'service stop', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure RunOrFail(Parameters: String; StepName: String);
var
  ResultCode: Integer;
begin
  if not Exec(ExpandConstant('{app}\ya-disk-sync.exe'), Parameters, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    RaiseException(StepName + ': не удалось запустить ya-disk-sync.exe');
  if ResultCode <> 0 then
    RaiseException(StepName + ': ya-disk-sync.exe завершился с кодом ' + IntToStr(ResultCode));
end;

function JsonString(Value: String): String;
var
  Escaped: String;
begin
  Escaped := Value;
  StringChangeEx(Escaped, '\', '\\', True);
  StringChangeEx(Escaped, '"', '\"', True);
  Result := '\"' + Escaped + '\"';
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ConfigPath: String;
  CreatedConfig: Boolean;
begin
  if CurStep = ssPostInstall then
  begin
    ConfigPath := GetConfigPath('');
    CreatedConfig := False;
    if not FileExists(ConfigPath) then
    begin
      RunOrFail('--config "' + ConfigPath + '" config init', 'config init');
      CreatedConfig := True;
    end;

    if CreatedConfig then
    begin
      RunOrFail('--config "' + ConfigPath + '" config set /paths/state_db ' + JsonString(GetStateDir('') + '\state.sqlite'), 'config set state_db');
      RunOrFail('--config "' + ConfigPath + '" config set /paths/logs_dir ' + JsonString(GetLogsDir('')), 'config set logs_dir');
      RunOrFail('--config "' + ConfigPath + '" config set /paths/staging_dir ' + JsonString(GetStagingDir('')), 'config set staging_dir');
    end;

    RunOrFail('--config "' + ConfigPath + '" config validate', 'config validate');
    RunOrFail('--config "' + ConfigPath + '" service install --force', 'service install');

    if WizardIsTaskSelected('startservice') then
      RunOrFail('service start', 'service start');
  end;
end;
