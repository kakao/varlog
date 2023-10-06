package buildinfo

import (
	"runtime/debug"
	"strings"
)

const (
	goOS        = "GOOS"
	goArch      = "GOARCH"
	vcsRevision = "vcs.revision"
	vcsTime     = "vcs.time"
)

var version = "devel"

type Info struct {
	*debug.BuildInfo
	settingsMap map[string]string

	Version string
}

func ReadVersionInfo() Info {
	info := Info{
		Version: version,
	}
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		info.BuildInfo = buildInfo
		info.settingsMap = make(map[string]string, len(buildInfo.Settings))
		for _, kv := range buildInfo.Settings {
			info.settingsMap[kv.Key] = kv.Value
		}
	}
	return info
}

func (info Info) String() string {
	var sb strings.Builder
	sb.WriteString("Version:     " + info.Version + "\n")
	sb.WriteString("Go Version:  " + info.GoVersion + "\n")
	sb.WriteString("Git Commit:  " + info.settingsMap[vcsRevision] + "\n")
	sb.WriteString("Built:       " + info.settingsMap[vcsTime] + "\n")
	sb.WriteString("OS/Arch:     " + info.settingsMap[goOS] + "/" + info.settingsMap[goArch])
	return sb.String()
}
