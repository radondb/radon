package proxy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/xelabs/go-mysqlstack/sqldb"
)

var (
	defaultMySQLVersionStr     = "5.7.25"
	defaultMySQLVersion        = serverVersion{5, 7, 25, ""}
	authenticationMySQLVersion = serverVersion{5, 7, 0, ""}
	versionRegex               = regexp.MustCompile(`([0-9]+)\.([0-9]+)\.([0-9]+)`)
)

type serverVersion struct {
	Major, Minor, Patch int
	Tag                 string
}

func (v *serverVersion) atLeast(compare serverVersion) bool {
	if v.Major > compare.Major {
		return true
	}
	if v.Major == compare.Major && v.Minor > compare.Minor {
		return true
	}
	if v.Major == compare.Major && v.Minor == compare.Minor && v.Patch >= compare.Patch {
		return true
	}
	return false
}

func (v *serverVersion) equal(compare serverVersion) bool {
	if v.Major == compare.Major && v.Minor == compare.Minor && v.Patch == compare.Patch {
		return true
	}
	return false
}

func (v *serverVersion) toStr() string {
	vStr := strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch) + "-" + v.Tag
	return vStr
}

// parseVersionString parse the string of version.
func parseVersionString(version string, withTag bool) (ver serverVersion, err error) {
	if withTag {
		versions := strings.SplitN(version, "-", 2)
		if len(versions) > 1 {
			ver.Tag = versions[1]
		}
	}

	v := versionRegex.FindStringSubmatch(version)
	if len(v) != 4 {
		return ver, fmt.Errorf("could not parse server version from: %s", version)
	}
	ver.Major, err = strconv.Atoi(string(v[1]))
	if err != nil {
		return ver, fmt.Errorf("could not parse server version from: %s", version)
	}
	ver.Minor, err = strconv.Atoi(string(v[2]))
	if err != nil {
		return ver, fmt.Errorf("could not parse server version from: %s", version)
	}
	ver.Patch, err = strconv.Atoi(string(v[3]))
	if err != nil {
		return ver, fmt.Errorf("could not parse server version from: %s", version)
	}
	return
}

// getBackendVersion get the backend MySQL version
func getBackendVersion(spanner *Spanner) (version serverVersion, err error) {
	log := spanner.log
	versionQuery := fmt.Sprintf("select version() as version")
	vr, err := spanner.ExecuteSingle(versionQuery)
	if err != nil || len(vr.Rows) == 0 {
		log.Error("proxy: get MySQL version error:%+v", err)
		return version, sqldb.NewSQLErrorf(sqldb.CR_VERSION_ERROR, "Cann't get MySQL version")
	}

	versionStr := vr.Rows[0][0].String()
	backendVersion, err := parseVersionString(versionStr, false)
	if err != nil {
		log.Error("proxy: parse MySQL version error:%+v", err)
		return version, sqldb.NewSQLErrorf(sqldb.CR_VERSION_ERROR, "Cann't get MySQL version")
	}
	return backendVersion, nil
}
