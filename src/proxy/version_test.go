package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type testcase struct {
	versionString string
	withTag       bool
	version       serverVersion
}

func TestVersionParseString(t *testing.T) {
	var testcases = []testcase{
		{
			versionString: "5.7.20-18-debug-log",
			withTag:       false,
			version:       serverVersion{5, 7, 20, ""},
		},
		{
			versionString: "8.0.16-18-debug-log",
			withTag:       false,
			version:       serverVersion{8, 0, 16, ""},
		},
		{
			versionString: "5.7.25-v1.0.7.2-26-gbbd35bf",
			withTag:       true,
			version:       serverVersion{5, 7, 25, "v1.0.7.2-26-gbbd35bf"},
		},
	}

	var failedcases = []testcase{
		{
			versionString: "8.0.",
			withTag:       false,
			version:       serverVersion{8, 0, 16, ""},
		},
	}

	for _, testcase := range testcases {
		v, err := parseVersionString(testcase.versionString, testcase.withTag)
		assert.Nil(t, err)
		assert.Equal(t, testcase.version, v)
	}

	for _, testcase := range failedcases {
		_, err := parseVersionString(testcase.versionString, testcase.withTag)
		assert.NotNil(t, err)
	}
}

func TestVersionSetServerVersion(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show databases", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
	}

	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		// the new client with the same backend, the version won't be set.
		client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client1.Close()
	}

	fakedbs.ResetAll()
	fakedbs.AddQuery("select version() as version", resultVersion57)
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.NotNil(t, err)

	}

	fakedbs.ResetAll()
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.NotNil(t, err)
	}
}

func TestVersionFunction(t *testing.T) {
	MySQLVersion := serverVersion{5, 7, 25, ""}
	MySQLVersion8 := serverVersion{8, 0, 3, ""}

	MySQLVersions := []serverVersion{
		{4, 0, 0, ""},
		{5, 0, 0, ""},
		{5, 6, 0, ""},
		{5, 7, 0, ""},
		{5, 7, 25, ""},
	}
	for _, Version := range MySQLVersions {
		isAtLeast := MySQLVersion.atLeast(Version)
		assert.Equal(t, true, isAtLeast)
	}
	isAtLeast := MySQLVersion.atLeast(MySQLVersion8)
	assert.Equal(t, false, isAtLeast)

	MySQLVersion6 := serverVersion{5, 7, 25, ""}
	isEqual := MySQLVersion.equal(MySQLVersion6)
	assert.Equal(t, true, isEqual)
	isEqual = MySQLVersion.equal(MySQLVersion8)
	assert.Equal(t, false, isEqual)

	MySQLVersion.toStr()
}
