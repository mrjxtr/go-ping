// Package config
package config

type Config struct {
	DefaultProbes []string

	UserAgent string
	Version   string
}

// canonical 204-no-content connectivity endpoints. tried in order, first 204 wins.
// anything else (200 with HTML, redirect, timeout) means captive portal or real outage.
var defaultProbes = []string{
	"https://www.google.com/generate_204",
	"https://connectivitycheck.gstatic.com/generate_204",
	"https://www.gstatic.com/generate_204",
}

const (
	userAgent = "go-ping/1.0"
	version   = "v0.0.3"
)

func NewConfig() Config {
	return Config{
		DefaultProbes: defaultProbes,

		UserAgent: userAgent,
		Version:   version,
	}
}
