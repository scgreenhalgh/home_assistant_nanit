package app

import (
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/mqtt"
	"time"
)

// Opts - application run options
type Opts struct {
	NanitCredentials NanitCredentials
	SessionFile      string
	DataDirectories  DataDirectories
	HTTPEnabled      bool
	HTTPTLSConfig    ServerTLSConfig
	MQTT             *mqtt.Opts
	RTMP             *RTMPOpts
	EventPolling     EventPollingOpts
	Notifications    NotificationOpts
}

// NanitCredentials - user credentials for Nanit account
type NanitCredentials struct {
	Email        string
	Password     string
	RefreshToken string
}

// DataDirectories - dictionary of dir paths
type DataDirectories struct {
	BaseDir  string
	VideoDir string
	LogDir   string
}

// RTMPOpts - options for RTMP streaming
type RTMPOpts struct {
	// IP:Port of the interface on which we should listen
	ListenAddr string

	// IP:Port under which can Cam reach the RTMP server
	PublicAddr string

	// PreferRemote has no effect: local is always primary, with the cloud stream as hot standby
	PreferRemote bool

	// Security: IP-based access control
	// AllowedIPs is a comma-separated list of IPs and/or CIDR ranges
	// Example: "192.168.1.100,172.30.32.0/24"
	AllowedIPs string

	// AllowedPresets is a comma-separated list of preset names
	// Valid presets: localhost, hassio, docker, private, frigate
	AllowedPresets string

	// LogDenied logs denied connection attempts (default: true)
	LogDenied bool

	// LogAllowed logs allowed connection attempts (default: false)
	LogAllowed bool
}

type EventPollingOpts struct {
	Enabled         bool
	PollingInterval time.Duration
	MessageTimeout  time.Duration
}

// NotificationOpts - options for notification event polling
type NotificationOpts struct {
	// Enabled enables notification event polling
	Enabled bool

	// PollInterval is the base polling interval (default 10s)
	PollInterval time.Duration

	// Jitter is the jitter factor 0-1, e.g. 0.3 = ±30% (default 0.3)
	Jitter float64

	// MaxBackoff is the maximum backoff on errors (default 5m)
	MaxBackoff time.Duration

	// MessageTimeout is how old messages can be to still be processed (default 5m)
	MessageTimeout time.Duration

	// EnableSleepTracking enables polling of /events and /stats/latest endpoints
	// for sleep tracking sensors (is_asleep, in_bed, times_woke_up, etc.)
	EnableSleepTracking bool

	// SleepEventPollInterval is the polling interval for sleep events (default 30s)
	SleepEventPollInterval time.Duration

	// StatsPollInterval is the polling interval for sleep stats (default 60s)
	StatsPollInterval time.Duration
}
