package main

import (
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/app"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/mqtt"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

// validatePort checks if a port string is a valid port number (1-65535)
func validatePort(portStr string) bool {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}
	return port >= 1 && port <= 65535
}

func main() {
	initLogger()
	logAppVersion()
	utils.LoadDotEnvFile()
	setLogLevel()

	// HTTP TLS configuration
	tlsConfig := app.ServerTLSConfig{
		Enabled:  utils.EnvVarBool("NANIT_TLS_ENABLED", false),
		CertFile: utils.EnvVarStr("NANIT_TLS_CERT_FILE", ""),
		KeyFile:  utils.EnvVarStr("NANIT_TLS_KEY_FILE", ""),
	}

	// Validate TLS config if enabled
	if err := tlsConfig.Validate(); err != nil {
		log.Fatal().Err(err).Msg("Invalid TLS configuration")
	}

	opts := app.Opts{
		NanitCredentials: app.NanitCredentials{
			Email:        utils.EnvVarStr("NANIT_EMAIL", ""),
			Password:     utils.EnvVarStr("NANIT_PASSWORD", ""),
			RefreshToken: utils.EnvVarStr("NANIT_REFRESH_TOKEN", ""),
		},
		SessionFile:     utils.EnvVarStr("NANIT_SESSION_FILE", "/data/session.json"),
		DataDirectories: ensureDataDirectories(),
		HTTPEnabled:     utils.EnvVarBool("NANIT_HTTP_ENABLED", false),
		HTTPTLSConfig:   tlsConfig,
		EventPolling: app.EventPollingOpts{
			// Event message polling disabled by default
			Enabled: utils.EnvVarBool("NANIT_EVENTS_POLLING", false),
			// 30 second default polling interval
			PollingInterval: utils.EnvVarSeconds("NANIT_EVENTS_POLLING_INTERVAL", 30*time.Second),
			// 300 second (5 min) default message timeout (unseen messages are ignored once they are this old)
			MessageTimeout: utils.EnvVarSeconds("NANIT_EVENTS_MESSAGE_TIMEOUT", 300*time.Second),
		},
	}

	if utils.EnvVarBool("NANIT_RTMP_ENABLED", true) {
		// Get port (default 1935)
		port := utils.EnvVarStr("NANIT_RTMP_PORT", "1935")
		if !validatePort(port) {
			log.Fatal().Str("port", port).Msg("Invalid NANIT_RTMP_PORT. Must be a valid port number (1-65535)")
		}
		listenAddr := ":" + port

		// Get public address - auto-detect if not set
		publicAddr := utils.EnvVarStr("NANIT_RTMP_ADDR", "")
		if publicAddr == "" {
			// Auto-detect local IP
			localIP := utils.GetOutboundIPString()
			if localIP == "" {
				log.Fatal().Msg("Failed to auto-detect local IP. Set NANIT_RTMP_ADDR manually.")
			}
			publicAddr = localIP + ":" + port
			log.Info().Str("detected_ip", localIP).Str("public_addr", publicAddr).Msg("Auto-detected local IP for RTMP")
		} else {
			// Validate provided address has port
			m := regexp.MustCompile(":([0-9]+)$").FindStringSubmatch(publicAddr)
			if len(m) != 2 {
				log.Fatal().Msg("Invalid NANIT_RTMP_ADDR. Must include port (e.g., 192.168.1.100:1935)")
			}
			extractedPort := m[1]
			if !validatePort(extractedPort) {
				log.Fatal().Str("port", extractedPort).Msg("Invalid port in NANIT_RTMP_ADDR. Must be 1-65535")
			}
			listenAddr = ":" + extractedPort // Use port from provided address
		}

		preferRemote := utils.EnvVarBool("NANIT_PREFER_REMOTE", false)

		// Security: IP-based access control
		allowedPresets := utils.EnvVarStr("NANIT_RTMP_ALLOWED_PRESETS", "")
		allowedIPs := utils.EnvVarStr("NANIT_RTMP_ALLOWED_IPS", "")
		logDenied := utils.EnvVarBool("NANIT_RTMP_LOG_DENIED", true)
		logAllowed := utils.EnvVarBool("NANIT_RTMP_LOG_ALLOWED", false)

		// Log security configuration
		if allowedPresets != "" || allowedIPs != "" {
			log.Info().
				Str("presets", allowedPresets).
				Str("allowed_ips", allowedIPs).
				Bool("log_denied", logDenied).
				Bool("log_allowed", logAllowed).
				Msg("RTMP security configuration")
		} else {
			log.Warn().Msg("No RTMP security configured - set NANIT_RTMP_ALLOWED_PRESETS or NANIT_RTMP_ALLOWED_IPS")
		}

		log.Info().Bool("prefer_remote", preferRemote).Str("public_addr", publicAddr).Msg("RTMP streaming configuration")

		opts.RTMP = &app.RTMPOpts{
			ListenAddr:     listenAddr,
			PublicAddr:     publicAddr,
			PreferRemote:   preferRemote,
			AllowedPresets: allowedPresets,
			AllowedIPs:     allowedIPs,
			LogDenied:      logDenied,
			LogAllowed:     logAllowed,

			DisableRemoteRelay: utils.EnvVarBool("NANIT_DISABLE_REMOTE_RELAY", false),
		}
	}

	if utils.EnvVarBool("NANIT_MQTT_ENABLED", false) {
		// Get RTMP address for stream URL sensor (if RTMP is enabled)
		rtmpAddr := ""
		if opts.RTMP != nil {
			rtmpAddr = opts.RTMP.PublicAddr
		}

		opts.MQTT = &mqtt.Opts{
			BrokerURL:        utils.EnvVarReqStr("NANIT_MQTT_BROKER_URL"),
			ClientID:         utils.EnvVarStr("NANIT_MQTT_CLIENT_ID", "nanit"),
			Username:         utils.EnvVarStr("NANIT_MQTT_USERNAME", ""),
			Password:         utils.EnvVarStr("NANIT_MQTT_PASSWORD", ""),
			TopicPrefix:      utils.EnvVarStr("NANIT_MQTT_PREFIX", "nanit"),
			DiscoveryEnabled: utils.EnvVarBool("NANIT_MQTT_DISCOVERY", true), // Enabled by default
			RTMPAddr:         rtmpAddr,
		}

		if opts.MQTT.DiscoveryEnabled {
			log.Info().Msg("Home Assistant MQTT discovery enabled")
		}

		// Notification polling requires MQTT to be enabled
		if utils.EnvVarBool("NANIT_NOTIFICATIONS_ENABLED", false) {
			opts.Notifications = app.NotificationOpts{
				Enabled:                true,
				PollInterval:           utils.EnvVarSeconds("NANIT_NOTIFICATIONS_POLL_INTERVAL", 10*time.Second),
				Jitter:                 utils.EnvVarFloat("NANIT_NOTIFICATIONS_JITTER", 0.3),
				MaxBackoff:             utils.EnvVarSeconds("NANIT_NOTIFICATIONS_MAX_BACKOFF", 5*time.Minute),
				MessageTimeout:         utils.EnvVarSeconds("NANIT_NOTIFICATIONS_MESSAGE_TIMEOUT", 5*time.Minute),
				EnableSleepTracking:    utils.EnvVarBool("NANIT_SLEEP_TRACKING_ENABLED", true), // Default enabled
				SleepEventPollInterval: utils.EnvVarSeconds("NANIT_SLEEP_EVENT_POLL_INTERVAL", 30*time.Second),
				StatsPollInterval:      utils.EnvVarSeconds("NANIT_STATS_POLL_INTERVAL", 60*time.Second),
			}
			log.Info().
				Dur("poll_interval", opts.Notifications.PollInterval).
				Float64("jitter", opts.Notifications.Jitter).
				Bool("sleep_tracking", opts.Notifications.EnableSleepTracking).
				Msg("Notification polling enabled")
		}
	}

	if opts.EventPolling.Enabled {
		log.Info().Msgf("Event polling enabled with an interval of %v", opts.EventPolling.PollingInterval)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	instance, err := app.NewApp(opts)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application")
	}

	runner := utils.RunWithGracefulCancel(instance.Run)

	<-interrupt
	log.Warn().Msg("Received interrupt signal, terminating")

	waitForCleanup := make(chan struct{}, 1)

	go func() {
		runner.Cancel()
		close(waitForCleanup)
	}()

	select {
	case <-interrupt:
		log.Fatal().Msg("Received another interrupt signal, forcing termination without clean up")
	case <-waitForCleanup:
		log.Info().Msg("Clean exit")
		return
	}
}
