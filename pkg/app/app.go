package app

import (
	"context"
	"strings"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/message"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/mqtt"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/notification"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/session"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/stream"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// App - application container
type App struct {
	Opts             Opts
	SessionStore     *session.Store
	BabyStateManager *baby.StateManager
	RestClient       *client.NanitClient
	MQTTConnection   *mqtt.Connection

	// RTMP server reference for remote streaming support
	rtmpServer *rtmpserver.RTMPServer

	// Notification manager for event polling
	notificationManager *notification.Manager
}

// NewApp - constructor
// Returns error if session store cannot be initialized
func NewApp(opts Opts) (*App, error) {
	sessionStore, err := session.InitSessionStore(opts.SessionFile)
	if err != nil {
		return nil, err
	}

	instance := &App{
		Opts:             opts,
		BabyStateManager: baby.NewStateManager(),
		SessionStore:     sessionStore,
		RestClient: &client.NanitClient{
			Email:        opts.NanitCredentials.Email,
			Password:     opts.NanitCredentials.Password,
			RefreshToken: opts.NanitCredentials.RefreshToken,
			SessionStore: sessionStore,
		},
	}

	if opts.MQTT != nil {
		instance.MQTTConnection = mqtt.NewConnection(*opts.MQTT)
	}

	return instance, nil
}

// Run - application main loop
func (app *App) Run(ctx utils.GracefulContext) {
	// Reauthorize if we don't have a token or we assume it is invalid
	if err := app.RestClient.MaybeAuthorize(false); err != nil {
		log.Error().Err(err).Msg("Failed to authorize with Nanit API")
		return
	}

	// Fetches babies info if they are not present in session
	if _, err := app.RestClient.EnsureBabies(); err != nil {
		log.Error().Err(err).Msg("Failed to fetch babies")
		return
	}

	// RTMP
	if app.Opts.RTMP != nil {
		if app.Opts.RTMP.PreferRemote {
			log.Warn().Msg("PreferRemote has no effect: the local stream is always primary, with the cloud stream as hot standby")
		}

		// Use new secure server if any security options are configured
		if app.Opts.RTMP.AllowedIPs != "" || app.Opts.RTMP.AllowedPresets != "" {
			serverCfg := rtmpserver.ServerConfig{
				ListenAddr:     app.Opts.RTMP.ListenAddr,
				AllowedIPs:     app.Opts.RTMP.AllowedIPs,
				AllowedPresets: app.Opts.RTMP.AllowedPresets,
				LogDenied:      app.Opts.RTMP.LogDenied,
				LogAllowed:     app.Opts.RTMP.LogAllowed,
			}

			server, err := rtmpserver.NewServer(serverCfg, app.BabyStateManager)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to initialize RTMP server with security config")
				return
			}

			app.rtmpServer = server

			// Run server in background with graceful shutdown
			ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				go func() {
					<-childCtx.Done()
					server.Close()
				}()
				server.Run(app.Opts.RTMP.ListenAddr)
			})
		} else {
			// Fallback to legacy server (allows all connections without security config)
			log.Warn().Msg("Starting RTMP server without security config - all connections will be allowed")
			app.rtmpServer = rtmpserver.StartRTMPServer(app.Opts.RTMP.ListenAddr, app.BabyStateManager)
		}
	}

	// MQTT
	if app.MQTTConnection != nil {
		// Register babies for MQTT discovery before starting connection
		app.MQTTConnection.RegisterBabies(app.SessionStore.Session.Babies)

		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			app.MQTTConnection.Run(app.BabyStateManager, childCtx)
		})

		// Start notification polling if enabled
		if app.Opts.Notifications.Enabled {
			app.startNotificationPolling(ctx)
		}
	}

	// Start reading the data from the stream
	for _, babyInfo := range app.SessionStore.Session.Babies {
		_babyInfo := babyInfo
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			app.handleBaby(_babyInfo, childCtx)
		})
	}

	// Start serving content over HTTP
	if app.Opts.HTTPEnabled {
		go serve(app.SessionStore.Session.Babies, app.Opts.DataDirectories, app.Opts.HTTPTLSConfig)
	}

	<-ctx.Done()
}

func (app *App) handleBaby(baby baby.Baby, ctx utils.GracefulContext) {
	if app.Opts.RTMP != nil || app.MQTTConnection != nil {
		// Websocket connection
		ws := client.NewWebsocketConnectionManager(baby.UID, baby.CameraUID, app.SessionStore.Session, app.RestClient, app.BabyStateManager)

		// One pacer per baby, shared by the pool of every websocket connection,
		// so a reconnect does not reset the STARTED rate limit and backoff
		pacer := stream.NewRequestPacer(stream.RequestPacerConfig{})

		ws.WithReadyConnection(func(conn *client.WebsocketConnection, childCtx utils.GracefulContext) {
			app.runWebsocket(baby.UID, conn, pacer, childCtx)
		})

		if app.Opts.EventPolling.Enabled {
			ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				app.pollMessages(childCtx, baby.UID, app.BabyStateManager)
			})
		}

		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			ws.RunWithinContext(childCtx)
		})
	}

	<-ctx.Done()
}

func (app *App) pollMessages(ctx utils.GracefulContext, babyUID string, babyStateManager *baby.StateManager) {
	ticker := time.NewTicker(app.Opts.EventPolling.PollingInterval)
	defer ticker.Stop()

	// Poll immediately on start, then at each interval
	for {
		newMessages := app.RestClient.FetchNewMessages(babyUID, app.Opts.EventPolling.MessageTimeout)

		for _, msg := range newMessages {
			switch msg.Type {
			case message.SoundEventMessageType:
				go babyStateManager.NotifySoundSubscribers(babyUID, time.Time(msg.Time))
			case message.MotionEventMessageType:
				go babyStateManager.NotifyMotionSubscribers(babyUID, time.Time(msg.Time))
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Continue to next poll iteration
		}
	}
}

func (app *App) runWebsocket(babyUID string, conn *client.WebsocketConnection, pacer *stream.RequestPacer, childCtx utils.GracefulContext) {
	// Reading sensor data
	conn.RegisterMessageHandler(func(m *client.Message, conn *client.WebsocketConnection) {
		// Sensor request initiated by us on start (or some other client, we don't care)
		if *m.Type == client.Message_RESPONSE && m.Response != nil {
			if *m.Response.RequestType == client.RequestType_GET_SENSOR_DATA && len(m.Response.SensorData) > 0 {
				processSensorData(babyUID, m.Response.SensorData, app.BabyStateManager)
			} else if *m.Response.RequestType == client.RequestType_GET_CONTROL && m.Response.Control != nil {
				processLight(babyUID, m.Response.Control, app.BabyStateManager)
			} else if *m.Response.RequestType == client.RequestType_GET_SETTINGS && m.Response.Settings != nil {
				processStandby(babyUID, m.Response.Settings, app.BabyStateManager)
			}
		} else

		// Communication initiated from a cam
		// Note: it sends the updates periodically on its own + whenever some significant change occurs
		if *m.Type == client.Message_REQUEST && m.Request != nil {
			if *m.Request.Type == client.RequestType_PUT_SENSOR_DATA && len(m.Request.SensorData_) > 0 {
				processSensorData(babyUID, m.Request.SensorData_, app.BabyStateManager)
			} else if *m.Request.Type == client.RequestType_PUT_CONTROL && m.Request.Control != nil {
				processLight(babyUID, m.Request.Control, app.BabyStateManager)
			} else if *m.Request.Type == client.RequestType_PUT_SETTINGS && m.Request.Settings != nil {
				processStandby(babyUID, m.Request.Settings, app.BabyStateManager)
			}
		}
	})

	if app.Opts.MQTT != nil && app.MQTTConnection != nil {
		app.MQTTConnection.RegisterLightHandler(func(enabled bool) {
			sendLightCommand(enabled, conn)
		})
		app.MQTTConnection.RegisterStandyHandler(func(enabled bool) {
			sendStandbyCommand(enabled, conn)
		})
	}

	// Get the initial state of the light
	conn.SendRequest(client.RequestType_GET_CONTROL, &client.Request{GetControl_: &client.GetControl{
		NightLight: utils.ConstRefBool(true),
	}})

	// Ask for sensor data (initial request)
	conn.SendRequest(client.RequestType_GET_SENSOR_DATA, &client.Request{
		GetSensorData: &client.GetSensorData{
			All: utils.ConstRefBool(true),
		},
	})

	// Ask for status
	// conn.SendRequest(client.RequestType_GET_STATUS, &client.Request{
	// 	GetStatus_: &client.GetStatus{
	// 		All: utils.ConstRefBool(true),
	// 	},
	// })

	// Ask for logs
	// conn.SendRequest(client.RequestType_GET_LOGS, &client.Request{
	// 	GetLogs: &client.GetLogs{
	// 		Url: utils.ConstRefStr("http://192.168.3.234:8080/log"),
	// 	},
	// })

	var cleanup func()

	// Streaming (local with remote failover)
	if app.Opts.RTMP != nil {
		pool := app.createHybridPool(babyUID, conn, pacer)

		// Convert GracefulContext to standard context
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-childCtx.Done()
			cancel()
		}()

		// Returns at once; the pool asks the camera only if local is not flowing
		pool.RequestLocalStream(ctx)

		cleanup = func() {
			// No STOPPED is sent here: by the time cleanup runs the websocket is
			// already closing, and on a reconnect the next pool keeps the push.
			pool.Close()
		}
	}

	<-childCtx.Done()
	if cleanup != nil {
		cleanup()
	}
}

// createHybridPool creates the stream pool for the given baby: local as primary,
// remote as hot standby (different network paths). It owns StreamType and StreamState.
func (app *App) createHybridPool(babyUID string, conn *client.WebsocketConnection, pacer *stream.RequestPacer) *stream.HybridStreamPool {
	return stream.NewHybridStreamPool(stream.HybridStreamPoolConfig{
		BabyUID:        babyUID,
		LocalURL:       app.getLocalStreamURL(babyUID), // e.g., rtmp://host/local/babyUID
		AuthToken:      app.SessionStore.GetAuthToken(),
		TokenProvider:  app.createTokenProvider(), // Provides fresh tokens when remote stream reconnects
		Conn:           conn,
		StateManager:   app.BabyStateManager,
		RTMPServer:     app.rtmpServer,
		StallThreshold: 3 * time.Second, // Re-evaluate if no packets for 3s
		OnAuthFailed:   app.onAuthFailed,
		Pacer:          pacer,

		DisableRemoteRelay: app.Opts.RTMP.DisableRemoteRelay,
	})
}

// getRemoteStreamURL builds the remote stream URL
// WARNING: The returned URL contains auth token - never log it directly
func (app *App) getRemoteStreamURL(babyUID string) string {
	return stream.BuildRemoteStreamURL(babyUID, app.SessionStore.GetAuthToken())
}

func (app *App) getLocalStreamURL(babyUID string) string {
	if app.Opts.RTMP != nil {
		tpl := "rtmp://{publicAddr}/local/{babyUid}"
		return strings.NewReplacer("{publicAddr}", app.Opts.RTMP.PublicAddr, "{babyUid}", babyUID).Replace(tpl)
	}

	return ""
}

// createTokenProvider creates a function that provides fresh auth tokens
// This is called by the remote relay when it needs to reconnect
func (app *App) createTokenProvider() stream.TokenProvider {
	return func() (string, error) {
		// Force re-authorization to get a fresh token
		if err := app.RestClient.MaybeAuthorize(true); err != nil {
			log.Error().Err(err).Msg("Failed to refresh auth token for remote stream")
			return "", err
		}
		return app.SessionStore.GetAuthToken(), nil
	}
}

// onAuthFailed is called when authentication fails and cannot be recovered
// This typically happens when the refresh token expires and 2FA is enabled
func (app *App) onAuthFailed(err error) {
	log.Error().
		Err(err).
		Msg("Authentication failed - remote streaming unavailable. " +
			"If you have 2FA enabled, you may need to get a new refresh token manually. " +
			"See documentation for instructions on obtaining a refresh token.")
}

// startNotificationPolling starts the notification polling manager
func (app *App) startNotificationPolling(ctx utils.GracefulContext) {
	// Build poller config from app options
	pollerConfig := notification.PollerConfig{
		PollInterval:   app.Opts.Notifications.PollInterval,
		Jitter:         app.Opts.Notifications.Jitter,
		MaxBackoff:     app.Opts.Notifications.MaxBackoff,
		MessageTimeout: app.Opts.Notifications.MessageTimeout,
		MessageLimit:   50,
		DedupMaxSize:   1000,
	}

	// Apply defaults if not set
	if pollerConfig.PollInterval == 0 {
		pollerConfig.PollInterval = 10 * time.Second
	}
	if pollerConfig.Jitter == 0 {
		pollerConfig.Jitter = 0.3
	}
	if pollerConfig.MaxBackoff == 0 {
		pollerConfig.MaxBackoff = 5 * time.Minute
	}
	if pollerConfig.MessageTimeout == 0 {
		pollerConfig.MessageTimeout = 5 * time.Minute
	}

	// Build sleep event poller config
	sleepEventPollerConfig := notification.SleepEventPollerConfig{
		PollInterval: app.Opts.Notifications.SleepEventPollInterval,
		Jitter:       pollerConfig.Jitter,
		MaxBackoff:   pollerConfig.MaxBackoff,
		EventLimit:   50,
		DedupMaxSize: 1000,
	}
	if sleepEventPollerConfig.PollInterval == 0 {
		sleepEventPollerConfig.PollInterval = 30 * time.Second
	}

	// Build stats poller config
	statsPollerConfig := notification.StatsPollerConfig{
		PollInterval: app.Opts.Notifications.StatsPollInterval,
		Jitter:       pollerConfig.Jitter,
		MaxBackoff:   pollerConfig.MaxBackoff,
	}
	if statsPollerConfig.PollInterval == 0 {
		statsPollerConfig.PollInterval = 60 * time.Second
	}

	// Build manager config
	managerConfig := notification.ManagerConfig{
		PollerConfig:           pollerConfig,
		TopicPrefix:            app.MQTTConnection.GetTopicPrefix(),
		Babies:                 app.SessionStore.Session.Babies,
		EnableSleepTracking:    app.Opts.Notifications.EnableSleepTracking,
		SleepEventPollerConfig: sleepEventPollerConfig,
		StatsPollerConfig:      statsPollerConfig,
	}

	// Create the MQTT adapter
	mqttAdapter := notification.NewPahoMQTTAdapter(app.MQTTConnection.GetClient())

	// Create the notification manager
	// Use the sleep tracking version if enabled, which also handles regular notifications
	if app.Opts.Notifications.EnableSleepTracking {
		app.notificationManager = notification.NewManagerWithSleepTracking(
			managerConfig,
			app.RestClient, // MessageFetcher
			app.RestClient, // SleepEventFetcher
			app.RestClient, // StatsFetcher
			mqttAdapter,
		)
		log.Info().
			Int("babies", len(managerConfig.Babies)).
			Dur("poll_interval", pollerConfig.PollInterval).
			Dur("sleep_event_interval", sleepEventPollerConfig.PollInterval).
			Dur("stats_interval", statsPollerConfig.PollInterval).
			Float64("jitter", pollerConfig.Jitter).
			Msg("Starting notification event polling with sleep tracking")
	} else {
		app.notificationManager = notification.NewManager(managerConfig, app.RestClient, mqttAdapter)
		log.Info().
			Int("babies", len(managerConfig.Babies)).
			Dur("poll_interval", pollerConfig.PollInterval).
			Float64("jitter", pollerConfig.Jitter).
			Msg("Starting notification event polling")
	}

	// Run in a child context
	ctx.RunAsChild(func(childCtx utils.GracefulContext) {
		// Convert GracefulContext to standard context
		stdCtx, cancel := context.WithCancel(context.Background())
		go func() {
			<-childCtx.Done()
			cancel()
		}()

		if err := app.notificationManager.Run(stdCtx); err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("Notification polling stopped with error")
		}
	})
}
