package client

import (
	"errors"
	"fmt"
	sync "sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/sacOO7/gowebsocket"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/session"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type readyState struct {
	Context    utils.GracefulContext
	Connection *WebsocketConnection
	Handlers   *handlerGroup
}

// handlerCleanupTimeout - how long run() waits for the ready handlers' cleanup before closing the socket
var handlerCleanupTimeout = 5 * time.Second

// handlerGroup - the ready handlers started within one websocket attempt
type handlerGroup struct {
	mu      sync.Mutex
	runners []utils.GracefulRunner
}

// wait - waits until the handlers returned, up to timeout. Returns false on timeout.
func (g *handlerGroup) wait(timeout time.Duration) bool {
	g.mu.Lock()
	runners := append([]utils.GracefulRunner(nil), g.runners...)
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		for _, runner := range runners {
			runner.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// WebsocketConnectionHandler - handler of ready connection
type WebsocketConnectionHandler func(*WebsocketConnection, utils.GracefulContext)

// WebsocketConnectionManager - connection manager
type WebsocketConnectionManager struct {
	BabyUID          string
	CameraUID        string
	Session          *session.Session
	API              *NanitClient
	BabyStateManager *baby.StateManager

	mu               sync.RWMutex
	readyState       *readyState
	readySubscribers []WebsocketConnectionHandler
}

// NewWebsocketConnectionManager - constructor
func NewWebsocketConnectionManager(babyUID string, cameraUID string, session *session.Session, api *NanitClient, babyStateManager *baby.StateManager) *WebsocketConnectionManager {
	manager := &WebsocketConnectionManager{
		BabyUID:          babyUID,
		CameraUID:        cameraUID,
		Session:          session,
		API:              api,
		BabyStateManager: babyStateManager,
	}

	manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop() // Ensure ticker is stopped even on panic

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.SendMessage(&Message{
					Type: Message_Type(Message_KEEPALIVE).Enum(),
				}); err != nil {
					log.Warn().Err(err).Msg("Failed to send keepalive")
				}
			}
		}
	})

	return manager
}

// WithReadyConnection - registers handler which will be called as a go routine upon ready connection
func (manager *WebsocketConnectionManager) WithReadyConnection(handler WebsocketConnectionHandler) {
	manager.mu.Lock()
	readyState := manager.readyState
	manager.readySubscribers = append(manager.readySubscribers, handler)
	manager.mu.Unlock()

	if readyState != nil {
		log.Debug().Msg("Immediately notifying ready handler")
		notifyReadyHandler(handler, *readyState)
	}
}

// RunWithinContext - starts websocket connection attempt loop
func (manager *WebsocketConnectionManager) RunWithinContext(ctx utils.GracefulContext) {
	utils.RunWithPerseverance(manager.run, ctx, utils.PerseverenceOpts{
		RunnerID:       fmt.Sprintf("websocket-%v", manager.CameraUID),
		ResetThreshold: 2 * time.Second,
		Cooldown: []time.Duration{
			// 2 * time.Second,
			30 * time.Second,
			2 * time.Minute,
			15 * time.Minute,
			1 * time.Hour,
		},
	})
}

func (manager *WebsocketConnectionManager) run(attempt utils.AttemptContext) {
	// Reauthorize if it is not a first try or we assume we don't have a valid token
	manager.API.MaybeAuthorize(attempt.GetTry() > 1)

	// Remote
	url := fmt.Sprintf("wss://api.nanit.com/focus/cameras/%v/user_connect", manager.CameraUID)
	auth := fmt.Sprintf("Bearer %v", manager.API.SessionStore.GetAuthToken())

	// Local
	// url := "wss://192.168.3.195:442"
	// auth := fmt.Sprintf("token %v", userCamToken)

	// -------

	var once sync.Once // Just because gowebsocket is buggy and can invoke OnDisconnect multiple times :-/
	handlers := &handlerGroup{}

	socket := gowebsocket.New(url)
	socket.RequestHeader.Set("Authorization", auth)

	// Handle new connection
	socket.OnConnected = func(socket gowebsocket.Socket) {
		log.Info().Str("url", url).Msg("Connected to websocket")

		go func() {
			conn := NewWebsocketConnection(&socket)
			readyState := readyState{attempt, conn, handlers}

			manager.mu.Lock()
			manager.readyState = &readyState
			subscribedHandlers := make([]WebsocketConnectionHandler, len(manager.readySubscribers))
			copy(subscribedHandlers, manager.readySubscribers)
			manager.mu.Unlock()

			manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(true))

			log.Trace().Int("num_handlers", len(subscribedHandlers)).Msg("Notifying websocket ready handlers")

			for _, handler := range subscribedHandlers {
				notifyReadyHandler(handler, readyState)
			}
		}()
	}

	// Handle failed attempts for connection
	socket.OnConnectError = func(err error, socket gowebsocket.Socket) {
		log.Error().Str("url", url).Err(err).Msg("Unable to establish websocket connection")
		attempt.Fail(err)
	}

	// Handle lost connection
	socket.OnDisconnected = func(err error, socket gowebsocket.Socket) {
		once.Do(func() {
			manager.BabyStateManager.Update(manager.BabyUID, *baby.NewState().SetWebsocketAlive(false))

			if err != nil {
				log.Error().Err(err).Msg("Disconnected from server")
				attempt.Fail(err)
			} else {
				log.Warn().Msg("Disconnected from server")
				attempt.Fail(errors.New("Server closed the connection"))
			}
		})
	}

	socket.OnBinaryMessage = func(data []byte, _ gowebsocket.Socket) {
		manager.handleBinaryMessage(data)
	}

	log.Trace().Msg("Connecting to websocket")
	socket.Connect()

	closeAfterHandlers(attempt, handlers, func() {
		if socket.IsConnected {
			log.Debug().Msg("Closing websocket")
			socket.Close()
		}
	})
}

// closeAfterHandlers - waits until the attempt ends, then lets its ready handlers finish
// their cleanup (which can still write to the socket, eg. STOPPED on shutdown) for up to
// handlerCleanupTimeout before it calls closeSocket
func closeAfterHandlers(attempt utils.GracefulContext, handlers *handlerGroup, closeSocket func()) {
	<-attempt.Done()
	if !handlers.wait(handlerCleanupTimeout) {
		log.Warn().Dur("timeout", handlerCleanupTimeout).Msg("Websocket handlers did not finish their cleanup in time, closing websocket anyway")
	}
	closeSocket()
}

// handleBinaryMessage processes incoming binary messages with nil-safety
func (manager *WebsocketConnectionManager) handleBinaryMessage(data []byte) {
	m := &Message{}
	err := proto.Unmarshal(data, m)
	if err != nil {
		log.Error().Err(err).Bytes("rawdata", data).Msg("Received malformed binary message")
		return
	}

	log.Debug().Stringer("data", m).Msg("Received message")

	manager.mu.RLock()
	readyState := manager.readyState
	manager.mu.RUnlock()

	// Safely handle nil readyState (can happen if message arrives before connection is ready)
	if readyState == nil || readyState.Connection == nil {
		log.Warn().Msg("Received message but connection not ready")
		return
	}

	go readyState.Connection.handleMessage(m)
}

func notifyReadyHandler(handler WebsocketConnectionHandler, state readyState) {
	// The lock spans RunAsChild, so wait() sees every handler that started before the
	// attempt was cancelled; RunAsChild does not start one after that.
	state.Handlers.mu.Lock()
	defer state.Handlers.mu.Unlock()
	runner := state.Context.RunAsChild(func(childCtx utils.GracefulContext) {
		handler(state.Connection, childCtx)
	})
	state.Handlers.runners = append(state.Handlers.runners, runner)
}
