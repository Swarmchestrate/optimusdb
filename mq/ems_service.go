package mq

//Update 22.09.2025
// Updated 18.02.2026 — pass actual msg.Destination to callback
import (
	"fmt"
	"optimusdb/logger"
	"sync"
	"time"

	"github.com/go-stomp/stomp"
)

type EMSService struct {
	cfg        Config
	retryDelay time.Duration

	mu       sync.RWMutex
	conn     *stomp.Conn
	stopped  chan struct{}
	onceStop sync.Once

	onMessage      func(dest string, msg *stomp.Message)
	onConnected    func()
	onDisconnected func(error)
}

// NewEMSService creates the service but does not block
func NewEMSService(cfg Config, retryDelay time.Duration) *EMSService {
	return &EMSService{
		cfg:        cfg,
		retryDelay: retryDelay,
		stopped:    make(chan struct{}),
	}
}

// Start runs the background connection loop
func (s *EMSService) Start() {
	go s.loop()
}

/*
	func (s *EMSService) loop() {
		for {
			select {
			case <-s.stopped:
				return
			default:
			}

			if !s.isConnected() {
				if err := s.connect(); err != nil {
					log.Printf("[EMS] Connect failed: %v (retry in %s)", err, s.retryDelay)
					time.Sleep(s.retryDelay)
					continue
				}
			}

			// heartbeat / check loop
			time.Sleep(s.retryDelay)
			if s.conn != nil && s.conn.Err() != nil {
				log.Printf("[EMS] Connection lost: %v", s.conn.Err())
				s.disconnect()
			}
		}
	}
*/
func (s *EMSService) loop() {
	const (
		initialDelay = 5 * time.Second
		maxDelay     = 5 * time.Minute
	)

	retryDelay := initialDelay

	for {
		select {
		case <-s.stopped:
			return
		default:
		}

		// -----------------------------
		// CONNECT PHASE
		// -----------------------------
		if !s.isConnected() {
			if err := s.connect(); err != nil {
				logger.Warn("[WARN] EMS connect failed: %v (retry in %s)", err, retryDelay)

				// Backoff before retry
				time.Sleep(retryDelay)

				// Exponential increase capped at 5 minutes
				retryDelay = nextDelay(retryDelay, maxDelay)
				continue
			}

			// Successful connection → reset delay
			retryDelay = initialDelay
		}

		// -----------------------------
		// HEALTH CHECK
		// -----------------------------
		time.Sleep(retryDelay)

		if s.isConnected() {
			err := s.Send("/queue/optimusdb-health", "text/plain", []byte("ping"))
			if err != nil {
				logger.Warn("[ERROR] EMS heartbeat failed → reconnecting: %v", err)
				s.disconnect()

				// Wait with backoff before reconnect
				time.Sleep(retryDelay)
				retryDelay = nextDelay(retryDelay, maxDelay)
			}
		}
	}
}

/*
///////Initial retries:
//
//5s → 10s → 20s → 40s → 80s → 160s → 5min (cap)
//
//After success:
//
//Delay resets to 5 seconds for fast health checks.
//After failure:
//
//Delay grows until maximum 5 minutes between attempts.
*/
func nextDelay(current, max time.Duration) time.Duration {
	// Double the delay
	next := current * 2

	// Cap at max allowed delay
	if next > max {
		return max
	}
	return next
}

func (s *EMSService) connect() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	opts := []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(s.cfg.User, s.cfg.Pass),
		stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
	}
	if s.cfg.ClientID != "" {
		opts = append(opts, stomp.ConnOpt.Header("client-id", s.cfg.ClientID))
	}

	conn, err := stomp.Dial("tcp", addr, opts...)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	//log.Printf("[EMS] Connected to STOMP at %s", addr)
	logger.Info("[INFO] OptimusDB Connected to EMS STOMP at %s", addr)
	if s.onConnected != nil {
		s.onConnected()
	}

	if s.cfg.Topic != "" {
		sub, err := conn.Subscribe(s.cfg.Topic, stomp.AckAuto)
		if err != nil {
			logger.Error("[ERROR] OptimusDB failed to subscribe to EMS STOMP at %v", err)
			return fmt.Errorf("subscribe failed: %w", err)
		}
		go func() {
			for msg := range sub.C {
				if s.onMessage != nil {
					// ── CHANGED 18.02.2026: pass actual message destination ──
					// msg.Destination = real topic (e.g. /topic/response_time_SENSOR)
					// s.cfg.Topic     = subscription pattern (e.g. /topic/>)
					dest := s.cfg.Topic
					if msg != nil && msg.Destination != "" {
						dest = msg.Destination
					}
					s.onMessage(dest, msg)
				}
			}
		}()
	}

	return nil
}

func (s *EMSService) disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Disconnect()
		s.conn = nil
		if s.onDisconnected != nil {
			s.onDisconnected(fmt.Errorf("connection closed"))
		}
	}
}

func (s *EMSService) isConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn != nil
}

func (s *EMSService) Send(dest, contentType string, body []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.conn == nil {
		return fmt.Errorf("not connected")
	}
	return s.conn.Send(dest, contentType, body)
}

func (s *EMSService) OnMessage(handler func(dest string, msg *stomp.Message)) {
	s.mu.Lock()
	s.onMessage = handler
	s.mu.Unlock()
}

func (s *EMSService) Stop() {
	s.onceStop.Do(func() {
		close(s.stopped)
	})
	s.disconnect()
}

func (s *EMSService) OnConnected(handler func()) {
	s.onConnected = handler
}
func (s *EMSService) OnDisconnected(handler func(err error)) {
	s.onDisconnected = handler
}
