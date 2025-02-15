package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type SessionManager struct {
	redisClient *redis.Client
	lockTTL     time.Duration
}

func NewSessionManager(redisClient *redis.Client, lockTTL time.Duration) *SessionManager {
	return &SessionManager{
		redisClient: redisClient,
		lockTTL:     lockTTL,
	}
}

// --------------------------------------
// Hash-based
// --------------------------------------

func (sm *SessionManager) ProcessWithSession(ctx context.Context, userID string) error {
	return sm.processWithRetry(ctx, userID)
}

func (sm *SessionManager) processWithRetry(ctx context.Context, userID string) error {
	sessionID, err := sm.getAvailableSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to get session: %v", err)
	}

	lockKey := fmt.Sprintf("lock:%s", sessionID)
	locked, err := sm.redisClient.SetNX(ctx, lockKey, userID, sm.lockTTL).Result()
	if err != nil {
		return fmt.Errorf("failed to lock session: %v", err)
	}
	if !locked {
		fmt.Printf("[Pod Process] Session %s is locked, creating new session for user %s\n", sessionID, userID)
		return sm.processWithRetry(ctx, userID)
	}

	fmt.Printf("[Pod Process] User %s starting process with sessionID: %s\n", userID, sessionID)
	time.Sleep(5 * time.Second) // Simulate API call

	if userID == "failing_user" {
		sm.redisClient.Del(ctx, lockKey)
		sm.redisClient.HDel(ctx, "sessions", sessionID)
		fmt.Printf("[Pod Process] Retrying with new session for user %s\n", userID)
		return sm.processWithRetry(ctx, userID)
	}

	fmt.Printf("[Pod Process] User %s completed process with sessionID: %s\n", userID, sessionID)

	sm.redisClient.Del(ctx, lockKey)
	sm.releaseSession(ctx, sessionID)

	return nil
}

func (sm *SessionManager) getAvailableSession(ctx context.Context) (string, error) {
	var selectedSession string

	err := sm.redisClient.Watch(ctx, func(tx *redis.Tx) error {
		sessions, err := tx.HGetAll(ctx, "sessions").Result()
		if err != nil {
			return err
		}

		if len(sessions) == 0 {
			fmt.Println("[Redis] Case 1: Creating first session")
			selectedSession = generateSessionID()
			return tx.HSet(ctx, "sessions", selectedSession, "in_use").Err()
		}

		for sessionID, status := range sessions {
			if status == "available" {
				fmt.Printf("[Redis] Case 3: Using existing session: %s\n", sessionID)
				selectedSession = sessionID
				return tx.HSet(ctx, "sessions", selectedSession, "in_use").Err()
			}
		}

		newSessionID := generateSessionID()
		fmt.Printf("[Redis] Case 2: Creating new session: %s\n", newSessionID)
		selectedSession = newSessionID
		return tx.HSet(ctx, "sessions", selectedSession, "in_use").Err()
	}, "sessions")

	return selectedSession, err
}

func (sm *SessionManager) releaseSession(ctx context.Context, sessionID string) error {
	err := sm.redisClient.HSet(ctx, "sessions", sessionID, "available").Err()
	fmt.Println("[Redis] Session released:", sessionID)
	fmt.Println("Error releasing session:", err)
	return err
}

func generateSessionID() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 12) // 12 character random string
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return fmt.Sprintf("session-%s", string(b))
}

// --------------------------------------
// List-based
// --------------------------------------

func (sm *SessionManager) NewProcessWithSession(ctx context.Context, userID string) error {
	sessionID, err := sm.newGetAvailableSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to get session: %v", err)
	}

	lockKey := fmt.Sprintf("lock:%s", sessionID)
	locked, err := sm.redisClient.SetNX(ctx, lockKey, userID, sm.lockTTL).Result()
	if err != nil {
		return fmt.Errorf("failed to lock session: %v", err)
	}
	if !locked {
		fmt.Printf("[Pod Process] Session %s is locked, returning to pool for user %s\n", sessionID, userID)
		return sm.NewProcessWithSession(ctx, userID)
	}

	fmt.Printf("[Pod Process] User %s starting process with sessionID: %s\n", userID, sessionID)
	time.Sleep(30 * time.Second) // Simulate API call

	if userID == "failing_user" {
		sm.redisClient.Del(ctx, lockKey)
		fmt.Printf("[Pod Process] Retrying with new session for user %s\n", userID)
		return sm.NewProcessWithSession(ctx, userID)
	}

	fmt.Printf("[Pod Process] User %s completed process with sessionID: %s\n", userID, sessionID)
	sm.redisClient.Del(ctx, lockKey)
	sm.returnSessionToPool(ctx, sessionID)

	return nil
}

func (sm *SessionManager) newGetAvailableSession(ctx context.Context) (string, error) {
	sessionID, err := sm.redisClient.RPop(ctx, "session_pool").Result()
	if err == redis.Nil {
		sessionID = generateSessionID()
		fmt.Printf("[Redis] Creating new session: %s\n", sessionID)
	} else if err != nil {
		return "", err
	}
	fmt.Printf("[Redis] Using session: %s\n", sessionID)
	return sessionID, nil
}

func (sm *SessionManager) returnSessionToPool(ctx context.Context, sessionID string) error {
	err := sm.redisClient.LPush(ctx, "session_pool", sessionID).Err()
	if err != nil {
		return fmt.Errorf("failed to return session to pool: %v", err)
	}
	fmt.Printf("[Redis] Session returned to pool: %s\n", sessionID)
	return nil
}

// --------------------------------------
// Main
// --------------------------------------

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	sm := NewSessionManager(rdb, 5*time.Minute)
	ctx := context.Background()

	// Stress test parameters
	numConcurrentUsers := 5

	// Create channels for results
	results := make(chan string, numConcurrentUsers)
	errors := make(chan error, numConcurrentUsers)

	start := time.Now()
	// Start concurrent users
	for i := 0; i < numConcurrentUsers; i++ {
		go func(userNum int) {
			userID := fmt.Sprintf("user%d", userNum)
			err := sm.NewProcessWithSession(ctx, userID)
			//
			// err := sm.ProcessWithSession(ctx, userID)

			if err != nil {
				errors <- fmt.Errorf("user %s error: %v", userID, err)
				return
			}
			results <- fmt.Sprintf("user %s completed successfully", userID)
		}(i)

	}

	// Collect results
	totalRequests := numConcurrentUsers
	for i := 0; i < totalRequests; i++ {
		select {
		case result := <-results:
			fmt.Printf("Success: %s\n", result)
		case err := <-errors:
			fmt.Printf("Error: %s\n", err)
		}
	}
	elapsed := time.Since(start).Seconds()
	fmt.Printf("Total time: %.2f seconds\n", elapsed)

	rdb.FlushAll(ctx)
}
