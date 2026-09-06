package handler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

const (
	passwordResetCodeTTL  = 10 * time.Minute
	passwordResetCooldown = time.Minute
	passwordResetAttempts = 5
)

type passwordResetChallenge struct {
	UserID   string `json:"user_id"`
	CodeHash string `json:"code_hash"`
}

var passwordResetDummyHash = func() []byte {
	hash, err := bcrypt.GenerateFromPassword([]byte("000000"), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return hash
}()

func (h *AuthHandler) passwordResetReady() bool {
	return h != nil && h.redisClient != nil && h.configInfo != nil && h.configInfo.Auth != nil &&
		h.configInfo.Auth.PasswordReset.Ready()
}

// RequestPasswordReset always returns the same success body for known and
// unknown emails so the endpoint cannot be used for account enumeration.
func (h *AuthHandler) RequestPasswordReset(c *gin.Context) {
	if !h.passwordResetReady() {
		c.Error(apperrors.NewServiceUnavailableError("Password reset is not configured"))
		return
	}
	var req struct {
		Email string `json:"email" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewValidationError("A valid email is required"))
		return
	}
	email, err := normalizePasswordResetEmail(req.Email)
	if err != nil {
		c.Error(apperrors.NewValidationError("A valid email is required"))
		return
	}
	ctx := c.Request.Context()
	keyID := passwordResetKeyID(email)
	allowed, err := h.redisClient.SetNX(ctx, "auth:password-reset:cooldown:"+keyID, "1", passwordResetCooldown).Result()
	if err != nil {
		c.Error(apperrors.NewServiceUnavailableError("Password reset is temporarily unavailable"))
		return
	}
	if !allowed {
		passwordResetRequestAccepted(c)
		return
	}

	code, err := generatePasswordResetCode()
	if err != nil {
		logger.Errorf(ctx, "password reset code generation failed: %v", err)
		passwordResetRequestAccepted(c)
		return
	}
	// Hash for every syntactically valid request, including unknown emails, so
	// account existence is not exposed by the large bcrypt timing difference.
	codeHash, err := bcrypt.GenerateFromPassword([]byte(code), bcrypt.DefaultCost)
	if err != nil {
		logger.Errorf(ctx, "password reset code hashing failed: %v", err)
		passwordResetRequestAccepted(c)
		return
	}
	user, err := h.userService.GetUserByEmail(ctx, email)
	if err != nil || user == nil || !user.IsActive {
		passwordResetRequestAccepted(c)
		return
	}
	challengeJSON, _ := json.Marshal(passwordResetChallenge{UserID: user.ID, CodeHash: string(codeHash)})
	challengeKey := "auth:password-reset:code:" + keyID
	if err := h.redisClient.Set(ctx, challengeKey, challengeJSON, passwordResetCodeTTL).Err(); err != nil {
		logger.Errorf(ctx, "password reset challenge persistence failed: %v", err)
		passwordResetRequestAccepted(c)
		return
	}
	sender := h.sendPasswordResetEmail
	if sender == nil {
		sender = h.sendConfiguredPasswordResetEmail
	}
	go func(userID string) {
		mailCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := sender(mailCtx, email, code); err != nil {
			_ = h.deletePasswordResetChallengeIfMatch(context.Background(), challengeKey, challengeJSON)
			logger.Errorf(context.Background(), "password reset email delivery failed for user %s: %v", userID, err)
		}
	}(user.ID)
	passwordResetRequestAccepted(c)
}

func passwordResetRequestAccepted(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "If the account exists, a verification code has been sent",
	})
}

func (h *AuthHandler) ConfirmPasswordReset(c *gin.Context) {
	if !h.passwordResetReady() {
		c.Error(apperrors.NewServiceUnavailableError("Password reset is not configured"))
		return
	}
	var req struct {
		Email       string `json:"email" binding:"required"`
		Code        string `json:"code" binding:"required"`
		NewPassword string `json:"new_password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(apperrors.NewValidationError("Email, code, and new password are required"))
		return
	}
	if err := service.ValidatePasswordPolicy(req.NewPassword); err != nil {
		c.Error(apperrors.NewValidationError(err.Error()))
		return
	}
	email, err := normalizePasswordResetEmail(req.Email)
	if err != nil || len(req.Code) != 6 || strings.Trim(req.Code, "0123456789") != "" {
		c.Error(apperrors.NewValidationError("Invalid or expired verification code"))
		return
	}
	ctx := c.Request.Context()
	keyID := passwordResetKeyID(email)
	challengeKey := "auth:password-reset:code:" + keyID
	raw, err := h.redisClient.Get(ctx, challengeKey).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			c.Error(apperrors.NewServiceUnavailableError("Password reset is temporarily unavailable"))
			return
		}
		_ = bcrypt.CompareHashAndPassword(passwordResetDummyHash, []byte(req.Code))
		c.Error(apperrors.NewValidationError("Invalid or expired verification code"))
		return
	}
	// Reserve the attempt BEFORE checking the code. Both the budget and the
	// compare-and-consume operation are tied to this exact challenge, so old
	// requests cannot exhaust/delete a freshly issued code.
	allowed, err := h.reservePasswordResetAttempt(ctx, challengeKey, raw)
	if err != nil {
		c.Error(apperrors.NewServiceUnavailableError("Password reset is temporarily unavailable"))
		return
	}
	var challenge passwordResetChallenge
	if !allowed || json.Unmarshal(raw, &challenge) != nil || challenge.UserID == "" {
		_ = bcrypt.CompareHashAndPassword(passwordResetDummyHash, []byte(req.Code))
		c.Error(apperrors.NewValidationError("Invalid or expired verification code"))
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(challenge.CodeHash), []byte(req.Code)) != nil {
		c.Error(apperrors.NewValidationError("Invalid or expired verification code"))
		return
	}
	consumed, err := h.consumePasswordResetChallenge(ctx, challengeKey, raw)
	if err != nil || !consumed {
		c.Error(apperrors.NewValidationError("Invalid or expired verification code"))
		return
	}
	// Consumption itself is the one-winner guard; no expiring per-email lock
	// is needed. Never restore a code after a DB error: the commit result can
	// be ambiguous after a lost connection. A fresh code is safe to request.
	if err := h.userService.AdminResetPassword(ctx, challenge.UserID, req.NewPassword); err != nil {
		logger.Errorf(ctx, "password reset persistence failed for user %s: %v", challenge.UserID, err)
		c.Error(apperrors.NewInternalServerError("Password reset failed; request a new verification code"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Password reset successfully"})
}

func (h *AuthHandler) reservePasswordResetAttempt(ctx context.Context, key string, raw []byte) (bool, error) {
	attemptKey := key + ":attempts:" + passwordResetKeyID(string(raw))
	n, err := h.redisClient.Eval(ctx, `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
local ttl = redis.call("PTTL", KEYS[1])
if ttl <= 0 then return 0 end
local attempts = redis.call("INCR", KEYS[2])
if attempts == 1 then redis.call("PEXPIRE", KEYS[2], ttl) end
if attempts > tonumber(ARGV[2]) then return 0 end
return 1`, []string{key, attemptKey}, string(raw), passwordResetAttempts).Int64()
	return n == 1, err
}

func (h *AuthHandler) consumePasswordResetChallenge(ctx context.Context, key string, raw []byte) (bool, error) {
	n, err := h.redisClient.Eval(ctx,
		`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
		[]string{key}, string(raw)).Int64()
	return n == 1, err
}

func (h *AuthHandler) deletePasswordResetChallengeIfMatch(ctx context.Context, key string, value []byte) error {
	return h.redisClient.Eval(ctx,
		`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
		[]string{key}, string(value)).Err()
}

func normalizePasswordResetEmail(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	address, err := mail.ParseAddress(value)
	if err != nil || strings.ToLower(address.Address) != value {
		return "", fmt.Errorf("invalid email")
	}
	return value, nil
}

func passwordResetKeyID(email string) string {
	sum := sha256.Sum256([]byte(email))
	return hex.EncodeToString(sum[:])
}

func generatePasswordResetCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}
