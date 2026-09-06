package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type passwordResetUserService struct {
	interfaces.UserService
	user       *types.User
	resetID    string
	resetValue string
	resetErr   error
}

func (s *passwordResetUserService) GetUserByEmail(context.Context, string) (*types.User, error) {
	return s.user, nil
}

func (s *passwordResetUserService) AdminResetPassword(_ context.Context, userID, password string) error {
	s.resetID, s.resetValue = userID, password
	return s.resetErr
}

func newPasswordResetHandler(t *testing.T, users *passwordResetUserService) (*AuthHandler, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	h := &AuthHandler{
		userService: users,
		redisClient: rdb,
		configInfo: &config.Config{Auth: &config.AuthConfig{PasswordReset: &config.PasswordResetConfig{
			Enabled: true, SMTPHost: "smtp.example.com", SMTPPort: 587,
			SMTPUsername: "sender@example.com", SMTPPassword: "secret", From: "sender@example.com",
		}}},
	}
	return h, mr
}

func authPasswordResetRouter(h *AuthHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Next()
		if len(c.Errors) == 0 || c.Writer.Written() {
			return
		}
		if appErr, ok := c.Errors.Last().Err.(*apperrors.AppError); ok {
			c.JSON(appErr.HTTPCode, gin.H{"success": false, "message": appErr.Message})
		}
	})
	r.POST("/request", h.RequestPasswordReset)
	r.POST("/confirm", h.ConfirmPasswordReset)
	return r
}

func postPasswordReset(t *testing.T, r http.Handler, path string, body map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestPasswordResetRequestAndConfirm(t *testing.T) {
	users := &passwordResetUserService{user: &types.User{ID: "user-1", Email: "alice@example.com", IsActive: true}}
	h, _ := newPasswordResetHandler(t, users)
	sentCode := make(chan string, 1)
	h.sendPasswordResetEmail = func(_ context.Context, email, code string) error {
		require.Equal(t, "alice@example.com", email)
		sentCode <- code
		return nil
	}
	r := authPasswordResetRouter(h)

	w := postPasswordReset(t, r, "/request", map[string]string{"email": "Alice@Example.com"})
	require.Equal(t, http.StatusOK, w.Code)
	code := <-sentCode
	require.Regexp(t, `^[0-9]{6}$`, code)

	w = postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "FreshPass9",
	})
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "user-1", users.resetID)
	require.Equal(t, "FreshPass9", users.resetValue)

	w = postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "AnotherPass9",
	})
	require.Equal(t, http.StatusBadRequest, w.Code, "code must be one-time")
}

func TestPasswordResetRequestDoesNotRevealUnknownEmail(t *testing.T) {
	h, _ := newPasswordResetHandler(t, &passwordResetUserService{})
	sent := make(chan struct{}, 1)
	h.sendPasswordResetEmail = func(context.Context, string, string) error { sent <- struct{}{}; return nil }
	w := postPasswordReset(t, authPasswordResetRouter(h), "/request", map[string]string{"email": "missing@example.com"})
	require.Equal(t, http.StatusOK, w.Code)
	select {
	case <-sent:
		t.Fatal("unknown email triggered delivery")
	default:
	}
}

func TestPasswordResetKnownAndUnknownEmailReturnSameBody(t *testing.T) {
	known, _ := newPasswordResetHandler(t, &passwordResetUserService{user: &types.User{ID: "user", IsActive: true}})
	unknown, _ := newPasswordResetHandler(t, &passwordResetUserService{})
	known.sendPasswordResetEmail = func(context.Context, string, string) error { return nil }
	body := map[string]string{"email": "fixture@example.com"}
	a := postPasswordReset(t, authPasswordResetRouter(known), "/request", body)
	b := postPasswordReset(t, authPasswordResetRouter(unknown), "/request", body)
	require.Equal(t, a.Code, b.Code)
	require.JSONEq(t, a.Body.String(), b.Body.String())
}

func TestPasswordResetRejectsWeakPasswordBeforeConsume(t *testing.T) {
	users := &passwordResetUserService{user: &types.User{ID: "user-1", Email: "alice@example.com", IsActive: true}}
	h, _ := newPasswordResetHandler(t, users)
	codeCh := make(chan string, 1)
	h.sendPasswordResetEmail = func(_ context.Context, _, value string) error { codeCh <- value; return nil }
	r := authPasswordResetRouter(h)
	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/request", map[string]string{"email": "alice@example.com"}).Code)
	code := <-codeCh
	require.Equal(t, http.StatusBadRequest, postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "password",
	}).Code)
	require.Empty(t, users.resetID)

	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "FreshPass9",
	}).Code, "weak-password validation must not consume the code")
}

func TestPasswordResetPersistenceFailureDoesNotRestoreConsumedCode(t *testing.T) {
	users := &passwordResetUserService{
		user:     &types.User{ID: "user-1", Email: "alice@example.com", IsActive: true},
		resetErr: errors.New("database unavailable"),
	}
	h, _ := newPasswordResetHandler(t, users)
	codeCh := make(chan string, 1)
	h.sendPasswordResetEmail = func(_ context.Context, _, code string) error { codeCh <- code; return nil }
	r := authPasswordResetRouter(h)
	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/request", map[string]string{"email": "alice@example.com"}).Code)
	code := <-codeCh

	w := postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "FreshPass9",
	})
	require.Equal(t, http.StatusInternalServerError, w.Code)

	users.resetErr = nil
	w = postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": code, "new_password": "FreshPass9",
	})
	require.Equal(t, http.StatusBadRequest, w.Code, "an ambiguous DB result must not make the code reusable")
}

func TestPasswordResetNewCodeResetsAttemptBudget(t *testing.T) {
	users := &passwordResetUserService{user: &types.User{ID: "user-1", Email: "alice@example.com", IsActive: true}}
	h, mr := newPasswordResetHandler(t, users)
	codeCh := make(chan string, 2)
	h.sendPasswordResetEmail = func(_ context.Context, _, code string) error { codeCh <- code; return nil }
	r := authPasswordResetRouter(h)
	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/request", map[string]string{"email": "alice@example.com"}).Code)
	<-codeCh
	for range 5 {
		require.Equal(t, http.StatusBadRequest, postPasswordReset(t, r, "/confirm", map[string]string{
			"email": "alice@example.com", "code": "000000", "new_password": "FreshPass9",
		}).Code)
	}

	mr.FastForward(passwordResetCooldown + time.Second)
	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/request", map[string]string{"email": "alice@example.com"}).Code)
	newCode := <-codeCh
	require.Equal(t, http.StatusBadRequest, postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": "000000", "new_password": "FreshPass9",
	}).Code)
	require.Equal(t, http.StatusOK, postPasswordReset(t, r, "/confirm", map[string]string{
		"email": "alice@example.com", "code": newCode, "new_password": "FreshPass9",
	}).Code)
}
