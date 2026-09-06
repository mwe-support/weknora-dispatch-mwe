package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestPasswordResetRoutesAllowUnauthenticatedPOSTOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"/api/v1/auth/password-reset/request", "/api/v1/auth/password-reset/confirm"} {
		if !isNoAuthAPI(path, http.MethodPost) || isNoAuthAPI(path, http.MethodGet) {
			t.Fatal("incorrect reset route authorization")
		}
		r := gin.New()
		r.Use(Auth(nil, nil, nil, nil, nil))
		r.POST(path, func(c *gin.Context) { c.Status(http.StatusNoContent) })
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
		if w.Code != http.StatusNoContent {
			t.Fatalf("reset route blocked without login: %d", w.Code)
		}
	}
}
