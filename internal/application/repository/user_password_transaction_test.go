package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPasswordResetTransactionRollsBackWhenSessionRevocationFails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.User{}))
	user := &types.User{ID: "user", Username: "fixture", Email: "fixture@example.com", PasswordHash: "original-hash"}
	require.NoError(t, db.Omit("tenant_id").Create(user).Error)
	r := &userRepository{db: db}
	// No auth_tokens table: fail the second write inside the real transaction.
	require.Error(t, r.ResetPasswordAndRevokeTokens(context.Background(), "user", "new-hash"))
	current, err := r.GetUserByID(context.Background(), "user")
	require.NoError(t, err)
	require.Equal(t, "original-hash", current.PasswordHash)
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}))
	require.NoError(t, db.Create(&types.AuthToken{ID: "token", UserID: "user", Token: "synthetic-session"}).Error)
	require.NoError(t, r.ResetPasswordAndRevokeTokens(context.Background(), "user", "new-hash"))
	current, err = r.GetUserByID(context.Background(), "user")
	require.NoError(t, err)
	require.Equal(t, "new-hash", current.PasswordHash)
	var token types.AuthToken
	require.NoError(t, db.First(&token, "id = ?", "token").Error)
	require.True(t, token.IsRevoked)
}
