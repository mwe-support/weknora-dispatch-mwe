package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGetUserByEmailIsCaseInsensitive(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:user_email_case?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&types.User{}); err != nil {
		t.Fatal(err)
	}
	repo := NewUserRepository(db)
	user := &types.User{ID: "user-1", Username: "Alina", Email: "Alina@Example.com", PasswordHash: "x", IsActive: true}
	if err := repo.CreateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}

	got, err := repo.GetUserByEmail(context.Background(), "alina@example.com")
	if err != nil || got == nil || got.ID != user.ID {
		t.Fatalf("case-insensitive lookup failed: user=%v err=%v", got, err)
	}
}
