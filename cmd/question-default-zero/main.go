// Apply the explicitly requested, one-time question-default reset. Dry-run is
// the default; database credentials come only from the deployment environment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	apply := flag.Bool("apply", false, "commit the one-time reset; omitted means rollback after validation")
	flag.Parse()
	for _, key := range []string{"DB_HOST", "DB_USER", "DB_NAME"} {
		if os.Getenv(key) == "" {
			fmt.Fprintln(os.Stderr, "missing deployment database environment:", key)
			os.Exit(1)
		}
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	u := url.URL{Scheme: "postgres", Host: os.Getenv("DB_HOST") + ":" + port, Path: "/" + os.Getenv("DB_NAME"), User: url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"))}
	query := url.Values{"sslmode": {"disable"}}
	u.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not connect to deployment database")
		os.Exit(1)
	}
	rollback := errors.New("dry-run rollback")
	var report repository.QuestionDefaultsReport
	err = db.WithContext(context.Background()).Transaction(func(tx *gorm.DB) error {
		var err error
		report, err = repository.NewProcessingRepository(tx).ResetQuestionDefaults(context.Background())
		if err != nil {
			return err
		}
		if !*apply {
			return rollback
		}
		return nil
	})
	if err != nil && !errors.Is(err, rollback) {
		fmt.Fprintln(os.Stderr, "question defaults operation failed:", err)
		os.Exit(1)
	}
	json.NewEncoder(os.Stdout).Encode(struct {
		Applied bool                              `json:"applied"`
		Report  repository.QuestionDefaultsReport `json:"report"`
	}{*apply, report})
}
