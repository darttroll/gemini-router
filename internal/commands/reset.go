package commands

import (
	"fmt"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

type ResetCommand struct {
	cfg   *config.Config
	store *storage.Store
}

func NewResetCommand(cfg *config.Config, store *storage.Store) *ResetCommand {
	return &ResetCommand{cfg: cfg, store: store}
}
func (c *ResetCommand) Run(worker string) error {
	if worker == "" {
		return fmt.Errorf("worker name is required. Usage: gemini-router reset <worker>")
	}
	if worker == "all" {
		return fmt.Errorf("reset 'all' is not supported by default, specify exact worker name")
	}
	capacity := c.cfg.Adaptive.RetryBudgetCapacity
	if capacity <= 0 {
		capacity = 10
	}
	if err := c.store.ResetControllerAvailability(worker, capacity); err != nil {
		return fmt.Errorf("failed to reset availability state for worker %s: %w", worker, err)
	}
	fmt.Printf("Availability breaker and retry budget reset for worker: %s (learned rate/concurrency preserved)\n", worker)
	return nil
}
