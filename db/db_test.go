package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestIsTransientTransactionError(t *testing.T) {
	writeConflict := mongo.CommandError{
		Code:    112,
		Name:    "WriteConflict",
		Message: "Please retry your operation or multi-document transaction.",
		Labels:  []string{transientTransactionErrorLabel},
	}

	t.Run("nil", func(t *testing.T) {
		assert.False(t, IsTransientTransactionError(nil))
	})
	t.Run("command error with label", func(t *testing.T) {
		assert.True(t, IsTransientTransactionError(writeConflict))
	})
	t.Run("wrapped command error with label", func(t *testing.T) {
		assert.True(t, IsTransientTransactionError(fmt.Errorf("syncWithPeer: %w", writeConflict)))
	})
	t.Run("write exception with label", func(t *testing.T) {
		we := mongo.WriteException{Labels: []string{transientTransactionErrorLabel}}
		assert.True(t, IsTransientTransactionError(we))
	})
	t.Run("command error without label", func(t *testing.T) {
		other := mongo.CommandError{Code: 11000, Name: "DuplicateKey"}
		assert.False(t, IsTransientTransactionError(other))
	})
	t.Run("plain error", func(t *testing.T) {
		assert.False(t, IsTransientTransactionError(errors.New("boom")))
	})
	t.Run("no documents", func(t *testing.T) {
		assert.False(t, IsTransientTransactionError(mongo.ErrNoDocuments))
	})
}
