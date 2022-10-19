package store

import (
	"go.uber.org/zap"
	"testing"

	"github.com/streamingfast/dstore"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/require"
)

func newTestBaseStore(
	t *testing.T,
	updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy,
	valueType string,
	store dstore.Store,
) *baseStore {
	if store == nil {
		store = dstore.NewMockStore(nil)
	}

	config, err := NewConfig("test", 0, "test.module.hash", updatePolicy, valueType, store)
	require.NoError(t, err)
	return &baseStore{
		Config: config,
		kv:     make(map[string][]byte),
		logger: zap.NewNop(),
	}
}
