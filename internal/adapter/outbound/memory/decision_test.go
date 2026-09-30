package memory_test

import (
	"testing"
	"time"

	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/decisiontest"
	"github.com/Thapanut/bidding-engine/internal/adapter/outbound/memory"
	"github.com/Thapanut/bidding-engine/internal/platform/system"
)

func TestDecisionStoreContract(t *testing.T) {
	decisiontest.Run(t, func(*testing.T) decisiontest.Harness {
		return decisiontest.Harness{Store: memory.NewDecisionStore(system.Clock{}), Now: time.Now}
	})
}
