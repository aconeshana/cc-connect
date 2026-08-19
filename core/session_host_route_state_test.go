package core

import "testing"

func TestInteractiveStateHostRouteIdentityUsesDurableSessionKey(t *testing.T) {
	state := &interactiveState{
		sessionHostRouteKey:        "feishu:chat:root:thread",
		sessionHostRouteGeneration: 7,
	}

	key, generation := state.hostRouteIdentity("/workspace:feishu:chat:root:thread")

	if key != "feishu:chat:root:thread" || generation != 7 {
		t.Fatalf("host route identity = %q/%d", key, generation)
	}
}
