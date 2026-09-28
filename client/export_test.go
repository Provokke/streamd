package client

// Test-only hooks for the external client_test package.

func SetHookBeforeSubscribeCall(f func()) { hookBeforeSubscribeCall = f }

// KillConn closes the current connection without telling the client.
func (c *Client) KillConn() {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()
	nc.Close()
}

func SetHookAfterPendingSwap(f func()) { hookAfterPendingSwap = f }

func SetHookBeforeCallOn(f func()) { hookBeforeCallOn = f }
