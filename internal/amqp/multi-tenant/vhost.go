package amqp

import (
	"fmt"
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *MultiTenantConsumer) shouldHandleVhost(vhost string) bool {
	if len(c.config.AllowedVHosts) == 0 {
		return true
	}

	for _, allowed := range c.config.AllowedVHosts {
		if allowed == vhost {
			return true
		}
	}

	return false
}

func (c *MultiTenantConsumer) buildVhostURL(vhost string) (string, error) {
	baseURL := c.config.BrokerURL
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse broker url: %w", err)
	}

	if vhost == "" {
		parsed.Path = "/"
		parsed.RawPath = ""
	} else {
		encoded := "/" + url.PathEscape(vhost)
		parsed.Path = encoded
		parsed.RawPath = encoded
	}

	return parsed.String(), nil
}

func (c *MultiTenantConsumer) getOrCreateVhostConnection(vhost string) (*amqp.Connection, error) {
	c.vhostMu.Lock()
	defer c.vhostMu.Unlock()

	// Check if we have a pooled connection
	if pooled, exists := c.vhostConnections[vhost]; exists {
		// Check if connection is still valid
		if pooled.conn != nil && !pooled.conn.IsClosed() {
			pooled.refCount++
			return pooled.conn, nil
		}
		// Connection is closed, remove it and create a new one
		delete(c.vhostConnections, vhost)
	}

	// Create a new connection
	vhostURL, err := c.buildVhostURL(vhost)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(vhostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to vhost %s: %w", vhost, err)
	}

	c.vhostConnections[vhost] = &pooledConnection{
		conn:     conn,
		refCount: 1,
	}

	return conn, nil
}

func (c *MultiTenantConsumer) releaseVhostConnection(vhost string) {
	c.vhostMu.Lock()
	defer c.vhostMu.Unlock()

	pooled, exists := c.vhostConnections[vhost]
	if !exists {
		return
	}

	pooled.refCount--
	if pooled.refCount <= 0 {
		_ = pooled.conn.Close()
		delete(c.vhostConnections, vhost)
	}
}
