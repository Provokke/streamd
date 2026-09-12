package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/Provokke/streamd/broker"
	"github.com/Provokke/streamd/client"
	"github.com/Provokke/streamd/server"
)

type pki struct {
	caPool *x509.CertPool
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &pki{caPool: pool, ca: ca, caKey: key}
}

// issue creates a leaf certificate valid for 127.0.0.1 / localhost.
func (p *pki) issue(t *testing.T, serial int64, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "leaf"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestTLS(t *testing.T) {
	p := newPKI(t)
	srvCfg := &tls.Config{Certificates: []tls.Certificate{p.issue(t, 2, x509.ExtKeyUsageServerAuth)}, MinVersion: tls.VersionTLS12}
	e := start(t, t.TempDir(), broker.Config{}, server.Config{TLS: srvCfg, AuthToken: "tok"})
	ctx := context.Background()

	c, err := client.Dial(ctx, e.addr, client.WithToken("tok"),
		client.WithTLS(&tls.Config{RootCAs: p.caPool}))
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer c.Close()
	sub, _ := c.Subscribe(ctx, "s", client.SubscribeOptions{})
	c.Publish(ctx, "s", "", []byte("secure"))
	if m := recv(t, sub); string(m.Value) != "secure" {
		t.Fatalf("got %+v", m)
	}

	// Plaintext client against a TLS server must not get in.
	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if pc, err := client.Dial(dctx, e.addr, client.WithToken("tok"), client.WithPingInterval(0)); err == nil {
		pc.Close()
		t.Fatal("plaintext client connected to TLS server")
	}
	// Untrusted CA is rejected.
	other := newPKI(t)
	if oc, err := client.Dial(dctx, e.addr, client.WithToken("tok"),
		client.WithTLS(&tls.Config{RootCAs: other.caPool})); err == nil {
		oc.Close()
		t.Fatal("client trusted the wrong CA")
	}
}

func TestMutualTLS(t *testing.T) {
	p := newPKI(t)
	srvCfg := &tls.Config{
		Certificates: []tls.Certificate{p.issue(t, 2, x509.ExtKeyUsageServerAuth)},
		ClientCAs:    p.caPool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	e := start(t, t.TempDir(), broker.Config{}, server.Config{TLS: srvCfg})
	ctx := context.Background()

	good, err := client.Dial(ctx, e.addr, client.WithTLS(&tls.Config{
		RootCAs: p.caPool, Certificates: []tls.Certificate{p.issue(t, 3, x509.ExtKeyUsageClientAuth)}}))
	if err != nil {
		t.Fatalf("mTLS dial: %v", err)
	}
	defer good.Close()
	if err := good.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// No client certificate: the server tears the connection down. In TLS 1.3 the
	// client's handshake can finish before the server rejects it, so the failure
	// surfaces on first use.
	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if nc, err := client.Dial(dctx, e.addr, client.WithTLS(&tls.Config{RootCAs: p.caPool}), client.WithPingInterval(0)); err == nil {
		defer nc.Close()
		if err := nc.Ping(dctx); err == nil {
			t.Fatal("connection without client cert was served")
		}
	}
}
