// Package e2e drives the real streamd and streamctl binaries.
package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var binDir string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() { // tests skip themselves; don't pay for building
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "streamd-e2e-")
	if err != nil {
		panic(err)
	}
	binDir = dir
	build := exec.Command("go", "build", "-o", dir, "github.com/Provokke/streamd/cmd/streamd", "github.com/Provokke/streamd/cmd/streamctl")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build failed:", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func skipShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("e2e builds and runs the real binaries; skipped with -short")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// syncBuf is a bytes.Buffer safe for exec's copier goroutine to write while the
// test reads it.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type daemon struct {
	t    *testing.T
	cmd  *exec.Cmd
	addr string
	data string
	log  *syncBuf
	done chan error
}

// startd launches streamd and waits until it accepts TCP connections.
func startd(t *testing.T, data string, extra ...string) *daemon {
	t.Helper()
	addr := freeAddr(t)
	args := append([]string{"-addr", addr, "-data", data}, extra...)
	d := &daemon{t: t, addr: addr, data: data, log: &syncBuf{}, done: make(chan error, 1)}
	d.cmd = exec.Command(filepath.Join(binDir, "streamd"), args...)
	d.cmd.Stderr = d.log
	if err := d.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { d.done <- d.cmd.Wait() }()
	t.Cleanup(func() { d.cmd.Process.Kill(); <-d.done })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return d
		}
		select {
		case err := <-d.done:
			t.Fatalf("streamd exited early: %v\n%s", err, d.log)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("streamd never listened\n%s", d.log)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (d *daemon) kill9() {
	d.cmd.Process.Signal(syscall.SIGKILL)
	<-d.done
	d.done <- nil // keep Cleanup's receive from blocking
}

func (d *daemon) term() error {
	d.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-d.done:
		d.done <- nil
		return err
	case <-time.After(10 * time.Second):
		d.t.Fatalf("streamd ignored SIGTERM\n%s", d.log)
		return nil
	}
}

// ctl runs streamctl and returns stdout. A nonzero exit is returned as err
// with stderr appended to the output.
func ctl(t *testing.T, addr string, stdin string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(binDir, "streamctl"), append([]string{"-addr", addr}, args...)...)
	cmd.Stdin = strings.NewReader(stdin)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		return out.String() + errb.String(), err
	}
	return out.String(), nil
}

func mustCtl(t *testing.T, addr, stdin string, args ...string) string {
	t.Helper()
	out, err := ctl(t, addr, stdin, args...)
	if err != nil {
		t.Fatalf("streamctl %v: %v\n%s", args, err, out)
	}
	return out
}

func TestCLIWorkflow(t *testing.T) {
	skipShort(t)
	d := startd(t, t.TempDir())
	a := d.addr
	if out := mustCtl(t, a, "", "ping"); !strings.HasPrefix(out, "pong") {
		t.Fatalf("ping: %q", out)
	}
	mustCtl(t, a, "", "create", "orders", "-partitions", "3")
	if out, err := ctl(t, a, "", "create", "orders", "-partitions", "5"); err == nil {
		t.Fatalf("changing partitions should fail: %s", out)
	}
	if out := mustCtl(t, a, "", "pub", "orders", "-key", "k1", "hello", "world"); !strings.Contains(out, "/0") {
		t.Fatalf("pub: %q", out)
	}
	// 10 lines from stdin, 4 per request -> 10 receipts.
	var in strings.Builder
	for i := 1; i <= 10; i++ {
		fmt.Fprintf(&in, "line-%d\n", i)
	}
	out := mustCtl(t, a, in.String(), "pub", "orders", "-key", "kb", "-partition", "1", "-batch", "4")
	if n := len(strings.Fields(out)); n != 10 {
		t.Fatalf("batch receipts: %d\n%s", n, out)
	}

	topics := mustCtl(t, a, "", "topics")
	if !strings.Contains(topics, "orders") || strings.Count(topics, "orders") != 3 {
		t.Fatalf("topics:\n%s", topics)
	}

	// Consume 5 from partition 1 as a group, then look at group state.
	got := mustCtl(t, a, "", "sub", "orders", "-group", "g1", "-from", "earliest", "-partition", "-1", "-n", "5")
	if len(strings.Split(strings.TrimSpace(got), "\n")) != 5 {
		t.Fatalf("sub output:\n%s", got)
	}
	time.Sleep(400 * time.Millisecond) // the exiting subscriber leaves the group
	groups := mustCtl(t, a, "", "groups", "-group", "g1")
	if !strings.Contains(groups, "g1") || !strings.Contains(groups, "orders") {
		t.Fatalf("groups:\n%s", groups)
	}

	// Reset the stopped group to earliest and replay everything: 11 messages.
	if out := mustCtl(t, a, "", "reset", "orders", "-group", "g1", "-to", "earliest"); strings.Count(out, "next offset 0") != 3 {
		t.Fatalf("reset:\n%s", out)
	}
	all := mustCtl(t, a, "", "sub", "orders", "-group", "g1", "-n", "11")
	if len(strings.Split(strings.TrimSpace(all), "\n")) != 11 || !strings.Contains(all, "hello world") {
		t.Fatalf("replay:\n%s", all)
	}
	if out, err := ctl(t, a, "", "reset", "orders", "-group", "g1", "-to", "banana"); err == nil {
		t.Fatalf("bad target accepted: %s", out)
	}

	stats := mustCtl(t, a, "", "stats")
	if !strings.Contains(stats, "messages_in") {
		t.Fatalf("stats:\n%s", stats)
	}
	mustCtl(t, a, "", "delete", "orders")
	if out := mustCtl(t, a, "", "topics"); strings.Contains(out, "orders") {
		t.Fatalf("topic survived delete:\n%s", out)
	}
	if out, err := ctl(t, a, "", "delete", "orders"); err == nil {
		t.Fatalf("second delete should fail: %s", out)
	}
	// Usage errors exit non-zero.
	if _, err := ctl(t, a, "", "bogus"); err == nil {
		t.Fatal("unknown command accepted")
	}
	if _, err := ctl(t, a, ""); err == nil {
		t.Fatal("no command accepted")
	}
}

func TestKill9RecoveryAndSigtermShutdown(t *testing.T) {
	skipShort(t)
	data := t.TempDir()
	d := startd(t, data)
	var in strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&in, "event-%03d\n", i)
	}
	mustCtl(t, d.addr, in.String(), "pub", "ev", "-batch", "50")
	mustCtl(t, d.addr, "", "sub", "ev", "-group", "g", "-from", "earliest", "-n", "120")
	time.Sleep(400 * time.Millisecond) // offsets flush interval
	d.kill9()

	// Simulate a torn write after the crash: garbage at the tail of the newest segment.
	segs, _ := filepath.Glob(filepath.Join(data, "topics", "ev", "p0000", "*.log"))
	if len(segs) == 0 {
		t.Fatal("no segment files")
	}
	f, _ := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 1, 0, 0xde, 0xad})
	f.Close()

	d2 := startd(t, data)
	if out := mustCtl(t, d2.addr, "", "topics"); !strings.Contains(out, "200") {
		t.Fatalf("data lost after kill -9:\n%s", out)
	}
	rest := mustCtl(t, d2.addr, "", "sub", "ev", "-group", "g", "-n", "80")
	lines := strings.Split(strings.TrimSpace(rest), "\n")
	if len(lines) != 80 || !strings.Contains(lines[0], "event-120") || !strings.Contains(lines[79], "event-199") {
		t.Fatalf("group did not resume at 120:\n%s...%s", lines[0], lines[len(lines)-1])
	}
	if !strings.Contains(d2.log.String(), "listening") {
		t.Fatalf("log: %s", d2.log)
	}
	// Graceful shutdown exits 0 and loses nothing.
	mustCtl(t, d2.addr, "", "pub", "ev", "last")
	if err := d2.term(); err != nil {
		t.Fatalf("SIGTERM exit: %v\n%s", err, d2.log)
	}
	if !strings.Contains(d2.log.String(), "shutting down") {
		t.Fatalf("no shutdown log:\n%s", d2.log)
	}
	d3 := startd(t, data)
	if out := mustCtl(t, d3.addr, "", "topics"); !strings.Contains(out, "201") {
		t.Fatalf("message published before SIGTERM lost:\n%s", out)
	}
}

func TestMetricsAndHealthEndpoint(t *testing.T) {
	skipShort(t)
	httpAddr := freeAddr(t)
	d := startd(t, t.TempDir(), "-http", httpAddr, "-default-partitions", "2")
	mustCtl(t, d.addr, "", "pub", "m", "x")
	deadline := time.Now().Add(5 * time.Second)
	var body string
	for {
		resp, err := http.Get("http://" + httpAddr + "/metrics")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(b)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, want := range []string{"streamd_messages_in_total 1", `streamd_partition_next_offset{topic="m",partition="0"}`} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	resp, err := http.Get("http://" + httpAddr + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	resp.Body.Close()
}

// ---- TLS / mTLS / token through the real flags ----

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

type certs struct{ ca, srvCert, srvKey, cliCert, cliKey, otherCA string }

func makeCerts(t *testing.T) certs {
	t.Helper()
	dir := t.TempDir()
	newCA := func(cn string) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
		c, _ := x509.ParseCertificate(der)
		return c, k, der
	}
	leaf := func(ca *x509.Certificate, cak *ecdsa.PrivateKey, serial int64, usage x509.ExtKeyUsage) (der []byte, key []byte) {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "leaf"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, cak)
		if err != nil {
			t.Fatal(err)
		}
		kb, _ := x509.MarshalECPrivateKey(k)
		return der, kb
	}
	ca, cak, cader := newCA("e2e-ca")
	_, _, otherDER := newCA("other-ca")
	c := certs{
		ca: filepath.Join(dir, "ca.pem"), srvCert: filepath.Join(dir, "srv.pem"), srvKey: filepath.Join(dir, "srv.key"),
		cliCert: filepath.Join(dir, "cli.pem"), cliKey: filepath.Join(dir, "cli.key"), otherCA: filepath.Join(dir, "other.pem"),
	}
	writePEM(t, c.ca, "CERTIFICATE", cader)
	writePEM(t, c.otherCA, "CERTIFICATE", otherDER)
	sd, sk := leaf(ca, cak, 2, x509.ExtKeyUsageServerAuth)
	writePEM(t, c.srvCert, "CERTIFICATE", sd)
	writePEM(t, c.srvKey, "EC PRIVATE KEY", sk)
	cd, ck := leaf(ca, cak, 3, x509.ExtKeyUsageClientAuth)
	writePEM(t, c.cliCert, "CERTIFICATE", cd)
	writePEM(t, c.cliKey, "EC PRIVATE KEY", ck)
	return c
}

func TestTLSMutualTLSAndTokenFlags(t *testing.T) {
	skipShort(t)
	c := makeCerts(t)
	d := startd(t, t.TempDir(), "-tls-cert", c.srvCert, "-tls-key", c.srvKey, "-tls-client-ca", c.ca, "-token", "s3cret")
	a := d.addr

	// Full credentials work.
	good := []string{"-tls", "-ca", c.ca, "-cert", c.cliCert, "-key", c.cliKey, "-token", "s3cret"}
	mustCtl(t, a, "", append(good, "pub", "sec", "classified")...)
	if out := mustCtl(t, a, "", append(good, "sub", "sec", "-from", "earliest", "-n", "1")...); !strings.Contains(out, "classified") {
		t.Fatalf("sub over mTLS: %q", out)
	}
	// Each missing/incorrect credential is rejected.
	bad := map[string][]string{
		"plaintext":      {"-token", "s3cret"},
		"no client cert": {"-tls", "-ca", c.ca, "-token", "s3cret"},
		"wrong CA":       {"-tls", "-ca", c.otherCA, "-cert", c.cliCert, "-key", c.cliKey, "-token", "s3cret"},
		"wrong token":    {"-tls", "-ca", c.ca, "-cert", c.cliCert, "-key", c.cliKey, "-token", "nope"},
		"no token":       {"-tls", "-ca", c.ca, "-cert", c.cliCert, "-key", c.cliKey},
	}
	for name, flags := range bad {
		if out, err := ctl(t, a, "", append(flags, "topics")...); err == nil {
			t.Errorf("%s was accepted:\n%s", name, out)
		}
	}
}

func TestFollowerProcessReplicatesFromLeaderProcess(t *testing.T) {
	skipShort(t)
	c := makeCerts(t)
	leader := startd(t, t.TempDir(), "-tls-cert", c.srvCert, "-tls-key", c.srvKey, "-token", "tok", "-default-partitions", "2")
	follower := startd(t, t.TempDir(), "-follow", leader.addr, "-follow-ca", c.ca, "-token", "tok")
	flags := []string{"-tls", "-ca", c.ca, "-token", "tok"}
	var in strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&in, "msg-%02d\n", i)
	}
	mustCtl(t, leader.addr, in.String(), append(flags, "pub", "rep", "-batch", "10")...)

	// The follower serves plaintext (it has no TLS flags) and is read-only. A
	// client asking for a topic it has not replicated yet gets an error rather
	// than creating a local topic that would conflict with the leader's.
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := ctl(t, follower.addr, "", "-token", "tok", "sub", "rep", "-from", "earliest", "-partition", "0", "-n", "1")
		if err == nil && strings.Contains(got, "msg-") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never replicated (last: %v %q)\n%s", err, got, follower.log)
		}
		time.Sleep(100 * time.Millisecond)
	}
	out, err := ctl(t, follower.addr, "", "-token", "tok", "topics")
	if err != nil || strings.Count(out, "rep") != 2 {
		t.Fatalf("follower should mirror the leader's 2 partitions: %v\n%s", err, out)
	}
	if out, err := ctl(t, follower.addr, "", "-token", "tok", "pub", "rep", "nope"); err == nil || !strings.Contains(out, "read-only") {
		t.Fatalf("follower accepted a write: %v %s", err, out)
	}
}
