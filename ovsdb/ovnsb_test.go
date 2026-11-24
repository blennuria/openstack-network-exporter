// SPDX-License-Identifier: Apache-2.0

package ovsdb

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/openstack-k8s-operators/openstack-network-exporter/config"
	"github.com/openstack-k8s-operators/openstack-network-exporter/ovsdb/ovnsb"
	"github.com/ovn-kubernetes/libovsdb/database/inmemory"
	"github.com/ovn-kubernetes/libovsdb/model"
	"github.com/ovn-kubernetes/libovsdb/server"
)

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// startSBServer runs an in-memory OVN_Southbound database listening on a
// unix socket or on a tcp address.
func startSBServer(t *testing.T, network, address string) *server.OvsdbServer {
	t.Helper()
	clientModel, err := ovnsb.FullDatabaseModel()
	if err != nil {
		t.Fatal(err)
	}
	logger := logr.Discard()
	db := inmemory.NewDatabase(map[string]model.ClientDBModel{"OVN_Southbound": clientModel}, &logger)
	dbModel, errs := model.NewDatabaseModel(ovnsb.Schema(), clientModel)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	srv, err := server.NewOvsdbServer(db, &logger, dbModel)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(network, address) }()
	waitFor(t, 2*time.Second, "the SB server", srv.Ready)
	return srv
}

// socketProxy stands for the SB database socket. Stopping it closes every
// client connection and removes the socket file, as a restart of the
// database does. The in-memory server cannot be used for that: its Close()
// only closes the listener and keeps accepted connections open.
//
// A silent proxy keeps its connections open and drops what the clients send,
// as the network does when the host of the database goes down. A slow proxy
// delays what the database sends.
type socketProxy struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	silent   atomic.Bool
	delay    atomic.Int64 // time.Duration
}

type clientConn struct {
	net.Conn
	proxy *socketProxy
}

func (c clientConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if c.proxy.silent.Load() {
		n = 0
	}
	return n, err
}

type serverConn struct {
	net.Conn
	proxy *socketProxy
}

func (c serverConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	time.Sleep(time.Duration(c.proxy.delay.Load()))
	return n, err
}

func startProxy(t *testing.T, path, backend string) *socketProxy {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	p := &socketProxy{listener: l}
	go func() {
		for {
			front, err := l.Accept()
			if err != nil {
				return
			}
			back, err := net.Dial("unix", backend)
			if err != nil {
				front.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, front, back)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(back, clientConn{front, p}); back.Close() }()
			go func() { _, _ = io.Copy(front, serverConn{back, p}); front.Close() }()
		}
	}()
	return p
}

func (p *socketProxy) stop() {
	p.listener.Close() // also unlinks the socket file
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

func listDatapaths() error {
	return listDatapathsWithin(time.Second)
}

func listDatapathsWithin(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var datapaths []ovnsb.DatapathBinding
	return SBList(ctx, &datapaths)
}

// The SB client is cached: a restart or leader change of the SB database
// must not leave it dead until the exporter restarts.
func TestSBReconnectAfterServerRestart(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "backend.sock")
	srv := startSBServer(t, "unix", backend)
	defer srv.Close()

	sock := filepath.Join(dir, "sb.sock")
	t.Setenv("OPENSTACK_NETWORK_EXPORTER_OVN_SB_CONNECTION", "unix:"+sock)
	if err := config.Parse(); err != nil {
		t.Fatal(err)
	}
	proxy := startProxy(t, sock, backend)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}

	proxy.stop()
	waitFor(t, 5*time.Second, "calls to fail while the SB database is down", func() bool { return listDatapaths() != nil })

	proxy = startProxy(t, sock, backend)
	defer proxy.stop()
	waitFor(t, 5*time.Second, "calls to succeed after the SB database is back", func() bool { return listDatapaths() == nil })
}

// A cluster member that goes down without closing its connections does not
// answer anymore. The client connected to it must not be kept: the next call
// has to reach another member.
func TestSBReconnectAfterServerSilence(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "backend.sock")
	srv := startSBServer(t, "unix", backend)
	defer srv.Close()

	sock := filepath.Join(dir, "sb.sock")
	proxy := startProxy(t, sock, backend)
	defer proxy.stop()

	useSBRemote(t, "unix:"+sock+",unix:"+backend)
	setSBProbeTimeout(t, 100*time.Millisecond)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}

	proxy.silent.Store(true)
	if err := listDatapaths(); err == nil {
		t.Fatal("a silent SB database answered")
	}
	waitFor(t, 2*time.Second, "the client of the silent SB database to be dropped", func() bool {
		return sbConnected() == ""
	})
	if err := listDatapaths(); err != nil {
		t.Fatalf("call after the SB database went silent: %v", err)
	}
	if got, want := sbConnected(), "unix:"+backend; got != want {
		t.Fatalf("connected to %s, want %s", got, want)
	}
}

// Closing the client of a silent SB database waits for the calls that are
// still in progress on it. The other callers must not wait with them to
// reach another member.
func TestSBReconnectDuringStuckCall(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "backend.sock")
	srv := startSBServer(t, "unix", backend)
	defer srv.Close()

	sock := filepath.Join(dir, "sb.sock")
	proxy := startProxy(t, sock, backend)
	defer proxy.stop()

	useSBRemote(t, "unix:"+sock+",unix:"+backend)
	setSBProbeTimeout(t, 100*time.Millisecond)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}

	proxy.silent.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stuck := make(chan error, 1)
	go func() {
		var datapaths []ovnsb.DatapathBinding
		stuck <- SBList(ctx, &datapaths)
	}()
	if err := listDatapathsWithin(200 * time.Millisecond); err == nil {
		t.Fatal("a silent SB database answered")
	}
	waitFor(t, 2*time.Second, "the client of the silent SB database to be dropped", func() bool {
		return sbConnected() == ""
	})
	if err := listDatapaths(); err != nil {
		t.Fatalf("call while another one is stuck: %v", err)
	}
	select {
	case err := <-stuck:
		t.Fatalf("the call had to wait for the stuck one, which ended with: %v", err)
	default:
	}

	cancel()
	<-stuck
}

// A call also times out when the caller has little time left or the server
// is slow. The client of a server that still answers must be kept.
func TestSBKeepSlowServer(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "backend.sock")
	srv := startSBServer(t, "unix", backend)
	defer srv.Close()

	sock := filepath.Join(dir, "sb.sock")
	proxy := startProxy(t, sock, backend)
	defer proxy.stop()

	useSBRemote(t, "unix:"+sock)
	setSBProbeTimeout(t, 2*time.Second)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}
	sbLock.Lock()
	db := sbConn
	sbLock.Unlock()

	proxy.delay.Store(int64(200 * time.Millisecond))
	if err := listDatapathsWithin(50 * time.Millisecond); err == nil {
		t.Fatal("a slow SB database answered in time")
	}
	waitFor(t, 5*time.Second, "the echo to the slow SB database", func() bool {
		sbLock.Lock()
		defer sbLock.Unlock()
		return !sbProbing || sbConn != db
	})
	sbLock.Lock()
	kept := sbConn == db
	sbLock.Unlock()
	if !kept {
		t.Fatal("the client of a slow SB database was dropped")
	}

	proxy.delay.Store(0)
	if err := listDatapaths(); err != nil {
		t.Fatalf("call to the SB database once fast again: %v", err)
	}
}

// setSBProbeTimeout sets how long the SB database has to answer an echo.
func setSBProbeTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	old := sbProbeTimeout
	sbProbeTimeout = timeout
	t.Cleanup(func() { sbProbeTimeout = old })
}

// useSBRemote points a disconnected SB client to remote and makes it try the
// endpoints in the order they are listed.
func useSBRemote(t *testing.T, remote string) {
	t.Helper()
	forget := func() {
		sbLock.Lock()
		defer sbLock.Unlock()
		if sbConn != nil {
			close(sbGone)
			sbConn.Close()
			sbConn = nil
		}
		sbOrderRemote = ""
		sbOrder = nil
	}
	forget()
	t.Cleanup(forget)

	shuffle := sbShuffle
	sbShuffle = func([]string) {}
	t.Cleanup(func() { sbShuffle = shuffle })

	t.Setenv("OPENSTACK_NETWORK_EXPORTER_OVN_SB_CONNECTION", remote)
	if err := config.Parse(); err != nil {
		t.Fatal(err)
	}
}

// Connections to the returned listener are established by the kernel and
// never answered unless something accepts them.
func listenTCP(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// freeTCPAddr returns a loopback address that nothing listens on.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l := listenTCP(t)
	defer l.Close()
	return l.Addr().String()
}

func TestSBEndpoints(t *testing.T) {
	tests := []struct {
		remote string
		want   []string
	}{
		{"tcp:10.0.0.1:6642", []string{"tcp:10.0.0.1:6642"}},
		{
			"tcp:10.0.0.1:6642,tcp:10.0.0.2:6642,tcp:10.0.0.3:6642",
			[]string{"tcp:10.0.0.1:6642", "tcp:10.0.0.2:6642", "tcp:10.0.0.3:6642"},
		},
		{
			"ssl:[fd00::1]:6642,ssl:[fd00::2]:6642",
			[]string{"ssl:[fd00::1]:6642", "ssl:[fd00::2]:6642"},
		},
		{
			" unix:/run/ovn/ovnsb_db.sock , tcp:10.0.0.1:6642,",
			[]string{"unix:/run/ovn/ovnsb_db.sock", "tcp:10.0.0.1:6642"},
		},
		{
			"tcp:10.0.0.1:6642,cid:2fb3ab4b-4b53-4b4b-9d73-9c6a5b0c5a0f,tcp:10.0.0.2:6642",
			[]string{"tcp:10.0.0.1:6642", "tcp:10.0.0.2:6642"},
		},
		{"", nil},
		{" , ", nil},
	}
	for _, tt := range tests {
		if got := sbEndpoints(tt.remote); !slices.Equal(got, tt.want) {
			t.Errorf("sbEndpoints(%q) = %q, want %q", tt.remote, got, tt.want)
		}
	}
}

// ovn-remote lists every member of a clustered SB database. The members
// that are down must be skipped.
func TestSBRemoteList(t *testing.T) {
	addr := freeTCPAddr(t)
	srv := startSBServer(t, "tcp", addr)
	defer srv.Close()

	useSBRemote(t, "tcp:"+freeTCPAddr(t)+",tcp:"+addr)
	if err := listDatapaths(); err != nil {
		t.Fatalf("with the first endpoint down: %v", err)
	}
}

// An endpoint that does not answer, as the address of a host that is down,
// must leave enough time to try the next ones.
func TestSBRemoteListSilentEndpoint(t *testing.T) {
	silent := listenTCP(t)
	defer silent.Close()
	addr := freeTCPAddr(t)
	srv := startSBServer(t, "tcp", addr)
	defer srv.Close()

	useSBRemote(t, "tcp:"+silent.Addr().String()+",tcp:"+addr)
	// The collectors give 2 seconds too: half of it is lost on the first endpoint.
	if err := listDatapathsWithin(2 * time.Second); err != nil {
		t.Fatalf("with a silent first endpoint: %v", err)
	}
}

// sbConnected returns the endpoint of the SB client, "" without a client.
func sbConnected() string {
	sbLock.Lock()
	defer sbLock.Unlock()
	if sbConn == nil {
		return ""
	}
	return sbEndpoint
}

func sbOrderNow() []string {
	sbLock.Lock()
	defer sbLock.Unlock()
	return slices.Clone(sbOrder)
}

// The endpoints are tried in the same order on every connection, except
// that the ones that fail or whose client is dropped go to the back.
func TestSBRemoteOrder(t *testing.T) {
	addr1 := freeTCPAddr(t)
	srv1 := startSBServer(t, "tcp", addr1)
	defer srv1.Close()
	addr2 := freeTCPAddr(t)
	srv2 := startSBServer(t, "tcp", addr2)
	defer srv2.Close()
	up1, up2, down := "tcp:"+addr1, "tcp:"+addr2, "tcp:"+freeTCPAddr(t)

	useSBRemote(t, down+","+up1+","+up2)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got, want := sbOrderNow(), []string{up1, up2, down}; !slices.Equal(got, want) {
		t.Fatalf("after %s failed: order %q, want %q", down, got, want)
	}

	// As when the connection is lost.
	sbLock.Lock()
	db := sbConn
	sbLock.Unlock()
	sbForget(db, "test")
	if err := listDatapaths(); err != nil {
		t.Fatalf("call after the client was dropped: %v", err)
	}
	if got := sbConnected(); got != up2 {
		t.Fatalf("connected to %s, want %s", got, up2)
	}
	if got, want := sbOrderNow(), []string{up2, down, up1}; !slices.Equal(got, want) {
		t.Fatalf("after the client of %s was dropped: order %q, want %q", up1, got, want)
	}
}
