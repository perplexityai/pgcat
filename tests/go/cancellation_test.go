package pgcat

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
)

type startupRecordingConn struct {
	net.Conn
	codes  chan uint32
	prefix [8]byte
	count  int
}

func (c *startupRecordingConn) Write(data []byte) (int, error) {
	if c.count < len(c.prefix) {
		c.count += copy(c.prefix[c.count:], data)
		if c.count == len(c.prefix) {
			c.codes <- binary.BigEndian.Uint32(c.prefix[4:])
		}
	}
	return c.Conn.Write(data)
}

func cancellationFixture(t *testing.T) (url.URL, url.URL) {
	t.Helper()
	dir := t.TempDir()
	certificate := filepath.Join(dir, "server.crt")
	key := filepath.Join(dir, "server.key")
	if err := exec.Command(
		"openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
		"-keyout", key, "-out", certificate, "-days", "1",
		"-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
	).Run(); err != nil {
		t.Fatalf("generate fixture certificate: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	backendPort := os.Getenv("PGCAT_TEST_POSTGRES_PORT")
	if backendPort == "" {
		backendPort = "5432"
	}
	parsedPort, err := strconv.Atoi(backendPort)
	if err != nil {
		t.Fatal(err)
	}
	password := os.Getenv("PGCAT_TEST_POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	config := fmt.Sprintf(`[general]
host = "127.0.0.1"
port = %d
worker_threads = 2
shutdown_timeout = 1000
admin_username = "admin"
admin_password = "fixture"
tls_certificate = %q
tls_private_key = %q
[pools.postgres]
pool_mode = "transaction"
default_role = "primary"
[pools.postgres.users.0]
username = "postgres"
password = %q
pool_size = 2
min_pool_size = 0
[pools.postgres.shards.0]
database = "postgres"
servers = [["127.0.0.1", %d, "primary"]]
`, port, certificate, key, password, parsedPort)
	configPath := filepath.Join(dir, "pgcat.toml")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	binaryPath := os.Getenv("PGCAT_BINARY")
	if binaryPath == "" {
		binaryPath = "../../target/debug/pgcat"
		if target := os.Getenv("CARGO_TARGET_DIR"); target != "" {
			binaryPath = filepath.Join(target, "debug", "pgcat")
		}
	}
	cmd := exec.Command(binaryPath, configPath)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	address := net.JoinHostPort("localhost", strconv.Itoa(port))
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture PgCat did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mainURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("postgres", password),
		Host:   address,
		Path:   "/postgres",
	}
	options := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {certificate}, "connect_timeout": {"5"}}
	mainURL.RawQuery = options.Encode()
	observerURL := mainURL
	observerURL.Host = net.JoinHostPort("127.0.0.1", backendPort)
	observerURL.RawQuery = "sslmode=disable"
	return mainURL, observerURL
}

func TestPlainCancelRequestForVerifiedTLSSession(t *testing.T) {
	mainURL, observerURL := cancellationFixture(t)
	address := mainURL.Host
	clientConfig, err := pgx.ParseConfig(mainURL.String())
	if err != nil {
		t.Fatal(err)
	}
	codes := make(chan uint32, 4)
	clientConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &startupRecordingConn{Conn: conn, codes: codes}, nil
	}
	observer, err := sql.Open("postgres", observerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	ctx, cancelAll := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAll()
	conn, err := pgx.ConnectConfig(ctx, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tlsConn, ok := conn.PgConn().Conn().(*tls.Conn)
	if !ok || len(tlsConn.ConnectionState().VerifiedChains) == 0 {
		t.Fatal("main session did not establish verified TLS")
	}
	query := fmt.Sprintf("SELECT pg_sleep(20) /* go_cancel_%d */", time.Now().UnixNano())
	defer observer.ExecContext(context.Background(),
		"SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query=$1 AND state='active'", query)
	result := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, query)
		result <- err
	}()
	active := func() bool {
		var found bool
		err := observer.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query=$1 AND state='active')", query).Scan(&found)
		if err != nil {
			t.Fatal(err)
		}
		return found
	}
	for !active() {
		if ctx.Err() != nil {
			t.Fatal("query never reached PostgreSQL")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code := <-codes; code != 80877103 {
		t.Fatalf("main session did not request SSL, startup code %d", code)
	}
	rawCancel, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer rawCancel.Close()
	_ = rawCancel.SetDeadline(time.Now().Add(2 * time.Second))
	if err := binary.Write(rawCancel, binary.BigEndian, [2]uint32{8, 80877103}); err != nil {
		t.Fatal(err)
	}
	var sslResponse [1]byte
	if _, err := io.ReadFull(rawCancel, sslResponse[:]); err != nil || sslResponse[0] != 'S' {
		t.Fatal("fixture listener did not accept cancellation TLS")
	}
	encryptedCancel := tls.Client(rawCancel, clientConfig.TLSConfig.Clone())
	if err := encryptedCancel.Handshake(); err != nil {
		t.Fatal(err)
	}
	invalidCancel := [4]uint32{16, 80877102, conn.PgConn().PID(), conn.PgConn().SecretKey() ^ 1}
	if err := binary.Write(encryptedCancel, binary.BigEndian, invalidCancel); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if n, err := encryptedCancel.Read(reply[:]); n != 0 || (err != io.EOF && err != io.ErrUnexpectedEOF) {
		t.Fatal("invalid cancellation did not silently close")
	}
	if !active() {
		t.Fatal("a mismatched cancellation key stopped the backend query")
	}
	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || pgError.Code != "57014" {
			t.Fatalf("unexpected query cancellation result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client query did not finish after cancellation")
	}
	select {
	case code := <-codes:
		if code != 80877102 {
			t.Fatalf("expected native plaintext CancelRequest, startup code %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("driver did not open a cancellation connection")
	}
	deadline := time.Now().Add(2 * time.Second)
	for active() {
		if time.Now().After(deadline) {
			t.Fatal("canceled query still active in PostgreSQL")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
