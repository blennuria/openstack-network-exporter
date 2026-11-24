// SPDX-License-Identifier: Apache-2.0

package ovsdb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/openstack-k8s-operators/openstack-network-exporter/config"
	"github.com/openstack-k8s-operators/openstack-network-exporter/log"
	"github.com/openstack-k8s-operators/openstack-network-exporter/ovsdb/ovnsb"
	ovsmodel "github.com/openstack-k8s-operators/openstack-network-exporter/ovsdb/ovs"
	"github.com/ovn-kubernetes/libovsdb/client"
	"github.com/ovn-kubernetes/libovsdb/model"
	"github.com/ovn-kubernetes/libovsdb/ovsdb"
)

var (
	sbLock  sync.Mutex
	sbConn  client.Client
	sbModel model.DatabaseModel
	// The endpoint sbConn is connected to.
	sbEndpoint string
	// Closed when sbConn is forgotten.
	sbGone chan struct{}
	// Set while an echo checks that sbConn still answers.
	sbProbing bool
	// The endpoints of sbOrderRemote in the order they are tried.
	sbOrderRemote string
	sbOrder       []string
)

// How long a server that let a call time out has to answer an echo before
// its client is forgotten. Tests change it.
var sbProbeTimeout = 5 * time.Second

func discoverSBEndpoint(ctx context.Context) (string, error) {
	var vswitch ovsmodel.OpenvSwitch
	if err := Get(ctx, &vswitch); err != nil {
		return "", fmt.Errorf("reading Open_vSwitch for ovn-remote: %w", err)
	}
	remote, ok := vswitch.ExternalIDs["ovn-remote"]
	if !ok || remote == "" {
		return "", fmt.Errorf("ovn-remote not found in Open_vSwitch external_ids")
	}
	return remote, nil
}

func loadTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(config.OvnSBCertificate(), config.OvnSBPrivateKey())
	if err != nil {
		return nil, fmt.Errorf("loading OVN SB TLS keypair: %w", err)
	}
	caCert, err := os.ReadFile(config.OvnSBCACert())
	if err != nil {
		return nil, fmt.Errorf("reading OVN SB CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

// ovn-remote lists the members of a clustered database separated by commas.
func sbEndpoints(remote string) []string {
	var endpoints []string
	for _, e := range strings.Split(remote, ",") {
		e = strings.TrimSpace(e)
		// "cid:<uuid>" is the cluster ID, not an endpoint.
		if e != "" && !strings.HasPrefix(e, "cid:") {
			endpoints = append(endpoints, e)
		}
	}
	return endpoints
}

// The members are tried in a random order, as OVN clients do, so that the
// exporters do not all query the same one. Tests replace it.
var sbShuffle = func(endpoints []string) {
	rand.Shuffle(len(endpoints), func(i, j int) {
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	})
}

// sbOrderOf returns the endpoints of remote in the order to try them. As OVN
// clients do, they are shuffled once and keep that order, except that the
// ones that fail go to the back. Call it with sbLock held.
func sbOrderOf(remote string) []string {
	if remote != sbOrderRemote {
		sbOrderRemote = remote
		sbOrder = sbEndpoints(remote)
		sbShuffle(sbOrder)
	}
	return slices.Clone(sbOrder)
}

// sbMoveLast makes endpoint the last one to try. Call it with sbLock held.
func sbMoveLast(endpoint string) {
	if i := slices.Index(sbOrder, endpoint); i >= 0 {
		sbOrder = append(slices.Delete(sbOrder, i, i+1), endpoint)
	}
}

// An endpoint that does not answer must leave time for the next ones: it
// only gets its share of what ctx has left for the untried endpoints.
func sbConnectEndpoint(ctx context.Context, schema model.ClientDBModel, opts []client.Option, endpoint string, untried int) (client.Client, error) {
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(untried))
		defer cancel()
	}

	log.Debugf("connecting to OVN SB DB: %s", endpoint)

	opts = append(slices.Clip(opts), client.WithEndpoint(endpoint))
	if strings.HasPrefix(endpoint, "ssl:") {
		tlsCfg, err := loadTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.WithTLSConfig(tlsCfg))
	}

	db, err := client.NewOVSDBClient(schema, opts...)
	if err != nil {
		return nil, fmt.Errorf("NewOVSDBClient (SB): %w", err)
	}
	if err = db.Connect(ctx); err != nil {
		return nil, fmt.Errorf("SB db.Connect: %w", err)
	}

	return db, nil
}

func sbConnect(ctx context.Context) (client.Client, error) {
	sbLock.Lock()
	defer sbLock.Unlock()

	if sbConn != nil {
		return sbConn, nil
	}
	// The time may have run out while waiting for the lock: do not blame the
	// endpoints for it.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connecting to the OVN SB DB: %w", err)
	}

	remote := config.OvnSBConnection()
	if remote == "" {
		var err error
		remote, err = discoverSBEndpoint(ctx)
		if err != nil {
			return nil, err
		}
	}
	endpoints := sbOrderOf(remote)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoint in OVN SB remote %q", remote)
	}

	schema, err := ovnsb.FullDatabaseModel()
	if err != nil {
		return nil, fmt.Errorf("OVN SB FullDatabaseModel: %w", err)
	}
	mod, errs := model.NewDatabaseModel(ovnsb.Schema(), schema)
	if len(errs) > 0 {
		for _, e := range errs {
			log.Errf("OVN SB model: %s", e)
		}
		return nil, errs[len(errs)-1]
	}

	opts := []client.Option{
		client.WithLogger(log.OvsdbLogger()),
	}

	var db client.Client
	for i, endpoint := range endpoints {
		db, err = sbConnectEndpoint(ctx, schema, opts, endpoint, len(endpoints)-i)
		if err == nil {
			sbEndpoint = endpoint
			break
		}
		if ctx.Err() != nil {
			break
		}
		sbMoveLast(endpoint)
		if i < len(endpoints)-1 {
			log.Warningf("%s, trying the next OVN SB endpoint", err)
		}
	}
	if err != nil {
		return nil, err
	}

	sbModel = mod
	sbConn = db
	sbGone = make(chan struct{})
	sbProbing = false
	go sbForgetOnDisconnect(db, sbGone)
	return db, nil
}

// libovsdb keeps Connected() true after server-side disconnect. It drops the
// disconnect notification when nobody waits for it yet: stop waiting once the
// client is forgotten otherwise, or the goroutine would never end.
func sbForgetOnDisconnect(db client.Client, gone <-chan struct{}) {
	select {
	case <-db.DisconnectNotify():
		sbForget(db, "lost connection to the OVN SB DB")
	case <-gone:
	}
}

func sbForget(db client.Client, reason string) {
	sbLock.Lock()
	if sbConn != db {
		sbLock.Unlock()
		return
	}
	sbConn = nil
	close(sbGone)
	// The next connection tries the other members of the cluster first.
	sbMoveLast(sbEndpoint)
	sbLock.Unlock()

	log.Warningf("%s, reconnecting on the next call", reason)
	// Close waits for the calls in progress on db, which may be stuck on a
	// server that does not answer: the other callers need not wait for them.
	db.Close()
}

// A server whose host goes down closes nothing: the calls time out until the
// kernel gives up on the connection, about 15 minutes later. But a call also
// times out when the caller has little time left or the server is slow: the
// client is only forgotten if it does not answer an echo either.
func sbProbe(db client.Client) {
	sbLock.Lock()
	defer sbLock.Unlock()
	if sbConn != db || sbProbing {
		return
	}
	sbProbing = true
	timeout := sbProbeTimeout

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := db.Echo(ctx); err != nil {
			if ctx.Err() != nil {
				// libovsdb reports a timeout as an incorrect response.
				err = fmt.Errorf("no answer within %s", timeout)
			}
			sbForget(db, fmt.Sprintf("OVN SB DB echo: %s", err))
			return
		}
		sbLock.Lock()
		defer sbLock.Unlock()
		if sbConn == db {
			sbProbing = false
		}
	}()
}

func SBList[T model.Model](ctx context.Context, results *[]T) error {
	db, err := sbConnect(ctx)
	if err != nil {
		return err
	}

	var t T
	info, err := sbModel.NewModelInfo(&t)
	if err != nil {
		return fmt.Errorf("SB NewModelInfo: %w", err)
	}

	res, err := db.Transact(ctx, ovsdb.Operation{
		Op:    ovsdb.OperationSelect,
		Table: info.Metadata.TableName,
	})
	if err != nil {
		switch {
		case errors.Is(err, client.ErrNotConnected):
			// Disconnect notification can be missed; drop the dead client here too.
			sbForget(db, "lost connection to the OVN SB DB")
		case errors.Is(err, context.DeadlineExceeded):
			sbProbe(db)
		}
		return fmt.Errorf("SB Transact: %w", err)
	}

	for _, r := range res {
		for _, row := range r.Rows {
			var value T
			info, _ = sbModel.NewModelInfo(&value)
			err = sbModel.Mapper.GetRowData(&row, info)
			if err != nil {
				log.Errf("SB Mapper.GetRowData: %s", err)
				return err
			}
			err = info.SetField("_uuid", row["_uuid"].(ovsdb.UUID).GoUUID)
			if err != nil {
				log.Errf("SB info.SetField: %s", err)
				return err
			}
			*results = append(*results, value)
		}
	}
	return nil
}
