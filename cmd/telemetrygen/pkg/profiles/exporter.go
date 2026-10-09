// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profiles

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/config"
)

func newGRPCClient(cfg *Config) (*grpc.ClientConn, pprofileotlp.GRPCClient, error) {
	conn, err := newGRPCConnection(cfg)
	if err != nil {
		return nil, nil, err
	}
	return conn, pprofileotlp.NewGRPCClient(conn), nil
}

func newGRPCConnection(cfg *Config) (*grpc.ClientConn, error) {
	var transportCredentials credentials.TransportCredentials
	if cfg.Insecure {
		transportCredentials = insecure.NewCredentials()
	} else {
		var err error
		transportCredentials, err = config.GetTLSCredentialsForGRPCExporter(
			cfg.CaFile,
			cfg.ClientAuth,
			cfg.InsecureSkipVerify,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS credentials: %w", err)
		}
	}

	conn, err := grpc.NewClient(
		cfg.Endpoint(),
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(cfg.MaxMessageMiB*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	return conn, nil
}

func metadataContext(ctx context.Context, headers map[string]string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.New(headers))
}
