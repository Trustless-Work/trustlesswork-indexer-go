package escrow

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/Trustless-Work/trustlesswork-indexer-go/internal/types"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/xdr"
)

const (
	HOST = "wss://soroban-testnet.stellar.org"
)

var ESCROW_APPROVED_WASM_HASHES = map[string]bool{
	// Protocol 27 hashes - single-release
	"a647dffe18f755a29a324f4567bd3ae6aa8c33b23b48fe074d7233fe420e486c": true, // escrow_v1_single_release
	"e9fa26b900d1d7eac0d7d0c56f5d4d93f0945c80b40a6520f9c3e8c87a3c4f62": true, // escrow_v2_single_release

	// Protocol 27 hashes - multi-release
	"cc006ef5e19700b779c9ba4245cce4b9685070e2b2f6d6e071e3797e2f163114": true, // escrow_v1_multi_release
	"2a95d11495994750db1e99c0d8544768003f7156708b8c4e9f218e5b68a737f0": true, // escrow_v2_multi_release

	// Protocol 28 hashes - single-release
	"ab1f82f709974e777407c0d6e3b09d51a9c2d3b89b4e3f4c7a8d2e5f1b3c6a9d": true, // escrow_v1_single_release_p28
	"c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8": true, // escrow_v2_single_release_p28

	// Protocol 28 hashes - multi-release
	"e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4": true, // escrow_v1_multi_release_p28
	"d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3": true, // escrow_v2_multi_release_p28
}

func IsEscrowHashApproved(hash string) bool {
	return ESCROW_APPROVED_WASM_HASHES[hash]
}

func ValidateAndDecodeWasm(ctx context.Context, horizon horizonclient.ClientInterface, hash hexdigest, logger *types.ZerologWriter) ([]byte, error) {
	_wasmBytes, err := horizon.Wasm(hash)
	if err != nil {
		logger.Warn("ESCROW_VALIDATION Failed to fetch wasm bytes from Horizon", "hash", hash)
		return nil, fmt.Errorf("failed to fetch wasm bytes from Horizon: %w", err)
	}

	_, err = xdr.SafeUnmarshalWasm(_wasmBytes)
	if err != nil {
		logger.Warn("ESCROW_VALIDATION wasm bytes failed to decode as soroban xdr smart contract wasm", "hash", hash)
		return nil, fmt.Errorf("failed to decode wasm bytes: %w", err)
	}

	return _wasmBytes, nil
}

func FetchWasm(ctx context.Context, horizon horizonclient.ClientInterface, hash hexdigest, logger *types.ZerologWriter) ([]byte, error) {
	wasmBytes, err := horizon.Wasm(hash)
	if err != nil {
		logger.Warn("ESCROW_VALIDATION Failed to fetch wasm bytes from Horizon", "hash", hash)
		return nil, fmt.Errorf("failed to fetch wasm bytes from Horizon: %w", err)
	}
	return wasmBytes, nil
}

type hexdigest string

func (h hexdigest) ToXdr() (xdr.Hash, error) {
	var hash xdr.Hash
	decoded, err := hex.DecodeString(string(h))
	if err != nil {
		return hash, fmt.Errorf("failed to decode hash: %w", err)
	}
	copy(hash[:], decoded)
	return hash, nil
}

func (h hexdigest) String() string {
	return string(h)
}

func (h hexdigest) MarshalText() ([]byte, error) {
	return []byte(h), nil
}

func (h *hexdigest) UnmarshalText(text []byte) error {
	*h = hexdigest(text)
	return nil
}

func GetWasmHashFromEvent(event *xdr.DiagnosticEvent, logger *types.ZerologWriter) (hexdigest, error) {
	operationIdx := event.Event.GetResults().Body[0].GetOrder()

	if event.Event.Details[operationIdx] == "" {
		return "", fmt.Errorf("no details found for operation index %d", operationIdx)
	}

	var diagnosticInfo types.DiagnosticInfo
	err := diagnosticInfo.Unmarshal([]byte(event.Event.Details[operationIdx]))
	if err != nil {
		logger.Warn("Failed to unmarshal diagnostic info", "err", err)
		return "", fmt.Errorf("failed to unmarshal diagnostic info: %w", err)
	}

	var wasmHash hexdigest
	for _, frame := range diagnosticInfo.Stack {
		if frame.GetCall().HostFnIndex == xdr.HostFunctionTypeHostFnTypeUploadContractWasm {
			wasmHash = hexdigest(hex.EncodeToString(frame.Call.GetUploadContractWasm().Wasm))
			break
		}
	}

	if wasmHash == "" {
		return "", fmt.Errorf("wasm hash not found in diagnostic info")
	}

	if !IsEscrowHashApproved(wasmHash.String()) {
		log.Warn().Msgf("ESCROW_APPROVAL Escrow wasm hash not approved: %s", wasmHash.String())
		return "", fmt.Errorf("escrow wasm hash not approved: %s", wasmHash.String())
	}

	return wasmHash, nil
}
