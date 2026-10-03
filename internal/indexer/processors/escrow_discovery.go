package processors

import (
	"context"
	"fmt"
	"sync"

	"github.com/creachadair/taskgroup"
	"go.stellar.org/go/xdr"
	"go.uber.org/zap"

	"github.com/Trustless-Work/trustlesswork-indexer-go/internal/database"
	"github.com/Trustless-Work/trustlesswork-indexer-go/internal/indexer/types"
	sdkxdr "github.com/Trustless-Work/trustlesswork-sdk-go/xdr"
)

// EscrowDiscovery polls Stellar for recent contract-instance state changes and
// detects new escrows whose code matches the approved factory/upgrade hashes.
type EscrowDiscovery struct {
	mu         sync.Mutex
	approved   []xdr.Hash
	instanceDB database.InstanceReaderWriter
	logger     *zap.Logger

	// externalRefs maps an ExternalRef to the WasmHash it currently points at.
	// This is used to resolve ContractExecutable::ExternalRef references.
	externalRefs map[xdr.Hash]xdr.Hash
}

// NewEscrowDiscovery constructs an EscrowDiscovery that writes to the given
// database and accepts only contracts whose code matches one of the supplied
// approved hashes.
func NewEscrowDiscovery(db database.InstanceReaderWriter, approved []xdr.Hash, logger *zap.Logger) *EscrowDiscovery {
	return &EscrowDiscovery{
		approved:     approved,
		instanceDB:   db,
		logger:       logger,
		externalRefs: make(map[xdr.Hash]xdr.Hash),
	}
}

// RunDiscovery polls the chain for new contract-instance StateMeta entries that
// look like escrow accounts and inserts them into the local DB when their code
// matches one of the approved hashes.
func (d *EscrowDiscovery) RunDiscovery(ctx context.Context, pgClient database.PostgresClient, maxPages int) error {
	if d.instanceDB == nil {
		d.logger.Info("escrow discovery disabled; no instance DB configured")
		return nil
	}

	var lastLedger uint32
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		latest, err := getLatestLedgerSeq(ctx, pgClient)
		if err != nil {
			d.logger.Error("failed to get latest ledger seq", zap.Error(err))
			return err
		}
		if latest == lastLedger {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pgClient.LedgerProgressNotify(ctx):
				continue
			}
		}

		pageSize := types.DiscoveryPageSize
		if maxPages > 0 {
			remaining := int(latest) - int(lastLedger)
			maxEntries := maxPages * pageSize
			if remaining < maxEntries {
				pageSize = remaining
				if pageSize < 1 {
					pageSize = 1
				}
			}
		}

		changes, err := d.fetchChanges(ctx, pgClient, lastLedger, latest, pageSize)
		if err != nil {
			return err
		}

		var wg taskgroup.Group
		for _, change := range changes {
			change := change
			wg.Go(func() error {
				return d.processChange(ctx, change)
			})
		}
		if err := wg.Wait(); err != nil {
			d.logger.Error("error processing escrow changes", zap.Error(err))
			// Continue processing remaining changes
		}

		lastLedger = latest
	}
}

func (d *EscrowDiscovery) fetchChanges(ctx context.Context, pgClient database.PostgresClient, start, end uint32, pageSize int) ([]xdr.LedgerEntryChange, error) {
	startSeq := ledgerSeqFromLedgerKey(start)
	endSeq := ledgerSeqFromLedgerKey(end)

	var changes []xdr.LedgerEntryChange
	currentSeq := startSeq
	pagination := false

	for currentSeq < endSeq {
		var batch []xdr.LedgerEntryChange
		var err error
		if pagination {
			batch, err = pgClient.GetChangesAfter(ctx, currentSeq, uint64(pageSize))
		} else {
			batch, err = pgClient.GetChangesUpTo(ctx, endSeq, uint64(pageSize))
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch contract state changes: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		changes = append(changes, batch...)
		if len(batch) < pageSize {
			break
		}
		pagination = true
		currentSeq = batch[len(batch)-1].LedgerSequence + 1
	}
	return changes, nil
}

func (d *EscrowDiscovery) processChange(ctx context.Context, change xdr.LedgerEntryChange) error {
	if change.Type != xdr.LedgerEntryChangeTypeContractStateData || change.ContractStateData == nil {
		return nil
	}

	meta, ok := change.ContractStateData.NewValue.Value.Data.StateMeta()
	if !ok {
		return nil
	}
	executable := meta.InstanceMeta.ContractExecutable

	if executable.Type != sdkxdr.ContractExecutableTypeContractExecutableTypeWasm &&
		executable.Type != sdkxdr.ContractExecutableTypeContractExecutableTypeAsset &&
		executable.Type != sdkxdr.ContractExecutableTypeContractExecutableTypeExternalRef {
		d.logger.Warn("ignoring contract-instance change with unsupported executable type",
			zap.Int32("type", int32(executable.Type)),
			zap.Uint32("ledger", change.LedgerSequence),
		)
		return nil
	}

	// Resolve the executable to a WasmHash for matching against the approved set.
	var wasmHash xdr.Hash
	switch executable.Type {
	case sdkxdr.ContractExecutableTypeContractExecutableTypeWasm:
		wasmHash = executable.GetWasmHash()
	case sdkxdr.ContractExecutableTypeContractExecutableTypeAsset:
		// Asset-backed instances are not discovered as escrows.
		return nil
	case sdkxdr.ContractExecutableTypeContractExecutableTypeExternalRef:
		extRef := executable.GetExternalRef()
		var resolved bool
		d.mu.Lock()
		hash, exists := d.externalRefs[extRef]
		d.mu.Unlock()
		if exists {
			wasmHash = hash
			resolved = true
		}
		if !resolved {
			d.logger.Warn("ignoring contract-instance change with unresolved ExternalRef",
				zap.String("external_ref", extRef.StringHex()),
				zap.Uint32("ledger", change.LedgerSequence),
			)
			return nil
		}
	default:
		d.logger.Warn("ignoring contract-instance change with unknown executable type",
			zap.Int32("type", int32(executable.Type)),
			zap.Uint32("ledger", change.LedgerSequence),
		)
		return nil
	}

	if wasmHash == (xdr.Hash{}) {
		return nil
	}

	d.mu.Lock()
	for _, hash := range d.approved {
		if hash == wasmHash {
			instance, ok := instanceFromChange(change, executable)
			d.mu.Unlock()
			if ok {
				if err := d.instanceDB.InsertInstance(ctx, instance); err != nil {
					d.logger.Warn("failed to insert instance", zap.Error(err))
				}
			}
			return nil
		}
	}
	d.mu.Unlock()
	return nil
}

func instanceFromChange(change xdr.LedgerEntryChange, executable sdkxdr.ContractExecutable) (types.ContractInstance, bool) {
	meta, ok := change.ContractStateData.NewValue.Value.Data.StateMeta()
	if !ok {
		return types.ContractInstance{}, false
	}

	wasmHash := executable.GetWasmHash()
	if wasmHash == (xdr.Hash{}) {
		return types.ContractInstance{}, false
	}

	return types.ContractInstance{
		Address:    change.ContractStateData.Address.Address.StringCanonical(),
		WasmHash:   wasmHash,
		LastLedger: change.ContractStateData.NewValue.LedgerKey.LedgerSequence,
	}, true
}
