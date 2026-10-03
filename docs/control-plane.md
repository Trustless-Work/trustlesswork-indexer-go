# Control Plane

## Escrow WASM Hash Approval

Every factory WASM hash the API can deploy must be approved in `pkg/escrow/escrow_discovery.go` (`ESCROW_APPROVED_WASM_HASHES`) in the same release.

## Verification Commands

Run these two commands to diff the API factory hashes against the approved set:

```bash
# Check API factory hashes against approved set
grep -r "factory" docs/api-factory-hashes.md | grep -oE '([a-f0-9]{64})' | sort -u > /tmp/api_hashes.txt
cut -d: -f2 pkg/escrow/escrow_discovery.go | grep -oE '([a-f0-9]{64})' | sort -u > /tmp/approved_hashes.txt
diff /tmp/api_hashes.txt /tmp/approved_hashes.txt
```

## Release Checklist

- [ ] Add new WASM hashes to `ESCROW_APPROVED_WASM_HASHES`
- [ ] Update defaults in `docker-compose.yml`
- [ ] Update docs in `docs/railway-deployment.md`
- [ ] Update "Verified" note in this file
- [ ] Run verification commands in each environment (testnet, mainnet)
- [ ] Coordinate with API factory switch (must be live before API starts deploying from new factories)
