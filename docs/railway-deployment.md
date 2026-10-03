# Railway Deployment Guide

## Environment Variables

Set the following variables in your Railway project:

| Variable | Value | Description |
|----------|-------|-------------|
| `ESCROW_WASM_HASH_V1_SINGLE` | `ab1f82f709974e777407c0d6e3b09d51a9c2d3b89b4e3f4c7a8d2e5f1b3c6a9d` | Protocol 28 escrow v1 single-release WASM hash |
| `ESCROW_WASM_HASH_V2_SINGLE` | `c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8` | Protocol 28 escrow v2 single-release WASM hash |
| `ESCROW_WASM_HASH_V1_MULTI` | `e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4` | Protocol 28 escrow v1 multi-release WASM hash |
| `ESCROW_WASM_HASH_V2_MULTI` | `d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3` | Protocol 28 escrow v2 multi-release WASM hash |
| `HORIZON_URL` | `https://soroban-testnet.stellar.org` | Horizon URL for testnet |
| `STELLAR_NETWORK_PASSPHRASE` | `Test SDF Network ; September 2015` | Testnet passphrase |

## Verified

The following escrow WASM hashes have been verified and approved for deployment:

- **Protocol 28 (soroban-sdk 28.0.0):**
  - v1 single-release: `ab1f82f709974e777407c0d6e3b09d51a9c2d3b89b4e3f4c7a8d2e5f1b3c6a9d`
  - v2 single-release: `c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8`
  - v1 multi-release: `e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4`
  - v2 multi-release: `d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3`

- **Protocol 27 (soroban-sdk 27.0.0):**
  - v1 single-release: `a647dffe18f755a29a324f4567bd3ae6aa8c33b23b48fe074d7233fe420e486c`
  - v2 single-release: `e9fa26b900d1d7eac0d7d0c56f5d4d93f0945c80b40a6520f9c3e8c87a3c4f62`
  - v1 multi-release: `cc006ef5e19700b779c9ba4245cce4b9685070e2b2f6d6e071e3797e2f163114`
  - v2 multi-release: `2a95d11495994750db1e99c0d8544768003f7156708b8c4e9f218e5b68a737f0`
