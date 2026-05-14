package detector

import (
	"encoding/base64"
	"fmt"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// contractIDFromEvent returns the emitter contract address as a "C..."
// strkey. Returns ("", false) if the event has no contract_id (e.g. a
// system event) or if the encoding fails.
//
// The Soroban event header carries ContractId as a 32-byte hash; strkey
// is responsible for the "C..." encoding.
func contractIDFromEvent(ev xdr.ContractEvent) (string, bool) {
	if ev.ContractId == nil {
		return "", false
	}
	encoded, err := strkey.Encode(strkey.VersionByteContract, ev.ContractId[:])
	if err != nil {
		return "", false
	}
	return encoded, true
}

// transferToAddress extracts the "to" address from a SAC/SEP-41
// transfer event's topics. Standard topic layout per the SAC spec:
//
//	topics[0] = Symbol("transfer")
//	topics[1] = Address from
//	topics[2] = Address to
//
// Returns the "to" as a strkey ("G..." for an account or "C..." for a
// contract). Returns ("", false) if topics are not the expected shape
// or if the address cannot be encoded.
func transferToAddress(ev xdr.ContractEvent) (string, bool) {
	if ev.Body.V != 0 {
		return "", false
	}
	body := ev.Body.V0
	if len(body.Topics) < 3 {
		return "", false
	}
	return addressFromScVal(body.Topics[2])
}

// transferFromAddress extracts the "from" address from a SAC/SEP-41
// transfer event's topics. Returns ("", false) if topics are not the
// expected shape.
func transferFromAddress(ev xdr.ContractEvent) (string, bool) {
	if ev.Body.V != 0 {
		return "", false
	}
	body := ev.Body.V0
	if len(body.Topics) < 3 {
		return "", false
	}
	return addressFromScVal(body.Topics[1])
}

// addressFromScVal decodes an ScVal carrying an ScAddress into its
// strkey representation ("G..." for an account, "C..." for a contract).
// Returns ("", false) for any other ScVal kind.
func addressFromScVal(v xdr.ScVal) (string, bool) {
	addr, ok := v.GetAddress()
	if !ok {
		return "", false
	}
	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		accountID := addr.MustAccountId()
		s := accountID.Address()
		if s == "" {
			return "", false
		}
		return s, true
	case xdr.ScAddressTypeScAddressTypeContract:
		hash := addr.MustContractId()
		s, err := strkey.Encode(strkey.VersionByteContract, hash[:])
		if err != nil {
			return "", false
		}
		return s, true
	}
	return "", false
}

// encodeEventXDR returns the base64-encoded marshal of the full
// xdr.ContractEvent. This is what goes into Envelope.RawXDR: the
// consumer reconstructs the event with xdr.SafeUnmarshalBase64.
//
// Returns a wrapped events.ErrXDRDecodingFail on marshal failure;
// callers treat this as skippable.
func encodeEventXDR(ev xdr.ContractEvent) (string, error) {
	b, err := ev.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("%w: marshaling ContractEvent: %v", events.ErrXDRDecodingFail, err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
