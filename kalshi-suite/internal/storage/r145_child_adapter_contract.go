package storage

import "strings"

// SystemExecutionAdapterContract is the machine-readable truth between a registered System and
// an order-producing handoff. Declaring venue/side/route cells is not enough: child systems are
// connected only when a concrete named adapter can currently carry their decision into an
// executable child or overlay without inventing a standalone order.
type SystemExecutionAdapterContract struct {
	SystemID       string `json:"system_id"`
	AdapterID      string `json:"adapter_id"`
	State          string `json:"state"`
	Connected      bool   `json:"connected"`
	CodePathExists bool   `json:"code_path_exists"`
	Reason         string `json:"reason"`
}

// Connected is static code-path capability only: an exact candidate can reach the named order
// handoff without rebuilding the system. It does not claim that the collector is currently fresh,
// that a candidate exists, or that Paper/LIVE is authorized. Runtime collection truth remains in
// ResearchSystemCollectionStat.State and economic/authority truth remains in their own ledgers.

var r145ChildExecutionAdapterContracts = map[string]SystemExecutionAdapterContract{
	"maker-salvage-matched-cohort": {
		SystemID: "maker-salvage-matched-cohort", AdapterID: "step7-maker-salvage-overlay",
		State: "CODE_PATH_CONNECTED_OVERLAY", Connected: true, CodePathExists: true,
		Reason: "the adapter runs only after a different System's exact maker order is admitted; it stamps that existing attempt with the matched maker-versus-taker decision and never creates a second order",
	},
}

// SystemExecutionAdapterStatus returns the current end-to-end execution connection. Unsupported
// product shapes and incomplete child adapters fail closed even when their registry still exposes
// exact cells for inspection and future implementation.
func SystemExecutionAdapterStatus(spec SystemExecutionSpec) SystemExecutionAdapterContract {
	base := SystemExecutionAdapterContract{SystemID: spec.SystemID}
	switch spec.HandoffClass {
	case SystemHandoffSingle:
		base.AdapterID, base.State, base.Connected, base.CodePathExists =
			"shared-exact-single", "CODE_PATH_CONNECTED", true, true
		base.Reason = "code-level exact single-order handoff reaches the shared Paper executor and accepted-Paper armed LIVE mirror; current collector freshness and economic authority are separate"
	case SystemHandoffBundle:
		base.AdapterID, base.State, base.Connected, base.CodePathExists =
			"native-rfq-bundle", "CODE_PATH_CONNECTED", true, true
		base.Reason = "code-level typed bundle reaches its native Paper/RFQ path and armed LIVE acceptance path; current collector freshness and economic authority are separate"
	case SystemHandoffChild:
		if child, ok := r145ChildExecutionAdapterContracts[spec.SystemID]; ok {
			return child
		}
		base.AdapterID = "required:named-executable-child"
		base.State = "WAITING_FOR_REGISTERED_CHILD_ADAPTER"
		base.Reason = "child handoff is declared but no explicit executable child/overlay contract is registered"
	case SystemHandoffUnsupported:
		base.AdapterID, base.State = "unsupported", "NEEDS_EXECUTION_ADAPTER"
		base.Reason = strings.Join(spec.UnsupportedReasons, "; ")
		if strings.TrimSpace(base.Reason) == "" {
			base.Reason = "no executable adapter is registered for this action shape"
		}
	default:
		base.AdapterID, base.State = "unregistered", "NEEDS_EXECUTION_ADAPTER"
		base.Reason = "system handoff class is not registered"
	}
	return base
}
