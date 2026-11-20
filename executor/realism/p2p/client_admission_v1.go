package p2p

// ClientAdmissionOriginV1 is an explicit opt-in for verified client ingress.
// Legacy TX_GOSSIP and validator gossip are unaffected.
const ClientAdmissionOriginV1 = "mbe-client-ack-v1"
const MessageClientAdmissionAckV1 = "CLIENT_ADMISSION_ACK_V1"

type ClientAdmissionReceiptV1 struct {
	ReferenceMessageID string `json:"reference_message_id"`
	Accepted           bool   `json:"accepted"`
	Reason             string `json:"reason,omitempty"`
}
