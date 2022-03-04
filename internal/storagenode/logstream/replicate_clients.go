package logstream

type replicateClients struct {
	clients []*replicateClient
}

func newReplicateClients() *replicateClients {
	return &replicateClients{}
}

func (rcs *replicateClients) add(client *replicateClient) {
	rcs.clients = append(rcs.clients, client)
}

func (rcs *replicateClients) close() {
	// Connector of backup replica is nil.
	if rcs != nil {
		clients := rcs.clients
		rcs.clients = nil
		for i := range clients {
			clients[i].stop()
		}
	}
}
