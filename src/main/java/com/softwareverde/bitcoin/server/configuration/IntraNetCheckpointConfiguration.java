package com.softwareverde.bitcoin.server.configuration;

public class IntraNetCheckpointConfiguration extends CheckpointConfiguration {
    public IntraNetCheckpointConfiguration() {
        _checkpoints.clear(); // Disable checkpoints on IntraNet.
    }
}
