CREATE TABLE script_types (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    type VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY script_types_uq (type)
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

INSERT INTO script_types (id, type) VALUES
    (1, 'UNKNOWN'), (2, 'CUSTOM_SCRIPT'), (3, 'PAY_TO_PUBLIC_KEY'), (4, 'PAY_TO_PUBLIC_KEY_HASH'), (5, 'PAY_TO_SCRIPT_HASH'),
    (6, 'SLP_GENESIS_SCRIPT'), (7, 'SLP_SEND_SCRIPT'), (8, 'SLP_MINT_SCRIPT'), (9, 'SLP_COMMIT_SCRIPT');

CREATE TABLE pending_blocks (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash BINARY(32) NOT NULL,
    previous_block_hash BINARY(32) NULL,
    timestamp BIGINT UNSIGNED NOT NULL,
    last_download_attempt_timestamp BIGINT UNSIGNED,
    failed_download_count INT UNSIGNED NOT NULL DEFAULT 0,
    priority BIGINT NOT NULL,
    was_downloaded TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY pending_blocks_uq (hash),
    INDEX pending_blocks_ix1 (priority) USING BTREE,
    INDEX pending_blocks_ix2 (was_downloaded, failed_download_count) USING BTREE,
    INDEX pending_blocks_ix3 (previous_block_hash) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE pending_transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash BINARY(32) NOT NULL,
    timestamp BIGINT UNSIGNED NOT NULL,
    last_download_attempt_timestamp BIGINT UNSIGNED,
    failed_download_count INT UNSIGNED NOT NULL DEFAULT 0,
    priority BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY pending_transactions_uq (hash),
    INDEX pending_transactions_ix1 (priority) USING BTREE,
    INDEX pending_transactions_ix2 (failed_download_count) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE pending_transaction_data (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    pending_transaction_id INT UNSIGNED NOT NULL,
    data LONGBLOB NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY pending_transaction_data_uq (pending_transaction_id),
    FOREIGN KEY pending_transaction_data_fk (pending_transaction_id) REFERENCES pending_transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE pending_transactions_dependent_transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    pending_transaction_id INT UNSIGNED NOT NULL,
    hash BINARY(32) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY pending_transaction_prevout_uq (pending_transaction_id, hash),
    FOREIGN KEY pending_transaction_prevout_fk (pending_transaction_id) REFERENCES pending_transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE addresses (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    address BINARY(20) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY addresses_uq (address)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE blocks (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash BINARY(32) NOT NULL,
    previous_block_id INT UNSIGNED,
    block_height INT UNSIGNED NOT NULL,
    blockchain_segment_id INT UNSIGNED,
    merkle_root BINARY(32) NOT NULL,
    version INT UNSIGNED NOT NULL DEFAULT '1',
    timestamp BIGINT UNSIGNED NOT NULL,
    difficulty BINARY(4) NOT NULL,
    nonce INT UNSIGNED NOT NULL,
    chain_work BINARY(32) NOT NULL,
    transaction_count INT UNSIGNED,
    byte_count INT UNSIGNED,
    PRIMARY KEY (id),
    UNIQUE KEY block_hash_uq (hash),
    UNIQUE KEY block_hash_uq2 (blockchain_segment_id, block_height),
    FOREIGN KEY block_previous_block_id_fk (previous_block_id) REFERENCES blocks (id),
    INDEX blocks_tx_count_ix (transaction_count) USING BTREE,
    INDEX blocks_height_ix (block_height) USING BTREE,
    INDEX blocks_work_ix (chain_work DESC) USING BTREE,
    INDEX blocks_work_ix2 (blockchain_segment_id, chain_work DESC) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE blockchain_segments (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    parent_blockchain_segment_id INT UNSIGNED,
    nested_set_left INT UNSIGNED,
    nested_set_right INT UNSIGNED,
    PRIMARY KEY (id),
    FOREIGN KEY blockchain_segments_parent_blockchain_segment_id (parent_blockchain_segment_id) REFERENCES blockchain_segments (id)
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

ALTER TABLE blocks ADD CONSTRAINT blocks_blockchain_segments_fk FOREIGN KEY (blockchain_segment_id) REFERENCES blockchain_segments (id);

CREATE TABLE transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hash BINARY(32) NOT NULL,
    byte_count INT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY transaction_hash_uq (hash)
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE block_transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    block_id INT UNSIGNED NOT NULL,
    transaction_id INT UNSIGNED NOT NULL,
    disk_offset BIGINT UNSIGNED NOT NULL,
    `index` INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY block_transactions_uq (block_id, transaction_id),
    FOREIGN KEY block_transactions_fk (block_id) REFERENCES blocks (id),
    FOREIGN KEY block_transactions_fk2 (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE unspent_transaction_outputs (
    transaction_hash BINARY(32) NOT NULL,
    `index` INT UNSIGNED NOT NULL,
    PRIMARY KEY (transaction_hash, `index`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE unconfirmed_transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id INT UNSIGNED NOT NULL,
    version INT UNSIGNED NOT NULL,
    lock_time BIGINT UNSIGNED NOT NULL,
    timestamp BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY unconfirmed_transaction_txid_uq (transaction_id),
    FOREIGN KEY unconfirmed_transaction_transaction_fk (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE unconfirmed_transaction_outputs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    unconfirmed_transaction_id INT UNSIGNED NOT NULL,
    `index` INT UNSIGNED NOT NULL,
    amount BIGINT UNSIGNED NOT NULL,
    locking_script BLOB NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY unconfirmed_transaction_output_tx_id_index_uq (unconfirmed_transaction_id, `index`),
    FOREIGN KEY unconfirmed_transaction_outputs_tx_id_fk (unconfirmed_transaction_id) REFERENCES unconfirmed_transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE unconfirmed_transaction_inputs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    unconfirmed_transaction_id INT UNSIGNED NOT NULL,
    `index` INT UNSIGNED NOT NULL,
    previous_transaction_hash BINARY(32) NOT NULL,
    previous_transaction_output_index INT UNSIGNED NOT NULL,
    sequence_number INT UNSIGNED NOT NULL,
    unlocking_script BLOB NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY transaction_inputs_tx_id_fk (unconfirmed_transaction_id) REFERENCES unconfirmed_transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE transaction_outputs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id INT UNSIGNED NOT NULL,
    output_index INT UNSIGNED NOT NULL,
    amount BIGINT UNSIGNED NOT NULL,
    address_id INT UNSIGNED,
    script_type_id INT UNSIGNED NOT NULL DEFAULT 1,
    slp_transaction_id INT UNSIGNED,
    PRIMARY KEY (id),
    UNIQUE KEY transaction_output_addresses_uq (transaction_id, output_index),
    FOREIGN KEY transaction_output_addresses_tx_fk (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE,
    FOREIGN KEY transaction_output_addresses_addr_fk (address_id) REFERENCES addresses (id) ON DELETE CASCADE,
    FOREIGN KEY transaction_output_addresses_scripts_type_id_fk (script_type_id) REFERENCES script_types (id),
    FOREIGN KEY transaction_output_addresses_slp_tx_fk (slp_transaction_id) REFERENCES transactions (id)
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE transaction_inputs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id INT UNSIGNED NOT NULL,
    input_index INT UNSIGNED NOT NULL,
    address_id INT UNSIGNED,
    PRIMARY KEY (id),
    UNIQUE KEY transaction_input_addresses_uq (transaction_id, input_index),
    FOREIGN KEY transaction_input_addresses_tx_fk (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE,
    FOREIGN KEY transaction_input_addresses_addr_fk (address_id) REFERENCES addresses (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE transaction_output_processor_queue (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY transaction_output_processor_queue_uq (transaction_id),
    FOREIGN KEY transaction_output_processor_queue_fk (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE validated_slp_transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id INT UNSIGNED NOT NULL,
    blockchain_segment_id INT UNSIGNED NOT NULL,
    is_valid TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY valid_slp_transactions_uq (transaction_id, blockchain_segment_id),
    FOREIGN KEY valid_slp_transactions_tx_id_fk (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE,
    FOREIGN KEY valid_slp_transactions_blockchain_segment_id_fk (blockchain_segment_id) REFERENCES blockchain_segments (id)
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE hosts (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    host VARCHAR(255) NOT NULL,
    is_banned TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
    banned_timestamp BIGINT UNSIGNED,
    PRIMARY KEY (id),
    UNIQUE KEY hosts_uq (host)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE nodes (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    host_id INT UNSIGNED NOT NULL,
    port INT UNSIGNED NOT NULL,
    first_seen_timestamp BIGINT UNSIGNED NOT NULL,
    last_seen_timestamp BIGINT UNSIGNED NOT NULL,
    connection_count INT UNSIGNED NOT NULL DEFAULT 1,
    last_handshake_timestamp BIGINT UNSIGNED,
    user_agent VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY nodes_uq (host_id, port),
    FOREIGN KEY nodes_host_id_fk (host_id) REFERENCES hosts (id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE node_features (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    node_id INT UNSIGNED NOT NULL,
    feature VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY node_features_uq (node_id, feature),
    FOREIGN KEY node_features_fk (node_id) REFERENCES nodes (id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;

CREATE TABLE node_blocks_inventory (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    node_id INT UNSIGNED NOT NULL,
    pending_block_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY node_blocks_uq (node_id, pending_block_id),
    FOREIGN KEY node_blocks_node_id_fk (node_id) REFERENCES nodes (id),
    FOREIGN KEY node_blocks_tx_fk (pending_block_id) REFERENCES pending_blocks (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

CREATE TABLE node_transactions_inventory (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    node_id INT UNSIGNED NOT NULL,
    pending_transaction_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY node_transactions_uq (node_id, pending_transaction_id),
    FOREIGN KEY node_transactions_node_id_fk (node_id) REFERENCES nodes (id),
    FOREIGN KEY node_transactions_tx_fk (pending_transaction_id) REFERENCES pending_transactions (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=LATIN1;

INSERT INTO metadata (version, timestamp) VALUES (2, UNIX_TIMESTAMP());
