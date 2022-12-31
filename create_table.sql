CREATE TABLE `file_metadata` (
   `file_metadata_id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `filename` TEXT,
   `size` INTEGER,
   `content_type` TEXT,
   `uid` TEXT,
   `storage_mode` TEXT,
   -- `storage_details` TEXT,
   -- `hash` TEXT,
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE `storage_nodes` (
	`storage_node_id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`uid` TEXT,
   `friendly_name` TEXT,
	`address` TEXT,
	`port_get_data` INTEGER,
   `port_store_data` INTEGER
);

CREATE TABLE `replicas` (
	`replica_id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`file_metadata_id` INTEGER,
	`storage_node_id` INTEGER,
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP,

	FOREIGN KEY(file_metadata_id) REFERENCES file_metadata(file_metadata_id),
	FOREIGN KEY(storage_node_id) REFERENCES storage_nodes(storage_node_id)
);
