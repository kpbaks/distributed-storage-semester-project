CREATE TABLE `file_metadata` (
   `file_metadata_id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `filename` TEXT,
   `size` INTEGER,
   `content_type` TEXT,
   -- `storage_mode` TEXT,
   -- `storage_details` TEXT,
   -- `hash` TEXT,
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE `storage_nodes` (
	`storage_node_id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`uid` TEXT,
	`address` TEXT,
	`port` INTEGER,
)

-- A fragment is a part of a file that is stored on a storage node
-- A fragment is uniquely identified by the hash of the file it belongs to
-- and the fragment number

CREATE TABLE `fragments` (
	`fragment_id` INTEGER PRIMARY KEY AUTOINCREMENT,
	`file_metadata_id` INTEGER,
	`storage_node_id` INTEGER,
	
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP
	FOREIGN KEY(file_metadata_id) REFERENCES file_metadata(file_metadata_id),
	FOREIGN KEY(storage_node_id) REFERENCES storage_nodes(storage_node_id)
)
