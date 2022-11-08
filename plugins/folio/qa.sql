CREATE TABLE FolioType (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE AuditLog (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  checked_on DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
  record_id INTEGER NOT NULL,
  status INTEGER NOT NULL,
  FOREIGN KEY (record_id) REFERENCES Record (id),
  FOREIGN KEY (status) REFERENCES Status (id)
);

CREATE TABLE Record (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  uuid VARCHAR NOT NULL,
  hrid VARCHAR,
  folio_type INTEGER,
  current_version VARCHAR,
  last_updated DATETIME,
  FOREIGN KEY (folio_type) REFERENCES FolioType (id)
);

CREATE TABLE Status (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  name VARCHAR NOT NULL
);

CREATE Table Errors (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  message VARCHAR NOT NULL,
  http_status_code INTEGER,
  log_id INTEGER,
  FOREIGN KEY (log_id) REFERENCES AuditLog (id)
);

CREATE TABLE JsonPayload (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  record_id INTEGER NOT NULL UNIQUE,
  payload BLOB NOT NULL,
  FOREIGN KEY (record_id) REFERENCES Record (id)
);

CREATE UNIQUE INDEX IdxRecord ON Record (id);

INSERT INTO FolioType (id, name) VALUES (0, "Holding");
INSERT INTO FolioType (id, name) VALUES (1, "Item");
INSERT INTO FolioType (id, name) VALUES (2, "Instance");

INSERT INTO Status (name) VALUES ("Exists");
INSERT INTO Status (name) VALUES ("Missing");
INSERT INTO Status (name) VALUES ("Error");
