package metadatanode

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn *sql.DB
}

func OpenDB(filename string) (*DB, error) {
	conn, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	var name string
	// No errors until Scan, scan requires a location to store the value..
	err = conn.QueryRow(
		"select name from sqlite_master where type='table' and name='file_blocks'").Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		if _, err = conn.Exec("CREATE TABLE file_blocks(blob, block)"); err != nil {
			log.Fatalln(err)
		}
	case err != nil:
		log.Fatalln(err)
	default:
	}

	return &DB{conn}, err
}

// This is not concurrency-safe since SQLite3 is not
func (self *DB) Append(key, value string) error {
	_, err := self.conn.Exec("INSERT INTO file_blocks VALUES(?, ?)", key, value)
	return err
}

func (self *DB) Get(key string) ([]string, error) {
	rows, err := self.conn.Query("SELECT block FROM file_blocks WHERE blob=?", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []string
	for rows.Next() {
		var b string
		err = rows.Scan(&b)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}

	return blocks, nil
}
