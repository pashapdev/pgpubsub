package pgpubsub

import (
	"context"
	"database/sql"
	"fmt"
)

type Queue struct {
	connString string
	db         *sql.DB
	channel    string
}

func New(connString, channel string) (*Queue, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	q := &Queue{connString: connString, db: db, channel: channel}
	if err := q.prepareDB(context.Background()); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) Publish(ctx context.Context, message string) error {
	query := fmt.Sprintf("INSERT INTO %s (payload) VALUES ($1)", q.channel) //nolint:gosec
	_, err := q.db.ExecContext(ctx, query, message)
	return err
}

func (q *Queue) Close() {
	q.db.Close()
}

func (q *Queue) Subscriber() *Subscriber {
	return NewSubscriber(q.connString, q.channel)
}

func (q *Queue) prepareDB(ctx context.Context) error {
	const (
		createTable = `CREATE TABLE IF NOT EXISTS %[1]s (
			id serial,
			payload text
		  );`

		createFunction = `CREATE OR REPLACE FUNCTION notify_%[1]s() RETURNS TRIGGER AS $$
		DECLARE 
			notification json;
		BEGIN
			notification = json_build_object('payload', NEW.payload);
			PERFORM pg_notify('%[1]s',notification::text);
			RETURN NULL; 
		END;
		$$ LANGUAGE plpgsql;`

		enableTrigger = `DROP TRIGGER IF EXISTS %[1]s_notify_%[1]s on %[1]s;
		CREATE TRIGGER %[1]s_notify_%[1]s
		AFTER INSERT ON %[1]s
			FOR EACH ROW EXECUTE PROCEDURE notify_%[1]s();`
	)
	queries := [3]string{
		fmt.Sprintf(createTable, q.channel),
		fmt.Sprintf(createFunction, q.channel),
		fmt.Sprintf(enableTrigger, q.channel)}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, query := range queries {
		if _, err = tx.ExecContext(ctx, query); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				return errRollback
			}
			return err
		}
	}

	return tx.Commit()
}
