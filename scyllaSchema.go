package elasticplugin

import (
	"database/sql"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gocql/gocql"
	"time"
)

type Adapter interface {
	MappingData(msg *message.Message) (*message.Message, error)
	// UnmarshalMessage transforms the Row obtained SelectQuery a Watermill message.
	UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error)
}

type scyllaSchema struct{}
type Model struct {
	UserID    gocql.UUID `json:"user_id"`
	M         string     `json:"m"`
	createdAt time.Time
}


func (s scyllaSchema) MappingData(msg *message.Message) (*message.Message, error) {
	return msg, nil
}

// UnmarshalMessage unmarshalling select query
func (s scyllaSchema) UnmarshalMessage(rows *sql.Rows) (msg *message.Message, err error) {
	var result Model
	err = rows.Scan(&result.UserID, &result.M)
	if err != nil {
		return nil, err
	}
	msg = message.NewMessage(watermill.NewULID(), []byte(fmt.Sprintf("%v", result)))
	return msg, nil
}