package thronos

import (
	"fmt"

	"gopkg.in/couchbase/gocb.v1"
)
const (
	delta   = 1
	initial = 0
	expiry  = 0
)

type CbConfig struct {
	ClAddress   string
	ClUsername  string
	ClPassword  string
	BktName     string
	BktPassword string
}

type Thronos struct {
	Conn *gocb.Bucket
}

func NewThronos(ClAddress string, ClUsername string, ClPassword string, BktName string, BktPassword string) *Thronos {
	config := CbConfig{
		ClAddress:   ClAddress,
		ClUsername:  ClUsername,
		ClPassword:  ClPassword,
		BktName:     BktName,
		BktPassword: BktPassword,
	}
	cbConn := cbConnection{config: &config}
	buck, err := cbConn.ConnectToBucket()
	if err != nil {
		return nil
	}
	cbHandler := new(Thronos)
	cbHandler.Conn = buck
	return cbHandler
}

func (t *Thronos) ExecuteQuery(statement string) error {
	query := gocb.NewN1qlQuery(statement)
	_, err := t.Conn.ExecuteN1qlQuery(query, nil)
	return err
}

func (t *Thronos) ExecuteQueryWithReturn(statement string) ([]interface{}, error) {
	query := gocb.NewN1qlQuery(statement)
	rows, err := t.Conn.ExecuteN1qlQuery(query, nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var result []interface{}
	var temp interface{}
	for rows.Next(&temp) {
		result = append(result, temp)
	}
	return result, nil
}

func (t *Thronos) GetByKey(key string) (map[string]interface{}, error) {
	var result map[string]interface{}
	_, err := t.Conn.Get(key, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (t *Thronos) Insert(data map[string]interface{}) error {
	_, err := t.Conn.Insert(data["key"].(string), data, 0)
	return err
}

func (t *Thronos) Upsert(data map[string]interface{}) error {
	_, err := t.Conn.Upsert(data["key"].(string), data, 0)
	return err
}

func (t *Thronos) GetKeyCounter(key string) (uint64, error) {
	curKeyValue, _, err := t.Conn.Counter(key, delta, initial, expiry)
	return curKeyValue, err
}