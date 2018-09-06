package thronos

import (
	"fmt"

	"gopkg.in/couchbase/gocb.v1"
)

type cbConnector struct {
	config *CbConfig
}

func (connector cbConnector) connectToBucket() (bucket *gocb.Bucket, err error) {
	var cluster *gocb.Cluster
	cluster, err = gocb.Connect(connector.config.ClAddress)
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: connector.config.ClUsername,
		Password: connector.config.ClPassword,
	})
	bucket, err = cluster.OpenBucket(connector.config.BktName, connector.config.BktPassword)
	return bucket, err
}

type CbConfig struct {
	ClAddress   string
	ClUsername  string
	ClPassword  string
	BktName     string
	BktPassword string
}

const (
	delta   = 1
	initial = 0
	expiry  = 0
)

type CbConnector interface {
	ConnectToBucket() (*gocb.Bucket, error)
}

type CbHandler struct {
	Conn *gocb.Bucket
}

func NewCouchbaseHandler(config *CbConfig) *CbHandler {
	cbConn := cbConnector{config: config}
	buck, err := cbConn.connectToBucket()
	if err != nil {
		return nil
	}
	cbHandler := new(CbHandler)
	cbHandler.Conn = buck
	return cbHandler
}

func (handler *CbHandler) Execute(statement string) error {
	query := gocb.NewN1qlQuery(statement)
	_, err := handler.Conn.ExecuteN1qlQuery(query, nil)
	return err
}

func (handler *CbHandler) Query(statement string) []interface{} {
	query := gocb.NewN1qlQuery(statement)
	rows, err := handler.Conn.ExecuteN1qlQuery(query, nil)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	var result []interface{}
	var temp interface{}
	for rows.Next(&temp) {
		result = append(result, temp)
	}
	return result
}

func (handler *CbHandler) GetByKey(key string) (map[string]interface{}, error) {
	var result map[string]interface{}
	_, err := handler.Conn.Get(key, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (handler *CbHandler) Insert(data map[string]interface{}) error {
	_, err := handler.Conn.Insert(data["key"].(string), data, 0)
	return err
}

func (handler *CbHandler) Upsert(data map[string]interface{}) error {
	_, err := handler.Conn.Upsert(data["key"].(string), data, 0)
	return err
}

func (handler *CbHandler) GetCounter(key string) (uint64, error) {
	curKeyValue, _, err := handler.Conn.Counter(key, delta, initial, expiry)
	return curKeyValue, err
}

func (handler *CbHandler) Delete(key string, cas uint64) error {
	_, err := handler.Conn.Remove(key, gocb.Cas(cas))
	return err
}

func (handler *CbHandler) GetByKeyAndCas(key string) (map[string]interface{}, uint64, error) {
	var result map[string]interface{}
	cas, err := handler.Conn.Get(key, &result)
	if err != nil {
		return nil, uint64(cas), err
	}
	return result, uint64(cas), nil
}

func (handler *CbHandler) Replace(data map[string]interface{}, cas uint64) (uint64, error) {
	gocbCas, err := handler.Conn.Replace(data["key"].(string), data, gocb.Cas(cas), 0)
	return uint64(gocbCas), err
}
