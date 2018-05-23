package thronos

import (
"gopkg.in/couchbase/gocb.v1"
)

type cbConnection struct {
	config *CbConfig
}

func (connector cbConnection) ConnectToBucket() (bucket *gocb.Bucket, err error) {
	var cluster *gocb.Cluster
	cluster, err = gocb.Connect(connector.config.ClAddress)
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: connector.config.ClUsername,
		Password: connector.config.ClPassword,
	})
	bucket, err = cluster.OpenBucket(connector.config.BktName, connector.config.BktPassword)
	return bucket, err
}