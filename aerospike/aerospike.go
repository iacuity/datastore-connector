package aerospike

import (
	"errors"
	"log"

	as "github.com/aerospike/aerospike-client-go/v5"
)

const (
	emptyBinName  = "_"
	emptyBinValue = true
)

type AerospikeConnector struct {
	client *as.Client
}

type AerospikeHost struct {
	Name string
	Port int
}

// return new Aerospike connector
func NewAerospikeConnector(aHosts []AerospikeHost) (*AerospikeConnector, error) {
	var hosts []*as.Host
	for _, host := range aHosts {
		hosts = append(hosts, &as.Host{Name: host.Name, Port: host.Port})
	}

	policy := as.NewClientPolicy()
	policy.FailIfNotConnected = true

	client, err := as.NewClientWithPolicyAndHost(policy, hosts...)
	if nil != err {
		return nil, err
	}

	if !client.IsConnected() {
		return nil, errors.New("Not able to connect Aerospike server!!!")
	}

	return &AerospikeConnector{
		client: client,
	}, nil
}

// expiryInSec value will be
// 0             : use namespace level ttl
// MaxUint32     : never expire
// MaxUint32 - 1 : do not update ttl incase of update record
// > 0           : Actual expiration in seconds
func (conn *AerospikeConnector) PutKey(namespace, set string, key interface{}, expiryInSec uint32) error {
	if nil == conn || nil == conn.client {
		return errors.New("Invalid Aerospike connector / client!!!")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return err
	}

	// binMap := as.BinMap{
	// 	emptyBinName: emptyBinValue,
	// }
	//conn.client.Put(as.NewWritePolicy(0, expiryInSec), akey, binMap)

	// its recommended to use PuBins over Put for performance reason
	bins := []*as.Bin{
		as.NewBin(emptyBinName, emptyBinValue),
	}
	conn.client.PutBins(as.NewWritePolicy(0, expiryInSec), akey, bins...)

	return nil
}

// expiryInSec value will be
// 0             : use namespace level ttl
// MaxUint32     : never expire
// MaxUint32 - 1 : do not update ttl incase of update record
// > 0           : Actual expiration in seconds
func (conn *AerospikeConnector) PutKeyValues(namespace, set string, key interface{}, values map[string]interface{}, expiryInSec uint32) error {
	if nil == conn || nil == conn.client {
		return errors.New("Invalid Aerospike connector / client!!!")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return err
	}

	conn.client.Put(as.NewWritePolicy(0, expiryInSec), akey, values)

	return nil
}

// delete the key
func (conn *AerospikeConnector) DeleteKey(namespace, set string, key interface{}) error {
	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return err
	}

	_, err = conn.client.Delete(nil, akey)

	return err
}

// check where any of the provided key is exists or not
func (conn *AerospikeConnector) AnyKeyExists(namespace, set string, keys []interface{}) (bool, error) {
	if nil == conn || nil == conn.client {
		return false, errors.New("Invalid Aerospike connector / client!!!")
	}

	var aKeys []*as.Key
	for _, key := range keys {
		akey, err := as.NewKey(namespace, set, key)
		if nil != err {
			return false, err
		}

		aKeys = append(aKeys, akey)
	}

	res, err := conn.client.BatchExists(nil, aKeys)
	if nil != err {
		return false, err
	}

	exist := false
	for _, val := range res {
		if val {
			exist = true
			break
		}
	}

	return exist, nil
}

// close the Aerospike client connection
func (conn *AerospikeConnector) Close() {
	conn.client.Close()
	log.Println("Closed Aerospike connection!!!")
}
