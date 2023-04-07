package aerospike

import (
	"errors"
	"log"

	as "github.com/aerospike/aerospike-client-go/v6"
)

const (
	emptyBinName  = ""
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
	policy.ConnectionQueueSize = 256

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

func NewAerospikeConnectorWithClientPolicy(aHosts []AerospikeHost, policy *as.ClientPolicy) (*AerospikeConnector, error) {
	var hosts []*as.Host

	for _, host := range aHosts {
		hosts = append(hosts, &as.Host{Name: host.Name, Port: host.Port})
	}

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

// return the value of given key
func (conn *AerospikeConnector) GetKey(namespace, set string, key interface{}, binNames []string) (map[string]interface{}, error) {
	if nil == conn || nil == conn.client {
		return nil, errors.New("Invalid Aerospike connector / client!!!")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return nil, err
	}

	record, err := conn.client.Get(as.NewPolicy(), akey, binNames...)

	if nil != err {
		return nil, err
	}

	return (map[string]interface{})(record.Bins), err
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

// PutKeyWithObject store the key with object
func (conn *AerospikeConnector) PutKeyWithObject(namespace, set string, key interface{}, object interface{}, expiryInSec uint32) error {
	if nil == conn || nil == conn.client {
		return errors.New("invalid Aerospike connector / client")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return err
	}

	conn.client.PutObject(as.NewWritePolicy(0, expiryInSec), akey, object)

	return nil
}

// GetObjectByKey return object for given keys
func (conn *AerospikeConnector) GetObjectByKey(namespace, set string, key, object interface{}) error {
	if nil == conn || nil == conn.client {
		return errors.New("invalid Aerospike connector / client")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return err
	}
	err = conn.client.GetObject(as.NewPolicy(), akey, object)
	return err
}

// GetAutomicCounter return automic counter for given key by increment with given value
func (conn *AerospikeConnector) GetAutomicCounter(namespace, set string, key interface{}, value int, expiryInSec uint32) (int, error) {
	if nil == conn || nil == conn.client {
		return 0, errors.New("invalid Aerospike connector / client")
	}

	akey, err := as.NewKey(namespace, set, key)
	if nil != err {
		return 0, err
	}

	bin := as.NewBin(emptyBinName, value)

	record, err := conn.client.Operate(
		as.NewWritePolicy(0, expiryInSec),
		akey,
		as.AddOp(bin),
		as.GetOp(),
	)

	if nil != err {
		return 0, err
	}

	return record.Bins[""].(int), err
}

// close the Aerospike client connection
func (conn *AerospikeConnector) Close() {
	conn.client.Close()
	log.Println("Closed Aerospike connection!!!")
}
