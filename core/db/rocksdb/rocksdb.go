package rocksdb

import (
	"path"
	"github.com/op/go-logging"
	"github.com/hyperledger/fabric/core/db"
	"github.com/tecbot/gorocksdb"
	"fmt"
	"os"
	"io"
	"github.com/spf13/viper"
	"strings"
)

const DBStoreType = "rocksdb"

const blockchainCF = "blockchainCF"
const stateCF = "stateCF"
const stateDeltaCF = "stateDeltaCF"
const indexesCF = "indexesCF"
const persistCF = "persistCF"

var columnfamilies = []string {
	blockchainCF, // blocks of the block chain
	stateCF,      // world state
	stateDeltaCF, // open transaction state
	indexesCF,    // tx uuid -> blockno
	persistCF,    // persistent per-peer state (consensus)
}

var dbLogger = logging.MustGetLogger("db")

var openchaindb db.OpenchainDB
var openchaindb_ptr *OpenchainRocksDB

type DbSnapshot struct {
*gorocksdb.Snapshot
}


type DbIterator struct {
	*gorocksdb.Iterator
}

const Name = `rocksdb`

type OpenchainRocksDB struct {
	DB           *gorocksdb.DB
	BlockchainCF *gorocksdb.ColumnFamilyHandle
	StateCF      *gorocksdb.ColumnFamilyHandle
	StateDeltaCF *gorocksdb.ColumnFamilyHandle
	IndexesCF    *gorocksdb.ColumnFamilyHandle
	PersistCF    *gorocksdb.ColumnFamilyHandle
}

func init() {
	openchaindb, _ = (db.Registry.Add(Name, func() db.OpenchainDB { return &OpenchainRocksDB{}}))
	openchaindb_ptr = openchaindb.(*OpenchainRocksDB)
}

func (openchainDB *OpenchainRocksDB) Type() string {
	return DBStoreType
}


func (openchainDB *OpenchainRocksDB) Start() {
	dbPath := getDBPath()
	fmt.Printf("dbPath inside Start",dbPath)
	missing, err := dirMissingOrEmpty(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Error while trying to open DB: %s", err))
	}
	dbLogger.Debugf("Is db path [%s] empty [%t]", dbPath, missing)

	if missing {
		fmt.Println("i am missing so creating a new one")
		err = os.MkdirAll(path.Dir(dbPath), 0755)
		if err != nil {
			panic(fmt.Sprintf("Error making directory path [%s]: %s", dbPath, err))
		}
	}

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	opts.SetCreateIfMissing(missing)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfNames := []string{"default"}
	cfNames = append(cfNames, columnfamilies...)
	var cfOpts []*gorocksdb.Options
	for range cfNames {
		cfOpts = append(cfOpts, opts)
	}

	db, cfHandlers, err := gorocksdb.OpenDbColumnFamilies(opts, dbPath, cfNames, cfOpts)

	if err != nil {
		panic(fmt.Sprintf("Error opening DB: %s", err))
	}

	openchainDB.DB = db
	openchainDB.BlockchainCF = cfHandlers[1]
	openchainDB.StateCF = cfHandlers[2]
	openchainDB.StateDeltaCF = cfHandlers[3]
	openchainDB.IndexesCF = cfHandlers[4]
	openchainDB.PersistCF = cfHandlers[5]
}


func (openchainDB *OpenchainRocksDB) GetFromBlockchain(key []byte) ([]byte, error) {
	return openchainDB.Get(openchainDB.BlockchainCF, key)
}


func (openchainDB *OpenchainRocksDB) GetFromPersistence(key []byte) ([]byte, error) {
	return openchainDB.Get(openchainDB.PersistCF, key)
}


 //Stop the db. Note this method has no guarantee correct behavior concurrent invocation.
func (openchainDB *OpenchainRocksDB) Stop() {
	        //openchainDB, _ = db.Registry.Get(Name)
		openchainDB.BlockchainCF.Destroy()
		openchainDB.StateCF.Destroy()
		openchainDB.StateDeltaCF.Destroy()
		openchainDB.IndexesCF.Destroy()
		openchainDB.PersistCF.Destroy()
		openchainDB.DB.Close()
}

func (openchainDB *OpenchainRocksDB) GetFromBlockchainSnapshot(snapshot db.Snapshot, key []byte) ([]byte, error) {
	return openchainDB.getFromSnapshot(snapshot, openchainDB.BlockchainCF, key)
}

// GetFromStateCF get value for given key from column family - stateCF
func (openchainDB *OpenchainRocksDB) GetFromState(key []byte) ([]byte, error) {
	return openchainDB.Get(openchainDB.StateCF, key)
}

// GetFromStateDeltaCF get value for given key from column family - stateDeltaCF
func (openchainDB *OpenchainRocksDB) GetFromStateDelta(key []byte) ([]byte, error) {
	return openchainDB.Get(openchainDB.StateDeltaCF, key)
}

// GetFromIndexesCF get value for given key from column family - indexCF
func (openchainDB *OpenchainRocksDB) GetFromIndexes(key []byte) ([]byte, error) {
	return openchainDB.Get(openchainDB.IndexesCF, key)
}

 //GetBlockchainCFIterator get iterator for column family - blockchainCF
func (openchainDB *OpenchainRocksDB) GetBlockchainIterator() db.Iterator {
	return openchainDB.GetCFIterator(openchainDB.BlockchainCF)
}


//GetPersistCFIterator get iterator for column family - persistCF
func (openchainDB *OpenchainRocksDB) GetPersistCFIterator() db.Iterator {
	return openchainDB.GetCFIterator(openchainDB.PersistCF)
}

 //GetStateCFIterator get iterator for column family - stateCF
func (openchainDB *OpenchainRocksDB) GetStateIterator() db.Iterator {
	return openchainDB.GetCFIterator(openchainDB.StateCF)
}


// GetIterator returns an iterator for the given column family
func (s *OpenchainRocksDB) GetCFIterator(cfHandler *gorocksdb.ColumnFamilyHandle) db.Iterator {
	opt := gorocksdb.NewDefaultReadOptions()
	opt.SetFillCache(true)
	defer opt.Destroy()
	return &DbIterator{s.DB.NewIteratorCF(opt,cfHandler)}
}



// GetStateCFSnapshotIterator get iterator for column family - stateCF. This iterator
// is based on a snapshot and should be used for long running scans, such as
// reading the entire state. Remember to call iterator.Close() when you are done.
func (openchainDB *OpenchainRocksDB) GetStateSnapshotIterator(snapshot db.Snapshot) db.Iterator {
	return openchainDB.getSnapshotIterator(snapshot, openchainDB.StateCF)
}

// GetStateDeltaCFIterator get iterator for column family - stateDeltaCF
func (openchainDB *OpenchainRocksDB) GetStateDeltaIterator() db.Iterator {
	return openchainDB.GetCFIterator(openchainDB.StateDeltaCF)
}

// GetSnapshot returns a point-in-time view of the DB. You MUST call snapshot.Release()
// when you are done with the snapshot.

func getDBPath() string {

	dbPath := viper.GetString("peer.fileSystemPath")

	if dbPath == "" {
		panic("DB path not specified in configuration file. Please check that property 'peer.fileSystemPath' is set")
	}
	if !strings.HasSuffix(dbPath, "/") {
		dbPath = dbPath + "/"
	}
	return dbPath + "db"
}

//// DeleteState delets ALL state keys/values from the DB. This is generally
//// only used during state synchronization when creating a new state from
//// a snapshot.
func (openchainDB *OpenchainRocksDB) DeleteState() error {
	err := openchainDB.DB.DropColumnFamily(openchainDB.StateCF)
	if err != nil {
		dbLogger.Errorf("Error dropping state CF: %s", err)
		return err
	}
	err = openchainDB.DB.DropColumnFamily(openchainDB.StateDeltaCF)
	if err != nil {
		dbLogger.Errorf("Error dropping state delta CF: %s", err)
		return err
	}
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	openchainDB.StateCF, err = openchainDB.DB.CreateColumnFamily(opts, stateCF)
	if err != nil {
		dbLogger.Errorf("Error creating state CF: %s", err)
		return err
	}
	openchainDB.StateDeltaCF, err = openchainDB.DB.CreateColumnFamily(opts, stateDeltaCF)
	if err != nil {
		dbLogger.Errorf("Error creating state delta CF: %s", err)
		return err
	}
	return nil
}

// Get returns the valud for the given column family and key


func (openchainDB *OpenchainRocksDB) Get(cfHandler *gorocksdb.ColumnFamilyHandle, key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	slice, err := openchainDB.DB.GetCF(opt, cfHandler, key)
	if err != nil {
		dbLogger.Errorf("Error while trying to retrieve key: %s", key)
		return nil, err
	}
	defer slice.Free()
	if slice.Data() == nil {
		return nil, nil
	}
	data := makeCopy(slice.Data())
	return data, nil
}

func (openchainDB *OpenchainRocksDB) PutToPersistence(key []byte, value []byte) error {
	return openchainDB.Put(openchainDB.StateDeltaCF, key,value)
}

// Put saves the key/value in the given column family
func (openchainDB *OpenchainRocksDB) Put(cfHandler *gorocksdb.ColumnFamilyHandle, key []byte, value []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	err := openchainDB.DB.PutCF(opt, cfHandler, key, value)
	if err != nil {
		dbLogger.Errorf("Error while trying to write key: %s", key)
		return err
	}
	return nil
}

func (openchainDB *OpenchainRocksDB) DeletePersist(key []byte) error {
	return openchainDB.Delete(openchainDB.PersistCF, key)
}


// Delete delets the given key in the specified column family
func (openchainDB *OpenchainRocksDB) Delete(cfHandler *gorocksdb.ColumnFamilyHandle, key []byte) error {
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	err := openchainDB.DB.DeleteCF(opt, cfHandler, key)
	if err != nil {
		dbLogger.Errorf("Error while trying to delete key: %s", key)
		return err
	}
	return nil
}

func (openchainDB *OpenchainRocksDB) getFromSnapshot(snapshot db.Snapshot, cfHandler *gorocksdb.ColumnFamilyHandle, key []byte) ([]byte, error) {
	opt := gorocksdb.NewDefaultReadOptions()
	defer opt.Destroy()
	snapshot_ptr := snapshot.(*DbSnapshot).Snapshot
	opt.SetSnapshot(snapshot_ptr)
	slice, err := openchainDB.DB.GetCF(opt, cfHandler, key)
	if err != nil {
		dbLogger.Errorf("Error while trying to retrieve key: %s", key)
		return nil, err
	}
	defer slice.Free()
	data := append([]byte(nil), slice.Data()...)
	return data, nil
}


func (openchainDB *OpenchainRocksDB) GetSnapshot() db.Snapshot {
	return &DbSnapshot{openchainDB.DB.NewSnapshot()}
}

func (openchainDB *OpenchainRocksDB) getSnapshotIterator(snapshot db.Snapshot, cfHandler *gorocksdb.ColumnFamilyHandle) db.Iterator {
	opt := gorocksdb.NewDefaultReadOptions()
	snapshot_ptr := snapshot.(*DbSnapshot).Snapshot
	defer opt.Destroy()
	opt.SetSnapshot(snapshot_ptr)
	return &DbIterator{openchainDB.DB.NewIteratorCF(opt,cfHandler)}
}

func dirMissingOrEmpty(path string) (bool, error) {
	dirExists, err := dirExists(path)
	if err != nil {
		return false, err
	}
	if !dirExists {
		return true, nil
	}

	dirEmpty, err := dirEmpty(path)
	if err != nil {
		return false, err
	}
	if dirEmpty {
		return true, nil
	}
	return false, nil
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func dirEmpty(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func makeCopy(src []byte) []byte {
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}











