/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rocksdb

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"io/ioutil"
	"fmt"
	"github.com/hyperledger/fabric/core/db"
	"bytes"
	"github.com/tecbot/gorocksdb"
)

const dbName = `rocksdb`

func TestMain(m *testing.M) {
	setupTestConfig()
	os.Exit(m.Run())
}

func TestGetDBPathEmptyPath(t *testing.T) {
	originalSetting := viper.GetString("peer.fileSystemPath")
	fmt.Printf("path path %s path path",originalSetting)
	viper.Set("peer.fileSystemPath", "")
	defer func() {
		x := recover()
		if x == nil {
			t.Fatal("A panic should have been caused here.")
			fmt.Printf("%s",x)
		}
	}()
	defer viper.Set("peer.fileSystemPath", originalSetting)


	fmt.Println(db.Registry.Registered())
	fmt.Printf("db name is %s ",dbName)
	openchaindb.Start()
	openchaindb.Stop()
}
//doesn't make sense anymore because if the directory is missing we are creating a new directory using the path
func TestStartDB_DirDoesNotExist(t *testing.T) {

	deleteTestDBPath()

	defer deleteTestDBPath()
	fmt.Println(db.Registry.Registered())
	fmt.Printf("db name is %s ",dbName)

	defer openchaindb.Stop()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Failed to open DB: %s", r)
		}
	}()
	openchaindb.Start()
}

func TestStartDB_NonEmptyDirExists(t *testing.T) {
	deleteTestDBPath()
	createNonEmptyTestDBPath()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("dbPath is already exists. DB open should throw error")
		}
	}()
	fmt.Println(db.Registry.Registered())
	fmt.Printf("db name is %s ",dbName)
	openchaindb.Start()
}

func TestWriteAndRead(t *testing.T) {
	deleteTestDBPath()
	fmt.Println(db.Registry.Registered())
	fmt.Printf("db name is %s ",dbName)

	openchaindb.Start()
	defer deleteTestDBPath()
	defer openchaindb.Stop()

	if db, ok := openchaindb.(*OpenchainRocksDB); ok {
		performBasicReadWrite(db, t)
	} else {
		fmt.Println("You dont know what you are doing")
	}
}


func TestDeleteState(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := openchaindb_ptr
	defer testDBWrapper.cleanup()
	openchainDB.Put(openchainDB.StateCF, []byte("key1"), []byte("value1"))
	openchainDB.Put(openchainDB.StateDeltaCF, []byte("key2"), []byte("value2"))
	openchainDB.DeleteState()
	value1, err := openchainDB.GetFromState([]byte("key1"))
	if err != nil {
		t.Fatalf("Error getting in value: %s", err)
	}
	if value1 != nil {
		t.Fatalf("A nil value expected. Found [%s]", value1)
	}

	value2, err := openchainDB.GetFromState([]byte("key2"))
	if err != nil {
		t.Fatalf("Error getting in value: %s", err)
	}
	if value2 != nil {
		t.Fatalf("A nil value expected. Found [%s]", value2)
	}
}

func TestDBSnapshot(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := openchaindb_ptr
	defer testDBWrapper.cleanup()

	// write key-values
	openchainDB.Put(openchainDB.BlockchainCF, []byte("key1"), []byte("value1"))
	openchainDB.Put(openchainDB.BlockchainCF, []byte("key2"), []byte("value2"))

	// create a snapshot
	snapshot := openchainDB.GetSnapshot()

	// add/delete/modify key-values
	openchainDB.Delete(openchainDB.BlockchainCF, []byte("key1"))
	openchainDB.Put(openchainDB.BlockchainCF, []byte("key2"), []byte("value2_new"))
	openchainDB.Put(openchainDB.BlockchainCF, []byte("key3"), []byte("value3"))

	// test key-values from latest data in db
	v1, _ := openchainDB.GetFromBlockchain([]byte("key1"))
	v2, _ := openchainDB.GetFromBlockchain([]byte("key2"))
	v3, _ := openchainDB.GetFromBlockchain([]byte("key3"))
	if !bytes.Equal(v1, nil) {
		t.Fatalf("Expected value from db is 'nil', found [%s]", v1)
	}
	if !bytes.Equal(v2, []byte("value2_new")) {
		t.Fatalf("Expected value from db [%s], found [%s]", "value2_new", v2)
	}
	if !bytes.Equal(v3, []byte("value3")) {
		t.Fatalf("Expected value from db [%s], found [%s]", "value3", v3)
	}

	// test key-values from snapshot
	v1, _ = openchainDB.GetFromBlockchainSnapshot(snapshot, []byte("key1"))
	v2, _ = openchainDB.GetFromBlockchainSnapshot(snapshot, []byte("key2"))
	v3, err := openchainDB.GetFromBlockchainSnapshot(snapshot, []byte("key3"))
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if !bytes.Equal(v1, []byte("value1")) {
		t.Fatalf("Expected value from db snapshot [%s], found [%s]", "value1", v1)
	}

	if !bytes.Equal(v2, []byte("value2")) {
		t.Fatalf("Expected value from db snapshot [%s], found [%s]", "value1", v2)
	}

	if !bytes.Equal(v3, nil) {
		t.Fatalf("Expected value from db snapshot is 'nil', found [%s]", v3)
	}
}

func TestDBIteratorAndSnapshotIterator(t *testing.T) {
	testDBWrapper := NewTestDBWrapper()
	testDBWrapper.CleanDB(t)
	openchainDB := openchaindb_ptr
	defer testDBWrapper.cleanup()

	// write key-values
	openchainDB.Put(openchainDB.StateCF, []byte("key1"), []byte("value1"))
	openchainDB.Put(openchainDB.StateCF, []byte("key2"), []byte("value2"))

	// create a snapshot
	snapshot := openchainDB.GetSnapshot()

	// add/delete/modify key-values
	openchainDB.Delete(openchainDB.StateCF, []byte("key1"))
	openchainDB.Put(openchainDB.StateCF, []byte("key2"), []byte("value2_new"))
	openchainDB.Put(openchainDB.StateCF, []byte("key3"), []byte("value3"))

	// test snapshot iterator
	itr := openchainDB.GetStateSnapshotIterator(snapshot)
	defer itr.Close()
	testIterator(t, itr, map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")})

	// test iterator over latest data in stateCF
	itr = openchainDB.GetStateIterator()
	defer itr.Close()
	testIterator(t, itr, map[string][]byte{"key2": []byte("value2_new"), "key3": []byte("value3")})

	openchainDB.Put(openchainDB.StateDeltaCF, []byte("key4"), []byte("value4"))
	openchainDB.Put(openchainDB.StateDeltaCF, []byte("key5"), []byte("value5"))
	itr = openchainDB.GetStateDeltaIterator()
	defer itr.Close()
	testIterator(t, itr, map[string][]byte{"key4": []byte("value4"), "key5": []byte("value5")})

	openchainDB.Put(openchainDB.BlockchainCF, []byte("key6"), []byte("value6"))
	openchainDB.Put(openchainDB.BlockchainCF, []byte("key7"), []byte("value7"))
	itr = openchainDB.GetBlockchainIterator()
	defer itr.Close()
	testIterator(t, itr, map[string][]byte{"key6": []byte("value6"), "key7": []byte("value7")})
}

// db helper functions
func testIterator(t *testing.T, itr db.Iterator, expectedValues map[string][]byte) {
	itrResults := make(map[string][]byte)
	itr.SeekToFirst()
	for ; itr.Valid(); itr.Next() {
		key := itr.KeyData()
		value := itr.ValueData()
		k := makeCopy(key)
		v := makeCopy(value)
		itrResults[string(k)] = v
	}
	if len(itrResults) != len(expectedValues) {
		t.Fatalf("Expected [%d] results from iterator, found [%d]", len(expectedValues), len(itrResults))
	}
	for k, v := range expectedValues {
		if !bytes.Equal(itrResults[k], v) {
			t.Fatalf("Wrong value for key [%s]. Expected [%s], found [%s]", k, itrResults[k], v)
		}
	}
}

func createNonEmptyTestDBPath() {
	dbPath := viper.GetString("peer.fileSystemPath")
	os.MkdirAll(dbPath+"/db/tmpFile", 0775)
}

func deleteTestDBPath() {
	dbPath := viper.GetString("peer.fileSystemPath")
	os.RemoveAll(dbPath)

}

func setupTestConfig() {
	fmt.Println("I am here in setup")
	tempDir, err := ioutil.TempDir("", "fabric-db-test")
	if err != nil {
		panic(err)
	}
	fmt.Println("Test setup completed successfully")
	viper.Set("peer.fileSystemPath", tempDir)
	fmt.Println(tempDir)
	deleteTestDBPath()
}

func performBasicReadWrite(openchainDB *OpenchainRocksDB, t *testing.T) {

	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	writeBatch.PutCF(openchainDB.BlockchainCF, []byte("dummyKey"), []byte("dummyValue"))
	writeBatch.PutCF(openchainDB.StateCF, []byte("dummyKey1"), []byte("dummyValue1"))
	writeBatch.PutCF(openchainDB.StateDeltaCF, []byte("dummyKey2"), []byte("dummyValue2"))
	writeBatch.PutCF(openchainDB.IndexesCF, []byte("dummyKey3"), []byte("dummyValue3"))
	err := openchainDB.DB.Write(opt, writeBatch)

	if err != nil {
		t.Fatalf("Error while writing to db: %s", err)
	}
	value, err := openchainDB.GetFromBlockchain([]byte("dummyKey"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue", value)
	}

	value, err = openchainDB.GetFromState([]byte("dummyKey1"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue1")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue1", value)
	}

	value, err = openchainDB.GetFromStateDelta([]byte("dummyKey2"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue2")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue2", value)
	}

	value, err = openchainDB.GetFromIndexes([]byte("dummyKey3"))
	if err != nil {
		t.Fatalf("read error = [%s]", err)
	}
	if !bytes.Equal(value, []byte("dummyValue3")) {
		t.Fatalf("read error. Bytes not equal. Expected [%s], found [%s]", "dummyValue3", value)
	}
}
