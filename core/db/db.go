package db

type ConnnectionManager interface {
	Stop()
	Start()
}

type OpenchainDB interface {
	ConnnectionManager
	StateManager
	Type() string
}

type StateManager interface {
	//A set of get methods which are used to interact with different column families or tables based on the underlying DB support
	GetFromBlockchain(key []byte) ([]byte, error)
	GetFromState(key []byte) ([]byte, error)
	GetFromStateDelta([]byte) ([]byte, error)
	GetFromPersistence([]byte) ([]byte, error)
	GetFromIndexes(key []byte) ([]byte, error)

        // GetSnapshot method gives a snapshot of the DB at a given point in time
	GetSnapshot() Snapshot
	GetFromBlockchainSnapshot(snapshot Snapshot, key []byte) ([]byte, error)

        // Iterators for dealing with different DB tables or column families
	GetBlockchainIterator() Iterator
	GetStateSnapshotIterator(snapshot Snapshot) Iterator
	GetStateIterator() Iterator
	GetStateDeltaIterator() Iterator

	//A Put method to interact with the Put column family
	PutToPersistence(key []byte, value []byte) error
	DeleteState() error
}





