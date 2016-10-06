package db

type Iterator interface {
	Valid() bool
	Next()
	Close()
	SeekToFirst()
	SeekToLast()
	Seek(key []byte)
	KeyData() []byte
	FreeKey()
	KeySize() int
	ValueData() []byte
	FreeValue()
	ValueSize() int
	ValidForPrefix(prefix []byte) bool
	Prev()
}
