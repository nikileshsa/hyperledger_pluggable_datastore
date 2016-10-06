package rocksdb

import "github.com/tecbot/gorocksdb"

type Iterator struct {
	*gorocksdb.Iterator
}

////non-copy values
//type Slice struct {
//	*gorocksdb.Slice
//}
//
//type Key struct {
//	*gorocksdb.Slice
//}

//type Value struct {
//	*gorocksdb.Slice
//}

func (iterator *DbIterator) KeyData() [] byte {
	return iterator.Iterator.Key().Data()
}

func (iterator *DbIterator) FreeKey() {
	iterator.Iterator.Key().Free()
}

func (iterator *DbIterator) KeySize() int {
	return iterator.Iterator.Key().Size()
}

func (iterator *DbIterator) ValueData() [] byte {
	return iterator.Iterator.Value().Data()
}

func (iterator *DbIterator) FreeValue() {
	iterator.Iterator.Key().Free()
}

func (iterator *DbIterator) ValueSize() int {
	return iterator.Iterator.Key().Size()
}

func (iterator *DbIterator) Prev() {
	iterator.Iterator.Prev()
}


func (iterator *DbIterator) Value() []byte {
	return iterator.Iterator.Value().Data()
}

func (i *Iterator) First() (key, value []byte) {
	i.Iterator.SeekToFirst()
	if !i.Valid() {
		return nil, nil
	}
	return i.Iterator.Key().Data(), i.Iterator.Value().Data()
}

func (i *Iterator) Valid() bool {
	return i.Iterator.Valid()
}

func (i *Iterator) Next() (key, value []byte) {
	i.Iterator.Next()
	if !i.Valid() {
		return nil, nil
	}
	return i.Iterator.Key().Data(), i.Iterator.Value().Data()
}

func (i *Iterator) Close() {
}

