package kv

type TestHelper interface {
	GetWorkingDirectory(store KeyValueStore) string
	GetEmptyInstance() KeyValueStore
}

type StubTestHelper struct {
}

func (StubTestHelper) GetWorkingDirectory(store KeyValueStore) string {
	return ""
}

func (StubTestHelper) GetEmptyInstance() KeyValueStore {
	return new(KvStoreStub)
}

func NewStubTestHelper() TestHelper {
	newTestHelper := new(StubTestHelper)
	return newTestHelper
}
