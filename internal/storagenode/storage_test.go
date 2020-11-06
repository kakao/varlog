package storagenode

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

func TestValidStorageName(t *testing.T) {
	var tests = []struct {
		in       string
		expected bool
	}{
		{"", false},
		{InMemoryStorageName, true},
		{PebbleStorageName, true},
	}
	for i := range tests {
		test := tests[i]
		t.Run(test.in, func(t *testing.T) {
			actual := ValidStorageName(test.in)
			if test.expected != (actual == nil) {
				t.Errorf("input=%v exepcted=%v actual=%v", test.in, test.expected, actual)
			}
		})
	}
}

func TestNewStorage(t *testing.T) {
	Convey("NewStorage", t, func() {

		Convey("With unknown name", func() {
			_, err := NewStorage("unknown")
			So(err, ShouldNotBeNil)
		})

		Convey(fmt.Sprintf("With %s", InMemoryStorageName), func() {
			s, err := NewStorage(InMemoryStorageName)
			So(err, ShouldBeNil)

			So(s.Name(), ShouldEqual, InMemoryStorageName)
			So(s.Path(), ShouldEqual, ":memory")
			So(s.Close(), ShouldBeNil)
		})

		Convey(fmt.Sprintf("With %s", PebbleStorageName), func() {

			Convey("Without path option", func() {
				_, err := NewStorage(PebbleStorageName)
				So(err, ShouldNotBeNil)
			})

			Convey("With path option", func() {
				tmpdir := t.TempDir()

				s, err := NewStorage(PebbleStorageName, WithPath(tmpdir))
				So(err, ShouldBeNil)

				So(s.Name(), ShouldEqual, PebbleStorageName)
				So(s.Path(), ShouldEqual, tmpdir)
				So(s.Close(), ShouldBeNil)
			})
		})
	})
}

func TestStorageOps(t *testing.T) {
	Convey("StorageOps for all storages", t, func() {
		const repeat = 100
		fz := fuzz.New()
		var storageList []Storage

		for name := range storages {
			storage, err := NewStorage(name, WithLogger(zap.L()), WithPath(t.TempDir()))
			So(err, ShouldBeNil)
			storageList = append(storageList, storage)
		}

		// test helper function
		forEach := func(f func(storage Storage)) func() {
			return func() {
				for _, storage := range storageList {
					f(storage)
				}
			}
		}

		Reset(func() {
			for _, storage := range storageList {
				So(storage.Close(), ShouldBeNil)
			}
		})

		Convey("It should not read an unwritten log", forEach(func(storage Storage) {
			for i := 0; i < repeat; i++ {
				var glsn types.GLSN
				fz.Fuzz(&glsn)
				_, err := storage.Read(glsn)
				So(err, ShouldNotBeNil)
			}
		}))

		Convey("It should not commit an unwritten log", forEach(func(storage Storage) {
			for i := 0; i < repeat; i++ {
				var glsn types.GLSN
				var llsn types.LLSN
				fz.Fuzz(&glsn)
				fz.Fuzz(&llsn)
				So(storage.Commit(llsn, glsn), ShouldNotBeNil)
			}
		}))

		Convey("Write logs", forEach(func(storage Storage) {
			for llsn := types.LLSN(1); llsn <= types.LLSN(repeat); llsn++ {
				var data []byte
				fz.NumElements(0, 1<<10).Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)
			}

			Convey(storage.Name()+" should not read uncommitted logs", func() {
				for glsn := types.GLSN(1); glsn <= types.GLSN(repeat); glsn++ {
					_, err := storage.Read(glsn)
					So(err, ShouldNotBeNil)
				}
			})

			// TODO (jun): Panic -> Error
			Convey(storage.Name()+" should panic when writing an unordered log", func() {
				So(func() {
					storage.Write(repeat+2, []byte("unordered"))
				}, ShouldPanic)
			})

			Convey(storage.Name()+" should commit written logs", func() {
				for llsn := types.LLSN(1); llsn <= types.LLSN(repeat); llsn++ {
					So(storage.Commit(llsn, types.GLSN(llsn)), ShouldBeNil)
				}

				Convey(storage.Name()+" should read committed logs", func() {
					for glsn := types.GLSN(1); glsn <= types.GLSN(repeat); glsn++ {
						logEntry, err := storage.Read(glsn)
						So(err, ShouldBeNil)
						So(logEntry.LLSN, ShouldEqual, types.LLSN(glsn))
					}
				})

				Convey(storage.Name()+" should scan log entries", func() {
					scanner, err := storage.Scan(1, 11)
					So(err, ShouldBeNil)

					expectedLLSN := types.MinLLSN
					for result := scanner.Next(); result.Valid(); result = scanner.Next() {
						So(result.Valid(), ShouldBeTrue)
						So(result.LogEntry.LLSN, ShouldEqual, expectedLLSN)
						So(result.LogEntry.GLSN, ShouldEqual, types.GLSN(expectedLLSN))
						expectedLLSN++
					}

					So(scanner.Close(), ShouldBeNil)
				})

				Convey(storage.Name()+" should delete committed logs", func() {
					So(storage.DeleteCommitted(types.GLSN(repeat)), ShouldBeNil)

					Convey(storage.Name()+" should not read deleted logs", func() {
						for glsn := types.GLSN(1); glsn <= types.GLSN(repeat); glsn++ {
							_, err := storage.Read(glsn)
							So(err, ShouldNotBeNil)
						}
					})
				})

				Convey(storage.Name()+".DeleteUncommitted should not delete committed data", func() {
					So(storage.DeleteUncommitted(1), ShouldNotBeNil)
				})
			})

			Convey(storage.Name()+".DeleteUncommitted should delete written data", func() {
				So(storage.DeleteUncommitted(1), ShouldBeNil)

				Convey(storage.Name()+" should not commit deleted logs", func() {
					for llsn := types.LLSN(1); llsn <= types.LLSN(repeat); llsn++ {
						So(storage.Commit(llsn, types.GLSN(llsn)), ShouldNotBeNil)
					}
				})
			})
		}))

	})
}

var globalLLSN = types.MinLLSN

func makeData(dataSize int64) []byte {
	fz := fuzz.New().NumElements(int(dataSize), int(dataSize))
	var data []byte
	fz.Fuzz(&data)
	return data
}

func makeStorage(name string, b *testing.B) (Storage, func()) {
	tmpdir, err := ioutil.TempDir("", "test_*")
	if err != nil {
		b.Fatal(err)
	}

	storage, err := NewStorage(name, WithLogger(zap.NewNop()), WithPath(tmpdir))
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		if err := storage.Close(); err != nil {
			b.Log(err)
		}
		if err := os.RemoveAll(tmpdir); err != nil {
			b.Log(err)
		}
	}

	return storage, cleanup
}

func benchmarkWrite(name string, dataSize int64, b *testing.B) {
	storage, cleanup := makeStorage(name, b)
	data := makeData(dataSize)
	globalLLSN = types.MinLLSN

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		storage.Write(globalLLSN, data)
		b.SetBytes(dataSize)
		globalLLSN++
	}
	b.StopTimer()

	cleanup()
}

func benchmarkWriteBatch(name string, dataNum int, dataSize int64, b *testing.B) {
	storage, cleanup := makeStorage(name, b)
	batchEnts := make([]WriteEntry, 0, dataSize)
	for i := 0; i < dataNum; i++ {
		batchEnts = append(batchEnts, WriteEntry{
			Data: makeData(dataSize),
		})
	}
	globalLLSN = types.MinLLSN

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		for i := range batchEnts {
			batchEnts[i].LLSN = globalLLSN
			globalLLSN++
		}
		b.StartTimer()
		storage.WriteBatch(batchEnts)
		b.SetBytes(int64(dataNum) * dataSize)
	}
	b.StopTimer()
	cleanup()
}

func BenchmarkInMemoryStorageWrite128(b *testing.B)  { benchmarkWrite(InMemoryStorageName, 128, b) }
func BenchmarkInMemoryStorageWrite256(b *testing.B)  { benchmarkWrite(InMemoryStorageName, 256, b) }
func BenchmarkInMemoryStorageWrite512(b *testing.B)  { benchmarkWrite(InMemoryStorageName, 512, b) }
func BenchmarkInMemoryStorageWrite1024(b *testing.B) { benchmarkWrite(InMemoryStorageName, 1024, b) }

func BenchmarkInMemoryStorageWriteBatch128(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 100, 128, b)
}

func BenchmarkInMemoryStorageWriteBatch256(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 100, 256, b)
}
func BenchmarkInMemoryStorageWriteBatch512(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 100, 512, b)
}
func BenchmarkInMemoryStorageWriteBatch1024(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 100, 1024, b)
}

func BenchmarkPebbleStorageWrite128(b *testing.B)  { benchmarkWrite(PebbleStorageName, 128, b) }
func BenchmarkPebbleStorageWrite256(b *testing.B)  { benchmarkWrite(PebbleStorageName, 256, b) }
func BenchmarkPebbleStorageWrite512(b *testing.B)  { benchmarkWrite(PebbleStorageName, 512, b) }
func BenchmarkPebbleStorageWrite1024(b *testing.B) { benchmarkWrite(PebbleStorageName, 1024, b) }

func BenchmarkPebbleStorageWriteBatch128(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 100, 128, b)
}
func BenchmarkPebbleStorageWriteBatch256(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 100, 256, b)
}
func BenchmarkPebbleStorageWriteBatch512(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 100, 512, b)
}
func BenchmarkPebbleStorageWriteBatch1024(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 100, 1024, b)
}
