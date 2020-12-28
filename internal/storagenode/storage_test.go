package storagenode

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestStorageName(t *testing.T) {
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

func TestStorageNewStorage(t *testing.T) {
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

func withForeachStorage(t *testing.T, f func(storage Storage)) func() {
	rand.Seed(time.Now().UnixNano())
	var storageList []Storage
	for name := range storages {
		// TODO (jun): Implement new methods and behavior of InMemoryStorage
		if name == InMemoryStorageName {
			continue
		}
		storage, err := NewStorage(name, WithLogger(zap.L()), WithPath(t.TempDir()))
		So(err, ShouldBeNil)
		storageList = append(storageList, storage)
	}

	Reset(func() {
		for _, storage := range storageList {
			So(storage.Close(), ShouldBeNil)
		}
	})

	return func() {
		for _, storage := range storageList {
			f(storage)
		}
	}
}

func TestStorageReadUnwrittenLog(t *testing.T) {
	Convey("When storage reads unwritten logs", t, func() {
		Convey("Storage should not read them", withForeachStorage(t, func(storage Storage) {
			fz := fuzz.New()
			var glsn types.GLSN
			for i := 0; i < 100; i++ {
				fz.Fuzz(&glsn)
				_, err := storage.Read(glsn)
				So(err, ShouldNotBeNil)
			}
		}))
	})
}

func TestStorageScanUnwrittenLog(t *testing.T) {
	Convey("When storage scans unwritten logs", t, func() {
		Convey("Storage should not scan them", withForeachStorage(t, func(storage Storage) {
			scanner, err := storage.Scan(1, 100)
			So(err, ShouldBeNil)
			So(scanner.Next().Valid(), ShouldBeFalse)
			So(scanner.Close(), ShouldBeNil)
		}))
	})
}

func TestStorageCommitUnwrittenLog(t *testing.T) {
	Convey("When storage commit unwritten logs", t, func() {
		Convey("Storage should not commit them", withForeachStorage(t, func(storage Storage) {
			fz := fuzz.New()
			var glsn types.GLSN
			var llsn types.LLSN
			for i := 0; i < 100; i++ {
				fz.Fuzz(&glsn)
				fz.Fuzz(&llsn)
				So(storage.Commit(llsn, glsn), ShouldNotBeNil)
			}
		}))
	})
}

func TestStorageWriteNotInOrder(t *testing.T) {
	Convey("When storage does not write logs in order", t, func() {
		Convey("it should return an error", withForeachStorage(t, func(storage Storage) {
			const (
				begin       = 1
				end         = 11
				nilChance   = 0.3
				minDataSize = 0
				maxDataSize = 1 << 10
			)

			fz := fuzz.New().NilChance(nilChance).NumElements(minDataSize, maxDataSize)
			for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)
			}

			So(storage.Write(end+1, []byte("unordered")), ShouldNotBeNil)
			So(storage.Write(end-1, []byte("unordered")), ShouldNotBeNil)
			So(storage.Write(end-2, []byte("unordered")), ShouldNotBeNil)
			So(storage.Write(types.InvalidLLSN, []byte("unordered")), ShouldNotBeNil)
			So(storage.Write(types.MaxLLSN, []byte("unordered")), ShouldNotBeNil)

			nextLLSN := types.LLSN(end)
			for i := 0; i < 100; i++ {
				var llsn types.LLSN
				fz.Fuzz(&llsn)

				var data []byte
				fz.Fuzz(&data)

				if llsn == nextLLSN {
					So(storage.Write(llsn, data), ShouldBeNil)
					nextLLSN = llsn
					continue
				}
				So(storage.Write(llsn, data), ShouldNotBeNil)
			}
		}))
	})
}

func TestStorageWriteConcurrently(t *testing.T) {
	Convey("When multiple goroutines write concurrently", t, func() {
		Convey("Storage should write logs in order", withForeachStorage(t, func(storage Storage) {
			const (
				begin         = 1
				end           = 101
				numGoroutines = 3
			)

			numWritten := int32(0)
			var wg sync.WaitGroup
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fz := fuzz.New()
					for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
						var data []byte
						fz.NumElements(0, 1<<10).Fuzz(&data)
						if err := storage.Write(llsn, data); err == nil {
							atomic.AddInt32(&numWritten, 1)
						}
					}
				}()
			}
			wg.Wait()
			So(atomic.LoadInt32(&numWritten), ShouldEqual, end-1)

			cb := storage.NewCommitBatch()
			for pos := begin; pos < end; pos++ {
				So(cb.Put(types.LLSN(pos), types.GLSN(pos)), ShouldBeNil)
			}
			So(cb.Apply(), ShouldBeNil)
			So(cb.Close(), ShouldBeNil)
		}))
	})
}

func TestStorageWriteBatchConcurrently(t *testing.T) {
	Convey("When multiple goroutines call WriteBatch concurrently", t, func() {
		Convey("Storage should commit only one of them", withForeachStorage(t, func(storage Storage) {
			const (
				begin = 1
				end   = 101
				mid   = (end - begin) / 2
			)
			fz := fuzz.New()

			wb1 := storage.NewWriteBatch()
			for llsn := types.LLSN(begin); llsn < types.LLSN(mid); llsn++ {
				var data []byte
				fz.NumElements(0, 1<<10).Fuzz(&data)
				So(wb1.Put(llsn, data), ShouldBeNil)
			}

			wb2 := storage.NewWriteBatch()
			for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.NumElements(0, 1<<10).Fuzz(&data)
				So(wb2.Put(llsn, data), ShouldBeNil)
			}
			So(wb2.Apply(), ShouldBeNil)
			So(wb2.Close(), ShouldBeNil)

			for llsn := types.LLSN(mid); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.NumElements(0, 1<<10).Fuzz(&data)
				So(wb1.Put(llsn, data), ShouldBeNil)
			}

			So(wb1.Apply(), ShouldNotBeNil)
			So(wb1.Close(), ShouldBeNil)
		}))
	})
}

func TestStorageWriteCommitRead(t *testing.T) {
	Convey("When arbitrary data is written, committed, and then read", t, func() {
		Convey("it should work well if log positions are correct", withForeachStorage(t, func(storage Storage) {
			const (
				begin       = 1
				end         = 101
				nilChance   = 0.3
				minDataSize = 0
				maxDataSize = 1 << 10
			)
			fz := fuzz.New().NilChance(nilChance).NumElements(minDataSize, maxDataSize)

			oldGLSN := types.InvalidGLSN
			for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)

				// bad GLSN
				badGLSN := types.GLSN(rand.Intn(int(oldGLSN + 1)))
				So(badGLSN, ShouldBeLessThanOrEqualTo, oldGLSN)
				So(storage.Commit(llsn, badGLSN), ShouldNotBeNil)

				// good GLSN
				newGLSN := oldGLSN + types.GLSN(rand.Intn(10)+1)
				So(newGLSN, ShouldBeGreaterThan, oldGLSN)

				So(storage.Commit(llsn, newGLSN), ShouldBeNil)
				logEntry, err := storage.Read(newGLSN)
				So(err, ShouldBeNil)
				if len(data) == 0 {
					So(len(logEntry.Data), ShouldBeZeroValue)
				} else {
					So(data, ShouldResemble, logEntry.Data)
				}
				oldGLSN = newGLSN
			}
		}))
	})
}

func TestStorageDeleteCommitted(t *testing.T) {
	Convey("DeleteCommitted", t, func() {
		const (
			begin       = 1
			end         = 101
			mid         = (end - begin) / 2
			nilChance   = 0.3
			minDataSize = 0
			maxDataSize = 1 << 10
		)

		fz := fuzz.New().NilChance(nilChance).NumElements(minDataSize, maxDataSize)

		Convey("When storage is empty", withForeachStorage(t, func(storage Storage) {
			Convey("Then "+storage.Name()+".DeleteCommitted should return an error", func() {
				So(storage.DeleteCommitted(types.GLSN(0)), ShouldNotBeNil)
				So(storage.DeleteCommitted(types.GLSN(2)), ShouldNotBeNil)
				So(storage.DeleteCommitted(types.GLSN(1)), ShouldBeNil)
			})
		}))

		Convey("Given storage that is has committed logs", withForeachStorage(t, func(storage Storage) {
			for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)
				So(storage.Commit(llsn, types.GLSN(llsn)), ShouldBeNil)
			}

			Convey("When "+storage.Name()+" calls DeleteCommitted with unwritten log position", func() {
				Convey("Then "+storage.Name()+" should return an error", func() {
					So(storage.DeleteCommitted(types.GLSN(end+1)), ShouldNotBeNil)
				})
			})

			So(storage.Write(types.LLSN(end), nil), ShouldBeNil)
			So(storage.DeleteCommitted(types.GLSN(end+1)), ShouldNotBeNil)
		}))
	})
}

func TestStorageDeleteUncommitted(t *testing.T) {
	Convey("DeleteUncommitted", t, func() {
		const (
			begin       = 1
			end         = 101
			mid         = (end - begin) / 2
			nilChance   = 0.3
			minDataSize = 0
			maxDataSize = 1 << 10
		)

		fz := fuzz.New().NilChance(nilChance).NumElements(minDataSize, maxDataSize)

		Convey("When storage is empty", withForeachStorage(t, func(storage Storage) {
			Convey("Then "+storage.Name()+".DeleteUncommitted should return nil", func() {
				So(storage.DeleteUncommitted(types.LLSN(1)), ShouldBeNil)
				So(storage.DeleteUncommitted(types.LLSN(0)), ShouldBeNil)
			})
		}))

		Convey("When uncommitted logs are stored in the storage", withForeachStorage(t, func(storage Storage) {
			for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)
			}

			Convey(storage.Name()+": Then DeleteUncommitted should not delete committed logs", func() {
				for pos := begin; pos < mid; pos++ {
					So(storage.Commit(types.LLSN(pos), types.GLSN(pos)), ShouldBeNil)
				}
				So(storage.DeleteUncommitted(types.LLSN(begin)), ShouldNotBeNil)
				So(storage.DeleteUncommitted(types.LLSN(mid)), ShouldBeNil)

			})

			Convey("Then "+storage.Name()+".DeleteUncommitted should delete uncommitted logs", func() {
				So(storage.DeleteUncommitted(types.LLSN(mid)), ShouldBeNil)

				Convey(storage.Name()+": deleted logs should not be committed", func() {
					for pos := begin; pos < mid; pos++ {
						So(storage.Commit(types.LLSN(pos), types.GLSN(pos)), ShouldBeNil)
					}
					for pos := mid; pos < end; pos++ {
						So(storage.Commit(types.LLSN(pos), types.GLSN(pos)), ShouldNotBeNil)
					}
				})

				Convey(storage.Name()+": rewriting logs to deleted ranges is okay", func() {
					for llsn := types.LLSN(mid); llsn < types.LLSN(end); llsn++ {
						var data []byte
						fz.Fuzz(&data)
						So(storage.Write(llsn, data), ShouldBeNil)
					}
				})
			})
		}))
	})
}

func TestStorageOps(t *testing.T) {
	Convey("StorageOps for all storages", t, func() {
		const (
			begin       = 1
			end         = 101
			mid         = (end - begin) / 2
			nilChance   = 0.3
			minDataSize = 0
			maxDataSize = 1 << 10
		)

		fz := fuzz.New().NilChance(nilChance).NumElements(minDataSize, maxDataSize)

		Convey("Given written logs", withForeachStorage(t, func(storage Storage) {
			// Write
			for llsn := types.LLSN(begin); llsn < types.LLSN(mid); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(storage.Write(llsn, data), ShouldBeNil)
			}
			// WriteBatch
			wb := storage.NewWriteBatch()
			for llsn := types.LLSN(mid); llsn < types.LLSN(end); llsn++ {
				var data []byte
				fz.Fuzz(&data)
				So(wb.Put(llsn, data), ShouldBeNil)
			}
			So(wb.Apply(), ShouldBeNil)
			So(wb.Close(), ShouldBeNil)

			// Concurrent CommitBatch
			Convey(storage.Name()+"should commit only one of the concurrent commit batches", func() {
				// CommitBatch 1
				cb1 := storage.NewCommitBatch()
				for llsn := types.LLSN(begin); llsn < types.LLSN(mid); llsn++ {
					So(cb1.Put(llsn, types.GLSN(llsn)), ShouldBeNil)
				}

				// CommitBatch 2
				cb2 := storage.NewCommitBatch()
				for llsn := types.LLSN(begin); llsn < types.LLSN(end); llsn++ {
					So(cb2.Put(llsn, types.GLSN(llsn)), ShouldBeNil)
				}
				So(cb2.Apply(), ShouldBeNil)
				So(cb2.Close(), ShouldBeNil)

				for llsn := types.LLSN(mid); llsn < types.LLSN(end); llsn++ {
					So(cb1.Put(llsn, types.GLSN(llsn)), ShouldBeNil)
				}

				So(cb1.Apply(), ShouldNotBeNil)
				So(cb1.Close(), ShouldBeNil)
			})

			// Read uncommitted logs
			Convey(storage.Name()+" should not read uncommitted logs", func() {
				for glsn := types.GLSN(begin); glsn < types.GLSN(end); glsn++ {
					_, err := storage.Read(glsn)
					So(err, ShouldNotBeNil)
				}
			})

			// Scan uncommitted logs
			Convey(storage.Name()+" should not scan uncommitted logs", func() {
				scanner, err := storage.Scan(begin, end)
				So(err, ShouldBeNil)
				So(scanner.Next().Valid(), ShouldBeFalse)
				So(scanner.Close(), ShouldBeNil)
			})

			Convey(storage.Name()+" should commit written logs", func() {
				// Commit
				for pos := begin; pos < mid; pos++ {
					So(storage.Commit(types.LLSN(pos), types.GLSN(pos)), ShouldBeNil)
				}

				// CommitBatch
				cb := storage.NewCommitBatch()
				for pos := mid; pos < end; pos++ {
					So(cb.Put(types.LLSN(pos), types.GLSN(pos)), ShouldBeNil)
				}
				So(cb.Apply(), ShouldBeNil)
				So(cb.Close(), ShouldBeNil)

				// Read committed logs
				Convey(storage.Name()+" should read committed logs", func() {
					for glsn := types.GLSN(begin); glsn < types.GLSN(end); glsn++ {
						logEntry, err := storage.Read(glsn)
						So(err, ShouldBeNil)
						So(logEntry.LLSN, ShouldEqual, types.LLSN(glsn))
					}
				})

				// Scan committed logs
				Convey(storage.Name()+" should scan log entries", func() {
					scanner, err := storage.Scan(begin, end)
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

				// DeleteCommitted logs (Trim)
				Convey(storage.Name()+" should delete committed logs", func() {
					// Delete committed logs [begin, mid]
					So(storage.DeleteCommitted(types.GLSN(mid+1)), ShouldBeNil)

					// Read [begin, mid]
					Convey(storage.Name()+" should not read deleted logs", func() {
						for glsn := types.GLSN(begin); glsn <= types.GLSN(mid); glsn++ {
							_, err := storage.Read(glsn)
							So(err, ShouldNotBeNil)
						}
					})

					// Scan [begin, mid+1)
					Convey(storage.Name()+" should not scan deleted logs", func() {
						scanner, err := storage.Scan(types.GLSN(begin), types.GLSN(mid+1))
						So(err, ShouldBeNil)
						So(scanner.Next().Valid(), ShouldBeFalse)
						So(scanner.Close(), ShouldBeNil)
					})

					// Scan [begin, end)
					Convey(storage.Name()+" should scan undeleted logs", func() {
						scanner, err := storage.Scan(types.GLSN(begin), types.GLSN(end))
						So(err, ShouldBeNil)
						scanResult := scanner.Next()
						So(scanResult.Valid(), ShouldBeTrue)
						So(scanResult.LogEntry.GLSN, ShouldEqual, types.GLSN(mid+1))
						So(scanner.Close(), ShouldBeNil)
					})

					Convey(storage.Name()+" should not write logs to trimmed range", func() {
						for llsn := types.LLSN(begin); llsn <= types.LLSN(mid); llsn++ {
							So(storage.Write(llsn, nil), ShouldNotBeNil)
						}
					})
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

	storage, err := NewStorage(name, WithLogger(zap.NewNop()), WithPath(tmpdir), WithEnableWriteFsync())
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
	batchEnts := make([]writtenEntry, 0, dataSize)
	for i := 0; i < dataNum; i++ {
		batchEnts = append(batchEnts, writtenEntry{
			data: makeData(dataSize),
		})
	}
	globalLLSN = types.MinLLSN

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		for i := range batchEnts {
			batchEnts[i].llsn = globalLLSN
			globalLLSN++
		}
		b.StartTimer()
		wb := storage.NewWriteBatch()
		for _, batchEnt := range batchEnts {
			wb.Put(batchEnt.llsn, batchEnt.data)
		}
		if err := wb.Apply(); err != nil {
			b.Error(err)
		}
		wb.Close()
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
	benchmarkWriteBatch(InMemoryStorageName, 10, 128, b)
}

func BenchmarkInMemoryStorageWriteBatch256(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 10, 256, b)
}
func BenchmarkInMemoryStorageWriteBatch512(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 10, 512, b)
}
func BenchmarkInMemoryStorageWriteBatch1024(b *testing.B) {
	benchmarkWriteBatch(InMemoryStorageName, 10, 1024, b)
}

func BenchmarkPebbleStorageWrite128(b *testing.B)  { benchmarkWrite(PebbleStorageName, 128, b) }
func BenchmarkPebbleStorageWrite256(b *testing.B)  { benchmarkWrite(PebbleStorageName, 256, b) }
func BenchmarkPebbleStorageWrite512(b *testing.B)  { benchmarkWrite(PebbleStorageName, 512, b) }
func BenchmarkPebbleStorageWrite1024(b *testing.B) { benchmarkWrite(PebbleStorageName, 1024, b) }

func BenchmarkPebbleStorageWriteBatch128(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 10, 128, b)
}
func BenchmarkPebbleStorageWriteBatch256(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 10, 256, b)
}
func BenchmarkPebbleStorageWriteBatch512(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 10, 512, b)
}
func BenchmarkPebbleStorageWriteBatch1024(b *testing.B) {
	benchmarkWriteBatch(PebbleStorageName, 10, 1024, b)
}
