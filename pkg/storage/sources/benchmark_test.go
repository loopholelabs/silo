package sources_test

/*
type sourceInfo struct {
	Name   string
	Source storage.StorageProvider
}

func BenchmarkSourcesRead(mb *testing.B) {
	PORT_9000 := testutils.SetupMinio(mb.Cleanup)

	mysources := make([]sourceInfo, 0)

	for _, s3block := range []int{512, 1024, 4096, 64 * 1024} {
		size := 1024 * 1024
		s3store, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", fmt.Sprintf("silosilo-%d", s3block), "file", uint64(size), s3block)
		if err != nil {
			panic(err)
		}

		mysources = append(mysources, sourceInfo{
			Name:   fmt.Sprintf("S3Minio_%d", s3block),
			Source: s3store,
		})
	}

	// Create some sources to test...
	mysources = append(mysources, sourceInfo{"MemoryStorage", sources.NewMemoryStorage(1024 * 1024 * 4)})
	cr := func(i int, s int) (storage.StorageProvider, error) {
		return sources.NewMemoryStorage(s), nil
	}
	ss, err := modules.NewShardedStorage(1024*1024*4, 1024*4, cr)
	if err != nil {
		panic(err)
	}
	mysources = append(mysources, sourceInfo{"ShardedMemoryStorage", ss})

	fileStorage, err := sources.NewFileStorageCreate("test_data", 1024*1024*4)
	if err != nil {
		panic(err)
	}
	defer func() {
		fileStorage.Close()
		os.Remove("test_data")
	}()
	mysources = append(mysources, sourceInfo{"FileStorage", fileStorage})

	// Test some different sources read speed...
	for _, s := range mysources {

		mb.Run(s.Name, func(b *testing.B) {

			// Do some concurrent reads...
			var wg sync.WaitGroup
			concurrency := make(chan bool, 1024)
			var totalData int64 = 0
			for i := 0; i < b.N; i++ {
				concurrency <- true
				wg.Add(1)
				go func() {
					buffer := make([]byte, 4096)
					offset := rand.Intn(int(s.Source.Size()) - len(buffer))
					n, err := s.Source.ReadAt(buffer, int64(offset))
					if n != len(buffer) || err != nil {
						panic(err)
					}
					atomic.AddInt64(&totalData, int64(n))
					wg.Done()
					<-concurrency
				}()
			}

			wg.Wait()

			b.SetBytes(4096)
		})
	}
}

func BenchmarkSourcesWrite(mb *testing.B) {
	PORT_9000 := testutils.SetupMinio(mb.Cleanup)

	mysources := make([]sourceInfo, 0)

	for _, s3block := range []int{512, 1024, 4096, 64 * 1024, 1024 * 1024} {
		size := 4 * 1024 * 1024
		s3store, err := sources.NewS3StorageCreate(fmt.Sprintf("localhost:%s", PORT_9000), "silosilo", "silosilo", fmt.Sprintf("silosilo-%d", s3block), "file", uint64(size), s3block)
		if err != nil {
			panic(err)
		}

		mysources = append(mysources, sourceInfo{
			Name:   fmt.Sprintf("S3Minio_%d", s3block),
			Source: s3store,
		})
	}

	// Create some sources to test...
	mysources = append(mysources, sourceInfo{"MemoryStorage", sources.NewMemoryStorage(1024 * 1024 * 4)})
	cr := func(i int, s int) (storage.StorageProvider, error) {
		return sources.NewMemoryStorage(s), nil
	}
	ss, err := modules.NewShardedStorage(1024*1024*4, 1024*4, cr)
	if err != nil {
		panic(err)
	}
	mysources = append(mysources, sourceInfo{"ShardedMemoryStorage", ss})

	fileStorage, err := sources.NewFileStorageCreate("test_data", 1024*1024*4)
	if err != nil {
		panic(err)
	}
	defer func() {
		fileStorage.Close()
		os.Remove("test_data")
	}()
	mysources = append(mysources, sourceInfo{"FileStorage", fileStorage})

	// Do sharded files...
	sharded_files := make(map[string]*sources.FileStorage)

	crf := func(i int, s int) (storage.StorageProvider, error) {
		name := fmt.Sprintf("test_data_shard_%d", len(sharded_files))
		fs, err := sources.NewFileStorageCreate(name, int64(s))
		if err != nil {
			panic(err)
		}
		sharded_files[name] = fs
		return sources.NewMemoryStorage(s), nil
	}
	nss, err := modules.NewShardedStorage(1024*1024*4, 1024*4, crf)
	if err != nil {
		panic(err)
	}
	mysources = append(mysources, sourceInfo{"ShardedFileStorage", nss})
	defer func() {
		for f, ms := range sharded_files {
			ms.Close()
			os.Remove(f)
		}
	}()

	// Test some different sources read speed...
	for _, s := range mysources {

		mb.Run(s.Name, func(b *testing.B) {

			// Do some concurrent reads...
			var wg sync.WaitGroup
			concurrency := make(chan bool, 1024)
			var totalData int64 = 0
			for i := 0; i < b.N; i++ {
				concurrency <- true
				wg.Add(1)
				go func() {
					buffer := make([]byte, 4096)
					offset := rand.Intn(int(s.Source.Size()) - len(buffer))
					n, err := s.Source.WriteAt(buffer, int64(offset))
					if n != len(buffer) || err != nil {
						panic(err)
					}
					atomic.AddInt64(&totalData, int64(n))
					wg.Done()
					<-concurrency
				}()
			}

			wg.Wait()

			b.SetBytes(4096)
		})
	}
}

*/
