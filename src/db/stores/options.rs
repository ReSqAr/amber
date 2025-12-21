use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options, WriteOptions};

pub(crate) fn make_options() -> Options {
    let mut opts = Options::default();

    // General
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get().try_into().unwrap_or(4));
    opts.optimize_level_style_compaction(64 * 1024 * 1024);

    // Block-based table
    let mut block_opts = BlockBasedOptions::default();

    // Bloom filter for point lookups
    block_opts.set_bloom_filter(10.0, false);

    // Cache data/index/filter blocks in memory.
    let cache = Cache::new_lru_cache(128 * 1024 * 1024);
    block_opts.set_block_cache(&cache);
    block_opts.set_cache_index_and_filter_blocks(true);

    // Reasonable block size
    block_opts.set_block_size(16 * 1024);

    opts.set_block_based_table_factory(&block_opts);

    // Compression: LZ4 is fast and usually good tradeoff.
    opts.set_compression_type(DBCompressionType::Lz4);

    // Dynamic level bytes works well for most workloads
    opts.set_level_compaction_dynamic_level_bytes(true);

    // Memtable / write buffers (see next section)
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(3);
    opts.set_min_write_buffer_number_to_merge(1);

    // Background threads for compaction/flush
    opts.set_max_background_jobs(4);

    // Smooth out I/O
    opts.set_bytes_per_sync(100 * 1024 * 1024);
    opts.set_wal_bytes_per_sync(100 * 1024 * 1024);

    opts
}

pub(crate) fn write_options() -> WriteOptions {
    let mut wo = WriteOptions::default();
    wo.disable_wal(true);
    wo.set_sync(false);
    wo
}
