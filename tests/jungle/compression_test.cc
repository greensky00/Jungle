/************************************************************************
Copyright 2017-2019 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "jungle_test_common.h"

#include "dummy_compression.h"
#include "internal_helper.h"

#include <vector>

#include <stdio.h>

static size_t NUM_RECORDS = 100;

int compression_correctness_test() {
    jungle::SizedBuf src(1024);
    jungle::SizedBuf::Holder h_src(src);
    jungle::RwSerializer rws_src(src);
    for (size_t ii=0; ii<1024/8; ++ii) {
        rws_src.putU64(ii);
    }

    jungle::Record rec;
    rec.kv.value = src;
    jungle::SizedBuf dst( dummy_get_max_size(nullptr, rec) );
    jungle::SizedBuf::Holder h_dst(dst);

    ssize_t comp_len = dummy_compress(nullptr, rec, dst);
    TestSuite::_msg("compressed len: %zd\n", comp_len);

    jungle::SizedBuf decomp(1024);
    jungle::SizedBuf::Holder h_decomp(decomp);
    dummy_decompress(nullptr, dst, decomp);

    CHK_Z( memcmp(src.data, decomp.data, src.size) );
    return 0;
}

int compression_small_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );
        CHK_EQ( val_str, value_out.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_mid_size_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );
        CHK_EQ( original_value_arr[ii], value_out.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_large_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(60, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );
        CHK_EQ( original_value_arr[ii], value_out.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

ssize_t selective_get_max_size(jungle::DB* db,
                               const jungle::Record& rec)
{
    // Compress only when the last byte of key is an odd number.
    if (rec.kv.key.data[rec.kv.key.size - 1] % 2 == 1) {
        return rec.kv.value.size * 2;
    }
    return 0;
}

int selective_compression_by_max_size_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = selective_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );
        CHK_EQ( original_value_arr[ii], value_out.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

ssize_t selective_compress
        ( std::function< ssize_t( jungle::DB*,
                                  const jungle::Record&,
                                  jungle::SizedBuf& ) > orig_compress,
          jungle::DB* db,
          const jungle::Record& rec,
          jungle::SizedBuf& dst )
{
    // Compress only when the last byte of key is an odd number.
    if (rec.kv.key.data[rec.kv.key.size - 1] % 2 == 1) {
        return orig_compress(db, rec, dst);
    }
    return 0;
}

int selective_compression_by_compress_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = std::bind( selective_compress,
                                           dummy_compress,
                                           std::placeholders::_1,
                                           std::placeholders::_2,
                                           std::placeholders::_3 );
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );
        CHK_EQ( original_value_arr[ii], value_out.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_with_tombstones_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Remove odd number records.
    for (size_t ii=1; ii<NUM_RECORDS; ii+=2) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        CHK_Z( db->del( jungle::SizedBuf(key_str) ) );
    }

    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        s = db->get( jungle::SizedBuf(key_str), value_out);
        if (ii % 2 == 0) {
            CHK_Z(s);
            CHK_EQ( original_value_arr[ii], value_out.toString() );
        } else {
            CHK_SM(s, 0);
        }

        // meta only: should succeed on tombstones.
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_log_store_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;
    config.logSectionOnly = true;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(60, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->setSN( ii+1, jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::KV kv_out;
        jungle::KV::Holder h_kv_out(kv_out);
        CHK_Z( db->getSN( ii+1, kv_out) );

        CHK_EQ( key_str, kv_out.key.toString() );
        CHK_EQ( original_value_arr[ii], kv_out.value.toString() );

        // meta only
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        CHK_Z( db->getRecordByKey( jungle::SizedBuf(key_str), rec_out, true ) );
        CHK_TRUE( rec_out.meta.empty() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_mutable_cb_test(bool with_meta) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    g_config.compactorSleepDuration_ms = 100;
    g_config.numCompactorThreads = 1;
    jungle::init(g_config);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;
    config.compactionCbDecompressValue = true;
    auto cb_func = [&](const jungle::CompactionCbParams& params,
                       jungle::SizedBuf& new_meta_out,
                       jungle::SizedBuf& new_value_out) {
        // Drops all odd number KVs.
        std::string num_str = std::string((char*)params.rec.kv.key.data + 1,
                                          params.rec.kv.key.size - 1);
        size_t num = atoi(num_str.c_str());
        if (num % 2 == 1) {
            return jungle::CompactionCbDecision::DROP;
        }
        // If even number, multiply it by 10, with different length.
        num *= 10;
        std::string new_value_str = "v" + TestSuite::lzStr(8, num);
        jungle::SizedBuf(new_value_str).copyTo(new_value_out);

        if (with_meta) {
            // And also set metadata.
            std::string new_meta_str = "m" + TestSuite::lzStr(6, num);
            jungle::SizedBuf(new_meta_str).copyTo(new_meta_out);
        }
        return jungle::CompactionCbDecision::KEEP;
    };
    config.mutableCompactionCbFunc = cb_func;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );
    CHK_Z( db->flushLogs() );

    // Do L0 compaction.
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    // Do full compaction.
    jungle::DBStats db_stats;
    CHK_Z(db->getStats(db_stats));
    uint64_t cur_max_table_idx = db_stats.maxTableIndex;

    CHK_Z(db->compactIdxUpto(jungle::CompactOptions(), cur_max_table_idx));

    // Wait until min table index becomes greater than the current max index.
    size_t tick = 0;
    const size_t MAX_TICK = 10;
    do {
        jungle::DBStats db_stats;
        CHK_Z(db->getStats(db_stats));
        if (db_stats.minTableIndex > cur_max_table_idx) {
            break;
        }
        std::stringstream ss;
        ss << "min table index: " << db_stats.minTableIndex
           << ", max table index: " << cur_max_table_idx
           << ", tick: " << tick;
        TestSuite::sleep_ms(500, ss.str());
        tick++;
    } while (tick < MAX_TICK);
    CHK_SM(tick, MAX_TICK);

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        char key_str[256];
        char meta_str[256];
        char value_str[256];
        sprintf(key_str, "k%06zu", ii);
        jungle::SizedBuf key(key_str);
        if (ii % 2 == 0) {
            jungle::Record rec_out;
            jungle::Record::Holder h_rec(rec_out);
            CHK_Z( db->getRecordByKey(key, rec_out) );
            sprintf(meta_str, "m%06zu", ii * 10);
            sprintf(value_str, "v%08zu", ii * 10);
            jungle::SizedBuf meta(meta_str);
            jungle::SizedBuf value(value_str);
            if (with_meta) {
                CHK_EQ(meta, rec_out.meta);
            }
            CHK_EQ(value, rec_out.kv.value);
        } else {
            jungle::SizedBuf val;
            jungle::SizedBuf::Holder h_val(val);
            CHK_NOT( db->get(key, val) );
        }
    }

    jungle::Iterator itr;
    CHK_Z( itr.init(db) );
    size_t idx = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h_rec(rec);
        s = itr.get(rec);
        if (!s) break;

        char key_str[256];
        char meta_str[256];
        char value_str[256];
        sprintf(key_str, "k%06zu", idx);
        sprintf(meta_str, "m%06zu", idx * 10);
        sprintf(value_str, "v%08zu", idx * 10);

        jungle::SizedBuf key(key_str);
        jungle::SizedBuf meta(meta_str);
        jungle::SizedBuf value(value_str);

        CHK_EQ(key, rec.kv.key);
        if (with_meta) {
            CHK_EQ(meta, rec.meta);
        }
        CHK_EQ(value, rec.kv.value);
        idx += 2;

    } while (itr.next().ok());
    CHK_Z( itr.close() );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}


int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

#ifdef SNAPPY_AVAILABLE
    std::cout << "TEST WITH SNAPPY" << std::endl;
#endif

    for (int ii=1; ii<argc; ++ii) {
        if ( ii < argc-1 &&
             !strcmp(argv[ii], "--num-records") ) {
            NUM_RECORDS = atoi(argv[++ii]);
            std::cout << "NUM_RECORDS = " << NUM_RECORDS << std::endl;
        }
    }

    //ts.options.printTestMessage = true;
    ts.doTest( "compression correctness test",
               compression_correctness_test );

    ts.doTest( "compression small value test",
               compression_small_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression mid size value test",
               compression_mid_size_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression large value test",
               compression_large_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "selective compression by cbGetMaxSize test",
               selective_compression_by_max_size_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "selective compression by cbCompress test",
               selective_compression_by_compress_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression with tombstones test",
               compression_with_tombstones_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression log store test",
               compression_log_store_test );

    ts.doTest( "compression mutable cb test",
               compression_mutable_cb_test,
               TestRange<bool>( {false, true} ) );

    return 0;
}

