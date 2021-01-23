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

#include "event_awaiter.h"
#include "test_common.h"

#include <libjungle/jungle.h>

#include <numeric>
#include <random>
#include <vector>

#include <stdio.h>

namespace general_stress_test {

int random_insert() {
    std::string filename;
    filename = TEST_SUITE_AUTO_PREFIX;

    std::string cmd = "rm " + filename + "/*";
    int rrr = ::system(cmd.c_str()); (void)rrr;

    jungle::DB* db;
    jungle::Status s;

    jungle::GlobalConfig g_conf;
    g_conf.numCompactorThreads = 1;
    //g_conf.numTableWriters = 0;
    jungle::init(g_conf);

    // Open DB.
    jungle::DBConfig config;
    config.purgeDeletedDocImmediately = false;
    config.fastIndexScan = true;
    config.minFileSizeToCompact = 16*1024*1024;
    config.maxL0TableSize = 64*1024*1024;
    config.maxL1TableSize = 128*1024*1024;
    config.compactionFactor = 500;
    config.sortingWindowOpt.enabled = true;
    //config.sortingWindowOpt.maxSize = 5*1024*1024;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::string value_str(512, 'x');
    const size_t NUM = 1000000;

    std::vector<size_t> idx_arr(NUM);
    std::iota(idx_arr.begin(), idx_arr.end(), 0);
    /*
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(idx_arr.begin(), idx_arr.end(), g);
    */
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t jj = std::rand() % NUM;
        std::swap(idx_arr[ii], idx_arr[jj]);
    }

    TestSuite::Progress pp{NUM, "rand insert"};
    for (size_t ii = 0; ii < NUM; ++ii) {
        jungle::Record rec;
        std::string key_str = TestSuite::lzStr(8, idx_arr[ii]);
        rec.kv.key = jungle::SizedBuf(key_str);
        rec.kv.value = jungle::SizedBuf(value_str);
        CHK_Z( db->setRecordByKey(rec) );
        pp.update(ii);
    }
    pp.done();
    db->sync(false);
    db->flushLogs();
    TestSuite::sleep_sec(10, "sleep");
#if 0
    pp = TestSuite::Progress(NUM * 3, "seq append");
    for (size_t ii = NUM; ii < NUM * 3; ++ii) {
        jungle::Record rec;
        std::string key_str = TestSuite::lzStr(8, ii);
        rec.kv.key = jungle::SizedBuf(key_str);
        rec.kv.value = jungle::SizedBuf(value_str);
        CHK_Z( db->setRecordByKey(rec) );
        pp.update(ii);
    }
    pp.done();

    pp = TestSuite::Progress(NUM, "rand insert");
    for (size_t ii = 0; ii < NUM; ++ii) {
        jungle::Record rec;
        std::string key_str = TestSuite::lzStr(8, idx_arr[ii]);
        rec.kv.key = jungle::SizedBuf(key_str);
        rec.kv.value = jungle::SizedBuf(value_str);
        CHK_Z( db->setRecordByKey(rec) );
        pp.update(ii);
    }
    pp.done();
    db->sync(false);
    db->flushLogs();
    for (size_t ii = 0; ii < 4; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
#endif
    pp = TestSuite::Progress(NUM, "verifying");
    for (size_t ii = 0; ii < NUM; ++ii) {
        jungle::Record rec;
        std::string key_str = TestSuite::lzStr(8, idx_arr[ii]);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
        pp.update(ii);
    }
    pp.done();

    TestSuite::sleep_sec(10, "sleep");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    return 0;
}

} using namespace general_stress_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.options.printTestMessage = true;
    ts.doTest("random insert", random_insert);
    return 0;
}

