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

#include "table_mgr.h"

#include "db_internal.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <map>
#include <vector>

namespace jungle {

// Used for log -> L0 flush.
void TableMgr::setTableFile( std::list<Record*>& batch,
                             std::list<uint64_t>& checkpoints,
                             bool bulk_load_mode,
                             TableFile* table_file,
                             uint32_t target_hash,
                             const SizedBuf& min_key,
                             const SizedBuf& next_min_key )
{
    table_file->setBatch( batch,
                          checkpoints,
                          min_key,
                          next_min_key,
                          target_hash,
                          bulk_load_mode );

    if (myLog->debugAllowed()) {
        _log_debug( myLog,
                    "Set batch table num %zu, hash %zu, "
                    "key1: %s key2: %s",
                    table_file->getNumber(), target_hash,
                    min_key.toReadableString().c_str(),
                    next_min_key.toReadableString().c_str() );
    }
}

// Used for in-place/inter-level compaction, split/merge.
void TableMgr::setTableFileOffset( std::list<uint64_t>& checkpoints,
                                   TableFile* src_file,
                                   TableFile* dst_file,
                                   std::vector<uint64_t>& offsets,
                                   uint64_t start_index,
                                   uint64_t count )
{
    const DBConfig* db_config = getDbConfig();
    (void)db_config;

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    size_t num_levels = mani->getNumLevels();
    size_t dst_level = (dst_file && dst_file->getTableInfo()) ?
                       dst_file->getTableInfo()->level : 0;

    const GlobalConfig* global_config = mgr->getGlobalConfig();
    const GlobalConfig::CompactionThrottlingOptions& t_opt =
        global_config->ctOpt;

    Status s;
    SizedBuf empty_key;
    std::list<Record*> recs_batch;

    Timer elapsed_timer;
    Timer sync_timer;
    Timer throttling_timer(t_opt.resolution_ms);
    sync_timer.setDurationMs(db_config->preFlushDirtyInterval_sec * 1000);

    uint64_t total_dirty = 0;
    uint64_t time_for_flush_us = 0;

   try {
    for (uint64_t ii = start_index; ii < start_index + count; ++ii) {
        if (!isCompactionAllowed()) {
            // To avoid file corruption, we should flush all cached pages
            // even for cancel.
            Timer cancel_timer;
            dst_file->sync();
            _log_info(myLog, "cancel sync %zu us", cancel_timer.getUs());
            throw Status(Status::COMPACTION_CANCELLED);
        }

        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        s = src_file->getByOffset(nullptr, offsets[ii], rec_out);
        if (!s) {
            _log_fatal(myLog, "failed to read record at %zu", offsets[ii]);
            assert(0);
            continue;
        }

        SizedBuf data_to_hash = rec_out.kv.key;
        if ( db_config->keyLenLimitForHash &&
             data_to_hash.size > db_config->keyLenLimitForHash ) {
            data_to_hash.size = db_config->keyLenLimitForHash;
        }
        uint32_t key_hash_val = getMurmurHash32(rec_out.kv.key);;
        uint64_t offset_out = 0; // not used.

        // WARNING:
        //   Since `rec_out` from `getByOffset` contains raw meta and
        //   compressed value (if enabled), we should skip processing
        //   meta and compression.
        dst_file->setSingle(key_hash_val, rec_out, offset_out,
                            true, dst_level + 1 == num_levels);
        total_dirty += rec_out.size();

        if (d_params.compactionItrScanDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionItrScanDelayUs);
        }

        // Periodic flushing to avoid burst disk write & freeze,
        // which have great impact on (user-facing) latency.
        if ( ( db_config->preFlushDirtySize &&
               db_config->preFlushDirtySize < total_dirty ) ||
             sync_timer.timeout() ) {
            Timer flush_time;
            dst_file->sync();
            // Resetting timer should be done after fsync,
            // as it may take long time.
            sync_timer.reset();
            throttling_timer.reset();
            total_dirty = 0;
            time_for_flush_us += flush_time.getUs();
        }

        // Do throttling, if enabled.
        TableMgr::doCompactionThrottling(t_opt, throttling_timer);
    }

    // Final commit, and generate snapshot on it.
    setTableFileItrFlush(dst_file, recs_batch, false);
    _log_info(myLog, "(end of batch) set total %zu records, %zu us, %zu us for flush",
              count, elapsed_timer.getUs(), time_for_flush_us);

   } catch (Status s) { // -----------------------------------
    _log_err(myLog, "got error: %d", (int)s);

    for (Record* rr: recs_batch) {
        if (!rr) continue;
        rr->free();
        delete rr;
    }
   }
}

// Instead of random read, using sorting window to maximize the read performance.
void TableMgr::setTableFileOffsetSW( std::list<uint64_t>& checkpoints,
                                     TableFile* src_file,
                                     TableFile* dst_file,
                                     std::vector<uint64_t>& offsets,
                                     uint64_t start_index,
                                     uint64_t count )
{
    const DBConfig* db_config = getDbConfig();
    (void)db_config;

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    size_t num_levels = mani->getNumLevels();
    size_t dst_level = (dst_file && dst_file->getTableInfo()) ?
                       dst_file->getTableInfo()->level : 0;

    const GlobalConfig* global_config = mgr->getGlobalConfig();
    const GlobalConfig::CompactionThrottlingOptions& t_opt =
        global_config->ctOpt;

    Status s;
    SizedBuf empty_key;
    std::list<Record*> recs_batch;

    Timer elapsed_timer;
    Timer sync_timer;
    Timer throttling_timer(t_opt.resolution_ms);
    sync_timer.setDurationMs(db_config->preFlushDirtyInterval_sec * 1000);

    uint64_t total_dirty = 0;
    uint64_t time_for_flush_us = 0;

    const size_t max_window_size = db_config->sortingWindowOpt.numRecords;
    const size_t max_window_records_bytes = db_config->sortingWindowOpt.maxSize;
    std::vector<Record*> records_window{max_window_size, nullptr};

    _log_info(myLog, "write batch %zu with sorting window %zu %zu",
              count, max_window_size, max_window_records_bytes);

   try {
    uint64_t cur_window_size = max_window_size;
    uint64_t cursor_s = start_index;
    uint64_t cursor_e = std::min(cursor_s + max_window_size, start_index + count);
    size_t round_count = 0;

    while (cursor_s < cursor_e) {
        Timer round_timer;
        Timer phase_timer;
        uint64_t s_time = 0, r_time = 0, w_time = 0;

        // <offset, vector index>
        std::map<uint64_t, uint64_t> offset_idx_map;

        // Sort phase.
        size_t idx = 0;
        for (uint64_t ii = cursor_s; ii < cursor_e; ++ii) {
            offset_idx_map[offsets[ii]] = idx++;
        }
        s_time = phase_timer.getUs();
        phase_timer.reset();

        // Read phase.
        uint64_t read_count = 0;
        uint64_t cur_window_records_bytes = 0;
        bool retry = false;
        for (auto& entry: offset_idx_map) {
            if (!isCompactionAllowed()) {
                // To avoid file corruption, we should flush all cached pages
                // even for cancel.
                Timer cancel_timer;
                dst_file->sync();
                _log_info(myLog, "cancel sync %zu us", cancel_timer.getUs());
                throw Status(Status::COMPACTION_CANCELLED);
            }
            Record* rec_out = new Record();
            s = src_file->getByOffset(nullptr, entry.first, *rec_out);
            if (!s) {
                _log_fatal(myLog, "failed to read record at %zu", entry.first);
                assert(0);
                delete rec_out;
                continue;
            }
            records_window[entry.second] = rec_out;
            cur_window_records_bytes += rec_out->size();
            read_count++;

            if (cur_window_records_bytes > max_window_records_bytes) {
                // The total size of records read is bigger than allowed memory size.
                // Reduce the window size and retry.

                // Reduce by 10x.
                uint64_t prev_window_size = cur_window_size;
                cur_window_size /= 10;

                _log_info(myLog, "exceeded window memory limit, window size %zu / %zu, "
                          "record size %zu / %zu, "
                          "reduced window size %zu -> %zu",
                          read_count, offset_idx_map.size(),
                          cur_window_records_bytes, max_window_records_bytes,
                          prev_window_size, cur_window_size);
                cursor_e = std::min(cursor_s + cur_window_size, start_index + count);
                for (size_t jj = 0; jj < records_window.size(); ++jj) {
                    if (!records_window[jj]) continue;
                    records_window[jj]->free();
                    delete records_window[jj];
                    records_window[jj] = nullptr;
                }
                retry = true;
                break;
            }
        }
        if (retry) continue;
        r_time = phase_timer.getUs();
        phase_timer.reset();

        // Write phase.
        for (uint64_t ii = 0; ii < offset_idx_map.size(); ++ii) {
            if (!records_window[ii]) {
                // This shouldn't happen (caused by above fatal error).
                assert(0);
                continue;
            }
            if (!isCompactionAllowed()) {
                // To avoid file corruption, we should flush all cached pages
                // even for cancel.
                Timer cancel_timer;
                dst_file->sync();
                _log_info(myLog, "cancel sync %zu us", cancel_timer.getUs());
                throw Status(Status::COMPACTION_CANCELLED);
            }

            Record& rec_out = *records_window[ii];

            SizedBuf data_to_hash = rec_out.kv.key;
            if ( db_config->keyLenLimitForHash &&
                 data_to_hash.size > db_config->keyLenLimitForHash ) {
                data_to_hash.size = db_config->keyLenLimitForHash;
            }
            uint32_t key_hash_val = getMurmurHash32(data_to_hash);
            uint64_t offset_out = 0; // not used.

            // WARNING:
            //   Since `rec_out` from `getByOffset` contains raw meta and
            //   compressed value (if enabled), we should skip processing
            //   meta and compression.
            dst_file->setSingle(key_hash_val, rec_out, offset_out,
                                true, dst_level + 1 == num_levels);
            total_dirty += rec_out.size();

            records_window[ii]->free();
            delete records_window[ii];
            records_window[ii] = nullptr;

            if (d_params.compactionItrScanDelayUs) {
                // If debug parameter is given, sleep here.
                Timer::sleepUs(d_params.compactionItrScanDelayUs);
            }

            // Periodic flushing to avoid burst disk write & freeze,
            // which have great impact on (user-facing) latency.
            if ( ( db_config->preFlushDirtySize &&
                   db_config->preFlushDirtySize < total_dirty ) ||
                 sync_timer.timeout() ) {
                Timer flush_time;
                dst_file->sync();
                // Resetting timer should be done after fsync,
                // as it may take long time.
                sync_timer.reset();
                throttling_timer.reset();
                total_dirty = 0;
                time_for_flush_us += flush_time.getUs();
            }

            // Do throttling, if enabled.
            TableMgr::doCompactionThrottling(t_opt, throttling_timer);
        }
        w_time = phase_timer.getUs();
        phase_timer.reset();

        // Gradually increase window (if reduced).
        if (cur_window_size < max_window_size) {
            cur_window_size *= 1.1;
            cur_window_size = std::min(cur_window_size, max_window_size);
        }

        // Everything is done, move window.
        _log_info(myLog, "setTableFileOffsetSW round %zu done, "
                  "%zu - %zu (%zu, %zu bytes), "
                  "%zu us, next window %zu, s_time %zu, r_time %zu, w_time %zu",
                  round_count, cursor_s, cursor_e,
                  cursor_e - cursor_s, cur_window_records_bytes,
                  round_timer.getUs(),
                  cur_window_size,
                  s_time, r_time, w_time);
        cursor_s = cursor_e;
        cursor_e = std::min(cursor_s + cur_window_size, start_index + count);
        round_count++;
    }

    // Final commit, and generate snapshot on it.
    setTableFileItrFlush(dst_file, recs_batch, false);
    _log_info(myLog, "(end of batch) set total %zu records, %zu us, %zu us for flush",
              count, elapsed_timer.getUs(), time_for_flush_us);

   } catch (Status s) { // -----------------------------------
    _log_err(myLog, "got error: %d", (int)s);

    for (Record* rr: recs_batch) {
        if (!rr) continue;
        rr->free();
        delete rr;
    }
    for (Record* rr: records_window) {
        if (!rr) continue;
        rr->free();
        delete rr;
    }
   }
}

void TableMgr::setTableFileItrFlush(TableFile* dst_file,
                                    std::list<Record*>& recs_batch,
                                    bool without_commit)
{
    SizedBuf empty_key;
    std::list<uint64_t> dummy_chk;

    dst_file->setBatch( recs_batch, dummy_chk,
                        empty_key, empty_key, _SCU32(-1),
                        without_commit );
    for (Record* rr: recs_batch) {
        rr->free();
        delete rr;
    }
    recs_batch.clear();
}

Status TableMgr::setBatch(std::list<Record*>& batch,
                          std::list<uint64_t>& checkpoints,
                          bool bulk_load_mode)
{
    // NOTE:
    //  This function deals with level-0 tables only,
    //  which means that it is always hash-partitioning.
    const DBConfig* db_config = getDbConfig();
    if (db_config->logSectionOnly) return Status::TABLES_ARE_DISABLED;

    std::unique_lock<std::mutex> l(L0Lock);
    Timer tt;
    Status s;

    // Not pure LSM: L0 is hash partitioned.
    EP( setBatchHash(batch, checkpoints, bulk_load_mode) );

    uint64_t elapsed_us = tt.getUs();
    _log_info(myLog, "L0 write done: %zu records, %zu us, %.1f iops",
              batch.size(), elapsed_us,
              (double)batch.size() * 1000000 / elapsed_us);

    numWrittenRecords += batch.size();
    return Status();
}

Status TableMgr::setBatchHash( std::list<Record*>& batch,
                               std::list<uint64_t>& checkpoints,
                               bool bulk_load_mode )
{
    Status s;
    std::list<TableInfo*> target_tables;

    DBMgr* db_mgr = DBMgr::getWithoutInit();
    DebugParams d_params = db_mgr->getDebugParams();

    _log_info(myLog, "Records: %zu", batch.size());

    // NOTE: write in parallel.
    size_t num_partitions = getNumL0Partitions();
    size_t max_writers = getDbConfig()->getMaxParallelWriters();

    // For the case where `num_partitions > num_writers`.
    for (size_t ii = 0; ii < num_partitions; ) {
        size_t upto_orig = std::min(ii + max_writers, num_partitions);

        // NOTE: request `req_writers - 1`, as the other one is this thread.
        size_t req_writers = upto_orig - ii;
        TableWriterHolder twh(db_mgr->tableWriterMgr(), req_writers - 1);

        // Lease may not succeed, adjust `upto`.
        size_t leased_writers = twh.leasedWriters.size();
        size_t upto = ii + leased_writers + 1;

        for (size_t jj = ii; jj < upto; ++jj) {
            size_t worker_idx = jj - ii;
            bool leased_thread = (jj + 1 < upto);

            TableWriterArgs local_args;
            local_args.myLog = myLog;

            TableWriterArgs* w_args = (leased_thread)
                                      ? &twh.leasedWriters[worker_idx]->writerArgs
                                      : &local_args;
            w_args->callerAwaiter.reset();

            // Find target table for the given hash number.
            std::list<TableInfo*> tables;
            s = mani->getL0Tables(jj, tables);
            if (!s) continue;

            TableInfo* target_table = getSmallestNormalTable(tables, jj);
            if (!target_table) {
                // Target table does not exist, skip.
                for (TableInfo*& entry: tables) entry->done();
                continue;
            }

            w_args->payload = TableWritePayload( this,
                                                 &batch,
                                                 &checkpoints,
                                                 target_table->file,
                                                 jj,
                                                 bulk_load_mode );
            // Release all tables except for the target.
            for (TableInfo*& entry: tables) {
                if (entry != target_table) entry->done();
            }
            target_tables.push_back(target_table);

            if (leased_thread) {
                // Leased threads.
                w_args->invoke();
            } else {
                // This thread.
                TableWriterMgr::doTableWrite(w_args);
            }
        }

        // Wait for each worker.
        for (size_t jj = ii; jj < upto - 1; ++jj) {
            size_t worker_idx = jj - ii;
            TableWriterArgs* w_args = &twh.leasedWriters[worker_idx]->writerArgs;
            while ( !w_args->payload.isEmpty() ) {
                w_args->callerAwaiter.wait_ms(1000);
                w_args->callerAwaiter.reset();
            }
        }

        if (d_params.tableSetBatchCb) {
            DebugParams::GenericCbParams p;
            d_params.tableSetBatchCb(p);
        }

        ii += leased_writers + 1;
    }

    // Release all target tables.
    for (TableInfo*& entry: target_tables) entry->done();

    return Status();
}

} // namespace jungle

