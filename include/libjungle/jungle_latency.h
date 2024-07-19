#pragma once

#include "latency_collector.h"
#include "latency_dump.h"

struct JungleLatency {
    static LatencyCollector* getLatencyCollector() {
        static LatencyCollector collector;
        return &collector;
    }

    static std::string dump() {
        LatencyDumpDefaultImpl dump_impl;
        return getLatencyCollector()->dump(&dump_impl);
    }
};
