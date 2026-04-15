#define private public
#include "../EsFileFerryPacker.h"
#undef private
#include "../EsFilePayloadProtocol.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "Util/logger.h"
#include "Network/TcpServer.h"
#include "Http/HttpSession.h"
#include "Util/NoticeCenter.h"
#include "Common/config.h"

using namespace toolkit;
using namespace mediakit;

#define TEST_CHECK(expr)                                                       \
    do {                                                                       \
        if (!(expr)) {                                                         \
            std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", #expr, __FILE__, \
                         __LINE__);                                            \
            std::fflush(stderr);                                               \
            std::abort();                                                      \
        }                                                                      \
    } while (0)

#undef assert
#define assert(expr) TEST_CHECK(expr)

namespace {
bool decodePacket(const std::vector<uint8_t> &raw, EsFilePacket &out) {
    if (raw.size() < kEsFileFixedHeaderSize + kEsFileCarrierPrefixSize) {
        return false;
    }
    if (!HasEsFileCarrierPrefix(raw.data(), raw.size())) {
        return false;
    }
    const uint8_t *p = raw.data() + kEsFileCarrierPrefixSize;
    if (!DecodeEsFilePacketHeader(p, raw.size() - kEsFileCarrierPrefixSize,
                                  out.header)) {
        return false;
    }
    if (out.header.magic != kEsFilePacketMagic) {
        return false;
    }
    if (out.header.total_len + kEsFileCarrierPrefixSize != raw.size()) {
        return false;
    }
    const auto need = static_cast<size_t>(kEsFileFixedHeaderSize + out.header.task_id_len + out.header.file_name_len + out.header.payload_len);
    if (need > raw.size() - kEsFileCarrierPrefixSize) {
        return false;
    }
    size_t pos = kEsFileFixedHeaderSize + kEsFileCarrierPrefixSize;
    out.task_id.assign(reinterpret_cast<const char *>(raw.data() + static_cast<long>(pos)), out.header.task_id_len);
    pos += out.header.task_id_len;
    out.file_name.assign(reinterpret_cast<const char *>(raw.data() + static_cast<long>(pos)), out.header.file_name_len);
    pos += out.header.file_name_len;
    out.payload.assign(raw.begin() + static_cast<long>(pos), raw.begin() + static_cast<long>(pos + out.header.payload_len));
    return true;
}

bool waitUntil(const std::chrono::milliseconds &timeout,
               const std::function<bool()> &pred,
               const std::chrono::milliseconds &step = std::chrono::milliseconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(step);
    }
    return pred();
}

void writeFile(const std::string &path, char fill, size_t size) {
    std::ofstream ofs(path, std::ios::binary);
    assert(ofs.is_open());
    std::string payload(size, fill);
    ofs.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    ofs.flush();
}
}

int main() {
    // 设置日志  [AUTO-TRANSLATED:50372045]
    // Set log
    Logger::Instance().add(std::make_shared<ConsoleChannel>("ConsoleChannel", LDebug));
    // 启动异步日志线程  [AUTO-TRANSLATED:c93cc6f4]
    // Start asynchronous log thread
    Logger::Instance().setWriter(std::make_shared<AsyncLogWriter>());
    // 启动一个http server主要是为了测试 api或者是mp4文件用
    auto rtspSrv = std::make_shared<toolkit::TcpServer>();
    rtspSrv->start<mediakit::HttpSession>(0, "127.0.0.1");
    const auto http_port = rtspSrv->getPort();
    assert(http_port != 0);
    const std::string http_base =
        "http://127.0.0.1:" + std::to_string(http_port);
    static int http_test_listener_tag = 0;
    std::atomic<int> slow_http_active{0};
    std::atomic<int> slow_http_max_active{0};
    std::atomic<int> slow_http_request_count{0};
    std::atomic<int> sim_http_active{0};
    std::atomic<int> sim_http_max_active{0};
    std::atomic<int> sim_http_request_count{0};
    std::atomic<int> sim_http_post_count{0};
    NoticeCenter::Instance().addListener(&http_test_listener_tag, Broadcast::kBroadcastHttpRequest, [&](BroadcastHttpRequestArgs) {
        static const std::string large_payload(3 * 1024 * 1024, 'L');
        static const std::string large_json_blob(2 * 1024 * 1024, 'J');
        if (parser.url() == "/path" && parser.method() == "POST") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json";
            invoker(200, header_out, parser.content());
            return;
        }
        if (parser.url() == "/index/api/version" &&
            (parser.method() == "GET" || parser.method() == "POST")) {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json; charset=utf-8";
            invoker(200, header_out,
                    std::string("{\"code\":0,\"msg\":\"success\",\"version\":\"2.3.2604121240\"}"));
            return;
        }
        if (parser.url() == "/test/1.mp4" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, std::string("HTTP_MP4_PAYLOAD"));
            return;
        }
        if (parser.url() == "/test/1.bin" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, std::string("HTTP_BIN_PAYLOAD"));
            return;
        }
        if (parser.url() == "/test/large.mp4" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, large_payload);
            return;
        }
        if (parser.url() == "/test/slow.bin" && parser.method() == "GET") {
            consumed = true;
            slow_http_request_count.fetch_add(1);
            const int active = slow_http_active.fetch_add(1) + 1;
            int observed = slow_http_max_active.load();
            while (active > observed &&
                   !slow_http_max_active.compare_exchange_weak(observed, active)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, std::string(256, 'S'));
            slow_http_active.fetch_sub(1);
            return;
        }
        if (parser.url() == "/sim/video.mp4" && parser.method() == "GET") {
            consumed = true;
            sim_http_request_count.fetch_add(1);
            const int active = sim_http_active.fetch_add(1) + 1;
            int observed = sim_http_max_active.load();
            while (active > observed &&
                   !sim_http_max_active.compare_exchange_weak(observed, active)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, large_payload);
            sim_http_active.fetch_sub(1);
            return;
        }
        if (parser.url() == "/sim/json/get" && parser.method() == "GET") {
            consumed = true;
            sim_http_request_count.fetch_add(1);
            const int active = sim_http_active.fetch_add(1) + 1;
            int observed = sim_http_max_active.load();
            while (active > observed &&
                   !sim_http_max_active.compare_exchange_weak(observed, active)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json; charset=utf-8";
            invoker(200, header_out,
                    std::string("{\"code\":0,\"type\":\"get\",\"payload\":\"") +
                        large_json_blob + "\"}");
            sim_http_active.fetch_sub(1);
            return;
        }
        if (parser.url() == "/sim/json/post" && parser.method() == "POST") {
            consumed = true;
            sim_http_request_count.fetch_add(1);
            sim_http_post_count.fetch_add(1);
            const int active = sim_http_active.fetch_add(1) + 1;
            int observed = sim_http_max_active.load();
            while (active > observed &&
                   !sim_http_max_active.compare_exchange_weak(observed, active)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(120));
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json; charset=utf-8";
            invoker(200, header_out,
                    std::string("{\"code\":0,\"type\":\"post\",\"request_size\":") +
                        std::to_string(parser.content().size()) +
                        ",\"payload\":\"" + large_json_blob + "\"}");
            sim_http_active.fetch_sub(1);
            return;
        }
    });


    auto &packer = EsFileFerryPacker::Instance();
    packer.setPacketCallback(nullptr);
    packer.clearTasks();
    packer.setChunkSize(64);

    std::mutex mtx;
    std::condition_variable cv;
    size_t bootstrap_count = 0;
    std::vector<std::vector<uint8_t>> task_packets;

    auto packet_collector = [&](const std::string &task_id,
                                std::vector<uint8_t> &&packet,
                                const EsFilePacketHeader &header) {
        (void)header;
        const auto packet_size = packet.size();
        const auto packet_hex = packet.empty() ? std::string() : hexmem(packet.data(), 6);
        std::lock_guard<std::mutex> lock(mtx);
        if (task_id == EsFileFerryPacker::kBootstrapTaskId) {
            ++bootstrap_count;
        } else {
            task_packets.emplace_back(std::move(packet));
        }
        DebugL << "task_id: " << task_id << ", packet size: " << packet_size << ",hex:" << packet_hex;
        cv.notify_all();
    };
    auto clearCollectedPackets = [&]() {
        std::lock_guard<std::mutex> lock(mtx);
        task_packets.clear();
    };
    packer.setPacketCallback(packet_collector);

    const bool ok = waitUntil(std::chrono::milliseconds(2000), [&]() {
        std::lock_guard<std::mutex> lock(mtx);
        return bootstrap_count >= 3;
    }, std::chrono::milliseconds(20));
    assert(ok);

    packer.clearTasks();
    packer.setChunkSize(64);
    packer.setTickPayloadQuotaBytes(64 * 3);
    clearCollectedPackets();
    const std::vector<std::string> fair_active_ids = {
        "fair_task_3", "fair_task_1", "fair_task_2"};
    const std::vector<std::string> fair_round_1 = packer.pickFairRound(fair_active_ids);
    const std::vector<std::string> fair_round_2 = packer.pickFairRound(fair_active_ids);
    const std::vector<std::string> fair_round_3 = packer.pickFairRound(fair_active_ids);
    const std::vector<std::string> fair_round_4 = packer.pickFairRound(fair_active_ids);
    assert((fair_round_1 == std::vector<std::string>{"fair_task_1", "fair_task_2", "fair_task_3"}));
    assert((fair_round_2 == std::vector<std::string>{"fair_task_2", "fair_task_3", "fair_task_1"}));
    assert((fair_round_3 == std::vector<std::string>{"fair_task_3", "fair_task_1", "fair_task_2"}));
    assert((fair_round_4 == std::vector<std::string>{"fair_task_1", "fair_task_2", "fair_task_3"}));
    assert(packer.computeTaskQuota(64 * 3, 3, 0) == 64);
    assert(packer.computeTaskQuota(64 * 3, 3, 1) == 64);
    assert(packer.computeTaskQuota(64 * 3, 3, 2) == 64);
    assert(packer.computeTaskQuota(64 * 3 + 2, 3, 0) == 65);
    assert(packer.computeTaskQuota(64 * 3 + 2, 3, 1) == 65);
    assert(packer.computeTaskQuota(64 * 3 + 2, 3, 2) == 64);

    packer.clearTasks();
    clearCollectedPackets();
    packer.setMaxHttpFetchConcurrency(2);
    slow_http_active.store(0);
    slow_http_max_active.store(0);
    slow_http_request_count.store(0);
    assert(packer.addHttpTask("slow_http_1", http_base + "/test/slow.bin", "GET", {}, "", "slow_1.bin"));
    assert(packer.addHttpTask("slow_http_2", http_base + "/test/slow.bin", "GET", {}, "", "slow_2.bin"));
    assert(packer.addHttpTask("slow_http_3", http_base + "/test/slow.bin", "GET", {}, "", "slow_3.bin"));
    assert(packer.addHttpTask("slow_http_4", http_base + "/test/slow.bin", "GET", {}, "", "slow_4.bin"));
    const bool slow_http_completed = waitUntil(std::chrono::milliseconds(5000), [&]() {
        auto infos = packer.getTaskInfos();
        size_t completed_count = 0;
        for (const auto &info : infos) {
            if ((info.task_id == "slow_http_1" || info.task_id == "slow_http_2" ||
                 info.task_id == "slow_http_3" || info.task_id == "slow_http_4") &&
                info.completed) {
                ++completed_count;
            }
        }
        return completed_count == 4;
    }, std::chrono::milliseconds(20));
    assert(slow_http_completed);
    assert(slow_http_request_count.load() == 4);
    assert(slow_http_max_active.load() <= 2);

    packer.clearTasks();
    clearCollectedPackets();
    packer.setChunkSize(0);
    packer.setTickPayloadQuotaBytes(0);
    packer.setMaxHttpFetchConcurrency(0);
    packer.setMaxTotalHttpBufferedBytes(0);
    sim_http_active.store(0);
    sim_http_max_active.store(0);
    sim_http_request_count.store(0);
    sim_http_post_count.store(0);
    std::atomic<uint64_t> sim_chunk_packet_count{0};
    packer.setPacketCallback([&](const std::string &task_id,
                                 std::vector<uint8_t> &&packet,
                                 const EsFilePacketHeader &header) {
        (void)packet;
        if (task_id == EsFileFerryPacker::kBootstrapTaskId) {
            return;
        }
        if (task_id.rfind("sim_", 0) == 0) {
            if (header.type == EsFilePacketType::FileChunk) {
                sim_chunk_packet_count.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
    });
    for (size_t i = 0; i < 20; ++i) {
        const auto task_id = "sim_video_task_" + std::to_string(i);
        assert(packer.addHttpTask(task_id, http_base + "/sim/video.mp4", "GET",
                                  {}, "", "sim_video.mp4"));
    }
    for (size_t i = 0; i < 15; ++i) {
        const auto task_id = "sim_json_get_task_" + std::to_string(i);
        assert(packer.addHttpTask(task_id, http_base + "/sim/json/get", "GET",
                                  {}, "", "sim_json_get.json"));
    }
    const std::string sim_post_body(8 * 1024, 'B');
    for (size_t i = 0; i < 15; ++i) {
        const auto task_id = "sim_json_post_task_" + std::to_string(i);
        assert(packer.addHttpTask(
            task_id, http_base + "/sim/json/post", "POST",
            {{"Content-Type", "application/json"}}, sim_post_body,
            "sim_json_post.json"));
    }
    uint64_t sim_max_total_buffered_bytes = 0;
    const bool sim_http_completed = waitUntil(std::chrono::milliseconds(60000), [&]() {
        {
            std::lock_guard<std::mutex> lock(packer._mtx);
            sim_max_total_buffered_bytes = std::max(
                sim_max_total_buffered_bytes, packer._total_http_buffered_bytes);
        }
        return sim_http_request_count.load() == 50 &&
               sim_http_post_count.load() == 15 &&
               sim_chunk_packet_count.load() > 0;
    }, std::chrono::milliseconds(20));
    {
        std::lock_guard<std::mutex> lock(packer._mtx);
        sim_max_total_buffered_bytes = std::max(
            sim_max_total_buffered_bytes, packer._total_http_buffered_bytes);
    }
    assert(sim_http_completed);
    const bool sim_infos_ready = waitUntil(std::chrono::milliseconds(60000), [&]() {
        auto infos = packer.getTaskInfos();
        size_t large_json_count = 0;
        for (const auto &info : infos) {
            if ((info.task_id.rfind("sim_json_get_task_", 0) == 0 ||
                 info.task_id.rfind("sim_json_post_task_", 0) == 0) &&
                info.file_size > 1024 * 1024) {
                ++large_json_count;
            }
        }
        return large_json_count == 30;
    }, std::chrono::milliseconds(20));
    assert(sim_infos_ready);
    assert(sim_http_request_count.load() == 50);
    assert(sim_http_post_count.load() == 15);
    assert(sim_http_max_active.load() <=
           static_cast<int>(EsFileFerryPacker::kDefaultHttpFetchConcurrency));
    assert(sim_max_total_buffered_bytes >
           EsFileFerryPacker::kDefaultMaxHttpBufferedBytesPerTask);
    assert(sim_max_total_buffered_bytes <=
           EsFileFerryPacker::kDefaultMaxHttpBufferedBytes +
               EsFileFerryPacker::kDefaultHttpBufferChunkBytes);
    assert(sim_chunk_packet_count.load() > 0);
    {
        auto infos = packer.getTaskInfos();
        size_t sim_info_count = 0;
        size_t large_json_count = 0;
        for (const auto &info : infos) {
            if (info.task_id.rfind("sim_", 0) != 0) {
                continue;
            }
            ++sim_info_count;
            if ((info.task_id.rfind("sim_json_get_task_", 0) == 0 ||
                 info.task_id.rfind("sim_json_post_task_", 0) == 0) &&
                info.file_size > 1024 * 1024) {
                ++large_json_count;
            }
        }
        assert(sim_info_count == 50);
        assert(large_json_count == 30);
    }
    packer.clearTasks();
    packer.setPacketCallback(packet_collector);
    clearCollectedPackets();

    packer.clearTasks();
    packer.setTickPayloadQuotaBytes(0);
    packer.setMaxHttpFetchConcurrency(0);
    clearCollectedPackets();

    const std::string tmp_file = "/tmp/esfileferry_packer_test.bin";
    writeFile(tmp_file, 'x', 400);

    assert(!packer.addTask("", tmp_file));
    assert(!packer.getLastError().empty());
    assert(packer.addFileTask("file_task", tmp_file, "sample.bin"));
    size_t packet_count_before_http = 0;
    {
        std::lock_guard<std::mutex> lock(mtx);
        packet_count_before_http = task_packets.size();
    }
    EsFileFerryPacker::HttpHeaders headers = {{"Content-Type", "application/json"}, {"X-Test", "1"}};
    assert(!packer.addHttpTask("bad_api_task", http_base + "/path", "PUT", headers, "{\"a\":1}", "bad_api.txt"));
    assert(!packer.getLastError().empty());
    assert(packer.addHttpTask("api_task", http_base + "/path", "POST", headers, "{\"a\":1}", "api.txt"));
    assert(packer.addHttpTask("http_file_task", http_base + "/test/1.mp4", "GET", {}, "", "1.mp4"));
    EsFileFerryPacker::HttpHeaders doc_headers = {{"Sec-Fetch-Dest", "document"}, {"Accept", "text/html,*/*;q=0.8"}};
    assert(packer.addHttpTask("http_file_document_task", http_base + "/test/1.mp4", "GET", doc_headers, "", "1.mp4"));
    assert(packer.addHttpTask("http_bin_document_task", http_base + "/test/1.bin", "GET", doc_headers, "", "1.bin"));
    assert(packer.addHttpTask("http_api_document_task", http_base + "/index/api/version", "GET", doc_headers, "", "version.json"));
    std::vector<std::string> large_task_ids;
    for (size_t i = 0; i < 5; ++i) {
        const auto task_id = "http_large_task_" + std::to_string(i + 1);
        assert(packer.addHttpTask(task_id, http_base + "/test/large.mp4", "GET", {}, "", "large.mp4"));
        large_task_ids.emplace_back(task_id);
    }
    bool packet_grew = false;
    for (size_t i = 0; i < 50; ++i) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (task_packets.size() > packet_count_before_http) {
                packet_grew = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(packet_grew);
    assert(!packer.getTaskInfos().empty());

    bool all_completed = false;
    for (size_t i = 0; i < 250; ++i) {
        auto infos = packer.getTaskInfos();
        bool file_done = false;
        bool api_done = false;
        bool http_file_done = false;
        bool http_file_document_done = false;
        bool http_bin_document_done = false;
        bool http_api_document_done = false;
        size_t large_done_count = 0;
        for (const auto &info : infos) {
            if (info.task_id == "file_task" && info.completed) {
                file_done = true;
            }
            if (info.task_id == "api_task" && info.completed) {
                api_done = true;
            }
            if (info.task_id == "http_file_task" && info.completed) {
                http_file_done = true;
            }
            if (info.task_id == "http_file_document_task" && info.completed) {
                http_file_document_done = true;
            }
            if (info.task_id == "http_bin_document_task" && info.completed) {
                http_bin_document_done = true;
            }
            if (info.task_id == "http_api_document_task" && info.completed) {
                http_api_document_done = true;
            }
            if (std::find(large_task_ids.begin(), large_task_ids.end(), info.task_id) != large_task_ids.end() &&
                info.completed) {
                ++large_done_count;
            }
        }
        if (file_done && api_done && http_file_done && http_file_document_done &&
            http_bin_document_done && http_api_document_done &&
            large_done_count == large_task_ids.size()) {
            all_completed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    assert(all_completed);

    size_t stable_bootstrap_count = 0;
    {
        std::lock_guard<std::mutex> lock(mtx);
        stable_bootstrap_count = bootstrap_count;
    }

    packer.removeTask("api_task");
    auto infos_after_remove = packer.getTaskInfos();
    const bool has_api_task = std::any_of(infos_after_remove.begin(), infos_after_remove.end(), [](const EsFilePackTaskInfo &info) {
        return info.task_id == "api_task";
    });
    assert(!has_api_task);

    packer.setPacketCallback(nullptr);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    {
        std::lock_guard<std::mutex> lock(mtx);
        assert(bootstrap_count == stable_bootstrap_count);
    }

    std::unordered_map<std::string, uint32_t> file_info_count;
    std::unordered_map<std::string, uint32_t> file_chunk_count;
    std::unordered_map<std::string, uint32_t> file_end_count;
    {
        std::lock_guard<std::mutex> lock(mtx);
        assert(!task_packets.empty());
        for (const auto &raw : task_packets) {
            assert(raw.size() >= 9);
            assert(HasEsFileCarrierPrefix(raw.data(), raw.size()));
            assert(ReadEsFileU32BE(raw.data() + kEsFileCarrierPrefixSize) ==
                   kEsFilePacketMagic);
            EsFilePacket packet;
            assert(decodePacket(raw, packet));
            if (packet.header.type == EsFilePacketType::FileInfo) {
                file_info_count[packet.task_id]++;
            } else if (packet.header.type == EsFilePacketType::FileChunk) {
                file_chunk_count[packet.task_id]++;
            } else if (packet.header.type == EsFilePacketType::FileEnd) {
                file_end_count[packet.task_id]++;
            }
        }
    }
    assert(file_info_count["file_task"] >= 1);
    assert(file_chunk_count["file_task"] >= 1);
    assert(file_end_count["file_task"] >= 1);
    assert(file_info_count["api_task"] >= 1);
    assert(file_chunk_count["api_task"] >= 1);
    assert(file_end_count["api_task"] >= 1);
    assert(file_info_count["http_file_task"] >= 1);
    assert(file_chunk_count["http_file_task"] >= 1);
    assert(file_end_count["http_file_task"] >= 1);
    assert(file_info_count["http_file_document_task"] >= 1);
    assert(file_chunk_count["http_file_document_task"] >= 1);
    assert(file_end_count["http_file_document_task"] >= 1);
    assert(file_info_count["http_bin_document_task"] >= 1);
    assert(file_chunk_count["http_bin_document_task"] >= 1);
    assert(file_end_count["http_bin_document_task"] >= 1);
    assert(file_info_count["http_api_document_task"] >= 1);
    assert(file_chunk_count["http_api_document_task"] >= 1);
    assert(file_end_count["http_api_document_task"] >= 1);
    for (const auto &task_id : large_task_ids) {
        assert(file_info_count[task_id] >= 1);
        assert(file_chunk_count[task_id] >= 1);
        assert(file_end_count[task_id] >= 1);
    }

    bool saw_chunk_header_prefix = false;
    {
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto &raw : task_packets) {
            EsFilePacket packet;
            assert(decodePacket(raw, packet));
            if (packet.task_id == "file_task" &&
                packet.header.type == EsFilePacketType::FileChunk) {
                assert(raw.size() >= 12);
                assert(raw[0] == 0x00);
                assert(raw[1] == 0x00);
                assert(raw[2] == 0x00);
                assert(raw[3] == 0x01);
                assert(raw[4] == 0x61);
                assert(raw[5] == 0x47);
                assert(raw[6] == 0x54);
                assert(raw[7] == 0x46);
                assert(raw[8] == 0x59);
                assert(raw[9] == 0x01);
                assert(raw[10] ==
                       static_cast<uint8_t>(EsFilePacketType::FileChunk));
                saw_chunk_header_prefix = true;
                break;
            }
        }
    }
    assert(saw_chunk_header_prefix);

    bool api_payload_ok = false;
    bool http_file_payload_ok = false;
    bool api_header_ok = false;
    bool http_file_header_ok = false;
    bool http_file_document_header_ok = false;
    bool http_file_document_payload_ok = false;
    bool http_bin_document_header_ok = false;
    bool http_bin_document_payload_ok = false;
    bool http_api_document_header_ok = false;
    bool http_api_document_has_chunk = false;
    bool http_api_document_payload_ok = false;
    {
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto &raw : task_packets) {
            EsFilePacket packet;
            assert(decodePacket(raw, packet));
            if (packet.task_id == "api_task" && packet.header.type == EsFilePacketType::FileInfo &&
                (packet.header.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find(":status: 200") != std::string::npos &&
                    text.find("Content-Type: application/json") != std::string::npos) {
                    api_header_ok = true;
                }
            }
            if (packet.task_id == "http_file_task" && packet.header.type == EsFilePacketType::FileInfo &&
                (packet.header.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find(":status: 200") != std::string::npos &&
                    text.find("Content-Type: application/octet-stream") != std::string::npos) {
                    http_file_header_ok = true;
                }
            }
            if (packet.task_id == "http_file_document_task" && packet.header.type == EsFilePacketType::FileInfo &&
                (packet.header.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find(":status: 200") != std::string::npos &&
                    text.find("Content-Type: application/octet-stream") != std::string::npos) {
                    http_file_document_header_ok = true;
                }
            }
            if (packet.task_id == "http_bin_document_task" && packet.header.type == EsFilePacketType::FileInfo &&
                (packet.header.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find(":status: 200") != std::string::npos &&
                    text.find("Content-Type: application/octet-stream") != std::string::npos) {
                    http_bin_document_header_ok = true;
                }
            }
            if (packet.task_id == "http_api_document_task" && packet.header.type == EsFilePacketType::FileInfo &&
                (packet.header.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find(":status: 200") != std::string::npos &&
                    text.find("Content-Type: application/json; charset=utf-8") != std::string::npos) {
                    http_api_document_header_ok = true;
                }
            }
            if (packet.task_id == "api_task" && packet.header.type == EsFilePacketType::FileChunk) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find("{\"a\":1}") != std::string::npos) {
                    api_payload_ok = true;
                }
            }
            if (packet.task_id == "http_file_task" && packet.header.type == EsFilePacketType::FileChunk) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find("HTTP_MP4_PAYLOAD") != std::string::npos) {
                    http_file_payload_ok = true;
                }
            }
            if (packet.task_id == "http_file_document_task" && packet.header.type == EsFilePacketType::FileChunk) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find("HTTP_MP4_PAYLOAD") != std::string::npos) {
                    http_file_document_payload_ok = true;
                }
            }
            if (packet.task_id == "http_bin_document_task" && packet.header.type == EsFilePacketType::FileChunk) {
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find("HTTP_BIN_PAYLOAD") != std::string::npos) {
                    http_bin_document_payload_ok = true;
                }
            }
            if (packet.task_id == "http_api_document_task" && packet.header.type == EsFilePacketType::FileChunk) {
                http_api_document_has_chunk = true;
                const std::string text(packet.payload.begin(), packet.payload.end());
                if (text.find("\"version\"") != std::string::npos &&
                    text.find("\"code\":0") != std::string::npos) {
                    http_api_document_payload_ok = true;
                }
            }
        }
    }
    assert(api_header_ok);
    assert(http_file_header_ok);
    assert(http_file_document_header_ok);
    assert(http_bin_document_header_ok);
    assert(http_api_document_header_ok);
    assert(api_payload_ok);
    assert(http_file_payload_ok);
    assert(http_file_document_payload_ok);
    assert(http_bin_document_payload_ok);
    assert(http_api_document_payload_ok);
    assert(http_api_document_has_chunk);

    packer.clearTasks();
    assert(packer.getTaskInfos().empty());
    std::remove(tmp_file.c_str());
    return 0;
}
