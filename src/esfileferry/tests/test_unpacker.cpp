#include "../EsFileFerryPacker.h"
#include "../EsFileFerryPlayer.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <curl/curl.h>
#include <iostream>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "Util/File.h"

namespace {
size_t onHttpWrite(char *ptr, size_t size, size_t nmemb, void *userdata) {
    const auto bytes = size * nmemb;
    auto *out = static_cast<std::string *>(userdata);
    out->append(ptr, bytes);
    return bytes;
}

struct HttpCallResult {
    bool ok = false;
    long status = 0;
    std::string body;
};

HttpCallResult doHttpCall(const std::string &url,
                          const std::string &method,
                          const std::string &body = std::string()) {
    HttpCallResult result;
    CURL *curl = curl_easy_init();
    if (!curl) {
        return result;
    }
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 1500L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 4000L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, onHttpWrite);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result.body);

    curl_slist *headers = nullptr;
    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                         static_cast<long>(body.size()));
        headers = curl_slist_append(headers, "Content-Type: application/json");
    } else {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    }
    if (headers) {
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }

    const auto code = curl_easy_perform(curl);
    if (code == CURLE_OK) {
        result.ok = true;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.status);
    }

    if (headers) {
        curl_slist_free_all(headers);
    }
    curl_easy_cleanup(curl);
    return result;
}

bool hasVersionShape(const std::string &body) {
    return body.find("\"code\"") != std::string::npos &&
           body.find("\"msg\"") != std::string::npos &&
           body.find("\"version\"") != std::string::npos;
}

std::string makeTempFilePath(const std::string &name) {
    return "./" + name;
}
}

int main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    auto &packer = EsFileFerryPacker::Instance();
    auto &unpacker = EsFileFerryUnPacker::Instance();
    packer.setPacketCallback(nullptr);
    packer.clearTasks();
    unpacker.clearTasks();
    packer.setChunkSize(4096);

    auto bridge_packets_to_unpacker =
        [&unpacker](const std::string &task_id,
                    std::vector<uint8_t> &&packet,
                    const EsFilePacketHeader &) {
        if (task_id == EsFileFerryPacker::kBootstrapTaskId || packet.empty()) {
            return;
        }
        size_t offset = 0;
        while (offset < packet.size()) {
            const auto remain = packet.size() - offset;
            const auto dynamic_step = static_cast<size_t>(1 + ((offset * 7) % 97));
            const auto step = std::min(remain, dynamic_step);
            unpacker.inputFrame(packet.data() + offset, step);
            offset += step;
        }
    };
    packer.setPacketCallback(bridge_packets_to_unpacker);

    auto run_case = [&](const std::string &file_path,
                        const std::vector<uint8_t> &source_data,
                        size_t chunk_size,
                        size_t rounds) {
        packer.setChunkSize(chunk_size);
        for (size_t round = 0; round < rounds; ++round) {
            std::mutex mtx;
            std::condition_variable cv;
            bool completed = false;
            uint64_t file_size = 0;
            uint32_t file_info_count = 0;
            uint32_t file_chunk_count = 0;
            uint32_t file_end_count = 0;
            bool seq_order_ok = true;
            bool has_seq = false;
            uint32_t last_seq = 0;
            std::string file_name;
            std::vector<uint8_t> received_data;

            const std::string task_id =
                "stability_task_" + std::to_string(chunk_size) + "_" +
                std::to_string(round);
            unpacker.setTaskCallback(
                task_id, [&](const EsTaskDataEvent &event) {
                    std::lock_guard<std::mutex> lock(mtx);
                    if (event.task_id != task_id) {
                        return;
                    }
                    if (has_seq && event.seq < last_seq) {
                        seq_order_ok = false;
                    }
                    has_seq = true;
                    last_seq = event.seq;
                    if (event.type == EsFilePacketType::FileInfo) {
                        ++file_info_count;
                        file_size = event.file_size;
                        file_name = event.file_name;
                    } else if (event.type == EsFilePacketType::FileChunk) {
                        ++file_chunk_count;
                        if (!event.payload.empty()) {
                            received_data.insert(received_data.end(),
                                                 event.payload.begin(),
                                                 event.payload.end());
                        }
                    } else if (event.type == EsFilePacketType::FileEnd) {
                        ++file_end_count;
                        completed = true;
                        cv.notify_all();
                    }
                });

            assert(packer.addFileTask(task_id, file_path, "stability.bin"));

            {
                std::unique_lock<std::mutex> lock(mtx);
                const bool ok = cv.wait_for(lock, std::chrono::seconds(8),
                                            [&]() { return completed; });
                assert(ok);
            }

            bool task_completed = false;
            for (size_t i = 0; i < 80; ++i) {
                auto infos = packer.getTaskInfos();
                for (const auto &info : infos) {
                    if (info.task_id == task_id && info.completed) {
                        task_completed = true;
                        break;
                    }
                }
                if (task_completed) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            assert(task_completed);

            assert(file_info_count >= 1);
            if (source_data.empty()) {
                assert(file_chunk_count == 0);
            } else {
                assert(file_chunk_count >= 1);
            }
            assert(file_end_count == 1);
            assert(file_size == source_data.size());
            assert(file_name == "stability.bin");
            assert(seq_order_ok);
            assert(received_data.size() == source_data.size());
            assert(received_data == source_data);

            unpacker.removeTask(task_id);
            packer.removeTask(task_id);
        }
    };

    const std::string file_path_large =
        makeTempFilePath("esfileferry_unpacker_stability_large.bin");
    std::vector<uint8_t> source_data_large;
    source_data_large.resize(1024 * 256 * 10);
    for (size_t i = 0; i < source_data_large.size(); ++i) {
        source_data_large[i] = static_cast<uint8_t>((i * 131 + 17) % 251);
    }
    {
        std::ofstream ofs(file_path_large, std::ios::binary);
        assert(ofs.is_open());
        ofs.write(reinterpret_cast<const char *>(source_data_large.data()),
                  static_cast<std::streamsize>(source_data_large.size()));
        ofs.flush();
    }

    const std::string file_path_small =
        makeTempFilePath("esfileferry_unpacker_stability_small.bin");
    std::vector<uint8_t> source_data_small;
    source_data_small.resize(37);
    for (size_t i = 0; i < source_data_small.size(); ++i) {
        source_data_small[i] = static_cast<uint8_t>((i * 17 + 3) % 253);
    }
    {
        std::ofstream ofs(file_path_small, std::ios::binary);
        assert(ofs.is_open());
        ofs.write(reinterpret_cast<const char *>(source_data_small.data()),
                  static_cast<std::streamsize>(source_data_small.size()));
        ofs.flush();
    }

    const std::string file_path_empty =
        makeTempFilePath("esfileferry_unpacker_stability_empty.bin");
    std::vector<uint8_t> source_data_empty;
    {
        std::ofstream ofs(file_path_empty, std::ios::binary);
        assert(ofs.is_open());
        ofs.flush();
    }

    run_case(file_path_large, source_data_large, 4096, 20);
    run_case(file_path_small, source_data_small, 7, 20);
    run_case(file_path_empty, source_data_empty, 1024, 10);

    {
        const std::string task_id = "mixed_annexb_ferry_task";
        std::mutex mtx;
        std::condition_variable cv;
        bool captured = false;
        std::vector<uint8_t> captured_packet;

        packer.setPacketCallback([&](const std::string &event_task_id,
                                     std::vector<uint8_t> &&packet,
                                     const EsFilePacketHeader &header) {
            if (event_task_id == task_id &&
                header.type == EsFilePacketType::FileInfo) {
                std::lock_guard<std::mutex> lock(mtx);
                if (!captured) {
                    captured_packet = std::move(packet);
                    captured = true;
                    cv.notify_all();
                }
            }
        });
        assert(packer.addFileTask(task_id, file_path_small, "mixed.bin"));
        {
            std::unique_lock<std::mutex> lock(mtx);
            const bool ok = cv.wait_for(lock, std::chrono::seconds(3),
                                        [&]() { return captured; });
            assert(ok);
        }
        packer.clearTasks();

        bool info_seen = false;
        unpacker.setTaskCallback(task_id, [&](const EsTaskDataEvent &event) {
            if (event.task_id == task_id &&
                event.type == EsFilePacketType::FileInfo) {
                info_seen = true;
            }
        });
        std::vector<uint8_t> mixed_frame = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06};
        mixed_frame.insert(mixed_frame.end(), captured_packet.begin(),
                           captured_packet.end());
        assert(unpacker.inputFrame(mixed_frame.data(), mixed_frame.size()));
        assert(info_seen);
        unpacker.removeTask(task_id);
        packer.setPacketCallback(bridge_packets_to_unpacker);
    }

    const auto version_get =
        doHttpCall("http://localhost:7080/index/api/version", "GET");
    const auto version_post =
        doHttpCall("http://localhost:7080/index/api/version", "POST", "{}");
    const bool version_api_ready =
        version_get.ok && version_get.status == 200 &&
        hasVersionShape(version_get.body) && version_post.ok &&
        version_post.status == 200 && hasVersionShape(version_post.body);

    const auto changeprotocol_api =
        doHttpCall("http://localhost:7080/changeprotocol/api", "GET");
    if (!changeprotocol_api.ok) {
        std::cerr << "http://localhost:7080/changeprotocol/api not reachable"
                  << std::endl;
    }

    if (version_api_ready) {
        auto run_http_api_task_case = [&](const std::string &task_id,
                                          const std::string &method,
                                          const std::string &body) {
            std::mutex mtx;
            std::condition_variable cv;
            bool completed = false;
            std::string received_text;

            unpacker.setTaskCallback(
                task_id, [&](const EsTaskDataEvent &event) {
                    std::lock_guard<std::mutex> lock(mtx);
                    if (event.task_id != task_id) {
                        return;
                    }
                    if (event.type == EsFilePacketType::FileChunk &&
                        !event.payload.empty()) {
                        received_text.append(
                            reinterpret_cast<const char *>(event.payload.data()),
                            event.payload.size());
                    } else if (event.type == EsFilePacketType::FileEnd) {
                        completed = true;
                        cv.notify_all();
                    }
                });

            const bool add_ok = packer.addHttpTask(
                task_id, "http://localhost:7080/index/api/version", method, {},
                body, "version.json");
            assert(add_ok);

            {
                std::unique_lock<std::mutex> lock(mtx);
                const bool ok = cv.wait_for(lock, std::chrono::seconds(8),
                                            [&]() { return completed; });
                assert(ok);
            }

            bool task_completed = false;
            for (size_t i = 0; i < 120; ++i) {
                auto infos = packer.getTaskInfos();
                for (const auto &info : infos) {
                    if (info.task_id == task_id && info.completed) {
                        task_completed = true;
                        break;
                    }
                }
                if (task_completed) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            assert(task_completed);
            assert(hasVersionShape(received_text));

            unpacker.removeTask(task_id);
            packer.removeTask(task_id);
        };

        run_http_api_task_case("api_version_get_task", "GET", "");
        run_http_api_task_case("api_version_post_task", "POST", "{}");
    } else {
        std::cerr
            << "skip packer api task tests, /index/api/version GET/POST not ready"
            << std::endl;
    }

    unpacker.clearTasks();
    packer.clearTasks();
    packer.setPacketCallback(nullptr);
    toolkit::File::delete_file(file_path_large, false);
    toolkit::File::delete_file(file_path_small, false);
    toolkit::File::delete_file(file_path_empty, false);
    curl_global_cleanup();

    return 0;
}
