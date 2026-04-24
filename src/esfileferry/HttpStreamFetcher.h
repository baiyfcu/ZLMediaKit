#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

class HttpStreamFetcher {
public:
    using HttpHeaders = std::vector<std::pair<std::string, std::string>>;
    using OnChunk = std::function<bool(const uint8_t *, size_t)>;
    using OnHeaders = std::function<void(uint32_t, const HttpHeaders &)>;

    struct TransferDiagnostics {
        uint32_t status_code = 0;
        bool headers_emitted = false;
        double name_lookup_ms = 0.0;
        double connect_ms = 0.0;
        double app_connect_ms = 0.0;
        double pretransfer_ms = 0.0;
        double starttransfer_ms = 0.0;
        double total_ms = 0.0;
        double download_bytes = 0.0;
        double content_length_bytes = 0.0;
        double download_speed_bytes_per_sec = 0.0;
    };

    static bool stream(const std::string &url,
                       const std::string &method,
                       const HttpHeaders &headers,
                       const std::string &body,
                       const OnChunk &on_chunk,
                       const OnHeaders &on_headers,
                       HttpHeaders *response_headers,
                       uint32_t *response_status_code,
                       std::string &err,
                       TransferDiagnostics *diagnostics = nullptr);
};
