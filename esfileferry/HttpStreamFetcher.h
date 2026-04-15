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

    static bool stream(const std::string &url,
                       const std::string &method,
                       const HttpHeaders &headers,
                       const std::string &body,
                       const OnChunk &on_chunk,
                       const OnHeaders &on_headers,
                       HttpHeaders *response_headers,
                       uint32_t *response_status_code,
                       std::string &err);
};
