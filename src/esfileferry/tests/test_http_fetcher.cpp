#include "../HttpStreamFetcher.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cctype>
#include <string>
#include <thread>

#include "Http/HttpSession.h"
#include "Network/TcpServer.h"
#include "Util/NoticeCenter.h"
#include "Util/logger.h"

using namespace toolkit;
using namespace mediakit;

namespace {

std::string findHeaderIgnoreCase(const HttpSession::KeyValue &headers,
                                 const std::string &name) {
    auto lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    for (const auto &entry : headers) {
        auto lower_key = entry.first;
        std::transform(lower_key.begin(), lower_key.end(), lower_key.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (lower_key == lower_name) {
            return entry.second;
        }
    }
    return std::string();
}

} // namespace

int main() {
    Logger::Instance().add(std::make_shared<ConsoleChannel>("ConsoleChannel", LDebug));
    Logger::Instance().setWriter(std::make_shared<AsyncLogWriter>());

    auto http_server = std::make_shared<TcpServer>();
    http_server->start<HttpSession>(0, "127.0.0.1");
    const auto http_port = http_server->getPort();
    assert(http_port != 0);
    const std::string http_base = "http://127.0.0.1:" + std::to_string(http_port);

    static int http_test_listener_tag = 0;
    NoticeCenter::Instance().addListener(&http_test_listener_tag, Broadcast::kBroadcastHttpRequest,
                                         [](BroadcastHttpRequestArgs) {
        if (parser.url() == "/headers" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "text/plain";
            header_out["X-Reply"] = "headers-ok";
            const auto &headers = parser.getHeader();
            std::string body;
            body += "user-agent=" + findHeaderIgnoreCase(headers, "User-Agent") + "\n";
            body += "accept=" + findHeaderIgnoreCase(headers, "Accept") + "\n";
            body += "accept-encoding=" + findHeaderIgnoreCase(headers, "Accept-Encoding") + "\n";
            body += "x-test=" + findHeaderIgnoreCase(headers, "X-Test") + "\n";
            body += "host=" + findHeaderIgnoreCase(headers, "Host") + "\n";
            invoker(200, header_out, body);
            return;
        }
        if (parser.url() == "/large" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/octet-stream";
            invoker(200, header_out, std::string(2 * 1024 * 1024, 'L'));
            return;
        }
        if (parser.url() == "/json" && parser.method() == "GET") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json";
            invoker(200, header_out,
                    std::string("{\"code\":0,\"type\":\"get\",\"payload\":\"") +
                        std::string(1024 * 1024, 'J') + "\"}");
            return;
        }
        if (parser.url() == "/post-json" && parser.method() == "POST") {
            consumed = true;
            HttpSession::KeyValue header_out;
            header_out["Content-Type"] = "application/json";
            invoker(200, header_out,
                    std::string("{\"code\":0,\"type\":\"post\",\"request_size\":") +
                        std::to_string(parser.content().size()) +
                        ",\"payload\":\"" + std::string(1024 * 1024, 'P') + "\"}");
            return;
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    bool saw_headers = false;
    uint32_t status_code = 0;
    HttpStreamFetcher::HttpHeaders response_headers;
    std::string body_text;
    std::string err;
    const bool ok = HttpStreamFetcher::stream(
        http_base + "/headers", "GET",
        {{"Host", "bad-host.example"},
         {"Content-Length", "999"},
         {"X-Test", "1"}},
        "",
        [&body_text](const uint8_t *data, size_t size) {
            body_text.append(reinterpret_cast<const char *>(data), size);
            return true;
        },
        [&saw_headers, &status_code](uint32_t code,
                                     const HttpStreamFetcher::HttpHeaders &) {
            saw_headers = true;
            status_code = code;
        },
        &response_headers, &status_code, err);

    assert(ok);
    assert(err.empty());
    assert(saw_headers);
    assert(status_code == 200);
    assert(body_text.find("user-agent=GbTransfer-Ferry/1.0") != std::string::npos);
    assert(body_text.find("accept=*/*") != std::string::npos);
    assert(body_text.find("accept-encoding=identity") != std::string::npos);
    assert(body_text.find("x-test=1") != std::string::npos);
    assert(body_text.find("host=bad-host.example") == std::string::npos);

    bool has_reply_header = false;
    for (const auto &header : response_headers) {
        if (header.first == "X-Reply" && header.second == "headers-ok") {
            has_reply_header = true;
            break;
        }
    }
    assert(has_reply_header);

    size_t chunk_callback_count = 0;
    std::string consume_err;
    const bool consume_ok = HttpStreamFetcher::stream(
        http_base + "/large", "GET", {}, "",
        [&chunk_callback_count](const uint8_t *, size_t) {
            ++chunk_callback_count;
            return false;
        },
        nullptr, nullptr, nullptr, consume_err);
    assert(!consume_ok);
    assert(chunk_callback_count >= 1);
    assert(consume_err == "consume http payload failed");

    std::string json_text;
    uint32_t json_status = 0;
    std::string json_err;
    const bool json_ok = HttpStreamFetcher::stream(
        http_base + "/json", "GET", {}, "",
        [&json_text](const uint8_t *data, size_t size) {
            json_text.append(reinterpret_cast<const char *>(data), size);
            return true;
        },
        [&json_status](uint32_t code, const HttpStreamFetcher::HttpHeaders &) {
            json_status = code;
        },
        nullptr, &json_status, json_err);
    assert(json_ok);
    assert(json_err.empty());
    assert(json_status == 200);
    assert(json_text.find("\"type\":\"get\"") != std::string::npos);
    assert(json_text.size() > 1024 * 1024);

    std::string post_text;
    uint32_t post_status = 0;
    std::string post_err;
    const std::string post_body(8 * 1024, 'B');
    const bool post_ok = HttpStreamFetcher::stream(
        http_base + "/post-json", "POST",
        {{"Content-Type", "application/json"}},
        post_body,
        [&post_text](const uint8_t *data, size_t size) {
            post_text.append(reinterpret_cast<const char *>(data), size);
            return true;
        },
        [&post_status](uint32_t code, const HttpStreamFetcher::HttpHeaders &) {
            post_status = code;
        },
        nullptr, &post_status, post_err);
    assert(post_ok);
    assert(post_err.empty());
    assert(post_status == 200);
    assert(post_text.find("\"type\":\"post\"") != std::string::npos);
    assert(post_text.find("\"request_size\":8192") != std::string::npos);
    assert(post_text.size() > 1024 * 1024);

    return 0;
}
