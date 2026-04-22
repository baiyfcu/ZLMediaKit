#include "HttpStreamFetcher.h"
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <curl/curl.h>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

namespace {

constexpr float kDefaultHttpStreamTimeoutSec = 0.0f;
constexpr long kDefaultHttpConnectTimeoutMs = 30 * 1000;
constexpr long kDefaultCurlBufferSize = 512 * 1024;

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

std::string trimAsciiWhitespace(std::string value) {
  auto is_space = [](unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  };
  while (!value.empty() && is_space(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  while (!value.empty() && is_space(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  return value;
}

bool isBlockedRequestHeader(const std::string &key_lower) {
  static const std::unordered_set<std::string> blocked = {
      "host",              "content-length", "connection", "keep-alive",
      "proxy-connection",  "transfer-encoding", "te",      "trailer",
      "upgrade"};
  return blocked.find(key_lower) != blocked.end();
}

HttpStreamFetcher::HttpHeaders makeEffectiveHeaders(
    const HttpStreamFetcher::HttpHeaders &headers) {
  HttpStreamFetcher::HttpHeaders out;
  out.reserve(headers.size() + 4);
  bool has_user_agent = false;
  bool has_accept = false;
  bool has_accept_encoding = false;
  for (const auto &header : headers) {
    const auto key_lower = toLowerCopy(header.first);
    if (key_lower.empty() || isBlockedRequestHeader(key_lower)) {
      continue;
    }
    if (key_lower == "user-agent") {
      has_user_agent = true;
    } else if (key_lower == "accept") {
      has_accept = true;
    } else if (key_lower == "accept-encoding") {
      has_accept_encoding = true;
    }
    out.emplace_back(header.first, header.second);
  }
  if (!has_user_agent) {
    out.emplace_back("User-Agent", "GbTransfer-Ferry/1.0");
  }
  if (!has_accept) {
    out.emplace_back("Accept", "*/*");
  }
  if (!has_accept_encoding) {
    out.emplace_back("Accept-Encoding", "identity");
  }
  return out;
}

bool isInterimStatusCode(uint32_t status_code) {
  return status_code >= 100 && status_code < 200;
}

struct CurlRequestContext {
  HttpStreamFetcher::OnChunk on_chunk;
  HttpStreamFetcher::OnHeaders on_headers;
  HttpStreamFetcher::HttpHeaders response_headers;
  HttpStreamFetcher::HttpHeaders pending_headers;
  uint32_t status_code = 0;
  uint32_t pending_status_code = 0;
  bool headers_emitted = false;
  bool consumer_ok = true;
  std::string consumer_err;

  void resetPendingHeaders(uint32_t status) {
    pending_status_code = status;
    pending_headers.clear();
  }

  void emitHeadersIfReady() {
    if (headers_emitted || pending_status_code == 0 ||
        isInterimStatusCode(pending_status_code)) {
      return;
    }
    status_code = pending_status_code;
    response_headers = pending_headers;
    headers_emitted = true;
    if (on_headers) {
      on_headers(status_code, response_headers);
    }
  }
};

void ensureCurlGlobalInit() {
  static std::once_flag once;
  std::call_once(once, []() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
  });
}

size_t onCurlHeader(char *buffer, size_t size, size_t nitems, void *userdata) {
  const auto bytes = size * nitems;
  auto *ctx = static_cast<CurlRequestContext *>(userdata);
  if (!ctx || !buffer || bytes == 0) {
    return bytes;
  }

  std::string line(buffer, bytes);
  const auto trimmed = trimAsciiWhitespace(line);
  if (trimmed.empty()) {
    ctx->emitHeadersIfReady();
    return bytes;
  }

  if (trimmed.rfind("HTTP/", 0) == 0) {
    const auto first_space = trimmed.find(' ');
    if (first_space != std::string::npos) {
      const auto second_space = trimmed.find(' ', first_space + 1);
      const auto code_text =
          trimmed.substr(first_space + 1,
                         second_space == std::string::npos
                             ? std::string::npos
                             : second_space - first_space - 1);
      const auto parsed = std::strtoul(code_text.c_str(), nullptr, 10);
      ctx->resetPendingHeaders(static_cast<uint32_t>(parsed));
    }
    return bytes;
  }

  const auto colon = trimmed.find(':');
  if (colon == std::string::npos) {
    return bytes;
  }
  auto key = trimAsciiWhitespace(trimmed.substr(0, colon));
  if (key.empty()) {
    return bytes;
  }
  auto value = trimAsciiWhitespace(trimmed.substr(colon + 1));
  ctx->pending_headers.emplace_back(std::move(key), std::move(value));
  return bytes;
}

size_t onCurlWrite(char *ptr, size_t size, size_t nmemb, void *userdata) {
  const auto bytes = size * nmemb;
  auto *ctx = static_cast<CurlRequestContext *>(userdata);
  if (!ctx || !ptr || bytes == 0) {
    return bytes;
  }

  ctx->emitHeadersIfReady();
  if (!ctx->on_chunk) {
    return bytes;
  }
  if (!ctx->on_chunk(reinterpret_cast<const uint8_t *>(ptr), bytes)) {
    ctx->consumer_ok = false;
    if (ctx->consumer_err.empty()) {
      ctx->consumer_err = "consume http payload failed";
    }
    return 0;
  }
  return bytes;
}

curl_slist *buildCurlHeaderList(
    const HttpStreamFetcher::HttpHeaders &headers) {
  curl_slist *list = nullptr;
  for (const auto &header : headers) {
    const auto line = header.first + ": " + header.second;
    auto *next = curl_slist_append(list, line.c_str());
    if (!next) {
      curl_slist_free_all(list);
      return nullptr;
    }
    list = next;
  }
  return list;
}

std::string mapCurlError(CURLcode code, const CurlRequestContext &ctx) {
  if (!ctx.consumer_ok && !ctx.consumer_err.empty()) {
    return ctx.consumer_err;
  }
  return curl_easy_strerror(code);
}

bool setupCurlRequest(CURL *curl, const std::string &url,
                      const std::string &method, const std::string &body,
                      curl_slist *request_headers,
                      CurlRequestContext *ctx,
                      std::string &err) {
  if (!curl || !ctx) {
    err = "curl init failed";
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                   kDefaultHttpConnectTimeoutMs);
  if (kDefaultHttpStreamTimeoutSec > 0.0f) {
    curl_easy_setopt(
        curl, CURLOPT_TIMEOUT_MS,
        static_cast<long>(kDefaultHttpStreamTimeoutSec * 1000.0f));
  }
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &onCurlHeader);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &onCurlWrite);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, kDefaultCurlBufferSize);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, request_headers);

  if (method == "GET") {
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    return true;
  }
  if (method == "POST") {
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(body.size()));
    return true;
  }

  err = "unsupported http method: " + method;
  return false;
}

} // namespace

bool HttpStreamFetcher::stream(const std::string &url,
                               const std::string &method,
                               const HttpHeaders &headers,
                               const std::string &body,
                               const OnChunk &on_chunk,
                               const OnHeaders &on_headers,
                               HttpHeaders *response_headers,
                               uint32_t *response_status_code,
                               std::string &err) {
  err.clear();
  ensureCurlGlobalInit();

  const auto effective_headers = makeEffectiveHeaders(headers);
  CurlRequestContext ctx;
  ctx.on_chunk = on_chunk;
  ctx.on_headers = on_headers;

  auto *curl = curl_easy_init();
  if (!curl) {
    err = "curl_easy_init failed";
    return false;
  }

  auto *request_headers = buildCurlHeaderList(effective_headers);
  if (!effective_headers.empty() && !request_headers) {
    curl_easy_cleanup(curl);
    err = "build curl headers failed";
    return false;
  }

  if (!setupCurlRequest(curl, url, method, body, request_headers, &ctx, err)) {
    curl_slist_free_all(request_headers);
    curl_easy_cleanup(curl);
    return false;
  }

  const auto code = curl_easy_perform(curl);
  long response_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
  if (response_code > 0 && ctx.pending_status_code == 0) {
    ctx.pending_status_code = static_cast<uint32_t>(response_code);
  }
  ctx.emitHeadersIfReady();
  if (ctx.status_code == 0 && response_code > 0) {
    ctx.status_code = static_cast<uint32_t>(response_code);
  }

  if (response_status_code) {
    *response_status_code = ctx.status_code;
  }
  if (response_headers) {
    *response_headers = ctx.response_headers;
  }

  curl_slist_free_all(request_headers);
  curl_easy_cleanup(curl);

  if (!ctx.consumer_ok) {
    err = ctx.consumer_err.empty() ? "consume http payload failed"
                                   : ctx.consumer_err;
    return false;
  }
  if (code != CURLE_OK) {
    err = mapCurlError(code, ctx);
    return false;
  }
  if (ctx.status_code < 200 || ctx.status_code >= 300) {
    err = "http status " + std::to_string(ctx.status_code);
    return false;
  }
  return true;
}
