#include "HttpStreamFetcher.h"
#include "Http/HttpRequester.h"
#include "Poller/EventPoller.h"
#include <algorithm>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <unordered_set>
#include <utility>

using namespace mediakit;
using namespace toolkit;

namespace {

constexpr float kDefaultHttpStreamTimeoutSec = 60.0f;

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
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

HttpStreamFetcher::HttpHeaders convertHeaders(
    const HttpClient::HttpHeader &headers) {
  HttpStreamFetcher::HttpHeaders out;
  out.reserve(headers.size());
  for (const auto &header : headers) {
    if (!header.first.empty()) {
      out.emplace_back(header.first, header.second);
    }
  }
  return out;
}

class HttpRequesterWithPoller : public HttpRequester {
public:
  using Ptr = std::shared_ptr<HttpRequesterWithPoller>;

  void setClientPoller(const EventPoller::Ptr &poller) {
    setPoller(poller);
  }

  void startWithTimeout(const std::string &url, float timeout_sec) {
    startRequester(url, nullptr, timeout_sec);
  }
};

class StreamingHttpRequester : public HttpRequesterWithPoller {
public:
  using Ptr = std::shared_ptr<StreamingHttpRequester>;
  using CompleteCB = std::function<void(const SockException &)>;

  void setStreamCallbacks(HttpStreamFetcher::OnChunk on_chunk,
                          HttpStreamFetcher::OnHeaders on_headers,
                          CompleteCB on_complete) {
    _on_chunk = std::move(on_chunk);
    _on_headers = std::move(on_headers);
    _on_complete = std::move(on_complete);
  }

  bool consumerOk() const {
    return _consumer_ok;
  }

  const std::string &consumerErr() const {
    return _consumer_err;
  }

  uint32_t responseStatusCode() const {
    return _status_code;
  }

  const HttpStreamFetcher::HttpHeaders &responseHeaders() const {
    return _headers;
  }

private:
  void onResponseHeader(const std::string &status,
                        const HttpHeader &headers) override {
    _status_code = static_cast<uint32_t>(std::strtoul(status.c_str(), nullptr, 10));
    _headers = convertHeaders(headers);
    if (_on_headers) {
      _on_headers(_status_code, _headers);
    }
  }

  void onResponseBody(const char *buf, size_t size) override {
    if (!_on_chunk || !buf || size == 0 || !_consumer_ok) {
      return;
    }
    if (!_on_chunk(reinterpret_cast<const uint8_t *>(buf), size)) {
      _consumer_ok = false;
      if (_consumer_err.empty()) {
        _consumer_err = "consume http payload failed";
      }
      shutdown(SockException(Err_shutdown, _consumer_err));
    }
  }

  void onResponseCompleted(const SockException &ex) override {
    if (_on_complete) {
      if (!_consumer_ok && !ex) {
        _on_complete(SockException(Err_shutdown, _consumer_err));
      } else {
        _on_complete(ex);
      }
    }
  }

private:
  HttpStreamFetcher::OnChunk _on_chunk;
  HttpStreamFetcher::OnHeaders _on_headers;
  CompleteCB _on_complete;
  bool _consumer_ok = true;
  std::string _consumer_err;
  uint32_t _status_code = 0;
  HttpStreamFetcher::HttpHeaders _headers;
};

struct RequestResult {
  std::mutex mtx;
  std::condition_variable cv;
  bool completed = false;
  SockException ex;
};

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
  const auto effective_headers = makeEffectiveHeaders(headers);
  auto poller = EventPollerPool::Instance().getPoller(false);
  auto requester = std::make_shared<StreamingHttpRequester>();
  RequestResult result;

  requester->setClientPoller(poller);
  requester->setMethod(method);
  requester->setAllowResendRequest(true);
  for (const auto &header : effective_headers) {
    requester->addHeader(header.first, header.second, true);
  }
  if (method == "POST") {
    requester->setBody(body);
  }
  requester->setStreamCallbacks(
      on_chunk, on_headers,
      [&result](const SockException &ex) {
        {
          std::lock_guard<std::mutex> lock(result.mtx);
          result.completed = true;
          result.ex = ex;
        }
        result.cv.notify_one();
      });
  poller->async([requester, url]() {
    requester->startWithTimeout(url, kDefaultHttpStreamTimeoutSec);
  }, false);

  {
    std::unique_lock<std::mutex> lock(result.mtx);
    result.cv.wait(lock, [&result]() {
      return result.completed;
    });
  }

  if (response_status_code) {
    *response_status_code = requester->responseStatusCode();
  }
  if (response_headers) {
    *response_headers = requester->responseHeaders();
  }

  if (!requester->consumerOk()) {
    if (err.empty()) {
      err = requester->consumerErr();
    }
    return false;
  }
  if (result.ex) {
    err = result.ex.what();
    return false;
  }
  const auto status_code = requester->responseStatusCode();
  if (status_code < 200 || status_code >= 300) {
    err = "http status " + std::to_string(status_code);
    return false;
  }
  return true;
}
