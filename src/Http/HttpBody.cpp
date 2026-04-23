/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include <atomic>
#include <csignal>
#include <sstream>
#include <tuple>

#ifndef _WIN32
#include <sys/mman.h>
#endif
#if defined(__linux__) || defined(__linux)
#include <sys/sendfile.h>
#endif

#include "Util/File.h"
#include "Util/NoticeCenter.h"
#include "Util/onceToken.h"
#include "Util/logger.h"
#include "Util/util.h"
#include "Util/uv_errno.h"

#include "Common/config.h"
#include "esfileferry/EsFileFerryPuller.h"
#include "esfileferry/EsFileFerryPlayer.h"
#include "HttpBody.h"
#include "HttpClient.h"
#include "HttpClientImp.h"
#include "Common/macros.h"

using namespace std;
using namespace toolkit;

namespace mediakit {

using HttpHeader = HttpClient::HttpHeader;

namespace {
const std::string kPlayChannelCompleteTimeoutMs = "http.play_channel_complete_timeout_ms";
static onceToken s_play_channel_timeout_token([]() {
    mINI::Instance()[kPlayChannelCompleteTimeoutMs] = 3600 * 1000;
});


std::string getHttpHeaderValue(const StrCaseMap &headers, const char *key) {
    auto it = headers.find(key);
    if (it == headers.end()) {
        return "";
    }
    return it->second;
}

int64_t getContentLengthOrUnknown(const StrCaseMap &headers) {
    auto value = getHttpHeaderValue(headers, "Content-Length");
    if (value.empty()) {
        return -1;
    }
    try {
        return std::stoll(value);
    } catch (...) {
        return -1;
    }
}

class HttpUrlClient : public HttpClientImp {
public:
    using Ptr = std::shared_ptr<HttpUrlClient>;
    using HeaderCB = std::function<void(int, const HttpHeader &)>;
    using BodyCB = std::function<void(const Buffer::Ptr &)>;
    using CompleteCB = std::function<void(const SockException &)>;

    void setHeaderCB(HeaderCB cb) {
        _on_header = std::move(cb);
    }

    void setBodyCB(BodyCB cb) {
        _on_body = std::move(cb);
    }

    void setCompleteCB(CompleteCB cb) {
        _on_complete = std::move(cb);
    }

    void setClientPoller(const EventPoller::Ptr &poller) {
        setPoller(poller);
    }

protected:
    void onResponseHeader(const std::string &status, const HttpHeader &headers) override {
        if (_on_header) {
            _on_header(atoi(status.data()), headers);
        }
    }

    void onResponseBody(const char *buf, size_t size) override {
        if (_on_body && size) {
            auto raw = BufferRaw::create();
            raw->assign(buf, size);
            _on_body(std::move(raw));
        }
    }

    void onResponseCompleted(const SockException &ex) override {
        if (_on_complete) {
            _on_complete(ex);
        }
    }

private:
    HeaderCB _on_header;
    BodyCB _on_body;
    CompleteCB _on_complete;
};
}

#if ENABLE_FERRY
namespace {
constexpr const char *kPlayChannelTaskIdHeader = "X-Play-Channel-Task-Id";

std::string trimPlayChannelCopy(std::string value) {
    size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        ++begin;
    }
    size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return value.substr(begin, end - begin);
}

bool parsePlayChannelResponseMetaPayload(const std::vector<uint8_t> &payload, int &status_code, HttpHeader &headers) {
    status_code = 200;
    headers.clear();
    std::string text(payload.begin(), payload.end());
    std::stringstream ss(text);
    std::string line;
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (start_with(line, ":status:")) {
            auto code_text = trimPlayChannelCopy(line.substr(std::string(":status:").size()));
            if (!code_text.empty()) {
                status_code = atoi(code_text.c_str());
                if (status_code <= 0) {
                    status_code = 200;
                }
            }
            continue;
        }
        auto pos = line.find(':');
        if (pos == std::string::npos) {
            continue;
        }
        auto key = trimPlayChannelCopy(line.substr(0, pos));
        auto value = trimPlayChannelCopy(line.substr(pos + 1));
        if (!key.empty()) {
            headers[key] = value;
        }
    }
    return !headers.empty() || status_code != 200;
}

void sanitizePlayChannelResponseHeaders(HttpHeader &headers) {
    for (auto it = headers.begin(); it != headers.end();) {
        auto key_lower = strToLower(it->first.data());
        if (key_lower == "connection" || key_lower == "keep-alive" || key_lower == "proxy-connection" ||
            key_lower == "te" || key_lower == "trailer" || key_lower == "upgrade" ||
            key_lower == "transfer-encoding") {
            it = headers.erase(it);
            continue;
        }
        ++it;
    }
}

std::string normalizePlayChannelSourceUrl(const std::string &url) {
    if (start_with(url, "rtp-http://")) {
        return "http://" + url.substr(sizeof("rtp-http://") - 1);
    }
    if (start_with(url, "rtp-https://")) {
        return "https://" + url.substr(sizeof("rtp-https://") - 1);
    }
    return url;
}

std::string resolvePlayChannelScheme(const std::string &url) {
    auto pos = url.find("://");
    if (pos == std::string::npos) {
        return "";
    }
    return url.substr(0, pos);
}

std::string makePlayChannelTaskId() {
    static atomic<uint64_t> s_task_index{0};
    return StrPrinter << "play_channel_" << ++s_task_index << "_" << getCurrentMillisecond();
}

class PlayChannelClientImp : public std::enable_shared_from_this<PlayChannelClientImp> {
public:
    using Ptr = std::shared_ptr<PlayChannelClientImp>;
    using HeaderCB = std::function<void(int, const HttpHeader &)>;
    using BodyCB = std::function<void(const Buffer::Ptr &)>;
    using CompleteCB = std::function<void(const SockException &)>;

    ~PlayChannelClientImp() {
        cleanupTask();
    }

    void setHeaderCB(HeaderCB cb) {
        _on_header = std::move(cb);
    }

    void setBodyCB(BodyCB cb) {
        _on_body = std::move(cb);
    }

    void setCompleteCB(CompleteCB cb) {
        _on_complete = std::move(cb);
    }

    void setClientPoller(const EventPoller::Ptr &poller) {
        _poller = poller;
    }

    void setMethod(std::string method) {
        _method = std::move(method);
    }

    void setCompleteTimeout(uint64_t timeout_ms) {
        _timeout_ms = timeout_ms;
    }

    void cancel(const SockException &ex) {
        auto self = shared_from_this();
        if (_poller) {
            _poller->async([self, ex]() {
                self->finish(ex);
            }, false);
            return;
        }
        finish(ex);
    }

    void start(const std::string &url, const StrCaseMap &request_header) {
        if (!_poller) {
            _poller = EventPollerPool::Instance().getPoller(false);
        }
        _ctx.url = url;
        _ctx.source_url = normalizePlayChannelSourceUrl(url);
        _ctx.scheme = resolvePlayChannelScheme(url);
        _ctx.method = _method.empty() ? "GET" : _method;
        _ctx.request_header = request_header;
        _ctx.timeout_ms = _timeout_ms;
        _ctx.task_id = makePlayChannelTaskId();
        _task_id = _ctx.task_id;
        InfoL << "play channel start register callback, task_id:" << _task_id
              << " url:" << _ctx.url
              << " source_url:" << _ctx.source_url
              << " timeout_ms:" << _ctx.timeout_ms;

        auto &unpacker = EsFileFerryUnPacker::Instance();
        std::weak_ptr<PlayChannelClientImp> weak_self = shared_from_this();
        unpacker.setTaskCallback(_task_id, [weak_self](const EsTaskDataEvent &event) {
            auto self = weak_self.lock();
            if (event.type == EsFilePacketType::FileInfo) {
                DebugL << "play channel callback recv file info, event_task_id:" << event.task_id
                       << " event_seq:" << event.seq
                       << " payload_len:" << event.payload.size()
                       << " file_size:" << event.file_size
                       << " completed:" << event.completed
                       << " has_self:" << !!self
                       << " self_task_id:" << (self ? self->_task_id : std::string())
                       << " has_poller:" << (self && self->_poller);
            }
            if (!self || !self->_poller) {
                WarnL << "play channel task callback dropped, task_id:" << event.task_id
                      << " type:" << static_cast<int>(event.type)
                      << " has_self:" << !!self
                      << " has_poller:" << (self && self->_poller);
                return;
            }
            self->_poller->async([self, event]() {
                if (event.type == EsFilePacketType::FileInfo) {
                    DebugL << "play channel async handle file info, task_id:" << self->_task_id
                           << " event_task_id:" << event.task_id
                           << " event_seq:" << event.seq
                           << " payload_len:" << event.payload.size()
                           << " completed:" << event.completed;
                }
                self->handleTaskEvent(event);
            }, false);
        });
        _task_registered = true;
        InfoL << "play channel emit add event, task_id:" << _task_id
              << " source_url:" << _ctx.source_url;
        NoticeCenter::Instance().emitEvent(Broadcast::kBroadcastPlayChannelTaskEvent, _ctx, std::string("add"));
        scheduleTimeout();
    }

private:
    static constexpr uint64_t kInitialHeaderTimeoutMs = 10 * 1000;

    HttpHeader buildDebugResponseHeader(HttpHeader header) const {
        if (!_task_id.empty()) {
            header[kPlayChannelTaskIdHeader] = _task_id;
        }
        return header;
    }

    void markTaskActivity(const EsTaskDataEvent &event) {
        _last_activity_ms.store(getCurrentMillisecond());
        if (!_saw_task_event.exchange(true)) {
            InfoL << "play channel first task event observed, task_id:" << _task_id
                  << " event_type:" << EsFilePacketTypeToString(event.type)
                  << " payload_len:" << event.payload.size()
                  << " completed:" << event.completed << " seq:" << event.seq;
        }
    }

    uint64_t currentTimeoutWindow() const {
        if (!_saw_task_event.load() && !_received_file_info) {
            return std::max<uint64_t>(_timeout_ms, kInitialHeaderTimeoutMs);
        }
        return _timeout_ms;
    }

    void scheduleTimeout() {
        if (!_poller || !_timeout_ms) {
            return;
        }
        _last_activity_ms.store(getCurrentMillisecond());
        std::weak_ptr<PlayChannelClientImp> weak_self = shared_from_this();
        _poller->doDelayTask(currentTimeoutWindow(), [weak_self]() -> uint64_t {
            auto self = weak_self.lock();
            if (!self || self->_completed) {
                return 0;
            }
            auto timeout_window = self->currentTimeoutWindow();
            auto last_activity_ms = self->_last_activity_ms.load();
            auto now_ms = getCurrentMillisecond();
            auto idle_ms = now_ms > last_activity_ms ? now_ms - last_activity_ms : 0;
            if (idle_ms < timeout_window) {
                return timeout_window - idle_ms;
            }
            self->emitFailure(504, "play channel request timeout", SockException(Err_timeout, "play channel request timeout"));
            return 0;
        });
    }

    void handleTaskEvent(const EsTaskDataEvent &event) {
        if (_completed || event.task_id != _task_id) {
            return;
        }
        markTaskActivity(event);
        switch (event.type) {
        case EsFilePacketType::TaskStatus: {
            std::string message = event.status.empty() ? "play channel task status error" : event.status;
            emitFailure(503, message, SockException(Err_other, message));
            break;
        }
        case EsFilePacketType::FileInfo: {
            _received_file_info = true;
            int response_code = 200;
            HttpHeader response_header;
            if ((event.flags & kEsFileFlagFileInfoHasHttpResponseHeaders) != 0 && !event.payload.empty()) {
                parsePlayChannelResponseMetaPayload(event.payload, response_code, response_header);
                sanitizePlayChannelResponseHeaders(response_header);
            }
            ensureHeaderReady(response_code, response_header);
            if (event.completed) {
                finish(SockException(Err_success, "play channel complete"));
            }
            break;
        }
        case EsFilePacketType::FileChunk:
        case EsFilePacketType::FileEnd: {
            auto terminal_event = event.type == EsFilePacketType::FileEnd || event.completed;
            if (!_header_emitted) {
                WarnL << "play channel chunk arrived before file info, task_id:" << _task_id
                      << " event_type:" << EsFilePacketTypeToString(event.type)
                      << " payload_len:" << event.payload.size()
                      << " completed:" << event.completed;
            }
            if (!_received_file_info) {
                if (terminal_event) {
                    emitFailure(503, "play channel response header missing",
                                SockException(Err_other, "play channel response header missing"));
                    break;
                }
                emitPayload(event.payload);
                break;
            }
            ensureHeaderReady(_response_code, _response_header);
            emitPayload(event.payload);
            if (event.type == EsFilePacketType::FileChunk && event.file_size > 0 &&
                event.received_size >= event.file_size) {
                terminal_event = true;
            }
            if (terminal_event) {
                finish(SockException(Err_success, "play channel complete"));
            }
            break;
        }
        default:
            if (event.completed) {
                if (!_received_file_info && !_header_emitted) {
                    emitFailure(503, "play channel response header missing", SockException(Err_other, "play channel response header missing"));
                } else {
                    ensureHeaderReady(_response_code, _response_header);
                    finish(SockException(Err_success, "play channel complete"));
                }
            }
            break;
        }
    }

    void ensureHeaderReady(int code, const HttpHeader &header) {
        if (_header_emitted) {
            return;
        }
        _response_code = code > 0 ? code : 200;
        _response_header = buildDebugResponseHeader(header);
        InfoL << "play channel emit response header, task_id:" << _task_id
              << " response_code:" << _response_code
              << " content_type:" << getHttpHeaderValue(_response_header, "Content-Type")
              << " content_length:" << getHttpHeaderValue(_response_header, "Content-Length")
              << " content_range:" << getHttpHeaderValue(_response_header, "Content-Range")
              << " debug_task_id:" << getHttpHeaderValue(_response_header, kPlayChannelTaskIdHeader);
        _header_emitted = true;
        if (_on_header) {
            _on_header(_response_code, _response_header);
        }
        flushPendingBody();
    }

    void emitPayload(const std::vector<uint8_t> &payload) {
        if (payload.empty()) {
            return;
        }
        auto raw = BufferRaw::create();
        raw->assign((const char *)payload.data(), payload.size());
        emitBody(raw);
    }

    void emitPayload(const std::string &payload) {
        if (payload.empty()) {
            return;
        }
        auto raw = BufferRaw::create();
        raw->assign(payload.data(), payload.size());
        emitBody(raw);
    }

    void emitBody(const Buffer::Ptr &buf) {
        if (!buf) {
            return;
        }
        if (!_header_emitted) {
            if (!_pending_body.empty()) {
                WarnL << "replace play channel pending body before header, task_id:" << _task_id
                      << " old_count:" << _pending_body.size()
                      << " new_size:" << buf->size();
                _pending_body.clear();
            }
            _pending_body.emplace_back(buf);
            return;
        }
        if (_on_body) {
            _on_body(buf);
        }
    }

    void flushPendingBody() {
        if (!_header_emitted || _pending_body.empty()) {
            return;
        }
        auto pending = std::move(_pending_body);
        _pending_body.clear();
        if (!_on_body) {
            return;
        }
        for (auto &buf : pending) {
            _on_body(buf);
        }
    }

    void emitFailure(int http_code, const std::string &message, const SockException &ex) {
        if (_completed) {
            return;
        }
        HttpHeader header;
        if (!message.empty()) {
            header["Content-Type"] = "text/plain; charset=utf-8";
        }
        ensureHeaderReady(http_code, header);
        emitPayload(message);
        finish(ex);
    }

    void finish(const SockException &ex) {
        if (_completed.exchange(true)) {
            return;
        }
        cleanupTask();
        if (_on_complete) {
            _on_complete(ex);
        }
    }

    void cleanupTask() {
        if (!_task_registered.exchange(false) || _task_id.empty()) {
            return;
        }
        InfoL << "play channel cleanup task, task_id:" << _task_id
              << " completed:" << _completed.load()
              << " header_emitted:" << _header_emitted;
        _pending_body.clear();
        NoticeCenter::Instance().emitEvent(Broadcast::kBroadcastPlayChannelTaskEvent, _ctx, std::string("del"));
        EsFileFerryPuller::Instance().removeTaskFrames(_task_id);
        EsFileFerryUnPacker::Instance().removeTask(_task_id);
    }

    HeaderCB _on_header;
    BodyCB _on_body;
    CompleteCB _on_complete;
    EventPoller::Ptr _poller;
    PlayChannelRequestContext _ctx;
    std::string _method = "GET";
    uint64_t _timeout_ms = 10 * 1000;
    std::string _task_id;
    int _response_code = 200;
    HttpHeader _response_header;
    bool _header_emitted = false;
    std::deque<Buffer::Ptr> _pending_body;
    std::atomic_bool _task_registered{false};
    std::atomic_bool _completed{false};
    std::atomic<uint64_t> _last_activity_ms{0};
    std::atomic_bool _saw_task_event{false};
    bool _received_file_info = false;
};

constexpr uint64_t PlayChannelClientImp::kInitialHeaderTimeoutMs;
}
#endif

HttpStringBody::HttpStringBody(string str) {
    _str = std::move(str);
}

int64_t HttpStringBody::remainSize() {
    return _str.size() - _offset;
}

Buffer::Ptr HttpStringBody::readData(size_t size) {
    size = MIN((size_t)remainSize(), size);
    if (!size) {
        // 没有剩余字节了  [AUTO-TRANSLATED:7bbaa343]
        // No remaining bytes
        return nullptr;
    }
    auto ret = std::make_shared<BufferString>(_str, _offset, size);
    _offset += size;
    return ret;
}

//////////////////////////////////////////////////////////////////
static mutex s_mtx;
static unordered_map<string /*file_path*/, std::tuple<char */*ptr*/, int64_t /*size*/, weak_ptr<char> /*mmap*/ > > s_shared_mmap;

#if defined(_WIN32)
static void mmap_close(HANDLE _hfile, HANDLE _hmapping, void *_addr) {
    if (_addr) {
        ::UnmapViewOfFile(_addr);
    }

    if (_hmapping) {
        ::CloseHandle(_hmapping);
    }

    if (_hfile != INVALID_HANDLE_VALUE) {
        ::CloseHandle(_hfile);
    }
}
#endif

// 删除mmap记录  [AUTO-TRANSLATED:c956201d]
// Delete mmap record
static void delSharedMmap(const string &file_path, char *ptr) {
    lock_guard<mutex> lck(s_mtx);
    auto it = s_shared_mmap.find(file_path);
    if (it != s_shared_mmap.end() && std::get<0>(it->second) == ptr) {
        s_shared_mmap.erase(it);
    }
}

static std::shared_ptr<char> getSharedMmap(const string &file_path, int64_t &file_size) {
    {
        lock_guard<mutex> lck(s_mtx);
        auto it = s_shared_mmap.find(file_path);
        if (it != s_shared_mmap.end()) {
            auto ret = std::get<2>(it->second).lock();
            if (ret) {
                // 命中mmap缓存  [AUTO-TRANSLATED:95131a66]
                // Hit mmap cache
                file_size = std::get<1>(it->second);
                return ret;
            }
        }
    }

    // 打开文件  [AUTO-TRANSLATED:55bfe68a]
    // Open file
    std::shared_ptr<FILE> fp(fopen(file_path.data(), "rb"), [](FILE *fp) {
        if (fp) {
            fclose(fp);
        }
    });
    if (!fp) {
        // 文件不存在  [AUTO-TRANSLATED:ed160bcf]
        // File does not exist
        file_size = -1;
        return nullptr;
    }


#if defined(_WIN32)
    auto fd = _fileno(fp.get());
#else
    // 获取文件大小  [AUTO-TRANSLATED:82974eea]
    // Get file size
    file_size = File::fileSize(fp.get());
    auto fd = fileno(fp.get());
#endif

    if (fd < 0) {
        WarnL << "fileno failed:" << get_uv_errmsg(false);
        return nullptr;
    }
#ifndef _WIN32
    auto ptr = (char *)mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        WarnL << "mmap " << file_path << " failed:" << get_uv_errmsg(false);
        return nullptr;
    }


    std::shared_ptr<char> ret(ptr, [file_size, fp, file_path](char *ptr) {
        munmap(ptr, file_size);
        delSharedMmap(file_path, ptr);
    });

#else
    auto hfile = ::CreateFileA(file_path.data(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

    if (hfile == INVALID_HANDLE_VALUE) {
        WarnL << "CreateFileA() " << file_path << " failed:";
        return nullptr;
    }

     LARGE_INTEGER FileSize; 
     GetFileSizeEx(hfile, &FileSize); //GetFileSize函数的拓展，可用于获取大于4G的文件大小
     file_size = FileSize.QuadPart;

    auto hmapping = ::CreateFileMapping(hfile, NULL, PAGE_READONLY, 0, 0, NULL);

    if (hmapping == NULL) {
        mmap_close(hfile, NULL, NULL);
        WarnL << "CreateFileMapping() " << file_path << " failed:";
        return nullptr;
    }

    auto addr_ = ::MapViewOfFile(hmapping, FILE_MAP_READ, 0, 0, 0);

    if (addr_ == nullptr) {
        mmap_close(hfile, hmapping, addr_);
		WarnL << "MapViewOfFile() " << file_path << " failed:";
        return nullptr;
    }

    std::shared_ptr<char> ret((char *)(addr_), [hfile, hmapping, file_path](char *addr_) {
        mmap_close(hfile, hmapping, addr_);
        delSharedMmap(file_path, addr_);
    });

#endif


#if 0
    if (file_size < 10 * 1024 * 1024 && file_path.rfind(".ts") != string::npos) {
        // 如果是小ts文件，那么尝试先加载到内存  [AUTO-TRANSLATED:0d96c5cd]
        // If it is a small ts file, try to load it into memory first
        auto buf = BufferRaw::create();
        buf->assign(ret.get(), file_size);
        ret.reset(buf->data(), [buf, file_path](char *ptr) {
            delSharedMmap(file_path, ptr);
        });
    }
#endif
    {
        lock_guard<mutex> lck(s_mtx);
        s_shared_mmap[file_path] = std::make_tuple(ret.get(), file_size, ret);
    }
    return ret;
}

HttpFileBody::HttpFileBody(const string &file_path, bool use_mmap) {
    if (use_mmap ) {
        _map_addr = getSharedMmap(file_path, _read_to);       
    }

    if (!_map_addr && _read_to != -1) {
        // mmap失败(且不是由于文件不存在导致的)或未执行mmap时，才进入fread逻辑分支  [AUTO-TRANSLATED:8c7efed5]
        // Only enter the fread logic branch when mmap fails (and is not due to file not existing) or when mmap is not executed
        _fp.reset(fopen(file_path.data(), "rb"), [](FILE *fp) {
            if (fp) {
                fclose(fp);
            }
        });
        if (!_fp) {
            // 文件不存在  [AUTO-TRANSLATED:ed160bcf]
            // File does not exist
            _read_to = -1;
            return;
        }
        if (!_read_to) {
            // _read_to等于0时，说明还未尝试获取文件大小  [AUTO-TRANSLATED:4e3ef6ca]
            // When _read_to equals 0, it means that the file size has not been attempted to be obtained yet
            // 加上该判断逻辑，在mmap失败时，可以省去一次该操作  [AUTO-TRANSLATED:b9b585de]
            // Adding this judgment logic can save one operation when mmap fails
            _read_to = File::fileSize(_fp.get());
        }
    }
}

void HttpFileBody::setRange(uint64_t offset, uint64_t max_size) {
    CHECK((int64_t)offset <= _read_to && (int64_t)(max_size + offset) <= _read_to);
    _read_to = max_size + offset;
    _file_offset = offset;
    if (_fp && !_map_addr) {
        fseek64(_fp.get(), _file_offset, SEEK_SET);
    }
}

int HttpFileBody::sendFile(int fd) {
#if defined(__linux__) || defined(__linux)
    if (!_fp) {
        return -1;
    }
    static onceToken s_token([]() { signal(SIGPIPE, SIG_IGN); });
    off_t off = _file_offset;
    return sendfile(fd, fileno(_fp.get()), &off, _read_to - _file_offset);
#else
    return -1;
#endif
}

class BufferMmap : public Buffer {
public:
    using Ptr = std::shared_ptr<BufferMmap>;
    BufferMmap(const std::shared_ptr<char> &map_addr, size_t offset, size_t size) {
        _map_addr = map_addr;
        _data = map_addr.get() + offset;
        _size = size;
    }
    // 返回数据长度  [AUTO-TRANSLATED:955f731c]
    // Return data length
    char *data() const override { return _data; }
    size_t size() const override { return _size; }

private:
    char *_data;
    size_t _size;
    std::shared_ptr<char> _map_addr;
};

int64_t HttpFileBody::remainSize() {
    return _read_to - _file_offset;
}

Buffer::Ptr HttpFileBody::readData(size_t size) {
    size = (size_t)(MIN(remainSize(), (int64_t)size));
    if (!size) {
        // 没有剩余字节了  [AUTO-TRANSLATED:7bbaa343]
        // No remaining bytes
        return nullptr;
    }
    if (!_map_addr) {
        // fread模式  [AUTO-TRANSLATED:c4dee2a3]
        // fread mode
        ssize_t iRead;
        auto ret = _pool.obtain2();
        ret->setCapacity(size + 1);
        do {
            iRead = fread(ret->data(), 1, size, _fp.get());
        } while (-1 == iRead && UV_EINTR == get_uv_error(false));

        if (iRead > 0) {
            // 读到数据了  [AUTO-TRANSLATED:7e5ada62]
            // Data is read
            ret->setSize(iRead);
            _file_offset += iRead;
            return std::move(ret);
        }
        // 读取文件异常，文件真实长度小于声明长度  [AUTO-TRANSLATED:89d09f9b]
        // File reading exception, the actual length of the file is less than the declared length
        _file_offset = _read_to;
        WarnL << "read file err:" << get_uv_errmsg();
        return nullptr;
    }

    // mmap模式  [AUTO-TRANSLATED:b8d616f1]
    // mmap mode
    auto ret = std::make_shared<BufferMmap>(_map_addr, _file_offset, size);
    _file_offset += size;
    return ret;
}

HttpUrlBody::HttpUrlBody(const std::string &url, const StrCaseMap &request_header) {
    GET_CONFIG(uint64_t, timeout_ms, kPlayChannelCompleteTimeoutMs);

    _url = url;
    _request_header = request_header;
    _alive_token = std::make_shared<char>(0);
    std::weak_ptr<void> weak_alive = _alive_token;
    auto self = this;
    auto client = std::make_shared<HttpUrlClient>();
    _client_holder = client;
    client->setClientPoller(EventPollerPool::Instance().getPoller(false));
    client->setMethod("GET");
    client->setCompleteTimeout(timeout_ms);
    client->setHeaderCB([weak_alive, self](int code, const HttpClient::HttpHeader &headers) {
        if (weak_alive.expired()) {
            return;
        }
        HttpUrlBody::HeaderReadyCB cb;
        int response_code = 200;
        StrCaseMap response_header;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            self->_response_code = code > 0 ? code : 200;
            self->_response_header = headers;
            self->_header_ready = true;
            if (!self->_header_cb_fired) {
                self->_header_cb_fired = true;
                cb = self->_header_ready_cb;
                response_code = self->_response_code;
                response_header = self->_response_header;
            }
        }
        self->_header_cv.notify_all();
        if (cb) {
            cb(response_code, response_header);
        }
    });
    client->setBodyCB([weak_alive, self](const Buffer::Ptr &buf) {
        if (weak_alive.expired() || !buf) {
            return;
        }
        std::function<void(const Buffer::Ptr &)> cb;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            if (self->_wait_cb) {
                cb = std::move(self->_wait_cb);
                self->_wait_cb = nullptr;
            } else {
                self->_cache.emplace_back(buf);
                return;
            }
        }
        cb(buf);
    });
    client->setCompleteCB([weak_alive, self](const SockException &ex) {
        if (weak_alive.expired()) {
            return;
        }
        std::function<void(const Buffer::Ptr &)> cb;
        HttpUrlBody::HeaderReadyCB header_cb;
        int response_code = 200;
        StrCaseMap response_header;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            self->_completed = true;
            self->_success = !ex;
            if (!self->_header_ready && ex) {
                self->_response_code = 503;
            }
            if (self->_wait_cb && self->_cache.empty()) {
                cb = std::move(self->_wait_cb);
                self->_wait_cb = nullptr;
            }
            if (!self->_header_cb_fired) {
                self->_header_cb_fired = true;
                header_cb = self->_header_ready_cb;
                response_code = self->_response_code;
                response_header = self->_response_header;
            }
        }
        self->_header_cv.notify_all();
        if (header_cb) {
            header_cb(response_code, response_header);
        }
        if (cb) {
            cb(nullptr);
        }
    });
    client->setClientPoller(EventPollerPool::Instance().getPoller(false));
    client->sendRequest(_url);
}

void HttpUrlBody::setHeaderReadyCB(HeaderReadyCB cb) {
    int code = 200;
    StrCaseMap header;
    bool do_cb = false;
    {
        std::lock_guard<std::mutex> lck(_mtx);
        if (_header_cb_fired) {
            do_cb = true;
            code = _response_code;
            header = _response_header;
        } else {
            _header_ready_cb = std::move(cb);
        }
    }
    if (do_cb && cb) {
        cb(code, header);
    }
}

void HttpUrlBody::waitHeaderReady() const {
    std::unique_lock<std::mutex> lck(_mtx);
    _header_cv.wait(lck, [this]() {
        return _header_ready || _completed;
    });
}

int64_t HttpUrlBody::remainSize() {
    return getContentLengthOrUnknown(responseHeader());
}

Buffer::Ptr HttpUrlBody::readData(size_t size) {
    std::lock_guard<std::mutex> lck(_mtx);
    if (_cache.empty()) {
        return nullptr;
    }
    auto ret = _cache.front();
    _cache.pop_front();
    return ret;
}

void HttpUrlBody::readDataAsync(size_t size, const std::function<void(const toolkit::Buffer::Ptr &buf)> &cb) {
    Buffer::Ptr ret;
    bool completed = false;
    {
        std::lock_guard<std::mutex> lck(_mtx);
        if (!_cache.empty()) {
            ret = _cache.front();
            _cache.pop_front();
        } else if (_completed) {
            completed = true;
        } else {
            _wait_cb = cb;
            return;
        }
    }
    if (ret) {
        cb(ret);
    } else if (completed) {
        cb(nullptr);
    }
}

int HttpUrlBody::responseCode() const {
    waitHeaderReady();
    std::lock_guard<std::mutex> lck(_mtx);
    return _response_code;
}

StrCaseMap HttpUrlBody::responseHeader() const {
    waitHeaderReady();
    std::lock_guard<std::mutex> lck(_mtx);
    return _response_header;
}

bool HttpUrlBody::requestCompleted() const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _completed;
}

bool HttpUrlBody::requestSuccess() const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _success;
}

#if ENABLE_FERRY
// play-channel-url
PlayChannelUrlBody::PlayChannelUrlBody(const std::string &url,
                                       const StrCaseMap &request_header,
                                       const toolkit::EventPoller::Ptr &poller) {
    GET_CONFIG(uint64_t, timeout_ms, kPlayChannelCompleteTimeoutMs);
    _url = url;
    _request_header = request_header;
    _alive_token = std::make_shared<char>(0);
    std::weak_ptr<void> weak_alive = _alive_token;
    auto self = this;
    auto client = std::make_shared<PlayChannelClientImp>();
    _client_holder = client;
    _on_connection_closed = [weak_alive, client]() {
        if (weak_alive.expired()) {
            return;
        }
        client->cancel(SockException(Err_shutdown, "http session disconnected"));
    };
    client->setClientPoller(poller ? poller : EventPollerPool::Instance().getPoller(false));
    client->setMethod("GET");
    client->setCompleteTimeout(timeout_ms);
    client->setHeaderCB([weak_alive, self](int code, const HttpClient::HttpHeader &headers) {
        if (weak_alive.expired()) {
            return;
        }
        HttpUrlBody::HeaderReadyCB cb;
        int response_code = 200;
        StrCaseMap response_header;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            self->_response_code = code > 0 ? code : 200;
            self->_response_header = headers;
            self->_header_ready = true;
            if (!self->_header_cb_fired) {
                self->_header_cb_fired = true;
                cb = self->_header_ready_cb;
                response_code = self->_response_code;
                response_header = self->_response_header;
            }
        }
        self->_header_cv.notify_all();
        if (cb) {
            cb(response_code, response_header);
        }
    });
    client->setBodyCB([weak_alive, self](const Buffer::Ptr &buf) {
        if (weak_alive.expired() || !buf) {
            return;
        }
        std::function<void(const Buffer::Ptr &)> cb;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            if (self->_wait_cb) {
                cb = std::move(self->_wait_cb);
                self->_wait_cb = nullptr;
            } else {
                self->_cache.emplace_back(buf);
                return;
            }
        }
        cb(buf);
    });
    client->setCompleteCB([weak_alive, self](const SockException &ex) {
        if (weak_alive.expired()) {
            return;
        }
        std::function<void(const Buffer::Ptr &)> cb;
        HttpUrlBody::HeaderReadyCB header_cb;
        int response_code = 200;
        StrCaseMap response_header;
        {
            std::lock_guard<std::mutex> lck(self->_mtx);
            self->_completed = true;
            self->_success = !ex;
            if (!self->_header_ready && ex) {
                self->_response_code = 503;
            }
            if (self->_wait_cb && self->_cache.empty()) {
                cb = std::move(self->_wait_cb);
                self->_wait_cb = nullptr;
            }
            if (!self->_header_cb_fired) {
                self->_header_cb_fired = true;
                header_cb = self->_header_ready_cb;
                response_code = self->_response_code;
                response_header = self->_response_header;
            }
        }
        self->_header_cv.notify_all();
        if (header_cb) {
            header_cb(response_code, response_header);
        }
        if (cb) {
            cb(nullptr);
        }
    });
    client->start(_url, _request_header);
}

void PlayChannelUrlBody::setHeaderReadyCB(HeaderReadyCB cb) {
    int code = 200;
    StrCaseMap header;
    bool do_cb = false;
    {
        std::lock_guard<std::mutex> lck(_mtx);
        if (_header_cb_fired) {
            do_cb = true;
            code = _response_code;
            header = _response_header;
        } else {
            _header_ready_cb = std::move(cb);
        }
    }
    if (do_cb && cb) {
        cb(code, header);
    }
}

void PlayChannelUrlBody::waitHeaderReady() const {
    std::unique_lock<std::mutex> lck(_mtx);
    _header_cv.wait(lck, [this]() {
        return _header_ready || _completed;
    });
}

int64_t PlayChannelUrlBody::remainSize() {
    return getContentLengthOrUnknown(responseHeader());
}

Buffer::Ptr PlayChannelUrlBody::readData(size_t size) {
    std::lock_guard<std::mutex> lck(_mtx);
    if (_cache.empty()) {
        return nullptr;
    }
    auto ret = _cache.front();
    _cache.pop_front();
    return ret;
}

void PlayChannelUrlBody::readDataAsync(size_t size, const std::function<void(const toolkit::Buffer::Ptr &buf)> &cb) {
    Buffer::Ptr ret;
    bool completed = false;
    {
        std::lock_guard<std::mutex> lck(_mtx);
        if (!_cache.empty()) {
            ret = _cache.front();
            _cache.pop_front();
        } else if (_completed) {
            completed = true;
        } else {
            _wait_cb = cb;
            return;
        }
    }
    if (ret) {
        cb(ret);
    } else if (completed) {
        cb(nullptr);
    }
}

int PlayChannelUrlBody::responseCode() const {
    waitHeaderReady();
    std::lock_guard<std::mutex> lck(_mtx);
    return _response_code;
}

StrCaseMap PlayChannelUrlBody::responseHeader() const {
    waitHeaderReady();
    std::lock_guard<std::mutex> lck(_mtx);
    return _response_header;
}

bool PlayChannelUrlBody::requestCompleted() const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _completed;
}

bool PlayChannelUrlBody::requestSuccess() const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _success;
}

void PlayChannelUrlBody::onConnectionClosed() {
    std::function<void()> cb;
    {
        std::lock_guard<std::mutex> lck(_mtx);
        if (_completed) {
            return;
        }
        cb = _on_connection_closed;
    }
    if (cb) {
        WarnL << "play channel body cancel on http session disconnect, url:" << _url;
        cb();
    }
}
#endif
//////////////////////////////////////////////////////////////////

HttpMultiFormBody::HttpMultiFormBody(const HttpArgs &args, const string &filePath, const string &boundary) {
    _fileBody = std::make_shared<HttpFileBody>(filePath);
    if (_fileBody->remainSize() < 0) {
        throw std::invalid_argument(StrPrinter << "open file failed：" << filePath << " " << get_uv_errmsg());
    }

    auto fileName = filePath;
    auto pos = filePath.rfind('/');
    if (pos != string::npos) {
        fileName = filePath.substr(pos + 1);
    }
    _bodyPrefix = multiFormBodyPrefix(args, boundary, fileName);
    _bodySuffix = multiFormBodySuffix(boundary);
    _totalSize = _bodyPrefix.size() + _bodySuffix.size() + _fileBody->remainSize();
}

int64_t HttpMultiFormBody::remainSize() {
    return _totalSize - _offset;
}

Buffer::Ptr HttpMultiFormBody::readData(size_t size) {
    if (_bodyPrefix.size()) {
        auto ret = std::make_shared<BufferString>(_bodyPrefix);
        _offset += _bodyPrefix.size();
        _bodyPrefix.clear();
        return ret;
    }

    if (_fileBody->remainSize()) {
        auto ret = _fileBody->readData(size);
        if (!ret) {
            // 读取文件出现异常，提前中断  [AUTO-TRANSLATED:5b8052d9]
            // An exception occurred while reading the file, and the process was interrupted prematurely
            _offset = _totalSize;
        } else {
            _offset += ret->size();
        }
        return ret;
    }

    if (_bodySuffix.size()) {
        auto ret = std::make_shared<BufferString>(_bodySuffix);
        _offset = _totalSize;
        _bodySuffix.clear();
        return ret;
    }

    return nullptr;
}

string HttpMultiFormBody::multiFormBodySuffix(const string &boundary) {
    return "\r\n--" + boundary + "--";
}

string HttpMultiFormBody::multiFormContentType(const string &boundary) {
    return StrPrinter << "multipart/form-data; boundary=" << boundary;
}

string HttpMultiFormBody::multiFormBodyPrefix(const HttpArgs &args, const string &boundary, const string &fileName) {
    string MPboundary = string("--") + boundary;
    _StrPrinter body;
    for (auto &pr : args) {
        body << MPboundary << "\r\n";
        body << "Content-Disposition: form-data; name=\"" << pr.first << "\"\r\n\r\n";
        body << pr.second << "\r\n";
    }
    body << MPboundary << "\r\n";
    body << "Content-Disposition: form-data; name=\""
         << "file"
         << "\"; filename=\"" << fileName << "\"\r\n";
    body << "Content-Type: application/octet-stream\r\n\r\n";
    return std::move(body);
}

HttpBufferBody::HttpBufferBody(Buffer::Ptr buffer) {
    _buffer = std::move(buffer);
}

int64_t HttpBufferBody::remainSize() {
    return _buffer ? _buffer->size() : 0;
}

Buffer::Ptr HttpBufferBody::readData(size_t size) {
    return Buffer::Ptr(std::move(_buffer));
}

} // namespace mediakit
