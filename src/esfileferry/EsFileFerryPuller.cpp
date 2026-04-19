#include "EsFileFerryPuller.h"
#include "EsFileFerryPlayer.h"
#include "Common/config.h"
#include "Thread/WorkThreadPool.h"

using namespace mediakit;
using namespace toolkit;

namespace {
constexpr size_t kMaxPendingFrameBytesSoftLimit = 32 * 1024 * 1024;
constexpr size_t kMaxPendingFrameBytesHardLimit = 64 * 1024 * 1024;
constexpr int64_t kQueueSpaceWaitStepMs = 20;
constexpr int64_t kQueueSpaceWaitSlowLogMs = 1000;
constexpr const char *kUnknownTaskBucketKey = "__unknown__";

struct FramePacketMeta {
    bool valid = false;
    std::string task_id;
    EsFilePacketType type = EsFilePacketType::Unknown;
    uint32_t seq = 0;
};

FramePacketMeta tryDecodeFramePacketMeta(const Frame::Ptr &frame) {
    FramePacketMeta meta;
    if (!frame || !frame->data() || frame->size() == 0) {
        return meta;
    }
    const auto *data =
        reinterpret_cast<const uint8_t *>(frame->data());
    const auto size = static_cast<size_t>(frame->size());
    if (!HasEsFileCarrierPrefix(data, size) ||
        size < kEsFileCarrierPrefixSize + kEsFileFixedHeaderSize) {
        return meta;
    }

    EsFilePacketHeader header;
    if (!DecodeEsFilePacketHeader(data + kEsFileCarrierPrefixSize,
                                  size - kEsFileCarrierPrefixSize, header)) {
        return meta;
    }

    const auto payload_offset =
        kEsFileCarrierPrefixSize + kEsFileFixedHeaderSize;
    const auto task_end =
        payload_offset + static_cast<size_t>(header.task_id_len);
    if (task_end > size) {
        return meta;
    }

    meta.valid = true;
    meta.type = header.type;
    meta.seq = header.seq;
    meta.task_id.assign(reinterpret_cast<const char *>(data + payload_offset),
                        header.task_id_len);
    return meta;
}

std::string packetTypeToString(int packetType) {
    return EsFilePacketTypeToString(static_cast<EsFilePacketType>(packetType));
}

std::string bucketKeyForMeta(const EsFileFerryPuller::PendingFrameMeta &meta) {
    if (meta.valid && !meta.task_id.empty()) {
        return meta.task_id;
    }
    return kUnknownTaskBucketKey;
}
}

EsFileFerryPuller &EsFileFerryPuller::Instance() {
    static std::shared_ptr<EsFileFerryPuller> instance(new EsFileFerryPuller());
    static EsFileFerryPuller &ref = *instance;
    return ref;
}

bool EsFileFerryPuller::startPull(const std::string &url, int rtp_type) {
  if (url.empty()) {
    OnError cb;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      _last_error = "pull url is empty";
      cb = _on_error;
    }
    if (cb) {
      cb("pull url is empty");
    }
    return false;
  }

  stopPull();
  startUnpackWorker();
  DebugL << "startPull, url:" << url << " rtp_type:" << rtp_type;
  auto poller = WorkThreadPool::Instance().getPoller();
  MediaPlayer::Ptr player = std::make_shared<MediaPlayer>(poller);
  std::weak_ptr<MediaPlayer> weak_player = player;
  player->setOnPlayResult([this](const SockException &ex) { onPlayResult(ex); });
  player->setOnShutdown([this](const SockException &ex) { onShutdown(ex); });
  (*player)[Client::kRtpType] = rtp_type;
  (*player)[Client::kWaitTrackReady] = false;
  (*player)[Client::kTimeoutMS] = 15000;
  (*player)[Client::kMediaTimeoutMS] = 15000;

  {
    std::lock_guard<std::mutex> lock(_mtx);
    _player = player;
    _retry_timer.reset();
    _rtp_type = rtp_type;
    _stream_url = url;
    _last_error.clear();
  }

  if (!weak_player.lock()) {
    OnError cb;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      _last_error = "player create failed";
      cb = _on_error;
    }
    if (cb) {
      cb("player create failed");
    }
    stopUnpackWorker();
    return false;
  }
  player->play(url);
  _running = true;
  return true;
}

void EsFileFerryPuller::stopPull() {
    MediaPlayer::Ptr player;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        player = _player;
        _retry_timer.reset();
    }

    if (player) {
        clearTrackDelegates();
        player->teardown();
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        _player.reset();
        _retry_timer.reset();
        _track_delegates.clear();
        _stream_url.clear();
    }
    stopUnpackWorker();
    _running = false;
}

void EsFileFerryPuller::removeTaskFrames(const std::string &task_id) {
    if (task_id.empty()) {
        return;
    }
    size_t removed_frames = 0;
    size_t removed_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_buckets.find(task_id);
        if (it == _task_buckets.end()) {
            return;
        }
        removed_frames = it->second.frames.size();
        removed_bytes = it->second.buffered_bytes;
        if (_pending_frame_bytes >= removed_bytes) {
            _pending_frame_bytes -= removed_bytes;
        } else {
            _pending_frame_bytes = 0;
        }
        _task_buckets.erase(it);
        _ready_task_ids.erase(
            std::remove(_ready_task_ids.begin(), _ready_task_ids.end(), task_id),
            _ready_task_ids.end());
    }
    if (removed_frames > 0 || removed_bytes > 0) {
        InfoL << "remove puller task bucket, task_id:" << task_id
              << " removed_frames:" << removed_frames
              << " removed_bytes:" << removed_bytes
              << " remain_pending_bytes:" << _pending_frame_bytes;
    }
    _queue_space_cv.notify_all();
}

std::string EsFileFerryPuller::getStreamUrl() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _stream_url;
}

std::string EsFileFerryPuller::getLastError() const {
    std::lock_guard<std::mutex> lock(_mtx);
    return _last_error;
}

void EsFileFerryPuller::setOnError(OnError cb) {
    std::lock_guard<std::mutex> lock(_mtx);
    _on_error = std::move(cb);
}

void EsFileFerryPuller::onPlayResult(const SockException &ex) {
    if (ex) {
        _running = false;
        OnError cb;
        auto should_retry = false;
        {
            std::lock_guard<std::mutex> lock(_mtx);
            cb = _on_error;
            _last_error = ex.what();
            should_retry = !_stream_url.empty();
        }
        if (cb) {
            cb(ex.what());
        }
        if (should_retry) {
            scheduleRetry();
        }
        return;
    }
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _retry_timer.reset();
    }
    attachTrackDelegates();
}

void EsFileFerryPuller::onShutdown(const SockException &ex) {
    _running = false;
    clearTrackDelegates();
    if (!ex) {
        return;
    }
    OnError cb;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        cb = _on_error;
        _last_error = ex.what();
    }
    if (cb) {
        cb(ex.what());
    }
    scheduleRetry();
}

void EsFileFerryPuller::scheduleRetry() {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_stream_url.empty() || _retry_timer) {
        return;
    }
    _retry_timer = std::make_shared<toolkit::Timer>(
        1.0f,
        [this]() {
            std::string url;
            int rtp_type = 0;
            {
                std::lock_guard<std::mutex> lock(_mtx);
                if (_stream_url.empty()) {
                    _retry_timer.reset();
                    return false;
                }
                url = _stream_url;
                rtp_type = _rtp_type.load();
                _retry_timer.reset();
            }
            startPull(url, rtp_type);
            return false;
        },
        nullptr);
}

void EsFileFerryPuller::startUnpackWorker() {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_unpack_thread.joinable()) {
        return;
    }
    _task_buckets.clear();
    _ready_task_ids.clear();
    _pending_frame_bytes = 0;
    _unpack_stop = false;
    _unpack_thread = std::thread([this]() { unpackWorkerLoop(); });
}

void EsFileFerryPuller::stopUnpackWorker() {
    std::thread join_thread;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _unpack_stop = true;
        _task_buckets.clear();
        _ready_task_ids.clear();
        _pending_frame_bytes = 0;
        if (_unpack_thread.joinable()) {
            join_thread = std::move(_unpack_thread);
        }
    }
    _frame_cv.notify_all();
    _queue_space_cv.notify_all();
    if (join_thread.joinable()) {
        join_thread.join();
    }
}

bool EsFileFerryPuller::enqueueFrame(const Frame::Ptr &frame) {
    if (!frame || !frame->data() || frame->size() == 0) {
        return true;
    }
    PendingFrame pending;
    pending.frame = frame;
    pending.size = static_cast<size_t>(frame->size());
    const auto decodedIncomingMeta = tryDecodeFramePacketMeta(frame);
    pending.meta.valid = decodedIncomingMeta.valid;
    pending.meta.task_id = decodedIncomingMeta.task_id;
    pending.meta.packet_type = static_cast<int>(decodedIncomingMeta.type);
    pending.meta.seq = decodedIncomingMeta.seq;

    size_t pending_bytes = 0;
    bool over_soft_limit = false;
    bool waited_for_space = false;
    auto wait_start = std::chrono::steady_clock::time_point{};
    {
        std::unique_lock<std::mutex> lock(_mtx);
        if (_unpack_stop) {
            return false;
        }
        if (pending.size > kMaxPendingFrameBytesHardLimit) {
            WarnL << "drop oversize frame before enqueue, frame_size:" << pending.size
                  << " task_id:" << (pending.meta.valid ? pending.meta.task_id : std::string("unknown"))
                  << " type:" << (pending.meta.valid ? packetTypeToString(pending.meta.packet_type) : "unknown")
                  << " seq:" << (pending.meta.valid ? pending.meta.seq : 0)
                  << " hard_limit:" << kMaxPendingFrameBytesHardLimit;
            return true;
        }
        while (!_unpack_stop &&
               _pending_frame_bytes + pending.size > kMaxPendingFrameBytesHardLimit) {
            if (wait_start == std::chrono::steady_clock::time_point{}) {
                wait_start = std::chrono::steady_clock::now();
            }
            waited_for_space = true;
            _queue_space_cv.wait_for(lock, std::chrono::milliseconds(kQueueSpaceWaitStepMs));
            if (!_unpack_stop) {
                const auto waited_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_start)
                        .count();
                if (waited_ms >= kQueueSpaceWaitSlowLogMs &&
                    waited_ms % kQueueSpaceWaitSlowLogMs < kQueueSpaceWaitStepMs) {
                    WarnL << "puller unpack queue wait for total space, frame_size:"
                          << pending.size
                          << " task_id:" << (pending.meta.valid ? pending.meta.task_id : std::string("unknown"))
                          << " type:" << (pending.meta.valid ? packetTypeToString(pending.meta.packet_type) : "unknown")
                          << " seq:" << (pending.meta.valid ? pending.meta.seq : 0)
                          << " pending_bytes:" << _pending_frame_bytes
                          << " wait_ms:" << waited_ms;
                }
            }
        }
        if (_unpack_stop) {
            return false;
        }
        if (_pending_frame_bytes + pending.size > kMaxPendingFrameBytesHardLimit) {
            WarnL << "drop frame after trim, frame_size:" << pending.size
                  << " task_id:" << (pending.meta.valid ? pending.meta.task_id : std::string("unknown"))
                  << " type:" << (pending.meta.valid ? packetTypeToString(pending.meta.packet_type) : "unknown")
                  << " seq:" << (pending.meta.valid ? pending.meta.seq : 0)
                  << " pending_bytes:" << _pending_frame_bytes
                  << " hard_limit:" << kMaxPendingFrameBytesHardLimit;
            return true;
        }
        const auto task_key = bucketKeyForMeta(pending.meta);
        auto &bucket = _task_buckets[task_key];
        if (!bucket.in_ready_queue) {
            _ready_task_ids.emplace_back(task_key);
            bucket.in_ready_queue = true;
        }
        bucket.buffered_bytes += pending.size;
        bucket.frames.emplace_back(std::move(pending));
        _pending_frame_bytes += pending.size;
        pending_bytes = _pending_frame_bytes;
        over_soft_limit = pending_bytes > kMaxPendingFrameBytesSoftLimit;
    }
    if (waited_for_space) {
        const auto waited_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - wait_start)
                .count();
        if (waited_ms >= kQueueSpaceWaitStepMs) {
            DebugL << "puller unpack queue resumed after wait, frame_size:" << frame->size()
                   << " task_id:" << (decodedIncomingMeta.valid ? decodedIncomingMeta.task_id : std::string("unknown"))
                   << " type:" << (decodedIncomingMeta.valid ? EsFilePacketTypeToString(decodedIncomingMeta.type) : "unknown")
                   << " seq:" << (decodedIncomingMeta.valid ? decodedIncomingMeta.seq : 0)
                   << " pending_bytes:" << pending_bytes
                   << " wait_ms:" << waited_ms;
        }
    }
    if (over_soft_limit) {
        static std::atomic<size_t> s_overflow_count{0};
        const auto overflow_count = ++s_overflow_count;
        if (overflow_count <= 5 || overflow_count % 100 == 0) {
            WarnL << "puller unpack queue over soft limit, frame_size:" << pending.size
                  << " task_id:" << (decodedIncomingMeta.valid ? decodedIncomingMeta.task_id : std::string("unknown"))
                  << " pending_bytes:" << pending_bytes
                  << " overflow_count:" << overflow_count;
        }
    }
    _frame_cv.notify_one();
    return true;
}

void EsFileFerryPuller::unpackWorkerLoop() {
    while (true) {
        PendingFrame pending;
        {
            std::unique_lock<std::mutex> lock(_mtx);
            _frame_cv.wait(lock, [this]() {
                return _unpack_stop || !_ready_task_ids.empty();
            });
            if (_unpack_stop && _ready_task_ids.empty()) {
                break;
            }
            const auto task_key = std::move(_ready_task_ids.front());
            _ready_task_ids.pop_front();
            auto bucket_it = _task_buckets.find(task_key);
            if (bucket_it == _task_buckets.end()) {
                continue;
            }
            bucket_it->second.in_ready_queue = false;
            if (bucket_it->second.frames.empty()) {
                _task_buckets.erase(bucket_it);
                continue;
            }
            pending = std::move(bucket_it->second.frames.front());
            bucket_it->second.frames.pop_front();
            if (bucket_it->second.buffered_bytes >= pending.size) {
                bucket_it->second.buffered_bytes -= pending.size;
            } else {
                bucket_it->second.buffered_bytes = 0;
            }
            if (!bucket_it->second.frames.empty()) {
                _ready_task_ids.emplace_back(task_key);
                bucket_it->second.in_ready_queue = true;
            } else {
                _task_buckets.erase(bucket_it);
            }
            if (_pending_frame_bytes >= pending.size) {
                _pending_frame_bytes -= pending.size;
            } else {
                _pending_frame_bytes = 0;
            }
        }
        _queue_space_cv.notify_all();
        if (pending.frame && pending.frame->data() && pending.frame->size() > 0) {
            EsFileFerryUnPacker::Instance().inputFrame(
                reinterpret_cast<const uint8_t *>(pending.frame->data()),
                static_cast<size_t>(pending.frame->size()));
        }
    }
}

void EsFileFerryPuller::attachTrackDelegates() {
    MediaPlayer::Ptr player;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        player = _player;
    }
    if (!player) {
        return;
    }

    clearTrackDelegates();

    auto tracks = player->getTracks(false);
    std::vector<std::pair<std::weak_ptr<Track>, FrameWriterInterface *>> delegates;
    delegates.reserve(tracks.size());
    for (auto &track : tracks) {
        if (!track) {
            continue;
        }
        auto ptr = track->addDelegate([this](const Frame::Ptr &frame) {
            if (!frame || !frame->data() || frame->size() == 0) {
                return true;
            }
            if (frame->getTrackType() == TrackType::TrackAudio) {
                return true;
            }
            enqueueFrame(frame);
            return true;
        });
        delegates.emplace_back(track, ptr);
    }

    std::lock_guard<std::mutex> lock(_mtx);
    _track_delegates = std::move(delegates);
}

void EsFileFerryPuller::clearTrackDelegates() {
    std::vector<std::pair<std::weak_ptr<Track>, FrameWriterInterface *>> delegates;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        delegates.swap(_track_delegates);
    }
    for (auto &item : delegates) {
        auto track = item.first.lock();
        if (track && item.second) {
            track->delDelegate(item.second);
        }
    }
}
