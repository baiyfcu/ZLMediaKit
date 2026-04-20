#include "EsFileFerryPuller.h"
#include "EsFileFerryPlayer.h"
#include "Common/config.h"
#include "Thread/WorkThreadPool.h"

using namespace mediakit;
using namespace toolkit;

namespace {
constexpr size_t kPendingFrameCountWarnThreshold = 2000;
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
    size_t prefix_size = 0;
    if (!DetectEsFileCarrierPrefixSize(data, size, prefix_size) ||
        size < prefix_size + kEsFileFixedHeaderSize) {
        return meta;
    }

    EsFilePacketHeader header;
    if (!DecodeEsFilePacketHeader(data + prefix_size, size - prefix_size,
                                  header)) {
        return meta;
    }

    const auto payload_offset = prefix_size + kEsFileFixedHeaderSize;
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
  //auto poller = WorkThreadPool::Instance().getPoller();
  MediaPlayer::Ptr player = std::make_shared<MediaPlayer>();
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
    // removed_bucket 使用 std::move 到线程池中析构，避免在主线程中析构导致的占用网络线程资源
    WorkThreadPool::Instance().getPoller()->async([=]() {
        size_t removed_frames = 0;
        size_t removed_bytes = 0;
        PendingTaskBucket removed_bucket;
        {
            std::lock_guard<std::mutex> lock(_mtx);
            auto it = _task_buckets.find(task_id);
            if (it == _task_buckets.end()) {
                return;
            }
            removed_frames = it->second.frames.size();
            removed_bytes = it->second.buffered_bytes;
            removed_bucket = std::move(it->second);
            _task_buckets.erase(it);
            _ready_task_ids.erase(std::remove(_ready_task_ids.begin(), _ready_task_ids.end(), task_id), _ready_task_ids.end());
        }
        if (removed_frames > 0 || removed_bytes > 0) {
            InfoL << "remove puller task bucket, task_id:" << task_id << " removed_frames:" << removed_frames << " removed_bytes:" << removed_bytes;
        }
    });
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
        if (_unpack_thread.joinable()) {
            join_thread = std::move(_unpack_thread);
        }
    }
    _frame_cv.notify_all();
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
    size_t pending_frames = 0;
    {
        std::unique_lock<std::mutex> lock(_mtx);
        if (_unpack_stop) {
            return false;
        }
        const auto task_key = bucketKeyForMeta(pending.meta);
        auto &bucket = _task_buckets[task_key];
        if (!bucket.in_ready_queue) {
            _ready_task_ids.emplace_back(task_key);
            bucket.in_ready_queue = true;
        }
        bucket.buffered_bytes += pending.size;
        bucket.frames.emplace_back(std::move(pending));
        for (const auto &entry : _task_buckets) {
            pending_bytes += entry.second.buffered_bytes;
            pending_frames += entry.second.frames.size();
        }
    }
    if (pending_frames > kPendingFrameCountWarnThreshold) {
        static std::atomic<size_t> s_overflow_count{0};
        const auto overflow_count = ++s_overflow_count;
        if (overflow_count <= 5 || overflow_count % 100 == 0) {
            WarnL << "puller unpack queue frame count over threshold, frame_size:" << pending.size
                  << " task_id:" << (decodedIncomingMeta.valid ? decodedIncomingMeta.task_id : std::string("unknown"))
                  << " pending_frames:" << pending_frames
                  << " pending_bytes:" << pending_bytes
                  << " threshold:" << kPendingFrameCountWarnThreshold
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
        }
        if (pending.frame && pending.frame->data() && pending.frame->size() > 0) {
            //uint8_t tembuf[]
            //    = { 00,   0x00, 0x00, 0x01, 0x61, 0x47, 0x54, 0x46, 0x59, 0x01, 0x01, 0x00, 0x17, 0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            //        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0x00, 0x00, 0x00, 0x00, 0x16, 0x14, 0x37, 0xb8, 0xff, 0xff, 0xff, 0xff, 0x00,
            //        0x00, 0x01, 0x3b, 0x00, 0x10, 0xe2, 0x58, 0x70, 0x6c, 0x61, 0x79, 0x5f, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x5f, 0x31, 0x34, 0x5f,
            //        0x31, 0x36, 0x35, 0x32, 0x35, 0x39, 0x39, 0x31, 0x2e, 0x6d, 0x70, 0x34, 0x3a, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x3a, 0x20, 0x32, 0x30,
            //        0x30, 0x0a, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x3a, 0x20, 0x6b, 0x65, 0x65, 0x70, 0x2d, 0x61, 0x6c, 0x69, 0x76,
            //        0x65, 0x0a, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x3a, 0x20, 0x33, 0x37, 0x30, 0x34, 0x32,
            //        0x33, 0x37, 0x33, 0x36, 0x0a, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x54, 0x79, 0x70, 0x65, 0x3a, 0x20, 0x76, 0x69, 0x64, 0x65,
            //        0x6f, 0x2f, 0x6d, 0x70, 0x34, 0x3b, 0x20, 0x63, 0x68, 0x61, 0x72, 0x73, 0x65, 0x74, 0x3d, 0x75, 0x74, 0x66, 0x2d, 0x38, 0x0a, 0x44, 0x61,
            //        0x74, 0x65, 0x3a, 0x20, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x41, 0x70, 0x72, 0x20, 0x32, 0x30, 0x20, 0x32, 0x30, 0x32, 0x36, 0x20, 0x31, 0x31,
            //        0x3a, 0x35, 0x31, 0x3a, 0x35, 0x38, 0x20, 0x47, 0x4d, 0x54, 0x0a, 0x4b, 0x65, 0x65, 0x70, 0x2d, 0x41, 0x6c, 0x69, 0x76, 0x65, 0x3a, 0x20,
            //        0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x3d, 0x31, 0x35, 0x2c, 0x20, 0x6d, 0x61, 0x78, 0x3d, 0x31, 0x30, 0x30, 0x0a, 0x53, 0x65, 0x72,
            //        0x76, 0x65, 0x72, 0x3a, 0x20, 0x5a, 0x4c, 0x4d, 0x65, 0x64, 0x69, 0x61, 0x4b, 0x69, 0x74, 0x28, 0x67, 0x69, 0x74, 0x20, 0x68, 0x61, 0x73,
            //        0x68, 0x3a, 0x2f, 0x2c, 0x62, 0x72, 0x61, 0x6e, 0x63, 0x68, 0x3a, 0x2c, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x20, 0x74, 0x69, 0x6d, 0x65, 0x3a,
            //        0x32, 0x30, 0x32, 0x36, 0x2d, 0x30, 0x34, 0x2d, 0x32, 0x30, 0x54, 0x30, 0x30, 0x3a, 0x33, 0x30, 0x3a, 0x31, 0x38, 0x29, 0x0a };
            //EsFileFerryUnPacker::Instance().inputFrame(tembuf, sizeof(tembuf));
            EsFileFerryUnPacker::Instance().inputFrame(reinterpret_cast<const uint8_t *>(pending.frame->data()), static_cast<size_t>(pending.frame->size()));
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
            auto len = frame->size();
            auto data = frame->data();
            if(len > 200 && len < 500)
                ErrorL << "video dts:" << frame->dts() << " size:" << len << " hex:" << hexmem(data, 50);
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
