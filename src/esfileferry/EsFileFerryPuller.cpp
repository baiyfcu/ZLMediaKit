#include "EsFileFerryPuller.h"
#include "EsFileFerryPlayer.h"
#include "Common/config.h"

using namespace mediakit;
using namespace toolkit;

namespace {
constexpr size_t kMaxPendingFrameBytesSoftLimit = 8 * 1024 * 1024;
constexpr size_t kMaxPendingFrameBytesHardLimit = 16 * 1024 * 1024;
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
    _pending_frames.clear();
    _pending_frame_bytes = 0;
    _unpack_stop = false;
    _unpack_thread = std::thread([this]() { unpackWorkerLoop(); });
}

void EsFileFerryPuller::stopUnpackWorker() {
    std::thread join_thread;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _unpack_stop = true;
        _pending_frames.clear();
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

void EsFileFerryPuller::trimPendingFramesLocked(
    size_t incomingFrameSize, size_t &droppedFrames, size_t &droppedBytes) {
    droppedFrames = 0;
    droppedBytes = 0;
    if (incomingFrameSize == 0) {
        return;
    }
    while (!_pending_frames.empty() &&
           _pending_frame_bytes + incomingFrameSize > kMaxPendingFrameBytesSoftLimit) {
        const auto droppedSize = _pending_frames.front().size;
        _pending_frames.pop_front();
        if (_pending_frame_bytes >= droppedSize) {
            _pending_frame_bytes -= droppedSize;
        } else {
            _pending_frame_bytes = 0;
        }
        droppedBytes += droppedSize;
        ++droppedFrames;
    }
}

bool EsFileFerryPuller::enqueueFrame(const Frame::Ptr &frame) {
    if (!frame || !frame->data() || frame->size() == 0) {
        return true;
    }
    PendingFrame pending;
    pending.frame = frame;
    pending.size = static_cast<size_t>(frame->size());

    size_t droppedFrames = 0;
    size_t droppedBytes = 0;
    size_t pending_bytes = 0;
    bool over_soft_limit = false;
    {
        std::unique_lock<std::mutex> lock(_mtx);
        if (_unpack_stop) {
            return false;
        }
        if (pending.size > kMaxPendingFrameBytesHardLimit) {
            WarnL << "drop oversize frame before enqueue, frame_size:" << pending.size
                  << " hard_limit:" << kMaxPendingFrameBytesHardLimit;
            return true;
        }
        trimPendingFramesLocked(pending.size, droppedFrames, droppedBytes);
        if (_pending_frame_bytes + pending.size > kMaxPendingFrameBytesHardLimit) {
            WarnL << "drop frame after trim, frame_size:" << pending.size
                  << " pending_bytes:" << _pending_frame_bytes
                  << " hard_limit:" << kMaxPendingFrameBytesHardLimit;
            return true;
        }
        _pending_frame_bytes += pending.size;
        pending_bytes = _pending_frame_bytes;
        over_soft_limit = pending_bytes > kMaxPendingFrameBytesSoftLimit;
        _pending_frames.emplace_back(std::move(pending));
    }
    if (droppedFrames > 0 || droppedBytes > 0) {
        WarnL << "trim puller pending frames for latest data, incoming_frame_size:"
              << pending.size << " dropped_frames:" << droppedFrames
              << " dropped_bytes:" << droppedBytes
              << " pending_bytes:" << pending_bytes;
    }
    if (over_soft_limit) {
        static std::atomic<size_t> s_overflow_count{0};
        const auto overflow_count = ++s_overflow_count;
        if (overflow_count <= 5 || overflow_count % 100 == 0) {
            WarnL << "puller unpack queue over soft limit, frame_size:" << pending.size
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
                return _unpack_stop || !_pending_frames.empty();
            });
            if (_unpack_stop && _pending_frames.empty()) {
                break;
            }
            pending = std::move(_pending_frames.front());
            _pending_frames.pop_front();
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
