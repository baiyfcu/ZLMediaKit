#include "EsFileFerryPuller.h"
#include "Common/config.h"

using namespace mediakit;
using namespace toolkit;

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
  DebugL << "startPull, url:" << url << " rtp_type:" << rtp_type;
  MediaPlayer::Ptr player = std::make_shared<MediaPlayer>();
  std::weak_ptr<MediaPlayer> weak_player = player;
  player->setOnPlayResult([this](const SockException &ex) { onPlayResult(ex); });
  player->setOnShutdown([this](const SockException &ex) { onShutdown(ex); });
  (*player)["rtp_type"] = rtp_type;
  (*player)["wait_track_ready"] = false;
  (*player)["protocol_timeout_ms"] = 15000;

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
        auto ptr = track->addDelegate([](const Frame::Ptr &frame) {
            if (!frame || !frame->data() || frame->size() == 0) {
                return true;
            }
            if (frame->getTrackType() == TrackType::TrackAudio) {
                return true;
            }
            const auto *data = reinterpret_cast<const uint8_t *>(frame->data());
            return EsFileFerryUnPacker::Instance().inputFrame(
                data, static_cast<size_t>(frame->size()));
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
