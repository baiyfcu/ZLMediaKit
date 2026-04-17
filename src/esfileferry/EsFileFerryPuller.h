#pragma once

#include "EsFileFerryPlayer.h"
#include "Extension/Track.h"
#include "Player/MediaPlayer.h"
#include "Poller/Timer.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

class EsFileFerryPuller {
public:
    using OnError = std::function<void(const std::string &)>;

    static EsFileFerryPuller &Instance();

    bool startPull(const std::string &url, int rtp_type = 0);
    void stopPull();
    std::string getStreamUrl() const;
    std::string getLastError() const;
    void setOnError(OnError cb);

private:
    EsFileFerryPuller() = default;
    EsFileFerryPuller(const EsFileFerryPuller &) = delete;
    EsFileFerryPuller &operator=(const EsFileFerryPuller &) = delete;

    void onPlayResult(const toolkit::SockException &ex);
    void onShutdown(const toolkit::SockException &ex);
    void attachTrackDelegates();
    void clearTrackDelegates();
    void scheduleRetry();

private:
    mutable std::mutex _mtx;
    mediakit::MediaPlayer::Ptr _player;
    std::shared_ptr<toolkit::Timer> _retry_timer;
    std::string _stream_url;
    std::string _last_error;
    std::vector<std::pair<std::weak_ptr<mediakit::Track>, mediakit::FrameWriterInterface *>> _track_delegates;
    std::atomic<bool> _running = {false};
    std::atomic<int> _rtp_type = {0};
    OnError _on_error;
};
