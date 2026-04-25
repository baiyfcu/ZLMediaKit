#pragma once

#include "Extension/Track.h"
#include "Player/MediaPlayer.h"
#include "Poller/Timer.h"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

class EsFileFerryPuller {
public:
    using OnError = std::function<void(const std::string &)>;
    struct PendingFrameMeta {
        bool valid = false;
        std::string task_id;
        int packet_type = 0;
        uint32_t seq = 0;
    };

    static EsFileFerryPuller &Instance();

    bool startPull(const std::string &url, int rtp_type = 0);
    void stopPull();
    void removeTaskFrames(const std::string &task_id);
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
    void startUnpackWorker();
    void stopUnpackWorker();
    bool enqueueFrame(const mediakit::Frame::Ptr &frame);
    void unpackWorkerLoop();

private:
    struct PendingFrame {
        mediakit::Frame::Ptr frame;
        size_t size = 0;
        PendingFrameMeta meta;
    };

    struct PendingTaskBucket {
        std::deque<PendingFrame> frames;
        size_t buffered_bytes = 0;
        bool in_ready_queue = false;
        bool in_processing = false;
    };

    mutable std::mutex _mtx;
    mediakit::MediaPlayer::Ptr _player;
    std::shared_ptr<toolkit::Timer> _retry_timer;
    std::string _stream_url;
    std::string _last_error;
    std::vector<std::pair<std::weak_ptr<mediakit::Track>, mediakit::FrameWriterInterface *>> _track_delegates;
    std::unordered_map<std::string, PendingTaskBucket> _task_buckets;
    std::deque<std::string> _ready_task_ids;
    std::condition_variable _frame_cv;
    std::vector<std::thread> _unpack_threads;
    size_t _pending_frame_count = 0;
    size_t _pending_frame_bytes = 0;
    bool _unpack_stop = true;
    std::atomic<bool> _running = {false};
    std::atomic<int> _rtp_type = {0};
    OnError _on_error;
};
