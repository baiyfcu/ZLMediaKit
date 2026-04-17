/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include "Common/config.h"
#include "MediaSource.h"
#include "Util/NoticeCenter.h"
#include "Util/logger.h"
#include "Util/onceToken.h"
#include "Util/util.h"
#include <assert.h>
#include <stdio.h>

using namespace std;
using namespace toolkit;

namespace mediakit {

bool loadIniConfig(const char *ini_path) {
    string ini;
    if (ini_path && ini_path[0] != '\0') {
        ini = ini_path;
    } else {
        ini = exePath() + ".ini";
    }
    try {
        mINI::Instance().parseFile(ini);
        NOTICE_EMIT(BroadcastReloadConfigArgs, Broadcast::kBroadcastReloadConfig);
        return true;
    } catch (std::exception &) {
        InfoL << "dump ini file to:" << ini;
        mINI::Instance().dumpFile(ini);
        return false;
    }
}
// //////////广播名称///////////  [AUTO-TRANSLATED:439b2d74]
// //////////Broadcast Name///////////
namespace Broadcast {
ZLMEDIAKIT_API const string kBroadcastMediaChanged = "kBroadcastMediaChanged";
ZLMEDIAKIT_API const string kBroadcastRecordMP4 = "kBroadcastRecordMP4";
ZLMEDIAKIT_API const string kBroadcastRecordTs = "kBroadcastRecordTs";
ZLMEDIAKIT_API const string kBroadcastHttpRequest = "kBroadcastHttpRequest";
ZLMEDIAKIT_API const string kBroadcastHttpAccess = "kBroadcastHttpAccess";
ZLMEDIAKIT_API const string kBroadcastOnGetRtspRealm = "kBroadcastOnGetRtspRealm";
ZLMEDIAKIT_API const string kBroadcastOnRtspAuth = "kBroadcastOnRtspAuth";
ZLMEDIAKIT_API const string kBroadcastMediaPlayed = "kBroadcastMediaPlayed";
ZLMEDIAKIT_API const string kBroadcastMediaPublish = "kBroadcastMediaPublish";
ZLMEDIAKIT_API const string kBroadcastFlowReport = "kBroadcastFlowReport";
ZLMEDIAKIT_API const string kBroadcastReloadConfig = "kBroadcastReloadConfig";
ZLMEDIAKIT_API const string kBroadcastPlayChannelTaskEvent = "kBroadcastPlayChannelTaskEvent";
ZLMEDIAKIT_API const string kBroadcastShellLogin = "kBroadcastShellLogin";
ZLMEDIAKIT_API const string kBroadcastNotFoundStream = "kBroadcastNotFoundStream";
ZLMEDIAKIT_API const string kBroadcastStreamNoneReader = "kBroadcastStreamNoneReader";
ZLMEDIAKIT_API const string kBroadcastHttpBeforeAccess = "kBroadcastHttpBeforeAccess";
ZLMEDIAKIT_API const string kBroadcastSendRtpStopped = "kBroadcastSendRtpStopped";
ZLMEDIAKIT_API const string kBroadcastRtpServerTimeout = "kBroadcastRtpServerTimeout";
ZLMEDIAKIT_API const string kBroadcastRtcSctpConnecting = "kBroadcastRtcSctpConnecting";
ZLMEDIAKIT_API const string kBroadcastRtcSctpConnected = "kBroadcastRtcSctpConnected";
ZLMEDIAKIT_API const string kBroadcastRtcSctpFailed = "kBroadcastRtcSctpFailed";
ZLMEDIAKIT_API const string kBroadcastRtcSctpClosed = "kBroadcastRtcSctpClosed";
ZLMEDIAKIT_API const string kBroadcastRtcSctpSend = "kBroadcastRtcSctpSend";
ZLMEDIAKIT_API const string kBroadcastRtcSctpReceived = "kBroadcastRtcSctpReceived";
ZLMEDIAKIT_API const string kBroadcastPlayerCountChanged = "kBroadcastPlayerCountChanged";

} // namespace Broadcast

// 通用配置项目  [AUTO-TRANSLATED:ca344202]
// General Configuration Items
namespace General {
#define GENERAL_FIELD "general."
ZLMEDIAKIT_API const string kMediaServerId = GENERAL_FIELD "mediaServerId";
ZLMEDIAKIT_API const string kFlowThreshold = GENERAL_FIELD "flowThreshold";
ZLMEDIAKIT_API const string kStreamNoneReaderDelayMS = GENERAL_FIELD "streamNoneReaderDelayMS";
ZLMEDIAKIT_API const string kMaxStreamWaitTimeMS = GENERAL_FIELD "maxStreamWaitMS";
ZLMEDIAKIT_API const string kEnableVhost = GENERAL_FIELD "enableVhost";
ZLMEDIAKIT_API const string kResetWhenRePlay = GENERAL_FIELD "resetWhenRePlay";
ZLMEDIAKIT_API const string kMergeWriteMS = GENERAL_FIELD "mergeWriteMS";
ZLMEDIAKIT_API const string kCheckNvidiaDev = GENERAL_FIELD "check_nvidia_dev";
ZLMEDIAKIT_API const string kEnableFFmpegLog = GENERAL_FIELD "enable_ffmpeg_log";
ZLMEDIAKIT_API const string kWaitTrackReadyMS = GENERAL_FIELD "wait_track_ready_ms";
ZLMEDIAKIT_API const string kWaitAudioTrackDataMS = GENERAL_FIELD "wait_audio_track_data_ms";
ZLMEDIAKIT_API const string kWaitAddTrackMS = GENERAL_FIELD "wait_add_track_ms";
ZLMEDIAKIT_API const string kUnreadyFrameCache = GENERAL_FIELD "unready_frame_cache";
ZLMEDIAKIT_API const string kBroadcastPlayerCountChanged = GENERAL_FIELD "broadcast_player_count_changed";
ZLMEDIAKIT_API const string kListenIP = GENERAL_FIELD "listen_ip";

static onceToken token([]() {
    mINI::Instance()[kFlowThreshold] = 1024;
    mINI::Instance()[kStreamNoneReaderDelayMS] = 20 * 1000;
    mINI::Instance()[kMaxStreamWaitTimeMS] = 15 * 1000;
    mINI::Instance()[kEnableVhost] = 0;
    mINI::Instance()[kResetWhenRePlay] = 1;
    mINI::Instance()[kMergeWriteMS] = 0;
    mINI::Instance()[kMediaServerId] = makeRandStr(16);
    mINI::Instance()[kCheckNvidiaDev] = 1;
    mINI::Instance()[kEnableFFmpegLog] = 0;
    mINI::Instance()[kWaitTrackReadyMS] = 10000;
    mINI::Instance()[kWaitAudioTrackDataMS] = 1000;
    mINI::Instance()[kWaitAddTrackMS] = 3000;
    mINI::Instance()[kUnreadyFrameCache] = 100;
    mINI::Instance()[kBroadcastPlayerCountChanged] = 0;
    mINI::Instance()[kListenIP] = "::";
});

} // namespace General

namespace Protocol {
ZLMEDIAKIT_API const string kModifyStamp = string(kFieldName) + "modify_stamp";
ZLMEDIAKIT_API const string kEnableAudio = string(kFieldName) + "enable_audio";
ZLMEDIAKIT_API const string kAddMuteAudio = string(kFieldName) + "add_mute_audio";
ZLMEDIAKIT_API const string kAutoClose = string(kFieldName) + "auto_close";
ZLMEDIAKIT_API const string kContinuePushMS = string(kFieldName) + "continue_push_ms";
ZLMEDIAKIT_API const string kPacedSenderMS = string(kFieldName) + "paced_sender_ms";

ZLMEDIAKIT_API const string kEnableHls = string(kFieldName) + "enable_hls";
ZLMEDIAKIT_API const string kEnableHlsFmp4 = string(kFieldName) + "enable_hls_fmp4";
ZLMEDIAKIT_API const string kEnableMP4 = string(kFieldName) + "enable_mp4";
ZLMEDIAKIT_API const string kEnableRtsp = string(kFieldName) + "enable_rtsp";
ZLMEDIAKIT_API const string kEnableRtmp = string(kFieldName) + "enable_rtmp";
ZLMEDIAKIT_API const string kEnableTS = string(kFieldName) + "enable_ts";
ZLMEDIAKIT_API const string kEnableFMP4 = string(kFieldName) + "enable_fmp4";

ZLMEDIAKIT_API const string kMP4AsPlayer = string(kFieldName) + "mp4_as_player";
ZLMEDIAKIT_API const string kMP4MaxSecond = string(kFieldName) + "mp4_max_second";
ZLMEDIAKIT_API const string kMP4SavePath = string(kFieldName) + "mp4_save_path";

ZLMEDIAKIT_API const string kHlsSavePath = string(kFieldName) + "hls_save_path";

ZLMEDIAKIT_API const string kHlsDemand = string(kFieldName) + "hls_demand";
ZLMEDIAKIT_API const string kRtspDemand = string(kFieldName) + "rtsp_demand";
ZLMEDIAKIT_API const string kRtmpDemand = string(kFieldName) + "rtmp_demand";
ZLMEDIAKIT_API const string kTSDemand = string(kFieldName) + "ts_demand";
ZLMEDIAKIT_API const string kFMP4Demand = string(kFieldName) + "fmp4_demand";

static onceToken token([]() {
    mINI::Instance()[kModifyStamp] = (int)ProtocolOption::kModifyStampRelative;
    mINI::Instance()[kEnableAudio] = 1;
    mINI::Instance()[kAddMuteAudio] = 1;
    mINI::Instance()[kContinuePushMS] = 15000;
    mINI::Instance()[kPacedSenderMS] = 0;
    mINI::Instance()[kAutoClose] = 0;

    mINI::Instance()[kEnableHls] = 1;
    mINI::Instance()[kEnableHlsFmp4] = 0;
    mINI::Instance()[kEnableMP4] = 0;
    mINI::Instance()[kEnableRtsp] = 1;
    mINI::Instance()[kEnableRtmp] = 1;
    mINI::Instance()[kEnableTS] = 1;
    mINI::Instance()[kEnableFMP4] = 1;

    mINI::Instance()[kMP4AsPlayer] = 0;
    mINI::Instance()[kMP4MaxSecond] = 3600;
    mINI::Instance()[kMP4SavePath] = "./www";

    mINI::Instance()[kHlsSavePath] = "./www";

    mINI::Instance()[kHlsDemand] = 0;
    mINI::Instance()[kRtspDemand] = 0;
    mINI::Instance()[kRtmpDemand] = 0;
    mINI::Instance()[kTSDemand] = 0;
    mINI::Instance()[kFMP4Demand] = 0;
});
} // !Protocol

// //////////HTTP配置///////////  [AUTO-TRANSLATED:a281d694]
// //////////HTTP Configuration///////////
namespace Http {
#define HTTP_FIELD "http."
ZLMEDIAKIT_API const string kSendBufSize = HTTP_FIELD "sendBufSize";
ZLMEDIAKIT_API const string kMaxReqSize = HTTP_FIELD "maxReqSize";
ZLMEDIAKIT_API const string kKeepAliveSecond = HTTP_FIELD "keepAliveSecond";
ZLMEDIAKIT_API const string kCharSet = HTTP_FIELD "charSet";
ZLMEDIAKIT_API const string kRootPath = HTTP_FIELD "rootPath";
ZLMEDIAKIT_API const string kVirtualPath = HTTP_FIELD "virtualPath";
ZLMEDIAKIT_API const string kNotFound = HTTP_FIELD "notFound";
ZLMEDIAKIT_API const string kDirMenu = HTTP_FIELD "dirMenu";
ZLMEDIAKIT_API const string kForbidCacheSuffix = HTTP_FIELD "forbidCacheSuffix";
ZLMEDIAKIT_API const string kForwardedIpHeader = HTTP_FIELD "forwarded_ip_header";
ZLMEDIAKIT_API const string kAllowCrossDomains = HTTP_FIELD "allow_cross_domains";
ZLMEDIAKIT_API const string kAllowIPRange = HTTP_FIELD "allow_ip_range";

static onceToken token([]() {
    mINI::Instance()[kSendBufSize] = 64 * 1024;
    mINI::Instance()[kMaxReqSize] = 4 * 10240;
    mINI::Instance()[kKeepAliveSecond] = 15;
    mINI::Instance()[kDirMenu] = true;
    mINI::Instance()[kVirtualPath] = "";
    mINI::Instance()[kCharSet] = "utf-8";

    mINI::Instance()[kRootPath] = "./www";
    mINI::Instance()[kNotFound] = StrPrinter << "<html>"
                                                "<head><title>404 Not Found</title></head>"
                                                "<body bgcolor=\"white\">"
                                                "<center><h1>您访问的资源不存在！</h1></center>"
                                                "<hr><center>"
                                             << kServerName
                                             << "</center>"
                                                "</body>"
                                                "</html>"
                                             << endl;
    mINI::Instance()[kForbidCacheSuffix] = "";
    mINI::Instance()[kForwardedIpHeader] = "";
    mINI::Instance()[kAllowCrossDomains] = 1;
    mINI::Instance()[kAllowIPRange] = "::1,127.0.0.1,172.16.0.0-172.31.255.255,192.168.0.0-192.168.255.255,10.0.0.0-10.255.255.255";
});

} // namespace Http

// //////////SHELL配置///////////  [AUTO-TRANSLATED:f023ec45]
// //////////SHELL Configuration///////////
namespace Shell {
#define SHELL_FIELD "shell."
ZLMEDIAKIT_API const string kMaxReqSize = SHELL_FIELD "maxReqSize";

static onceToken token([]() { mINI::Instance()[kMaxReqSize] = 1024; });
} // namespace Shell

// //////////RTSP服务器配置///////////  [AUTO-TRANSLATED:950e1981]
// //////////RTSP Server Configuration///////////
namespace Rtsp {
#define RTSP_FIELD "rtsp."
ZLMEDIAKIT_API const string kAuthBasic = RTSP_FIELD "authBasic";
ZLMEDIAKIT_API const string kHandshakeSecond = RTSP_FIELD "handshakeSecond";
ZLMEDIAKIT_API const string kKeepAliveSecond = RTSP_FIELD "keepAliveSecond";
ZLMEDIAKIT_API const string kDirectProxy = RTSP_FIELD "directProxy";
ZLMEDIAKIT_API const string kLowLatency = RTSP_FIELD"lowLatency";
ZLMEDIAKIT_API const string kRtpTransportType = RTSP_FIELD"rtpTransportType";

static onceToken token([]() {
    // 默认Md5方式认证  [AUTO-TRANSLATED:6155d989]
    // Default Md5 authentication
    mINI::Instance()[kAuthBasic] = 0;
    mINI::Instance()[kHandshakeSecond] = 15;
    mINI::Instance()[kKeepAliveSecond] = 15;
    mINI::Instance()[kDirectProxy] = 1;
    mINI::Instance()[kLowLatency] = 0;
    mINI::Instance()[kRtpTransportType] = -1;
});
} // namespace Rtsp

// //////////RTMP服务器配置///////////  [AUTO-TRANSLATED:8de6f41f]
// //////////RTMP Server Configuration///////////
namespace Rtmp {
#define RTMP_FIELD "rtmp."
ZLMEDIAKIT_API const string kHandshakeSecond = RTMP_FIELD "handshakeSecond";
ZLMEDIAKIT_API const string kKeepAliveSecond = RTMP_FIELD "keepAliveSecond";
ZLMEDIAKIT_API const string kDirectProxy = RTMP_FIELD "directProxy";
ZLMEDIAKIT_API const string kEnhanced = RTMP_FIELD "enhanced";

static onceToken token([]() {
    mINI::Instance()[kHandshakeSecond] = 15;
    mINI::Instance()[kKeepAliveSecond] = 15;
    mINI::Instance()[kDirectProxy] = 1;
    mINI::Instance()[kEnhanced] = 0;
});
} // namespace Rtmp

// //////////RTP配置///////////  [AUTO-TRANSLATED:23cbcb86]
// //////////RTP Configuration///////////
namespace Rtp {
#define RTP_FIELD "rtp."
ZLMEDIAKIT_API const string kVideoMtuSize = RTP_FIELD "videoMtuSize";
ZLMEDIAKIT_API const string kAudioMtuSize = RTP_FIELD "audioMtuSize";
ZLMEDIAKIT_API const string kRtpMaxSize = RTP_FIELD "rtpMaxSize";
ZLMEDIAKIT_API const string kLowLatency = RTP_FIELD "lowLatency";
ZLMEDIAKIT_API const string kH264StapA = RTP_FIELD "h264_stap_a";

static onceToken token([]() {
    mINI::Instance()[kVideoMtuSize] = 1400;
    mINI::Instance()[kAudioMtuSize] = 600;
    mINI::Instance()[kRtpMaxSize] = 10;
    mINI::Instance()[kLowLatency] = 0;
    mINI::Instance()[kH264StapA] = 1;
});
} // namespace Rtp

// //////////组播配置///////////  [AUTO-TRANSLATED:dc39b9d6]
// //////////Multicast Configuration///////////
namespace MultiCast {
#define MULTI_FIELD "multicast."
ZLMEDIAKIT_API const string kAddrMin = MULTI_FIELD "addrMin";
ZLMEDIAKIT_API const string kAddrMax = MULTI_FIELD "addrMax";
ZLMEDIAKIT_API const string kUdpTTL = MULTI_FIELD "udpTTL";

static onceToken token([]() {
    mINI::Instance()[kAddrMin] = "239.0.0.0";
    mINI::Instance()[kAddrMax] = "239.255.255.255";
    mINI::Instance()[kUdpTTL] = 64;
});
} // namespace MultiCast

// //////////录像配置///////////  [AUTO-TRANSLATED:19de3e96]
// //////////Recording Configuration///////////
namespace Record {
#define RECORD_FIELD "record."
ZLMEDIAKIT_API const string kAppName = RECORD_FIELD "appName";
ZLMEDIAKIT_API const string kSampleMS = RECORD_FIELD "sampleMS";
ZLMEDIAKIT_API const string kFileBufSize = RECORD_FIELD "fileBufSize";
ZLMEDIAKIT_API const string kFastStart = RECORD_FIELD "fastStart";
ZLMEDIAKIT_API const string kFileRepeat = RECORD_FIELD "fileRepeat";
ZLMEDIAKIT_API const string kEnableFmp4 = RECORD_FIELD "enableFmp4";

static onceToken token([]() {
    mINI::Instance()[kAppName] = "record";
    mINI::Instance()[kSampleMS] = 500;
    mINI::Instance()[kFileBufSize] = 64 * 1024;
    mINI::Instance()[kFastStart] = false;
    mINI::Instance()[kFileRepeat] = false;
    mINI::Instance()[kEnableFmp4] = false;
});
} // namespace Record

// //////////HLS相关配置///////////  [AUTO-TRANSLATED:873cc84c]
// //////////HLS Related Configuration///////////
namespace Hls {
#define HLS_FIELD "hls."
ZLMEDIAKIT_API const string kSegmentDuration = HLS_FIELD "segDur";
ZLMEDIAKIT_API const string kSegmentNum = HLS_FIELD "segNum";
ZLMEDIAKIT_API const string kSegmentKeep = HLS_FIELD "segKeep";
ZLMEDIAKIT_API const string kSegmentDelay = HLS_FIELD "segDelay";
ZLMEDIAKIT_API const string kSegmentRetain = HLS_FIELD "segRetain";
ZLMEDIAKIT_API const string kFileBufSize = HLS_FIELD "fileBufSize";
ZLMEDIAKIT_API const string kBroadcastRecordTs = HLS_FIELD "broadcastRecordTs";
ZLMEDIAKIT_API const string kDeleteDelaySec = HLS_FIELD "deleteDelaySec";
ZLMEDIAKIT_API const string kFastRegister = HLS_FIELD "fastRegister";

static onceToken token([]() {
    mINI::Instance()[kSegmentDuration] = 2;
    mINI::Instance()[kSegmentNum] = 3;
    mINI::Instance()[kSegmentKeep] = false;
    mINI::Instance()[kSegmentDelay] = 0;
    mINI::Instance()[kSegmentRetain] = 5;
    mINI::Instance()[kFileBufSize] = 64 * 1024;
    mINI::Instance()[kBroadcastRecordTs] = false;
    mINI::Instance()[kDeleteDelaySec] = 10;
    mINI::Instance()[kFastRegister] = false;
});
} // namespace Hls

// //////////Rtp代理相关配置///////////  [AUTO-TRANSLATED:7b285587]
// //////////Rtp Proxy Related Configuration///////////
namespace RtpProxy {
#define RTP_PROXY_FIELD "rtp_proxy."
ZLMEDIAKIT_API const string kDumpDir = RTP_PROXY_FIELD "dumpDir";
ZLMEDIAKIT_API const string kTimeoutSec = RTP_PROXY_FIELD "timeoutSec";
ZLMEDIAKIT_API const string kPortRange = RTP_PROXY_FIELD "port_range";
ZLMEDIAKIT_API const string kH264PT = RTP_PROXY_FIELD "h264_pt";
ZLMEDIAKIT_API const string kH265PT = RTP_PROXY_FIELD "h265_pt";
ZLMEDIAKIT_API const string kPSPT = RTP_PROXY_FIELD "ps_pt";
ZLMEDIAKIT_API const string kOpusPT = RTP_PROXY_FIELD "opus_pt";
ZLMEDIAKIT_API const string kGopCache = RTP_PROXY_FIELD "gop_cache";
ZLMEDIAKIT_API const string kRtpG711DurMs = RTP_PROXY_FIELD "rtp_g711_dur_ms";
ZLMEDIAKIT_API const string kUdpRecvSocketBuffer = RTP_PROXY_FIELD "udp_recv_socket_buffer";

static onceToken token([]() {
    mINI::Instance()[kDumpDir] = "";
    mINI::Instance()[kTimeoutSec] = 15;
    mINI::Instance()[kPortRange] = "30000-35000";
    mINI::Instance()[kH264PT] = 98;
    mINI::Instance()[kH265PT] = 99;
    mINI::Instance()[kPSPT] = 96;
    mINI::Instance()[kOpusPT] = 100;
    mINI::Instance()[kGopCache] = 1;
    mINI::Instance()[kRtpG711DurMs] = 100;
    mINI::Instance()[kUdpRecvSocketBuffer] = 4 * 1024 * 1024;
});
} // namespace RtpProxy

namespace Client {
ZLMEDIAKIT_API const string kNetAdapter = "net_adapter";
ZLMEDIAKIT_API const string kRtpType = "rtp_type";
ZLMEDIAKIT_API const string kRtspBeatType = "rtsp_beat_type";
ZLMEDIAKIT_API const string kRtspUser = "rtsp_user";
ZLMEDIAKIT_API const string kRtspPwd = "rtsp_pwd";
ZLMEDIAKIT_API const string kRtspPwdIsMD5 = "rtsp_pwd_md5";
ZLMEDIAKIT_API const string kTimeoutMS = "protocol_timeout_ms";
ZLMEDIAKIT_API const string kMediaTimeoutMS = "media_timeout_ms";
ZLMEDIAKIT_API const string kBeatIntervalMS = "beat_interval_ms";
ZLMEDIAKIT_API const string kBenchmarkMode = "benchmark_mode";
ZLMEDIAKIT_API const string kWaitTrackReady = "wait_track_ready";
ZLMEDIAKIT_API const string kPlayTrack = "play_track";
ZLMEDIAKIT_API const string kProxyUrl = "proxy_url";
ZLMEDIAKIT_API const string kRtspSpeed = "rtsp_speed";
ZLMEDIAKIT_API const string kLatency = "latency";
ZLMEDIAKIT_API const string kPassPhrase = "passPhrase";
} // namespace Client

} // namespace mediakit

#ifdef ENABLE_MEM_DEBUG

extern "C" {
extern void *__real_malloc(size_t);
extern void __real_free(void *);
extern void *__real_realloc(void *ptr, size_t c);
void *__wrap_malloc(size_t c);
void __wrap_free(void *ptr);
void *__wrap_calloc(size_t __nmemb, size_t __size);
void *__wrap_realloc(void *ptr, size_t c);
}

#define BLOCK_TYPES 16
#define MIN_BLOCK_SIZE 128

static int get_mem_block_type(size_t c) {
    int ret = 0;
    while (c > MIN_BLOCK_SIZE && ret + 1 < BLOCK_TYPES) {
        c >>= 1;
        ++ret;
    }
    return ret;
}

std::vector<size_t> getBlockTypeSize() {
    std::vector<size_t> ret;
    ret.resize(BLOCK_TYPES);
    size_t block_size = MIN_BLOCK_SIZE;
    for (auto i = 0; i < BLOCK_TYPES; ++i) {
        ret[i] = block_size;
        block_size <<= 1;
    }
    return ret;
}

class MemThreadInfo {
public:
    using Ptr = std::shared_ptr<MemThreadInfo>;
    atomic<uint64_t> mem_usage { 0 };
    atomic<uint64_t> mem_block { 0 };
    atomic<uint64_t> mem_block_map[BLOCK_TYPES];

    static MemThreadInfo *Instance(bool is_thread_local) {
        if (!is_thread_local) {
            static auto instance = new MemThreadInfo(is_thread_local);
            return instance;
        }
        static auto thread_local instance = new MemThreadInfo(is_thread_local);
        return instance;
    }

    ~MemThreadInfo() {
        // printf("%s %d\r\n", __FUNCTION__, (int) _is_thread_local);
    }

    MemThreadInfo(bool is_thread_local) {
        _is_thread_local = is_thread_local;
        if (_is_thread_local) {
            // 确保所有线程退出后才能释放全局内存统计器  [AUTO-TRANSLATED:edb51704]
            // Ensure that all threads exit before releasing the global memory statistics
            total_mem = Instance(false);
        }
        // printf("%s %d\r\n", __FUNCTION__, (int) _is_thread_local);
    }

    void *operator new(size_t sz) { return __real_malloc(sz); }

    void operator delete(void *ptr) { __real_free(ptr); }

    void addBlock(size_t c) {
        if (total_mem) {
            total_mem->addBlock(c);
        }
        mem_usage += c;
        ++mem_block_map[get_mem_block_type(c)];
        ++mem_block;
    }

    void delBlock(size_t c) {
        if (total_mem) {
            total_mem->delBlock(c);
        }
        mem_usage -= c;
        --mem_block_map[get_mem_block_type(c)];
        if (0 == --mem_block) {
            delete this;
        }
    }

private:
    bool _is_thread_local;
    MemThreadInfo *total_mem = nullptr;
};

class MemThreadInfoLocal {
public:
    MemThreadInfoLocal() {
        ptr = MemThreadInfo::Instance(true);
        ptr->addBlock(1);
    }

    ~MemThreadInfoLocal() { ptr->delBlock(1); }

    MemThreadInfo *get() const { return ptr; }

private:
    MemThreadInfo *ptr;
};

// 该变量主要确保线程退出后才能释放MemThreadInfo变量  [AUTO-TRANSLATED:a72494b0]
// This variable mainly ensures that the MemThreadInfo variable can be released only after the thread exits
static thread_local MemThreadInfoLocal s_thread_mem_info;

uint64_t getTotalMemUsage() {
    return MemThreadInfo::Instance(false)->mem_usage.load();
}

uint64_t getTotalMemBlock() {
    return MemThreadInfo::Instance(false)->mem_block.load();
}

uint64_t getTotalMemBlockByType(int type) {
    assert(type < BLOCK_TYPES);
    return MemThreadInfo::Instance(false)->mem_block_map[type].load();
}

uint64_t getThisThreadMemUsage() {
    return MemThreadInfo::Instance(true)->mem_usage.load();
}

uint64_t getThisThreadMemBlock() {
    return MemThreadInfo::Instance(true)->mem_block.load();
}

uint64_t getThisThreadMemBlockByType(int type) {
    assert(type < BLOCK_TYPES);
    return MemThreadInfo::Instance(true)->mem_block_map[type].load();
}

class MemCookie {
public:
    static constexpr uint32_t kMagic = 0xFEFDFCFB;
    uint32_t magic;
    uint32_t size;
    MemThreadInfo *alloc_info;
    char ptr;
};

#define MEM_OFFSET offsetof(MemCookie, ptr)

#if (defined(__linux__) && !defined(ANDROID)) || defined(__MACH__)
#define MAX_STACK_FRAMES 128
#define MEM_WARING
#include <execinfo.h>
#include <limits.h>
#include <sys/resource.h>
#include <sys/wait.h>

static void print_mem_waring(size_t c) {
    void *array[MAX_STACK_FRAMES];
    int size = backtrace(array, MAX_STACK_FRAMES);
    char **strings = backtrace_symbols(array, size);
    printf("malloc big memory:%d, back trace:\r\n", (int)c);
    for (int i = 0; i < size; ++i) {
        printf("[%d]: %s\r\n", i, strings[i]);
    }
    __real_free(strings);
}
#endif

static void init_cookie(MemCookie *cookie, size_t c) {
    cookie->magic = MemCookie::kMagic;
    cookie->size = c;
    cookie->alloc_info = s_thread_mem_info.get();
    cookie->alloc_info->addBlock(c);

#if defined(MEM_WARING)
    static auto env = getenv("MEM_WARN_SIZE");
    static size_t s_mem_waring_size = atoll(env ? env : "0");
    if (s_mem_waring_size > 1024 && c >= s_mem_waring_size) {
        print_mem_waring(c);
    }
#endif
}

static void un_init_cookie(MemCookie *cookie) {
    cookie->alloc_info->delBlock(cookie->size);
}

void *__wrap_malloc(size_t c) {
    c += MEM_OFFSET;
    auto cookie = (MemCookie *)__real_malloc(c);
    if (cookie) {
        init_cookie(cookie, c);
        return &cookie->ptr;
    }
    return nullptr;
}

void __wrap_free(void *ptr) {
    if (!ptr) {
        return;
    }
    auto cookie = (MemCookie *)((char *)ptr - MEM_OFFSET);
    if (cookie->magic != MemCookie::kMagic) {
        __real_free(ptr);
        return;
    }
    un_init_cookie(cookie);
    __real_free(cookie);
}

void *__wrap_calloc(size_t __nmemb, size_t __size) {
    auto size = __nmemb * __size;
    auto ret = malloc(size);
    if (ret) {
        memset(ret, 0, size);
    }
    return ret;
}

void *__wrap_realloc(void *ptr, size_t c) {
    if (!ptr) {
        return malloc(c);
    }

    auto cookie = (MemCookie *)((char *)ptr - MEM_OFFSET);
    if (cookie->magic != MemCookie::kMagic) {
        return __real_realloc(ptr, c);
    }

    un_init_cookie(cookie);
    c += MEM_OFFSET;
    cookie = (MemCookie *)__real_realloc(cookie, c);
    if (cookie) {
        init_cookie(cookie, c);
        return &cookie->ptr;
    }
    return nullptr;
}

void *operator new(std::size_t size) {
    auto ret = malloc(size);
    if (ret) {
        return ret;
    }
    throw std::bad_alloc();
}

void operator delete(void *ptr) noexcept {
    free(ptr);
}

void operator delete(void *ptr, std::size_t) noexcept {
    free(ptr);
}

void *operator new[](std::size_t size) {
    auto ret = malloc(size);
    if (ret) {
        return ret;
    }
    throw std::bad_alloc();
}

void operator delete[](void *ptr) noexcept {
    free(ptr);
}

void operator delete[](void *ptr, std::size_t) noexcept {
    free(ptr);
}
#endif
