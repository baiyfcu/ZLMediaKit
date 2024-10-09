﻿/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#ifndef ZLMEDIAKIT_SVAC_DEMUXER_H
#define ZLMEDIAKIT_SVAC_DEMUXER_H
#ifdef ENABLE_MP4
#include "MP4.h"
#include "Extension/Track.h"
#include "Util/ResourcePool.h"
namespace mediakit {

class SvacDemuxer : public TrackSource {
public:
    using Ptr = std::shared_ptr<SvacDemuxer>;

    ~SvacDemuxer() override;

    /**
     * 打开文件
     * @param file mp4文件路径
     * Open file
     * @param file mp4 file path
     
     * [AUTO-TRANSLATED:a64c5a6b]
     */
    void openMP4(const std::string &file);

    /**
     * @brief 关闭 mp4 文件
     * @brief Close mp4 file
     
     * [AUTO-TRANSLATED:527865d9]
     */
    void closeMP4();

    /**
     * 移动时间轴至某处
     * @param stamp_ms 预期的时间轴位置，单位毫秒
     * @return 时间轴位置
     * Move timeline to a specific location
     * @param stamp_ms Expected timeline position, in milliseconds
     * @return Timeline position
     
     * [AUTO-TRANSLATED:51ce0f6d]
     */
    int64_t seekTo(int64_t stamp_ms);

    /**
     * 读取一帧数据
     * @param keyFrame 是否为关键帧
     * @param eof 是否文件读取完毕
     * @return 帧数据,可能为空
     * Read a frame of data
     * @param keyFrame Whether it is a key frame
     * @param eof Whether the file has been read completely
     * @return Frame data, may be empty
     
     * [AUTO-TRANSLATED:adf550de]
     */
    Frame::Ptr readFrame(bool &keyFrame, bool &eof);

    /**
     * 获取所有Track信息
     * @param trackReady 是否要求track为就绪状态
     * @return 所有Track
     * Get all Track information
     * @param trackReady Whether to require the track to be ready
     * @return All Tracks
     
     * [AUTO-TRANSLATED:c07ad51a]
     */
    std::vector<Track::Ptr> getTracks(bool trackReady) const override;

    /**
     * 获取文件长度
     * @return 文件长度，单位毫秒
     * Get file length
     * @return File length, in milliseconds
     
     
     * [AUTO-TRANSLATED:dcd865d6]
     */
    uint64_t getDurationMS() const;

private:
    int getAllTracks();
    void onVideoTrack(uint32_t track_id, CodecId object, int width, int height, const void *extra, size_t bytes);
    void onAudioTrack(uint32_t track_id, CodecId object, int channel_count, int bit_per_sample, int sample_rate, const void *extra, size_t bytes);
    Frame::Ptr makeFrame(uint32_t track_id, const toolkit::Buffer::Ptr &buf, int64_t pts, int64_t dts);

private:
    std::string _raw_data;
    int32_t _raw_data_offset = 0;
    uint64_t _dts = 0;

    uint64_t _duration_ms = 0;
    std::unordered_map<int, Track::Ptr> _tracks;
    toolkit::ResourcePool<toolkit::BufferRaw> _buffer_pool;
};


}//namespace mediakit
#endif//ENABLE_MP4
#endif //ZLMEDIAKIT_MP4DEMUXER_H
