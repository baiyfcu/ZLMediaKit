/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#ifndef ZLMEDIAKIT_SVAC3_H
#define ZLMEDIAKIT_SVAC3_H

#include "Extension/Frame.h"
#include "Extension/Track.h"

#define SVAC3_TYPE(v) ( ((uint64_t)0xffff00000000  | (uint8_t)(v))& 0x1f)

namespace mediakit{

void splitSvac3(const char *ptr, size_t len, size_t prefix, const std::function<void(const char *, size_t, size_t)> &cb);
size_t prefixSizeSvac3(const char *ptr, size_t len);

template<typename Parent>
class Svac3FrameHelper : public Parent{
public:
    friend class FrameImp;
    friend class toolkit::ResourcePool_l<Svac3FrameHelper>;
    using Ptr = std::shared_ptr<Svac3FrameHelper>;

    enum {
        NAL_IDR = 4,
        NAL_NRAP = 1,
        NAL_RAPI = 2,
        NAL_CRR_RL = 0xe,
        NAL_CRR_DP = 0x11,
        NAL_SPS = 7,
        NAL_PPS = 8,
        NAL_PH = 3,
    };

    template<typename ...ARGS>
    Svac3FrameHelper(ARGS &&...args): Parent(std::forward<ARGS>(args)...) {
        this->_codec_id = CodecSVAC3;
    }

    bool keyFrame() const override {
        return true;
        auto nal_ptr = (uint8_t *) this->data() + this->prefixSize();
        return SVAC3_TYPE(*nal_ptr) == NAL_IDR && decodeAble();
    }

    bool configFrame() const override {
        auto nal_ptr = (uint8_t *) this->data() + this->prefixSize();
        switch (SVAC3_TYPE(*nal_ptr)) {
            case NAL_SPS:
            case NAL_PPS: return true;
            default: return false;
        }
    }

    bool dropAble() const override {
        auto nal_ptr = (uint8_t *) this->data() + this->prefixSize();
        switch (SVAC3_TYPE(*nal_ptr)) {
            default: return false;
        }
    }

    bool decodeAble() const override {
        auto nal_ptr = (uint8_t *) this->data() + this->prefixSize();
        auto type = SVAC3_TYPE(*nal_ptr);
        // 多slice情况下, first_mb_in_slice 表示其为一帧的开始  [AUTO-TRANSLATED:80e88e88]
        // // In the case of multiple slices, first_mb_in_slice indicates the start of a frame
        return true;
    }
};

/**
 * 264帧类
 * 264 frame class
 
 * [AUTO-TRANSLATED:342ccb1e]
 */
using Svac3Frame = Svac3FrameHelper<FrameImp>;

/**
 * 防止内存拷贝的H264类
 * 用户可以通过该类型快速把一个指针无拷贝的包装成Frame类
 * H264 class that prevents memory copying
 * Users can quickly wrap a pointer into a Frame class without copying using this type
 
 * [AUTO-TRANSLATED:ff9be1c8]
 */
using Svac3FrameNoCacheAble = Svac3FrameHelper<FrameFromPtr>;

/**
 * 264视频通道
 * 264 video channel
 
 * [AUTO-TRANSLATED:6936e76d]
 */
class Svac3Track : public VideoTrack {
public:
    using Ptr = std::shared_ptr<Svac3Track>;

    /**
     * 不指定sps pps构造h264类型的媒体
     * 在随后的inputFrame中获取sps pps
     * Construct a media of h264 type without specifying sps pps
     * Get sps pps in the subsequent inputFrame
     
     * [AUTO-TRANSLATED:84d01c7f]
     */
    Svac3Track() = default;

    /**
     * 构造h264类型的媒体
     * @param sps sps帧数据
     * @param pps pps帧数据
     * @param sps_prefix_len 264头长度，可以为3个或4个字节，一般为0x00 00 00 01
     * @param pps_prefix_len 264头长度，可以为3个或4个字节，一般为0x00 00 00 01
     * Construct a media of h264 type
     * @param sps sps frame data
     * @param pps pps frame data
     * @param sps_prefix_len 264 header length, can be 3 or 4 bytes, generally 0x00 00 00 01
     * @param pps_prefix_len 264 header length, can be 3 or 4 bytes, generally 0x00 00 00 01
     
     
     * [AUTO-TRANSLATED:702c1433]
     */
    Svac3Track(const std::string &sps, const std::string &pps, int sps_prefix_len = 4, int pps_prefix_len = 4);

    bool ready() const override;
    CodecId getCodecId() const override;
    int getVideoHeight() const override;
    int getVideoWidth() const override;
    float getVideoFps() const override;
    bool inputFrame(const Frame::Ptr &frame) override;
    toolkit::Buffer::Ptr getExtraData() const override;
    void setExtraData(const uint8_t *data, size_t size) override;
    bool update() override;
    std::vector<Frame::Ptr> getConfigFrames() const override;

private:
    Sdp::Ptr getSdp(uint8_t payload_type) const override;
    Track::Ptr clone() const override;
    bool inputFrame_l(const Frame::Ptr &frame);
    void insertConfigFrame(const Frame::Ptr &frame);

private:
    bool _latest_is_config_frame = false;
    int _width = 0;
    int _height = 0;
    float _fps = 0;
    std::string _sps;
    std::string _pps;
};

template <typename FrameType>
Frame::Ptr createConfigFrame(const std::string &data, uint64_t dts, int index) {
    auto frame = FrameImp::create<FrameType>();
    frame->_prefix_size = 4;
    frame->_buffer.assign("\x00\x00\x00\x01", 4);
    frame->_buffer.append(data);
    frame->_dts = dts;
    frame->setIndex(index);
    return frame;
}

}//namespace mediakit

#endif //ZLMEDIAKIT_SVAC3_H
