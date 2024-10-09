/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include "SvacDemuxer.h"
#include "Util/logger.h"
#include "Extension/Factory.h"
#include "Util/File.h"
using namespace std;
using namespace toolkit;

namespace mediakit {


/******************************************************************************
*                             Redefine Data Type
*******************************************************************************/
// Redefine integer data type
typedef signed char        INT8;
typedef signed short       INT16;
typedef signed int         INT32;
typedef unsigned char      UINT8;
typedef unsigned short     UINT16;
typedef unsigned int       UINT32;

#if defined(__GNUC__)
typedef          long long INT64;
typedef unsigned long long UINT64;
#else
typedef          __int64   INT64;
typedef unsigned __int64   UINT64;
#endif

// Redefine float data type
typedef float              FLOAT32;
typedef double             FLOAT64;

// Redefine bool data type
typedef char               BOOL8;
typedef short              BOOL16;
typedef int                BOOL32;

static INT32 SVAC3D_NalIsStart(UINT32 type)
{
    if (type == 0x7 || // SVAC_SPS
        type == 0x8 || // SVAC_PPS
        type == 0x3) { // SVAC_PH
        return 1;
    }
    return 0;
}

static INT32 SVAC3D_NalIsPatch(UINT32 type)
{
    if (type == 0x4 || // SVAC_IDR
        type == 0x1 || // SVAC_NRAP
        type == 0x2 || // SVAC_RAPI
        type == 0xe || // SVAC_CRR_RL
        type == 0x11) { // SVAC_CRR_DP
        return 1;
    }
    return 0;
}

static INT32 SVAC3DecLoadAU(UINT8* pStream, UINT32 iStreamLen, UINT32* pFrameLen, UINT32* pType)
{
    UINT32 i;
    UINT32 type;
    UINT64 state = 0xffffffffffff;
    BOOL32 bFrameStartFound = 0;

    *pFrameLen = 0;
    if (pStream == NULL) {
        return -1;
    }

    for (i = 0; i < iStreamLen; i++) {
        state = (state << 0x8) | pStream[i];
        if ((state & 0xffffffff0000) != 0x000000010000) {
            continue;
        }

        type = (UINT32)(state & 0x1f);

        //char hexBuf[50]; // 足够大的缓冲区
        //formatHex(pStream, hexBuf, sizeof(hexBuf));


        if (SVAC3D_NalIsPatch(type) == 1) {
            if (i + 1 < iStreamLen &&
                pStream[i + 1] == 0) {
                bFrameStartFound = 1;
            }
            //printf("a obj:%p i:%d hexBuf: %s type:%d\n", pStream + i, i, hexBuf, type);
            continue;
        }

        if (SVAC3D_NalIsStart(type) == 1) {
            if (bFrameStartFound == 1) {
                *pFrameLen = i - 0x5;
                *pType = type;
                //printf("b obj:%p i:%d hexBuf: %s type:%d\n", pStream+ i, i, hexBuf, type);
                return 0;
            }
        }
    }

    *pFrameLen = i;
    return -1;
}

SvacDemuxer::~SvacDemuxer() {
    closeMP4();
}

void SvacDemuxer::openMP4(const string &file) {
    closeMP4();
    _raw_data = std::move(File::loadFile(file));
    getAllTracks();
    _duration_ms = 0;
}

void SvacDemuxer::closeMP4() {
}

int SvacDemuxer::getAllTracks() {
    onVideoTrack(0,CodecSVAC3,2560,1440,0,0);
    return 0;
}

void SvacDemuxer::onVideoTrack(uint32_t track, CodecId codec_id, int width, int height, const void *extra, size_t bytes) {
    auto video = Factory::getTrackByCodecId(codec_id);
    if (!video) {
        return;
    }
    video->setIndex(track);
    _tracks.emplace(track, video);
    if (extra && bytes) {
        video->setExtraData((uint8_t *)extra, bytes);
    }
}

void SvacDemuxer::onAudioTrack(uint32_t track, CodecId codec_id, int channel_count, int bit_per_sample, int sample_rate, const void *extra, size_t bytes) {
    auto audio = Factory::getTrackByCodecId(codec_id, sample_rate, channel_count, bit_per_sample / channel_count);
    if (!audio) {
        return;
    }
    audio->setIndex(track);
    _tracks.emplace(track, audio);
    if (extra && bytes) {
        audio->setExtraData((uint8_t *)extra, bytes);
    }
}

int64_t SvacDemuxer::seekTo(int64_t stamp_ms) {
    if(stamp_ms ==0){
        _raw_data_offset = 0;
        return 0;
    }
    return -1;
    return stamp_ms;
}

struct Context {
    Context(SvacDemuxer *ptr) : thiz(ptr) {}
    SvacDemuxer *thiz;
    int flags = 0;
    int64_t pts = 0;
    int64_t dts = 0;
    uint32_t track_id = 0;
    BufferRaw::Ptr buffer;
};

Frame::Ptr SvacDemuxer::readFrame(bool &keyFrame, bool &eof) {
    keyFrame = false;
    eof = false;
    UINT32 frame_len = 0;
    UINT32 frame_type;
    auto ret = SVAC3DecLoadAU((UINT8 *)_raw_data.data() + _raw_data_offset, _raw_data.size() - _raw_data_offset, &frame_len, &frame_type);
    if(ret == -1){
        eof = true;
        return nullptr;
    }
    Context ctx(this);
    ctx.pts = _dts;
    ctx.dts = _dts;
    ctx.flags = 0;
    ctx.track_id = 0;

    ctx.buffer = ctx.thiz->_buffer_pool.obtain2();
    ctx.buffer->setCapacity(frame_len + 1);
    ctx.buffer->setSize(frame_len);
    auto buf =  ctx.buffer->data();
    memcpy(buf, _raw_data.data() + _raw_data_offset, frame_len);
    _raw_data_offset += frame_len;

    keyFrame = true;
    _dts += 40;
    return makeFrame(ctx.track_id, ctx.buffer, ctx.pts, ctx.dts);
}

Frame::Ptr SvacDemuxer::makeFrame(uint32_t track_id, const Buffer::Ptr &buf, int64_t pts, int64_t dts) {
    auto it = _tracks.find(track_id);
    if (it == _tracks.end()) {
        return nullptr;
    }
    Frame::Ptr ret;
    auto codec = it->second->getCodecId();
    switch (codec) {
        case CodecSVAC3:{
            ret = Factory::getFrameFromBuffer(codec, std::move(buf), dts, pts);
            break;
        }

        default: {
            ret = Factory::getFrameFromBuffer(codec, std::move(buf), dts, pts);
            break;
        }
    }
    if (ret) {
        ret->setIndex(track_id);
        it->second->inputFrame(ret);
    }
    return ret;
}

vector<Track::Ptr> SvacDemuxer::getTracks(bool ready) const {
    vector<Track::Ptr> ret;
    for (auto &pr : _tracks) {
        if (ready && !pr.second->ready()) {
            continue;
        }
        ret.push_back(pr.second);
    }
    return ret;
}

uint64_t SvacDemuxer::getDurationMS() const {
    return _duration_ms;
}

}//namespace mediakit
