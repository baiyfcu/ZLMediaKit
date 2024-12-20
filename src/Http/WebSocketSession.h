﻿/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#ifndef ZLMEDIAKIT_WEBSOCKETSESSION_H
#define ZLMEDIAKIT_WEBSOCKETSESSION_H

#include "HttpSession.h"
#include "Network/TcpServer.h"

/**
 * 数据发送拦截器
 * Data Send Interceptor
 
 * [AUTO-TRANSLATED:5eaf7060]
 */
class SendInterceptor{
public:
    using onBeforeSendCB =std::function<ssize_t (const toolkit::Buffer::Ptr &buf)>;

    virtual ~SendInterceptor() = default;
    virtual void setOnBeforeSendCB(const onBeforeSendCB &cb) = 0;
};

/**
 * 该类实现了Session派生类发送数据的截取
 * 目的是发送业务数据前进行websocket协议的打包
 * This class implements the interception of data sent by the Session derived class.
 * The purpose is to package the websocket protocol before sending business data.
 
 * [AUTO-TRANSLATED:15c96e5f]
 */
template <typename SessionType>
class SessionTypeImp : public SessionType, public SendInterceptor{
public:
    using Ptr = std::shared_ptr<SessionTypeImp>;

    SessionTypeImp(const mediakit::Parser &header, const mediakit::HttpSession &parent, const toolkit::Socket::Ptr &pSock) :
            SessionType(pSock) {}

    /**
     * 设置发送数据截取回调函数
     * @param cb 截取回调函数
     * Set the send data interception callback function
     * @param cb Interception callback function
     
     * [AUTO-TRANSLATED:3e74fcdd]
     */
    void setOnBeforeSendCB(const onBeforeSendCB &cb) override {
        _beforeSendCB = cb;
    }

protected:
    /**
     * 重载send函数截取数据
     * @param buf 需要截取的数据
     * @return 数据字节数
     * Overload the send function to intercept data
     * @param buf Data to be intercepted
     * @return Number of data bytes
     
     * [AUTO-TRANSLATED:d3304949]
     */
    ssize_t send(toolkit::Buffer::Ptr buf) override {
        if (_beforeSendCB) {
            return _beforeSendCB(buf);
        }
        return SessionType::send(std::move(buf));
    }

private:
    onBeforeSendCB _beforeSendCB;
};

template <typename SessionType>
class SessionCreator {
public:
    // 返回的Session必须派生于SendInterceptor，可以返回null  [AUTO-TRANSLATED:6cc95812]
    // The returned Session must be derived from SendInterceptor, and can return null
    toolkit::Session::Ptr operator()(const mediakit::Parser &header, const mediakit::HttpSession &parent, const toolkit::Socket::Ptr &pSock, mediakit::WebSocketHeader::Type &data_type){
        return std::make_shared<SessionTypeImp<SessionType> >(header,parent,pSock);
    }
};

/**
* 通过该模板类可以透明化WebSocket协议，
* 用户只要实现WebSock协议下的具体业务协议，譬如基于WebSocket协议的Rtmp协议等
 * Through this template class, the WebSocket protocol can be transparently implemented.
 * Users only need to implement specific business protocols under the WebSock protocol, such as the Rtmp protocol based on the WebSocket protocol.
 
 * [AUTO-TRANSLATED:07e2e8a5]
*/
template<typename Creator, typename HttpSessionType = mediakit::HttpSession, mediakit::WebSocketHeader::Type DataType = mediakit::WebSocketHeader::TEXT>
class WebSocketSessionBase : public HttpSessionType {
public:
    WebSocketSessionBase(const toolkit::Socket::Ptr &pSock) : HttpSessionType(pSock){}

    // 收到eof或其他导致脱离TcpServer事件的回调  [AUTO-TRANSLATED:6d48b35c]
    // Callback when receiving eof or other events that cause disconnection from TcpServer
    void onError(const toolkit::SockException &err) override{
        HttpSessionType::onError(err);
        if(_session){
            _session->onError(err);
        }
    }
    // 每隔一段时间触发，用来做超时管理  [AUTO-TRANSLATED:823ffe1f]
    // Triggered every period of time, used for timeout management
    void onManager() override{
        if (_session) {
            _session->onManager();
        } else {
            HttpSessionType::onManager();
        }
        if (!_session) {
            // websocket尚未链接  [AUTO-TRANSLATED:164129da]
            // websocket is not yet connected
            return;
        }
        if (_recv_ticker.elapsedTime() > 30 * 1000) {
            HttpSessionType::shutdown(toolkit::SockException(toolkit::Err_timeout, "websocket timeout"));
        } else if (_recv_ticker.elapsedTime() > 10 * 1000) {
            // 没收到回复，每10秒发送次ping 包  [AUTO-TRANSLATED:31b4dc13]
            // No reply received, send a ping packet every 10 seconds
            mediakit::WebSocketHeader header;
            header._fin = true;
            header._reserved = 0;
            header._opcode = mediakit::WebSocketHeader::PING;
            header._mask_flag = false;
            HttpSessionType::encode(header, nullptr);
        }
    }

    void attachServer(const toolkit::Server &server) override{
        HttpSessionType::attachServer(server);
        _weak_server = const_cast<toolkit::Server &>(server).shared_from_this();
    }

protected:
    /**
     * websocket客户端连接上事件
     * @param header http头
     * @return true代表允许websocket连接，否则拒绝
     * websocket client connection event
     * @param header http header
     * @return true means allowing websocket connection, otherwise refuse
     
     * [AUTO-TRANSLATED:d857fb0f]
     */
    bool onWebSocketConnect(const mediakit::Parser &header) override{
        // 创建websocket session类  [AUTO-TRANSLATED:099f6963]
        // Create websocket session class
        auto data_type = DataType;
        _session = _creator(header, *this, HttpSessionType::getSock(), data_type);
        if (!_session) {
            // 此url不允许创建websocket连接  [AUTO-TRANSLATED:47480366]
            // This url is not allowed to create websocket connection
            return false;
        }
        auto strongServer = _weak_server.lock();
        if (strongServer) {
            _session->attachServer(*strongServer);
        }

        // 此处截取数据并进行websocket协议打包  [AUTO-TRANSLATED:89053032]
        // Intercept data here and package it with websocket protocol
        std::weak_ptr<WebSocketSessionBase> weakSelf = std::static_pointer_cast<WebSocketSessionBase>(HttpSessionType::shared_from_this());
        std::dynamic_pointer_cast<SendInterceptor>(_session)->setOnBeforeSendCB([weakSelf, data_type](const toolkit::Buffer::Ptr &buf) {
            auto strongSelf = weakSelf.lock();
            if (strongSelf) {
                mediakit::WebSocketHeader header;
                header._fin = true;
                header._reserved = 0;
                header._opcode = data_type;
                header._mask_flag = false;
                strongSelf->HttpSessionType::encode(header, buf);
            }
            return buf->size();
        });

        // 允许websocket客户端  [AUTO-TRANSLATED:3a06f181]
        // Allow websocket client
        return true;
    }

    /**
     * 开始收到一个webSocket数据包
     * Start receiving a webSocket data packet
     
     * [AUTO-TRANSLATED:0f16a5b5]
     */
    void onWebSocketDecodeHeader(const mediakit::WebSocketHeader &packet) override{
        // 新包，原来的包残余数据清空掉  [AUTO-TRANSLATED:0fd23412]
        // New package, the residual data of the original package is cleared
        _payload_section.clear();
    }

    /**
     * 收到websocket数据包负载
     * Receive websocket data packet payload
     
     * [AUTO-TRANSLATED:b317988d]
     */
    void onWebSocketDecodePayload(const mediakit::WebSocketHeader &packet,const uint8_t *ptr,size_t len,size_t recved) override {
        _payload_section.append((char *)ptr,len);
    }

    /**
     * 接收到完整的一个webSocket数据包后回调
     * @param header 数据包包头
     * Callback after receiving a complete webSocket data packet
     * @param header Data packet header
     
     * [AUTO-TRANSLATED:f506a7c5]
     */
    void onWebSocketDecodeComplete(const mediakit::WebSocketHeader &header_in) override {
        auto header = const_cast<mediakit::WebSocketHeader&>(header_in);
        auto  flag = header._mask_flag;
        header._mask_flag = false;
        _recv_ticker.resetTime();
        switch (header._opcode){
            case mediakit::WebSocketHeader::CLOSE:{
                HttpSessionType::encode(header,nullptr);
                HttpSessionType::shutdown(toolkit::SockException(toolkit::Err_shutdown, "recv close request from client"));
                break;
            }
            
            case mediakit::WebSocketHeader::PING:{
                header._opcode = mediakit::WebSocketHeader::PONG;
                HttpSessionType::encode(header,std::make_shared<toolkit::BufferString>(_payload_section));
                break;
            }
            
            case mediakit::WebSocketHeader::CONTINUATION:
            case mediakit::WebSocketHeader::TEXT:
            case mediakit::WebSocketHeader::BINARY:{
                if (!header._fin) {
                    // 还有后续分片数据, 我们先缓存数据，所有分片收集完成才一次性输出  [AUTO-TRANSLATED:75d21e17]
                    // There is subsequent fragment data, we cache the data first, and output it all at once after all fragments are collected
                    _payload_cache.append(std::move(_payload_section));
                    if (_payload_cache.size() < MAX_WS_PACKET) {
                        // 还有内存容量缓存分片数据  [AUTO-TRANSLATED:621da1f9]
                        // There is memory capacity to cache fragment data
                        break;
                    }
                    // 分片缓存太大，需要清空  [AUTO-TRANSLATED:98882d1f]
                    // Fragment cache is too large, need to be cleared
                }
                if (!_session)
                    break;
                // 最后一个包  [AUTO-TRANSLATED:dcf860cf]
                // Last package
                if (_payload_cache.empty()) {
                    // 这个包是唯一个分片  [AUTO-TRANSLATED:94802e24]
                    // This package is the only fragment
                    _session->onRecv(std::make_shared<mediakit::WebSocketBuffer>(header._opcode, header._fin, std::move(_payload_section)));
                    break;
                }

                // 这个包由多个分片组成  [AUTO-TRANSLATED:044123f1]
                // This package consists of multiple fragments
                _payload_cache.append(std::move(_payload_section));
                _session->onRecv(std::make_shared<mediakit::WebSocketBuffer>(header._opcode, header._fin, std::move(_payload_cache)));
                _payload_cache.clear();
                break;
            }
            
            default: break;
        }
        _payload_section.clear();
        header._mask_flag = flag;
    }

    /**
     * 发送数据进行websocket协议打包后回调
     * Callback after sending data and packaging it with websocket protocol
     
     * [AUTO-TRANSLATED:3327ce78]
    */
    void onWebSocketEncodeData(toolkit::Buffer::Ptr buffer) override{
        HttpSessionType::send(std::move(buffer));
    }

private:
    std::string _payload_cache;
    std::string _payload_section;
    std::weak_ptr<toolkit::Server> _weak_server;
    toolkit::Session::Ptr _session;
    Creator _creator;
    toolkit::Ticker _recv_ticker;
};


template<typename SessionType,typename HttpSessionType = mediakit::HttpSession, mediakit::WebSocketHeader::Type DataType = mediakit::WebSocketHeader::TEXT>
class WebSocketSession : public WebSocketSessionBase<SessionCreator<SessionType>,HttpSessionType,DataType>{
public:
    WebSocketSession(const toolkit::Socket::Ptr &pSock) : WebSocketSessionBase<SessionCreator<SessionType>,HttpSessionType,DataType>(pSock){}
};

#endif //ZLMEDIAKIT_WEBSOCKETSESSION_H
