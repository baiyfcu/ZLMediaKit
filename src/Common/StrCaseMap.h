/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#ifndef ZLMEDIAKIT_STRCASEMAP_H
#define ZLMEDIAKIT_STRCASEMAP_H

#include <map>
#include <string>
#include <utility>
#include "Util/util.h"

namespace mediakit {

struct StrCaseCompare {
    bool operator()(const std::string &__x, const std::string &__y) const { return strcasecmp(__x.data(), __y.data()) < 0; }
};

class StrCaseMap : public std::multimap<std::string, std::string, StrCaseCompare> {
public:
    using Super = std::multimap<std::string, std::string, StrCaseCompare>;

    std::string &operator[](const std::string &k) {
        auto it = find(k);
        if (it == end()) {
            it = Super::emplace(k, "");
        }
        return it->second;
    }

    template <typename K, typename V>
    void emplace(K &&k, V &&v) {
        auto it = find(k);
        if (it != end()) {
            return;
        }
        Super::emplace(std::forward<K>(k), std::forward<V>(v));
    }

    template <typename K, typename V>
    void emplace_force(K &&k, V &&v) {
        Super::emplace(std::forward<K>(k), std::forward<V>(v));
    }
};

} // namespace mediakit

#endif // ZLMEDIAKIT_STRCASEMAP_H
