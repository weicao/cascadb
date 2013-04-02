// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>
#include <algorithm>

#include "msg.h"
#include "keycomp.h"

using namespace std;
using namespace cascadb;

size_t Msg::size() const
{
    size_t sz = 1 + 4 + key.size();
    if (type == Put) {
        sz += (4 + value.size());
    }
    return sz;
}

bool Msg::read_from(BlockReader& reader)
{
    if (!reader.readUInt8((uint8_t*)&type)) return false;
    if (!reader.readSlice(key)) return false;
    if (type == Put) {
        if (!reader.readSlice(value)) return false;
    }
    return true;
}

bool Msg::write_to(BlockWriter& writer)
{
    if (!writer.writeUInt8((uint8_t)type)) return false;
    if (!writer.writeSlice(key)) return false;
    if (type == Put) {
        if (!writer.writeSlice(value)) return false;
    }
    return true;
}

void Msg::destroy()
{
    switch(type) {
    case Put: {
        key.destroy();
        value.destroy();
    }
    break;
    case Del: {
        key.destroy();
    }
    break;
    default:
    ;
    }
}

MsgBuf::~MsgBuf()
{
    for(ContainerType::iterator it = container_.begin(); 
        it != container_.end(); it++ ) {
        it->destroy();
    }
    container_.clear();
}

void MsgBuf::write(const Msg& msg)
{
    MsgBuf::Iterator it = container_.lower_bound(msg, KeyComp(comp_));
    if (it == end() || it->key != msg.key) {
        container_.insert(it, msg);
        size_ += msg.size();
    } else {
        size_ -= it->size();
        it->destroy();
        *it = msg;
        size_ += msg.size();
    }
}

void MsgBuf::append(MsgBuf::Iterator first, MsgBuf::Iterator last)
{
    MsgBuf::Iterator it = container_.begin();
    MsgBuf::Iterator jt = first;
    KeyComp comp(comp_);

    while(jt != last) {
        it = container_.lower_bound(it, jt->key, comp);
        if (it == container_.end() || it->key != jt->key) {
            // important, it maybe invalid after insertion
            it = container_.insert(it, *jt);
            size_ += jt->size();
        } else {
            size_ -= it->size();
            it->destroy();
            *it = *jt;
            size_ += it->size();
        }
        jt ++;
    }
}

MsgBuf::Iterator MsgBuf::find(Slice key)
{
    return container_.lower_bound(key, KeyComp(comp_));
}

void MsgBuf::clear()
{
    container_.clear();
    size_ = 0;
}

bool MsgBuf::read_from(BlockReader& reader)
{
    uint32_t cnt = 0;
    if (!reader.readUInt32(&cnt)) return false;
    // container_.resize(cnt);
    // for (size_t i = 0; i < cnt; i++ ) {
    //     if (!container_[i].read_from(reader)) return false;
    //     size_ += container_[i].size();
    // }
    for (size_t i = 0; i < cnt; i++ ) {
        Msg msg;
        if (!msg.read_from(reader)) return false;
        size_ += msg.size();
        container_.push_back(msg);
    }
    return true;
}

bool MsgBuf::write_to(BlockWriter& writer)
{
    if (!writer.writeUInt32(container_.size())) return false;
    for (ContainerType::iterator it = container_.begin();
        it != container_.end(); it ++ ) {
        if (!it->write_to(writer)) return false;
    }
    return true;
}
