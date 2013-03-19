// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "logger.h"

using namespace std;
using namespace cascadb;

LoggerLevel cascadb::g_logger_level = kInfo;

// set default logger
static ConsoleLogger console_logger;
Logger *cascadb::g_logger = &console_logger;

void cascadb::init_logger(LoggerLevel level)
{
    g_logger_level = level;
}

void cascadb::init_logger(const std::string &path, LoggerLevel level)
{
    g_logger_level = level;

    g_logger = new FileLogger(path);
    assert(g_logger);
}