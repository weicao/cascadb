// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_LOGGER_H_
#define CASCADB_UTIL_LOGGER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <iostream>
#include <sstream>

#include "sys/sys.h"

namespace cascadb {

enum LoggerLevel {
    kTrace,
    kInfo,
    kWarn,
    kError,
    kFatal
};

static const char* logger_level_names[] = {
    "TRACE",
    "INFO",
    "WARN",
    "ERROR",
    "FATAL"
};

class Logger {
public:
    virtual ~Logger() {}

    virtual void write(const std::string &line) {
        fprintf(file_, "%s\n", line.c_str());
        fflush(file_);
    }
protected:
    FILE *file_;
};

class ConsoleLogger : public Logger {
public:
    ConsoleLogger()
    {
        file_ = stderr;
    }
};

class FileLogger : public Logger {
public:
    FileLogger(const std::string& path)
    {
        file_ = fopen(path.c_str(), "a+");
        if (!file_) {
            fprintf(stderr, "cannot open logger %s\n", path.c_str());
            exit(1);
        }
    }

    ~FileLogger() {
        fclose(file_);
    }
};

class LoggerFormat {
public:
    LoggerFormat(Logger* logger) 
    : logger_(logger)
    {
    }

    ~LoggerFormat() {
        logger_->write(os_.str());
    }

    std::ostream& get(LoggerLevel level) {
        os_ << "| " << now() << " | " << logger_level_names[level];
        return os_;
    }

private:
    Logger *logger_;
    std::ostringstream os_;
};

extern LoggerLevel g_logger_level;
extern Logger *g_logger;

extern void init_logger(LoggerLevel level);

extern void init_logger(const std::string &path, LoggerLevel level);

#define SHORT_FILE (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define LOG(level, msg) \
    if (level >= g_logger_level) {\
        LoggerFormat(g_logger).get(level) << " | " << SHORT_FILE << ":" << __LINE__ << " | " << msg;\
    }

#define LOG_TRACE(msg)  LOG(kTrace, msg)
#define LOG_INFO(msg)   LOG(kInfo, msg)
#define LOG_WARN(msg)   LOG(kWarn, msg)
#define LOG_ERROR(msg)  LOG(kError, msg)
#define LOG_FATAL(msg)  LOG(kFatal, msg)

}

#endif
