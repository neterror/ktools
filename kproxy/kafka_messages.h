#pragma once

#include <QtCore>
#include <qjsondocument.h>
#include <qvariant.h>

constexpr const char kMediaProtobuf[] = "protobuf";
constexpr const char kMediaBinary[] = "binary";



template<typename T>
struct InputMessage {
    QString key;
    QString topic;
    qint32 offset;
    qint32 partition;
    T value;
};



struct OutputMessage {
    QString key;
    QString topic;
    QJsonDocument value;
};
