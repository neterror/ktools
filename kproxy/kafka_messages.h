#pragma once

#include <QtCore>

struct InputMessage {
    QString key;
    qint32 offset;
    qint32 partition;
    QString topic;
    QJsonObject value;
};


struct OutputMessage {
    QString topic;
    QString key;
    QJsonDocument value;
};
