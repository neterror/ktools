#pragma once

#include <QtCore>

struct InputMessage {
    QString key;
    QString topic;
    qint32 offset;
    qint32 partition;
    QJsonObject value;
};


struct OutputMessage {
    QString key;
    QString topic;
    QJsonDocument value;
};
