#pragma once
#include "kafka_proxy_v3.h"
#include <QtCore>

class TopicsDelete: public QObject {
    Q_OBJECT
    KafkaProxyV3& mProxy;
    QString mPattern;
    QQueue<KafkaProxyV3::Topic> mMarkedForDelete;
private slots:
    void onTopicList(QList<KafkaProxyV3::Topic> topics);
    void onDeleted();
public:
    TopicsDelete(KafkaProxyV3& proxy);
    ~TopicsDelete();
    void patternDelete(const QString& pattern);
    void executeDelete();
signals:
    void error(QString msg);
    void deleted();
    void confirm();
};
