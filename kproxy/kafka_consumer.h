#pragma once
#include "kafka_proxy_v2.h"
#include <QStateMachine>
#include <QObject>

class KafkaConsumer : public QObject {
    Q_OBJECT
    KafkaProxyV2& mKafkaProxy;
    QStateMachine mSM;
    QString mTopic;
    QString mGroup;

    qint32 mLastOffset;
private slots:
    void onSuccess();
    void onFailed();
public:
    KafkaConsumer(KafkaProxyV2& proxy, QString group, QString topic);
    void start();
    void stop();
signals:
    void received(KafkaProxyV2::Message message);
    void commitOffset();
    void readAgain();
    void stopRequest();
    void finished(QString message);
};
