#pragma once
#include "kafka_proxy_v2.h"
#include "kafka_messages.h"
#include <QStateMachine>
#include <QObject>
#include <qjsondocument.h>
#include <qstringview.h>

class KafkaConsumer : public QObject {
    Q_OBJECT
    KafkaProxyV2& mKafkaProxy;
    QStateMachine mSM;
    QStringList mTopics;
    QString mGroup;

private slots:
    void onSuccess();
    void onFailed();
public:
    KafkaConsumer(KafkaProxyV2& proxy, QString group, QStringList topic);
    void start();
    void stop();
signals:
    void receivedJson(InputMessage<QJsonDocument> message);
    void receivedBinary(InputMessage<QByteArray> message);
    void readAgain();
    void stopRequest();
    void finished(QString message);
};
