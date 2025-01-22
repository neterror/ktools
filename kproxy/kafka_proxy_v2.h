#pragma once
#include "http_client.h"
#include <qjsondocument.h>
#include "kafka_messages.h"


class KafkaProxyV2 : public HttpClient {
    Q_OBJECT
    QString mInstanceId;
    QString mGroupName;
    QNetworkReply* mPendingRead {nullptr};
public:

    QString instanceId() const {return mInstanceId;}
    void deleteInstanceId();

    KafkaProxyV2(QString server, QString user, QString password);
    void requestInstanceId(const QString& groupName);
    void subscribe(const QString& topic);
    void getRecords();
    void stopReading();

    void commitOffset(QString topic, qint32 offset);
    void getOffset(const QString& group, const QString& topic);
    
signals:
    void obtainedInstanceId(QString intanceId);
    void subscribed(QString topics);
    void finished(QString message);
    void received(InputMessage message);
    void failed(QString message);
    void receivedOffset(qint32 offset);
    void offsetCommitted();
};
