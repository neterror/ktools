#pragma once
#include "http_client.h"
#include <qjsondocument.h>
#include <qstringview.h>
#include "kafka_messages.h"


class KafkaProxyV2 : public HttpClient {
    Q_OBJECT
    QString mInstanceId;
    QString mGroupName;
    QString mMediaType;
    QNetworkReply* mPendingRead {nullptr};

    qint32 reportInputJson(const QJsonObject& obj);
    qint32 reportInputBinary(const QJsonObject& obj);
    
public:

    QString instanceId() const {return mInstanceId;}
    void deleteInstanceId();

    KafkaProxyV2(QString server, QString user, QString password, QString mediaType = "");
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
    void receivedJson(InputMessage<QJsonDocument> message);
    void receivedBinary(InputMessage<QByteArray> message);

    void failed(QString message);
    void receivedOffset(qint32 offset);
    void offsetCommitted();
};
