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
    void reportInputJson(const QJsonObject& obj);
    void reportInputBinary(const QJsonObject& obj);
    bool isValid(const QByteArray& data, qint32& schemaId);
public:

    QString instanceId() const {return mInstanceId;}
    void deleteInstanceId();

    KafkaProxyV2(QString server, QString user, QString password, QString mediaType = "");
    void requestInstanceId(QString groupName);
    void subscribe(const QStringList& topic);
    void getRecords();
    void stopReading();

    void commitOffset(QString topic, qint32 offset);
    void getOffset(const QString& group, const QString& topic);
    
signals:
    void obtainedInstanceId(QString intanceId);
    void subscribed(QString topics);
    void finished(QString message);
    void receivedJson(InputMessage<QJsonDocument> message);
    void receivedBinary(qint32 schemaId, InputMessage<QByteArray> message);
    void receivedOffset(QString topic, qint32 offset);
    void readingComplete();

    void failed(QString message);
    void offsetCommitted();
};
