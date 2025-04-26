#pragma once
#include "http_client.h"
#include <qjsondocument.h>
#include <qstringview.h>
#include "kafka_messages.h"
#include <QElapsedTimer>


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
    void deleteOldInstanceId(const QString& instance, const QString& group);

    KafkaProxyV2(QString server, QString user, QString password, bool verbose, QString mediaType = "");
    void initialize(QString groupName) override;
    void subscribe(const QStringList& topic);
    void getRecords();
    void stopReading();

    void commitOffset(QString topic, qint32 offset);
    void commitAllOffsets();
    void getOffset(const QString& group, const QString& topic);

    void sendBinary(const QString& key, const QString& topic, const QList<QByteArray>& data) override;
    void sendJson(const QString& key, const QString& topic, const QJsonDocument& json) override;
signals:
    void subscribed(QString topics);
    void finished(QString message);
    void receivedJson(InputMessage<QJsonDocument> message);
    void receivedBinary(qint32 schemaId, InputMessage<QByteArray> message);
    void readingComplete();
    void oldInstanceDeleted(QString message);

    void offsetCommitted();
};
