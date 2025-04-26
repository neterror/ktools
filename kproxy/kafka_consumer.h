#pragma once
#include "kafka_proxy_v2.h"
#include "kafka_messages.h"
#include <QStateMachine>
#include <QObject>
#include <qjsondocument.h>
#include <qstringview.h>

class KafkaConsumer : public QObject {
    Q_OBJECT
    std::unique_ptr<KafkaProxyV2> mProxy;
    QStateMachine mSM;
    QString mGroupName;

    QString generateRandomId();
    void createProxy(bool verbose, const QString& mediaType);
    QString instanceBackupFile(const QString& group);

private slots:
    void onSuccess();
    void onFailed();
public:
    KafkaConsumer(const QString& group, const QStringList& topics, bool verbose, const QString& mediaType);
    void start();
    void stop();
signals:
    void failed(QString message);
    void receivedJson(InputMessage<QJsonDocument> message);
    void receivedBinary(qint32 schemaId, InputMessage<QByteArray> message);
    void stopRequest();
    void finished(QString message);
};
