#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include "http_client.h"
#include "kafka_messages.h"
#include "schema_registry.h"

class KafkaProtobufProducer : public QObject {
    Q_OBJECT
    std::unique_ptr<HttpClient> mProxy;
    std::unique_ptr<SchemaRegistry> mRegistry;

    QStateMachine mSM;
    QQueue<OutputBinaryMessage> mQueue;
                        
    void createObjects();
    QMap<QString, qint32> mTopicSchemaId;
    static QString randomId();
    bool mVerbose;
private slots:
    void onRequestClusterId();
    void onRequestSchema();
    void onSchemaReceived(QList<SchemaRegistry::Schema> schemas);

    void onSend();
    void onWaitForData();
public:
    KafkaProtobufProducer(bool verbose);
    static QByteArray addSchemaRegistryId(qint32 schemaId, const QByteArray& data);
    void send(OutputBinaryMessage message);
    void stop();
signals:
    void schemaReady();
    void newData();
    void error();

    void messageSent();
    void failed(QString message);
};

