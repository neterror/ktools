#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include "kafka_proxy_v3.h"
#include "kafka_messages.h"
#include "schema_registry.h"

class KafkaProtobufProducer : public QObject {
    Q_OBJECT
    std::unique_ptr<KafkaProxyV3> mProxy;
    std::unique_ptr<SchemaRegistry> mRegistry;

    QStateMachine mSM;
    QQueue<OutputBinaryMessage> mQueue;
                        
    void createObjects();
    QMap<QString, qint32> mTopicSchemaId;
private slots:
    void onRequestClusterId();
    void onRequestSchema();
    void onSchemaReceived(QList<SchemaRegistry::Schema> schemas);

    void onSend();
    void onWaitForData();
public:
    KafkaProtobufProducer();
    void send(OutputBinaryMessage message);
    void stop();
signals:
    void schemaReady();
    void newData();
    void error();

    void messageSent();
    void failed(QString message);
};

