#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include "http_client.h"
#include "kafka_messages.h"
#include "schema_registry.h"
#include <pqueue/pqueue.h>

class KafkaProtobufProducer : public QObject {
    Q_OBJECT
    std::unique_ptr<HttpClient> mProxy;
    std::unique_ptr<SchemaRegistry> mRegistry;

    QStateMachine mSM;
    std::unique_ptr<PQueue> mPersistentQueue;
                        
    void createObjects();
    QMap<QString, qint32> mTopicSchemaId;
    static QString randomId();
    bool mVerbose;
    QString mLocalSchemaFile;
    void saveLocalSchema(const QList<SchemaRegistry::Schema>& schemas);
    QList<SchemaRegistry::Schema> loadLocalSchema();
    void updateSchemaIds(const QList<SchemaRegistry::Schema>& schemas);
private slots:
    void onRequestClusterId();
    void onRequestSchema();
    void onSchemaReadingFailed(const QString& reason);
    void onSchemaReceived(QList<SchemaRegistry::Schema> schemas);

    void onSend();
    void onFailedSend();
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

