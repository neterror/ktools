#pragma once
#include "http_client.h"
#include <qjsondocument.h>

//the schema topic is created with
// --replication-factor 3 --config cleanup.policy=compact

class SchemaRegistry : public HttpClient {
    Q_OBJECT
    QJsonDocument registerProtobufRequest(const QString& subject, const QByteArray& protofileData, const QString& referenceSubject, qint32 referenceVersion);
public:
    struct Reference {
        QString name;
        QString subject;
        qint32 version;
    };

    struct Schema {
        qint32 schemaId;
        QString schema;
        QString schemaType;
        QString subject;
        qint32 version;
        QList<Reference> references;
    };

    SchemaRegistry(QString server, QString user, QString password);

    void getSchemas();
    bool registerProtobuf(const QString& subject, const QByteArray& protofileData, const QString& referenceSubject = "", qint32 referenceVersion = -1);
    void deleteSchema(const QString& subject, qint32 version);
signals:
    void registeredSchemas(QList<Schema> schemas);
    void schemaDeleted(bool success, QString subject, qint32 version);
};
