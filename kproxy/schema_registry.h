#pragma once
#include "http_client.h"
#include <qjsondocument.h>

//the schema topic is created with
// --replication-factor 3 --config cleanup.policy=compact

class SchemaRegistry : public HttpClient {
    Q_OBJECT
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

    SchemaRegistry(QString server, QString user, QString password, bool verbose);

    void getSchemas();
    bool createSchema(const QString& subject, const QByteArray& schema, const QString& schemaType, const QList<Schema>& references);
    void deleteSchema(const QString& subject, qint32 version);
signals:
    void schemaList(QList<Schema> schemas);
    void schemaDeleted(bool success, QString subject, qint32 version);
    void schemaCreated(qint32 schemaId);
    void failed(QString message);

private:
    QJsonDocument createSchemaJson(const QString& subject, const QByteArray& schema, const QString& schemaType, const QList<Schema>& references);
};
