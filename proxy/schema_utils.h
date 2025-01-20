#pragma once
#include "schema_registry.h"
#include <QtCore>

class SchemaUtils: public QObject {
    Q_OBJECT
    SchemaRegistry& mRegistry;
    SchemaRegistry::Schema mSchema;

private slots:
    void onSchemaList(QList<SchemaRegistry::Schema> list);
    void onSchemaDeleted(bool success, QString subject, qint32 version);
public:
    SchemaUtils(SchemaRegistry& registry);
    void deleteSchemaId(qint32 schemaId);
signals:
    void error(QString msg);
    void deleted(bool success, qint32 schemaId, QString subject, qint32 version);
};
