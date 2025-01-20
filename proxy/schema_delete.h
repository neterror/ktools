#pragma once
#include "schema_registry.h"
#include <QtCore>

class SchemaDelete: public QObject {
    Q_OBJECT
    SchemaRegistry& mRegistry;
    SchemaRegistry::Schema mDelete;

private slots:
    void onSchemaToDeleteList(QList<SchemaRegistry::Schema> list);
    void onSchemaDeleted(bool success, QString subject, qint32 version);
public:
    SchemaDelete(SchemaRegistry& registry);
    void deleteSchemaId(qint32 schemaId);
signals:
    void error(QString msg);
    void deleted(bool success, qint32 schemaId, QString subject, qint32 version);
};
