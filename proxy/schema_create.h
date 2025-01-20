#pragma once
#include "schema_registry.h"
#include <QtCore>

class SchemaCreate: public QObject {
    Q_OBJECT
    SchemaRegistry& mRegistry;
    QList<SchemaRegistry::Schema> mReferences;
    QString mSubject;
    QByteArray mSchema;
    QString mSchemaType;

    QList<qint32> mReferenceIds;

    static std::optional<SchemaRegistry::Schema> find(const QList<SchemaRegistry::Schema>& list, qint32 id);

private slots:
    void onSchemaList(QList<SchemaRegistry::Schema> list);
public:
    SchemaCreate(SchemaRegistry& registry);
    void createSchema(const QString& subject, const QByteArray& schema, const QString& schemaType, QList<qint32> references);
signals:
    void error(QString msg);
    void created(int schemaId);
};
