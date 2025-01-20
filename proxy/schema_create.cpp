#include "schema_create.h"
#include "schema_registry.h"

SchemaCreate::SchemaCreate(SchemaRegistry& registry) : mRegistry(registry) {
    connect(&mRegistry, &SchemaRegistry::schemaCreated, this, &SchemaCreate::created);
    connect(&mRegistry, &SchemaRegistry::schemaList, this, &SchemaCreate::onSchemaList);
}



void SchemaCreate::createSchema(const QString& subject, const QByteArray& schema, const QString& schemaType, QList<qint32> references) {

    if (references.isEmpty()) {
        mRegistry.createSchema(subject, schema, schemaType, {});
    } else {
        mReferenceIds = references;
        mSubject = subject;
        mSchemaType = schemaType;
        mSchema = schema;
        mRegistry.getSchemas();
    }
}


std::optional<SchemaRegistry::Schema> SchemaCreate::find(const QList<SchemaRegistry::Schema>& list, qint32 id) {
    for (const auto& schema: list) {
        if (schema.schemaId == id) {
            return schema;
        }
    }
    return std::nullopt;
}


void SchemaCreate::onSchemaList(QList<SchemaRegistry::Schema> list) {
    QList<SchemaRegistry::Schema> references;
    for(const auto& id: mReferenceIds) {
        if (auto schema = find(list, id); schema) {
            references.append(*schema);
        } else {
            emit error(QString("Missing schemaId %1").arg(id));
            return;
        }
    }

    mRegistry.createSchema(mSubject, mSchema, mSchemaType, references);
}
