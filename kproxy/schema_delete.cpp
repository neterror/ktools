#include "schema_delete.h"
#include "schema_registry.h"

SchemaDelete::SchemaDelete(SchemaRegistry& registry) : mRegistry(registry) {
    connect(&mRegistry, &SchemaRegistry::schemaList, this, &SchemaDelete::onSchemaToDeleteList);
    connect(&mRegistry, &SchemaRegistry::schemaDeleted, this, &SchemaDelete::onSchemaDeleted);
}

void SchemaDelete::deleteSchemaId(qint32 schemaId) {
    mDelete.schemaId = schemaId;
    mRegistry.getSchemas();
}


void SchemaDelete::onSchemaToDeleteList(QList<SchemaRegistry::Schema> list) {
    for (const auto& schema: list) {
        if (schema.schemaId == mDelete.schemaId) {
            mDelete = schema;
            mRegistry.deleteSchema(schema.subject, schema.version);
            return;
        }
    }
    emit error(QString("Could not find schemaId %1").arg(mDelete.schemaId));
}


void SchemaDelete::onSchemaDeleted(bool success, QString subject, qint32 version) {
    emit deleted(success, mDelete.schemaId, subject, version);
}


