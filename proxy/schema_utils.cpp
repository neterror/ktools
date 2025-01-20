#include "schema_utils.h"
#include "schema_registry.h"

SchemaUtils::SchemaUtils(SchemaRegistry& registry) : mRegistry(registry) {
    connect(&mRegistry, &SchemaRegistry::registeredSchemas, this, &SchemaUtils::onSchemaList);
    connect(&mRegistry, &SchemaRegistry::schemaDeleted, this, &SchemaUtils::onSchemaDeleted);
}

void SchemaUtils::deleteSchemaId(qint32 schemaId) {
    mSchema.schemaId = schemaId;
    mRegistry.getSchemas();
}


void SchemaUtils::onSchemaList(QList<SchemaRegistry::Schema> list) {
    for (const auto& schema: list) {
        if (schema.schemaId == mSchema.schemaId) {
            mSchema = schema;
            mRegistry.deleteSchema(schema.subject, schema.version);
            return;
        }
    }
    emit error(QString("Could not find schemaId %1").arg(mSchema.schemaId));
}


void SchemaUtils::onSchemaDeleted(bool success, QString subject, qint32 version) {
    emit deleted(success, mSchema.schemaId, subject, version);
}
