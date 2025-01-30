#include "schema_registry.h"
#include "http_client.h"
#include <qdebug.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qnetworkreply.h>


SchemaRegistry::SchemaRegistry(QString server, QString user, QString password, bool verbose) : HttpClient(server, user, password, verbose) {

}


void SchemaRegistry::getSchemas() {
    mRest.get(requestV3("schemas"), this, [this](QRestReply& reply){
        if (!reply.isHttpStatusSuccess()) {
            emit failed(QString("error: %1").arg(reply.httpStatus()));
            return;
        }

        auto json = reply.readJson();
        if (!json || !json->isArray()) {
            qWarning() << "Unexpected json reply" << json;
            emit failed(QString("Unexpected json reply %1").arg(json->toJson()));
            return;
        }
        
        QList<Schema> report;
        for(const auto item: json->array()) {
            auto schema = item.toObject();

            auto result = Schema{
                schema["id"].toInt(),
                schema["schema"].toString(),
                schema["schemaType"].toString(),
                schema["subject"].toString(),
                schema["version"].toInt()
            };

            if (schema.contains("references") && schema["references"].isArray()){
                auto references = schema["references"].toArray();
                for(const auto& reference: references) {
                    auto obj = reference.toObject();
                    Reference ref{
                        obj["name"].toString(),
                        obj["subject"].toString(),
                        obj["version"].toInt()
                    };
                    result.references << ref;
                }
            }

            
            report.append(result);
        }
        emit schemaList(report);
    });
}


bool SchemaRegistry::createSchema(const QString& subject,
                                    const QByteArray& schema,
                                    const QString& schemaType,
                                    const QList<SchemaRegistry::Schema>& references)
{
    auto json = createSchemaJson(subject, schema, schemaType, references);
    if (json.isEmpty()) {
        return false;
    }
    auto request = requestV3("subjects/" + subject + "/versions");
    auto reply = mRest.post(request, json, this, [this](QRestReply &reply) {
        bool success = true;
        if (reply.error() != QNetworkReply::NoError) {
            qWarning() << "error: " << reply.error() << reply.errorString();
            success = false;
        }

        auto json = reply.readJson();
        if (!json) {
            emit failed("Failed to read JSON reply");
            return;
        }
        
        auto obj = json->object();
        if (obj.contains("id")) {
            emit schemaCreated(obj["id"].toInt());
        } else {
            auto msg = json->toJson(QJsonDocument::Indented);
            emit failed(msg);
        }
    });
    
    return true;
}


void SchemaRegistry::deleteSchema(const QString& subject, qint32 version) {
    QString url = QString("subjects/%1/versions/%2").arg(subject).arg(version);
    auto reply = mRest.deleteResource(requestV3(url), this, [this,subject,version](QRestReply &reply) {
        auto json = reply.readJson();
        if (json && json->isObject()) {
            auto obj = json->object();
            if (obj.contains("error_code")) {
                qWarning() << "schema deletion failed:" << obj["message"].toString();
                emit schemaDeleted(false, subject, version);
                return;
            }
        }

        emit schemaDeleted(true, subject, version);
    });
}



QJsonDocument SchemaRegistry::createSchemaJson(const QString& subject,
                                               const QByteArray& schema,
                                               const QString& schemaType,
                                               const QList<Schema>& references)

{
    auto json = QJsonObject {
        {"schema", QString(schema)},
        {"schemaType", schemaType}
    };

    auto array = QJsonArray();
    for (const auto& schema: references) {
        array.append(QJsonObject{
                {"name", schema.subject},
                {"subject", schema.subject},
                {"version", schema.version},
            });
    }

    if (!array.isEmpty()) {
        json["references"] = array;
    }

    return QJsonDocument(json);
}
