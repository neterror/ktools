#include "schema_registry.h"
#include "http_client.h"
#include <qdebug.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qnetworkreply.h>


SchemaRegistry::SchemaRegistry(QString server, QString user, QString password) : HttpClient(server, user, password) {

}


void SchemaRegistry::getSchemas() {
    mRest.get(requestV3("schemas"), this, [this](QRestReply& reply){
        if (!reply.isHttpStatusSuccess()) {
            emit ready(false, QString("error: %1").arg(reply.httpStatus()));
            return;
        }

        auto json = reply.readJson();
        if (!json || !json->isArray()) {
            qWarning() << "Unexpected json reply" << json;
            emit ready(false, QString("Unexpected json reply %1").arg(json->toJson()));
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
        emit registeredSchemas(report);
        emit ready(true);
    });
}


bool SchemaRegistry::registerProtobuf(const QString& subject,
                                      const QByteArray& protofileData,
                                      const QString& referenceSubject,
                                      qint32 referenceVersion)
{
    auto json = registerProtobufRequest(subject, protofileData, referenceSubject, referenceVersion);
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
            emit ready(false, "Failed to read JSON reply");
            return;
        }
        
        auto obj = json->object();
        QString msg;
        if (obj.contains("id")) {
            msg = QString("Registered schema_id %1").arg(obj["id"].toInt());
            success = true;
        } else {
            if (obj.contains("error_code")) {
                msg = json->toJson(QJsonDocument::Indented);
                success = false;
            }
        }
        emit ready(success, msg);
    });
    
    return true;
}


void SchemaRegistry::deleteSchema(const QString& subject, qint32 version) {
    QString url = QString("subjects/%1/versions/%2").arg(subject).arg(version);
    auto reply = mRest.deleteResource(requestV3(url), this, [this,subject,version](QRestReply &reply) {
        auto json = reply.readJson();
        if (json || json->isObject()) {
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



QJsonDocument SchemaRegistry::registerProtobufRequest(const QString& subject,
                                                      const QByteArray& protofileData,
                                                      const QString& referenceSubject,
                                                      qint32 referencedVersion)
{
    auto json = QJsonObject {
        {"schema", QString(protofileData)},
        {"schemaType", "PROTOBUF"}
    };

    if (!referenceSubject.isEmpty()) {
        json["references"] = QJsonArray {
            QJsonObject{
                {"name", referenceSubject},
                {"subject", referenceSubject},
                {"version", referencedVersion},
            }
        };
    }
    return QJsonDocument(json);
}
