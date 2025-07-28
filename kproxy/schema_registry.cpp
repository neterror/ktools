#include "schema_registry.h"
#include "http_client.h"
#include <qdebug.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qnetworkreply.h>


SchemaRegistry::SchemaRegistry(QString server, QString user, QString password, bool verbose) : HttpClient(server, user, password, verbose) {

}


void SchemaRegistry::readSchema(quint32 schemaId) {
    auto path = QString("schemas/ids/%1").arg(schemaId);
    mRest.get(requestV3(path), this, [this](QRestReply& reply){
        if (!reply.isHttpStatusSuccess()) {
            emit failed(QString("error: %1").arg(reply.httpStatus()));
            return;
        }
	
	if (auto json = reply.readJson()) {
	    auto text = (*json)["schema"].toString();
	    emit schemaText(text);
	} else {
	    emit schemaText("Error: Failed to retrieve the schema");
	}
    });
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

void SchemaRegistry::getLatestSchemaId(const QString& subject) {
    QString url = "/subjects/" + subject + "/versions/-1";
    auto request = requestV3(url); //-1 is for the latest version
    mRest.get(request, this, [this,subject](QRestReply& reply){
        if (reply.error() != QNetworkReply::NoError) {
            emit subjectSchemaId(subject, -1);
        } else {
            auto schemaId = -1;
            if (auto json = reply.readJson(); json) {
                auto obj = json->object();
                schemaId =  obj.contains("id") ? obj["id"].toInt() : -1;
            }
            emit subjectSchemaId(subject, schemaId);
        }
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
    QString url = QString("subjects/%1/versions/%2?permanent=true").arg(subject).arg(version);
    auto reply = mRest.deleteResource(requestV3(url), this, [this,subject,version](QRestReply &reply) {
        auto json = reply.readJson();
        if (json && json->isObject()) {
            auto obj = json->object();
            if (obj.contains("error_code")) {
                qWarning() << "schema deletion failed:" << obj["message"].toString();
                emit schemaDeleted(false);
                return;
            }
        }

        emit schemaDeleted(true);
    });
}


void SchemaRegistry::deleteSchema(const QString& subject, bool permanently) {
    QString encodedSubject = QUrl::toPercentEncoding(subject, QByteArray(), "/");
    QString url = QString("subjects/%1").arg(encodedSubject);
    //1. Do a soft delete (as described here): https://docs.confluent.io/platform/current/schema-registry/schema-deletion-guidelines.html#hard-delete-schema
    qDebug().noquote() << "soft delete:" << url;
    mRest.deleteResource(requestV3(url), this, [this,encodedSubject,permanently](QRestReply &reply) {
        auto json = reply.readJson();
        if (json && json->isObject()) {
            auto obj = json->object();
            if (obj.contains("error_code")) {
                qWarning() << "schema deletion failed:" << obj["message"].toString();
                emit schemaDeleted(false);
            }
        }

        if (!permanently) {
            emit schemaDeleted(true);
            return;
        }
            
	//2. Now do the hard delete
        //repeat again the deletion, this time with permanent=true
        QString url = QString("subjects/%1?permanent=true").arg(encodedSubject);
	qDebug().noquote() << "permanent delete:" << url;

        mRest.deleteResource(requestV3(url), this, [this,encodedSubject,permanently](QRestReply &reply) {
            auto json = reply.readJson();
            if (json && json->isObject()) {
                auto obj = json->object();
                if (obj.contains("error_code")) {
                    qWarning() << "schema deletion failed:" << obj["message"].toString();
                    emit schemaDeleted(false);
                    return;
                }
            }
        });

        emit schemaDeleted(true);
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
