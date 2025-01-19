#include "kafka_proxy_v3.h"
#include <qjsondocument.h>

KafkaProxyV3::KafkaProxyV3(QString server, QString user, QString password) : HttpClient(server, user, password) {
    
}

QJsonArray KafkaProxyV3::getDataArray(QRestReply& reply, QString& errorMsg) {
    if (!reply.isHttpStatusSuccess()) {
        errorMsg = QString("HTTP error %1").arg(reply.httpStatus());
        return {};
    }

    auto json = reply.readJson();
    if (!json || !((*json)["data"].isArray())) {
        errorMsg = "Unexpected JSON reply in GetClusterID";
        return {};
    }
    auto result = (*json)["data"].toArray();
    if (result.empty()) {
        errorMsg = "Empty data array";
    }
    return result;
}


void KafkaProxyV3::getClusterId() {
    mRest.get(requestV3("v3/clusters"), this, [this](QRestReply& reply){
        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (data.empty()) {
            emit ready(false, errorMsg);
            emit initialized(false);
        } else {
            auto obj = data[0].toObject();
            mClusterID = obj["cluster_id"].toString();
            emit ready(true, mClusterID);
            emit initialized(true);
        }
    });
}


void KafkaProxyV3::listTopics() {
    auto url = QString("v3/clusters/%1/topics").arg(mClusterID);
    auto request = requestV3(url);
    mRest.get(request, this, [this](QRestReply& reply) {
        QString errorMsg;
        auto array = getDataArray(reply, errorMsg);
        if (array.empty()) {
            emit ready(false, errorMsg);
            return;
        }

        QList<Topic> result;
        for(const auto& item: array) {
            auto obj = item.toObject();
            result.emplaceBack(Topic{
                    obj["topic_name"].toString(),
                    obj["is_internal"].toBool(),
                });
        }
        emit topics(result);
        emit ready(true, "");
    });
}

void KafkaProxyV3::readTopicConfig(const QString& name) {
    auto url = QString("v3/clusters/%1/topics/%2/configs").arg(mClusterID).arg(name);
    mRest.get(requestV3(url), this, [this](QRestReply& reply) {
        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (data.empty()) {
            emit ready(false, errorMsg);
            emit initialized(false);
            return;
        }

        QList<TopicConfig> result;
        for(const auto& item: data) {
            auto obj = item.toObject();
            TopicConfig config;
            config.name = obj["name"].toString();
            config.value = obj["value"].toString();

            config.isDefault = obj["is_default"].toBool();
            config.isReadOnly = obj["is_read_only"].toBool();
            config.isSensitive = obj["is_sensitive"].toBool();

            result << config;
        }
        emit topicConfig(result);
        emit ready(true);
    });
}


void KafkaProxyV3::createTopic(const QString& topic, bool isCompact, qint32 replicationFactor) {
    auto payload = QJsonObject {
        {"topic_name", topic},
        {"replication_factor", replicationFactor}
    };

    QJsonArray configs;
    configs << QJsonObject{
            {"name", "cleanup.policy"}, {"value", isCompact ? "compact" : "delete"},
        };
    payload["configs"] = configs;
    
    auto url = QString("v3/clusters/%1/topics").arg(mClusterID);
    mRest.post(requestV3(url), QJsonDocument(payload), this, [this](QRestReply &reply) {
        if (reply.isHttpStatusSuccess()) {
            emit ready(true);
        } else {
            QString msg = "Failed to create the topic: ";
            if (auto doc = reply.readJson(); doc) {
                auto obj = doc->object();
                if (obj.contains("message")) {
                    msg += obj["message"].toString();
                }
            }
            emit ready(false, msg);
        }
    });
}

void KafkaProxyV3::deleteTopic(const QString& topic) {
    auto url = QString("v3/clusters/%1/topics/%2").arg(mClusterID).arg(topic);
    mRest.deleteResource(requestV3(url), this, [this](QRestReply &reply) {
        if (reply.isHttpStatusSuccess()) {
            emit ready(true);
        } else {
            QString msg = "Failed to delete the topic: ";
            if (auto doc = reply.readJson(); doc) {

                auto obj = doc->object();
                if (obj.contains("message")) {
                    msg += obj["message"].toString();
                }
            }
            emit ready(false, msg);
        }
    });
}



void KafkaProxyV3::sendProtobufData(const QString& topic, const QString& key, const QJsonDocument& json) {
    auto url = QString("v3/clusters/%1/topics/%2/records").arg(mClusterID).arg(topic);
    QJsonObject payload;
    if (!key.isEmpty()) {
        payload["key"] = QJsonObject {
            {"type", "STRING"},
            {"data", key}
        };
    }

    payload["value"] = QJsonObject {
        {"data", json.object()}
    };
    
    mRest.post(requestV3(url), QJsonDocument(payload), this, [this](QRestReply &reply) {
        auto data = reply.readJson();
        if (!data || !data->isObject()) {
            emit ready(false, "Unkown error");
            return;
        }

        auto obj = data->object();
        auto errorCode = obj["error_code"].toInt();
        if (errorCode == 200) {
            emit ready(true);
        } else {
            auto errorMsg = obj["message"].toString();
            emit ready(false, errorMsg);
        }
    });
}
