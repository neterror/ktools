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
        errorMsg = "Unexpected JSON reply. Missing data";
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
            emit failed(errorMsg);
        } else {
            auto obj = data[0].toObject();
            mClusterID = obj["cluster_id"].toString();
            emit initialized(mClusterID);
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
            emit failed(errorMsg);
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
        emit topicList(result);
    });
}

void KafkaProxyV3::readTopicConfig(const QString& name) {
    auto url = QString("v3/clusters/%1/topics/%2/configs").arg(mClusterID).arg(name);
    mRest.get(requestV3(url), this, [this](QRestReply& reply) {
        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (data.empty()) {
            emit failed(errorMsg);
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
            emit topicCreated();
        } else {
            QString msg = "Failed to create the topic: ";
            if (auto doc = reply.readJson(); doc) {
                auto obj = doc->object();
                if (obj.contains("message")) {
                    msg += obj["message"].toString();
                }
            }
            emit failed(msg);
        }
    });
}

void KafkaProxyV3::deleteTopic(const QString& topic) {
    auto url = QString("v3/clusters/%1/topics/%2").arg(mClusterID).arg(topic);
    mRest.deleteResource(requestV3(url), this, [this](QRestReply &reply) {
        if (reply.isHttpStatusSuccess()) {
            emit topicDeleted();
        } else {
            QString msg = "Failed to delete the topic: ";
            if (auto doc = reply.readJson(); doc) {

                auto obj = doc->object();
                if (obj.contains("message")) {
                    msg += obj["message"].toString();
                }
            }
            emit failed(msg);
        }
    });
}



void KafkaProxyV3::sendMessage(const QString& key, const QString& topic, const QJsonDocument& json) {
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
            emit failed("Unkown error");
            return;
        }

        auto obj = data->object();
        auto errorCode = obj["error_code"].toInt();
        if (errorCode == 200) {
            emit messageSent();
        } else {
            auto errorMsg = obj["message"].toString();
            emit failed(errorMsg);
        }
    });
}


void KafkaProxyV3::listGroups() {
    auto url = QString("v3/clusters/%1/consumer-groups").arg(mClusterID);
    mRest.get(requestV3(url), this, [this](QRestReply& reply){
        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (!errorMsg.isEmpty()) {
            emit failed(errorMsg);
            return;
        }

        QList<Group> result;
        for(const auto& item: data) {
            auto obj = item.toObject();
            result.append({
                    obj["consumer_group_id"].toString(),
                    obj["state"].toString()
                });
        }
        emit groupList(result);
    });
}


void KafkaProxyV3::getGroupLag(const QString& group) {
    auto url = QString("v3/clusters/%1/consumer-groups/%2/lags").arg(mClusterID).arg(group);
    mRest.get(requestV3(url), this, [this,group](QRestReply& reply){
        if (reply.httpStatus() == 404) {
            emit failed(QString("group %1 not found").arg(group));
            return;
        }
        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (!errorMsg.isEmpty()) {
            emit failed(errorMsg);
            return;
        }

        QList<GroupLag> result;
        for(const auto& item: data) {
            auto obj = item.toObject();
            result.append({
                    obj["consumer_group_id"].toString(),
                    obj["consumer_id"].toString(),
                    obj["topic_name"].toString(),
                    obj["current_offset"].toInt(),
                    obj["log_end_offset"].toInt(),
                    obj["lag"].toInt()
                });
        }
        emit groupLags(result);
    });
}

void KafkaProxyV3::getGroupLagSummary(const QString& group) {
    auto url = QString("v3/clusters/%1/consumer-groups/%2/lag-summary").arg(mClusterID).arg(group);
    mRest.get(requestV3(url), this, [this,group](QRestReply& reply){
        if (reply.httpStatus() == 404) {
            emit failed(QString("group %1 not found").arg(group));
            return;
        }

        auto data = reply.readJson();
        if (!data || !data->isObject()) {
            emit failed("Could not read lag summary");
            return;
        }
        auto obj = data->object();
        GroupLagSummary result;
        result.groupName = obj["consumer_group_id"].toString();
        result.consumerId = obj["consumer_id"].toString();
        result.topic = obj["max_lag_topic_name"].toString();
        result.maxLag = obj["max_lag"].toInt();
        result.totalLag = obj["total_lag"].toInt();

        emit groupLagSummary(result);
    });
}
        

void KafkaProxyV3::getGroupConsumers(const QString& group) {
    auto url = QString("v3/clusters/%1/consumer-groups/%2/consumers").arg(mClusterID).arg(group);
    mRest.get(requestV3(url), this, [this,group](QRestReply& reply){
        if (reply.httpStatus() == 404) {
            emit failed(QString("group %1 not found").arg(group));
            return;
        }

        QString errorMsg;
        auto data = getDataArray(reply, errorMsg);
        if (!errorMsg.isEmpty()) {
            emit failed(errorMsg);
            return;
        }

        QList<Consumer> result;
        for(const auto& item: data) {
            auto obj = item.toObject();
            result.append({
                    obj["consumer_group_id"].toString(),
                    obj["consumer_id"].toString(),
                    obj["client_id"].toString(),
                });
        }
        emit consumerList(result);
    });
}
        
