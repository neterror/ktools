#include "kafka_proxy_v2.h"
#include "http_client.h"
#include <qsslerror.h>

KafkaProxyV2::KafkaProxyV2(QString server, QString user, QString password) : HttpClient(server, user, password) {
}


void KafkaProxyV2::requestInstanceId(const QString& groupName) {
    auto url = QString("consumers/%1").arg(groupName);
    auto json = QJsonObject {
        {"format", "protobuf"},

        {"fetch.min.bytes", 1}, //when at least 1 byte is available - report it immediately, don't wait the timeout
        {"consumer.request.timeout.ms", 10000},
        
        {"auto.offset.reset", "earliest"},
        {"auto.commit.enable", "false"},

        {"auto.commit.enable", false} //explicitly set which messages are processed
    };

    mGroupName = groupName;
    mRest.post(requestV2(url), QJsonDocument{json}, this, [this](QRestReply &reply) {
        auto json = reply.readJson();
        if (!json || !json->isObject()) {
            emit failed("Failed to obtain instanceId");
            return;
        }
        
        auto obj = json->object();
        if (obj.contains("instance_id")) {
            mInstanceId = obj["instance_id"].toString();
            emit obtainedInstanceId(mInstanceId);
        } else {
            emit failed(obj["message"].toString());
        }
    });
}

void KafkaProxyV2::deleteInstanceId() {
    auto url = QString("consumers/%1/instances/%2").arg(mGroupName).arg(mInstanceId);
    mRest.deleteResource(requestV2(url), this, [this](QRestReply &reply) {
        QString message;
        if (reply.isHttpStatusSuccess()) {
            message = QString("instance %1 deleted").arg(mInstanceId);
        } else {
            QStringList words;
            words << "error deleting instance" << mInstanceId << reply.readText();
            message = words.join(" ");
        }
        
        qDebug() << "instanceID" << mInstanceId << "deleted";
        emit finished(message);
    });
}

void KafkaProxyV2::subscribe(const QString& topic) {
    qDebug().noquote() << "Joining consumer group" << mGroupName << "instance" << mInstanceId;

    //1. Subscribe for a topic
    auto url = QString("consumers/%1/instances/%2/subscription").arg(mGroupName).arg(mInstanceId);
    auto obj = QJsonObject {
        {"topics", QJsonArray{topic}}
    };

    mRest.post(requestV2(url), QJsonDocument{obj}, this, [this,url](QRestReply &reply) {
        if (!reply.isHttpStatusSuccess()) {
            qWarning() << "failed to subscribe";
            emit failed("failed to subscribe");
            return;
        }
            
        mRest.get(requestV2(url), this,[this](QRestReply& reply){
            auto json = reply.readJson();
            if (!json || !json->isObject()) {
                emit failed("failed to subscribe");
            }
            auto obj = json->object();
            auto topics = obj["topics"].toArray();
            QString subscription;
            bool ok;
            for (const auto& t: topics) {
                if (!subscription.isEmpty()) subscription += ", ";
                subscription += t.toString();
            }
            emit subscribed(subscription);
        });
    });
}


void KafkaProxyV2::getRecords() {
    auto url = QString("consumers/%1/instances/%2/records").arg(mGroupName).arg(mInstanceId);
    mRest.get(requestV2(url,true), this, [this](QRestReply& reply){
        auto json = reply.readJson();
        qint32 lastOffset = -1;
        if (!json || !json->isArray()) {
            qWarning().noquote() << "received invalid records - array in the json";
        } else {
            for (const auto& item: json->array()) {
                auto obj = item.toObject();
                Message message;
                message.key = obj["key"].toString();
                message.offset = obj["offset"].toInt();
                message.partition = obj["partition"].toInt();
                message.topic = obj["topic"].toString();
                message.value = obj["value"].toObject();

                lastOffset = message.offset;
                emit received(message);
            }
            
        }
        emit receivedOffset(lastOffset);
    });
}


void KafkaProxyV2::commitOffset(QString topic, qint32 offset) {
    auto array = QJsonArray{
        QJsonObject {
            {"topic", topic},
            {"partition", 0}, //todo - add partition as parameter
            {"offset", offset}
        }
    };
    auto json = QJsonObject {
        {"offsets", array}
    };

    auto url = QString("consumers/%1/instances/%2/offsets").arg(mGroupName).arg(mInstanceId);
    mRest.post(requestV2(url), QJsonDocument{json}, this, [this](QRestReply &reply) {
        if (!reply.isHttpStatusSuccess()) {
            emit failed(QString("error %1").arg(reply.httpStatus()));
        } else {
            emit offsetCommitted();
        }
    });
}


void KafkaProxyV2::getOffset(const QString& group, const QString& topic) {
    auto url = QString("consumers/%1/instances/%2/offsets").arg(mGroupName).arg(mInstanceId);
    auto array = QJsonArray{
        QJsonObject {
            {"topic", topic},
            {"partition", 0}
        }
    };
    auto json = QJsonObject {
        {"partitions", array}
    };
    
    mRest.get(requestV2(url), QJsonDocument{json}, this, [this](QRestReply &reply) {
        qDebug().noquote() << reply.readText();
    });
}
