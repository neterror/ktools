#include "kafka_proxy_v2.h"
#include "http_client.h"
#include "kafka_messages.h"
#include <qjsondocument.h>
#include <qsslerror.h>
#include <qstringview.h>

KafkaProxyV2::KafkaProxyV2(QString server, QString user, QString password, QString mediaType) :
    HttpClient(server, user, password),
    mMediaType(mediaType)
{
}


void KafkaProxyV2::requestInstanceId(QString groupName) {
    mGroupName = std::move(groupName);
    qDebug().noquote() << "requestInstanceId with group" << mGroupName;
    auto url = QString("consumers/%1").arg(mGroupName);
    auto json = QJsonObject {
        {"format", mMediaType},

        {"fetch.min.bytes", 1}, //when at least 1 byte is available - report it immediately, don't wait the timeout
        {"consumer.request.timeout.ms", 10000},
        
        {"auto.offset.reset", "earliest"},
        {"auto.commit.enable", "false"},

        {"auto.commit.enable", false} //explicitly set which messages are processed
    };

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
    qDebug().noquote() << "request to delete" << url;
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

void KafkaProxyV2::subscribe(const QStringList& topics) {
    auto url = QString("consumers/%1/instances/%2/subscription").arg(mGroupName).arg(mInstanceId);

    auto array = QJsonArray();
    for(const auto& topic: topics) {
        array << topic;
    }
    auto obj = QJsonObject {
        {"topics", array}
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

void KafkaProxyV2::stopReading() {
    if (mPendingRead) {
        mPendingRead->abort();
        mPendingRead = nullptr;
    }
}


void KafkaProxyV2::getRecords() {
    auto url = QString("consumers/%1/instances/%2/records").arg(mGroupName).arg(mInstanceId);
    mPendingRead = mRest.get(requestV2(url,mMediaType), this, [this](QRestReply& reply){
        if (!mPendingRead->isReadable()) {
            mPendingRead = nullptr;
            return;
        }
        auto json = reply.readJson();
        if (!json || !json->isArray()) {
            QString error = QString("Read error. ");
            if (json->isObject()) {
                auto obj = json->object();
                error += obj["message"].toString();
            }
            emit failed(error);
            return;
        }

        for (const auto& item: json->array()) {
            auto obj = item.toObject();
            if (mMediaType == kMediaProtobuf) {
                reportInputJson(obj);
            } else if (mMediaType == kMediaBinary) {
                reportInputBinary(obj);
            } else {
                qWarning() << "invalid media type";
                continue;
            }

            auto offset = obj["offset"].toInt();
            auto topic = obj["topic"].toString();

            emit receivedOffset(topic,offset);
        }
        emit readingComplete();

    });
}

void KafkaProxyV2::reportInputJson(const QJsonObject& obj) {
    InputMessage<QJsonDocument> input;
    input.key = obj["key"].toString();
    input.offset = obj["offset"].toInt();
    input.partition = obj["partition"].toInt();
    input.topic = obj["topic"].toString();
    input.value = QJsonDocument{obj["value"].toObject()};
    emit receivedJson(input);
}

void KafkaProxyV2::reportInputBinary(const QJsonObject& obj) {
    InputMessage<QByteArray> input;
    input.key = obj["key"].toString();
    input.offset = obj["offset"].toInt();
    input.partition = obj["partition"].toInt();
    input.topic = obj["topic"].toString();
    input.value = QByteArray::fromBase64(obj["value"].toString().toUtf8());

    emit receivedBinary(input);
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
