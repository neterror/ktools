#include "kafka_proxy_v2.h"
#include "http_client.h"
#include "kafka_messages.h"
#include <qjsondocument.h>
#include <qsslerror.h>
#include <qstringview.h>

KafkaProxyV2::KafkaProxyV2(QString server, QString user, QString password, bool verbose, QString mediaType) :
    HttpClient(server, user, password, verbose),
    mMediaType(mediaType)
{
}


void KafkaProxyV2::initialize(QString groupName) {
    mGroupName = std::move(groupName);
    debugLog(QString("requestInstanceId with group %1, mediaType %2").arg(mGroupName).arg(mMediaType));
    auto url = QString("consumers/%1").arg(mGroupName);
    auto json = QJsonObject {
        {"format", mMediaType},
        {"fetch.min.bytes", 1}, //when at least 1 byte is available - report it immediately, don't wait the timeout
        {"consumer.request.timeout.ms", 10000},
        {"auto.offset.reset", "earliest"},
        {"auto.commit.enable", false} //explicitly set which messages are processed
    };

    mRest.post(requestV2(url, mMediaType), QJsonDocument{json}, this, [this](QRestReply &reply) {
        auto json = reply.readJson();
        if (!json || !json->isObject()) {
            debugLog("Failed to obtain instanceId");
            emit failed("Failed to obtain instanceId");
            return;
        }
        
        auto obj = json->object();
        if (obj.contains("instance_id")) {
            mInstanceId = obj["instance_id"].toString();
            debugLog(QString("obtained instanceId %1").arg(mInstanceId));
            emit initialized(mInstanceId);
        } else {
            auto msg = obj["message"].toString();
            debugLog(QString("Failed to obtain instanceId %1").arg(msg));
            emit failed(msg);
        }
    });
}

void KafkaProxyV2::deleteInstanceId() {
    auto url = QString("consumers/%1/instances/%2").arg(mGroupName).arg(mInstanceId);
    debugLog(QString("delete instanceId %1").arg(mInstanceId));
    mRest.deleteResource(requestV2(url), this, [this](QRestReply &reply) {
        QString message;
        if (reply.isHttpStatusSuccess()) {
            message = QString("instance %1 deleted").arg(mInstanceId);
            debugLog(QString("instanceId %1 deleted").arg(mInstanceId));
        } else {
            QStringList words;
            words << "error deleting instance" << mInstanceId << reply.readText();
            message = words.join(" ");
            debugLog(message);
        }
        emit finished(message);
    });

    QTimer::singleShot(5000, [this]{
        debugLog("delete instance killed after timeout");
        emit finished("delete timeout");
    });
}

void KafkaProxyV2::deleteOldInstanceId(const QString& instanceId, const QString& group) {
    auto url = QString("consumers/%1/instances/%2").arg(group).arg(instanceId);
    debugLog(QString("delete instanceId %1").arg(instanceId));
    mRest.deleteResource(requestV2(url), this, [this](QRestReply &reply) {
        emit oldInstanceDeleted(reply.readText());
    });
}



void KafkaProxyV2::subscribe(const QStringList& topics) {
    auto url = QString("consumers/%1/instances/%2/subscription").arg(mGroupName).arg(mInstanceId);
    debugLog(QString("subscribe to %1").arg(topics.join(",")));
    auto array = QJsonArray();
    for(const auto& topic: topics) {
        array << topic;
    }
    auto obj = QJsonObject {
        {"topics", array}
    };

    mRest.post(requestV2(url), QJsonDocument{obj}, this, [this,url](QRestReply &reply) {
        if (!reply.isHttpStatusSuccess()) {
            debugLog("failed to subscribe - bad http status");
            emit failed("failed to subscribe - bad http status");
            return;
        }
            
        mRest.get(requestV2(url), this,[this](QRestReply& reply){
            auto json = reply.readJson();
            if (!json || !json->isObject()) {
                debugLog("failed to subscribe - broken json");
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
            debugLog(QString("subscribed to %1").arg(subscription));
            emit subscribed(subscription);
        });
    });
}

void KafkaProxyV2::stopReading() {
    if (mPendingRead) {
        debugLog("Stop reading request");
        mPendingRead->abort();
        mPendingRead = nullptr;
    }
}


void KafkaProxyV2::getRecords() {
    auto url = QString("consumers/%1/instances/%2/records").arg(mGroupName).arg(mInstanceId);
    debugLog(QString("getRecords: %1").arg(url));
    mPendingRead = mRest.get(requestV2(url,mMediaType), this, [this](QRestReply& reply){
        debugLog("getRecords received data");
        if (!mPendingRead->isReadable()) {
            debugLog("socket not readable");
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
            debugLog(error);
            emit readingComplete();
            return;
        }

        QMap<QString, qint32> offsets;
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

            if (mVerbose) {
                auto offset = obj["offset"].toInt();
                auto topic = obj["topic"].toString();
                offsets[topic] = offset; //keep the last offset from a topic
            }
        } 
        if (mVerbose) {
            for (const auto& topic: offsets.keys()) {
                debugLog(QString("received offset %1 from topic %2").arg(offsets[topic]).arg(topic));
            }
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
    auto value = QByteArray::fromBase64(obj["value"].toString().toUtf8());

    auto key = obj["key"].toString().toUtf8();
    input.key = QByteArray::fromBase64(key);

    qint32 schemaId;
    if (isValid(value, schemaId)) {
        input.value = value.mid(6);
        emit receivedBinary(schemaId, input);
    } else {
        qWarning() << "failed binary reception on topic" << input.topic << obj["value"].toString();
    }
}


bool KafkaProxyV2::isValid(const QByteArray& data, qint32& schemaId) {
    schemaId = -1;
    if (data.size() < 5) {
        qWarning() << "invalid input data size";
        return false;
    }
    auto b = (const quint8*)data.data();
    if (b[0] != 0) {
        qWarning() << "invalid magic byte";
        return false;
    }

    if (b[5] != 0) {
        qWarning() << "Unexpected b[5]";
        return false;
    }
    schemaId = (b[1] << 24) | (b[2] << 16) | (b[3] << 8) | (b[4] << 0);
    return true;
}



void KafkaProxyV2::commitAllOffsets() {
    //When the post body is empty, it commits all the records that have been fetched by the consumer instance.
    auto url = QString("consumers/%1/instances/%2/offsets").arg(mGroupName).arg(mInstanceId);
    mRest.post(requestV2(url), QJsonDocument{}, this, [this](QRestReply &reply) {
        emit offsetCommitted();
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
    mRest.post(requestV2(url), QJsonDocument{json}, this, [this, offset, topic](QRestReply &reply) {
        if (!reply.isHttpStatusSuccess()) {
            emit failed(QString("error %1").arg(reply.httpStatus()));
        } else {
            debugLog(QString("committed offset %1 for topic %2").arg(offset).arg(topic));
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


void KafkaProxyV2::sendJson(const QString& key, const QString& topic, const QJsonDocument& json) {
    qCritical() << "send json not implemented in KafkaProxyV2";
}

void KafkaProxyV2::sendBinary(const QString& key, const QString& topic, const QList<QByteArray>& data) {
    QJsonArray records;
    for (const auto& item: data) {
        QJsonObject record;
        if (!key.isEmpty()) { //there is no option to send the value in binary (base64) and leave the key as string
            auto base64Key = key.toUtf8().toBase64(); 
            record["key"] = QString(base64Key);
        }
        record["value"] = QString(item.toBase64());
        records << record;
    }

    auto payload = QJsonObject {
        {"records", records} 
    };

    debugLog(QString("send %1 messages").arg(records.size()));
    auto url = QString("topics/%1").arg(topic);
    mRest.post(requestV2(url, kMediaBinary), QJsonDocument{payload}, this,
               [this](QRestReply &reply) {
                   debugLog(reply.readText());
                   emit messageSent();
               });
}    



