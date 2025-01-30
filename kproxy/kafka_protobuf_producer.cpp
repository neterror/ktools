#include "kafka_protobuf_producer.h"
#include "http_client.h"
#include "kafka_messages.h"
#include "kafka_proxy_v3.h"
#include "kafka_proxy_v2.h"
#include "schema_registry.h"
#include <qstringliteral.h>


KafkaProtobufProducer::KafkaProtobufProducer(QString groupName, bool verbose) : mGroupName{groupName}, mVerbose{verbose}
{
    createObjects();

    auto readSchema = new QState(&mSM);
    auto getClusterId = new QState(&mSM);
    auto waitForData = new QState(&mSM);
    auto send = new QState(&mSM);

    readSchema->addTransition(this, &KafkaProtobufProducer::schemaReady, getClusterId);
    getClusterId->addTransition(mProxy.get(), &HttpClient::initialized, waitForData);

    waitForData->addTransition(this, &KafkaProtobufProducer::newData, send);
    send->addTransition(mProxy.get(), &HttpClient::messageSent, waitForData);
    send->addTransition(mProxy.get(), &HttpClient::failed, waitForData);

    connect(readSchema,  &QState::entered, this, &KafkaProtobufProducer::onRequestSchema);
    connect(getClusterId, &QState::entered, this, &KafkaProtobufProducer::onRequestClusterId);
    connect(waitForData, &QState::entered, this, &KafkaProtobufProducer::onWaitForData);
    connect(send, &QState::entered, this, &KafkaProtobufProducer::onSend);

    mSM.setInitialState(readSchema);
    mSM.start();
}


void KafkaProtobufProducer::onRequestSchema() {
    mTopicSchemaId.clear();
    mRegistry->getSchemas();
}


void KafkaProtobufProducer::onRequestClusterId() {
    mProxy->initialize(mGroupName);
}

void KafkaProtobufProducer::onSchemaReceived(QList<SchemaRegistry::Schema> schemas) {
    const auto kValueSuffix = QStringLiteral("-value");
    for (const auto& schema: schemas) {
        const auto& s = schema.subject;
        if (!s.endsWith(kValueSuffix)) continue;

        auto topicLength = s.length() - kValueSuffix.length();
        auto topic = s.left(topicLength);
        mTopicSchemaId[topic] = schema.schemaId;
        qDebug().noquote() << "topic" << topic << "schemaId = " << schema.schemaId;
    }
    emit schemaReady();
}

void KafkaProtobufProducer::onWaitForData() {
    if (!mQueue.isEmpty()) {
        emit newData();
    }
}

void KafkaProtobufProducer::onSend() {
    if (mQueue.isEmpty()) {
        qWarning() << "Empty queue for proxy send!";
        emit error();
        return;
    }

    auto data = mQueue.dequeue();
    qint32 schemaId = -1;
    auto it = mTopicSchemaId.find(data.topic);
    if (it != mTopicSchemaId.end()) {
        schemaId = *it;
    }

    mProxy->sendBinary(data.key, data.topic, {addSchemaRegistryId(schemaId, data.value)});
}

QByteArray KafkaProtobufProducer::addSchemaRegistryId(qint32 schemaId, const QByteArray& input) {
    if (schemaId == -1) return input;

    QByteArray data;
    auto header = std::array<quint8,6> {0x00, //magic
                                        (quint8)(schemaId >> 24),
                                        (quint8)(schemaId >> 16),
                                        (quint8)(schemaId >> 8),
                                        (quint8)(schemaId >> 0),
                                        0x00}; //the first message in the proto file
    std::copy(header.begin(), header.end(), std::back_inserter(data));
    std::copy(input.begin(), input.end(), std::back_inserter(data));
    return data;
}

void KafkaProtobufProducer::send(OutputBinaryMessage data) {
    mQueue.emplace_back(std::move(data));
    emit newData();
}

void KafkaProtobufProducer::stop() {
    mSM.stop();
}

void KafkaProtobufProducer::createObjects() {
    QSettings settings;
    auto proxyServer = settings.value("ConfluentRestProxy/server").toString();
    auto proxyUser = settings.value("ConfluentRestProxy/user").toString();
    auto proxyPass = settings.value("ConfluentRestProxy/password").toString();

    auto schemaServer = settings.value("ConfluentSchemaRegistry/server").toString();
    auto schemaUser = settings.value("ConfluentSchemaRegistry/user").toString();
    auto schemaPass = settings.value("ConfluentSchemaRegistry/password").toString();

    mProxy.reset(new KafkaProxyV2(proxyServer, proxyUser, proxyPass, mVerbose, kMediaBinary));
    //mProxy.reset(new KafkaProxyV3(proxyServer, proxyUser, proxyPass));

    mRegistry.reset(new SchemaRegistry(schemaServer, schemaUser, schemaPass));

    connect(mProxy.get(), &KafkaProxyV3::messageSent, this, &KafkaProtobufProducer::messageSent);
    connect(mProxy.get(), &KafkaProxyV3::failed, this, &KafkaProtobufProducer::failed);
    connect(mRegistry.get(), &SchemaRegistry::schemaList, this, &KafkaProtobufProducer::onSchemaReceived);
    
}

