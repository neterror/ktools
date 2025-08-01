#include "kafka_protobuf_producer.h"
#include "http_client.h"
#include "kafka_messages.h"
#include "kafka_proxy_v3.h"
#include "kafka_proxy_v2.h"
#include "schema_registry.h"
#include <qdebug.h>
#include <qjsonarray.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qstringliteral.h>


KafkaProtobufProducer::KafkaProtobufProducer(bool verbose): mVerbose{verbose}
{
    createObjects();

    auto readSchema = new QState(&mSM);
    auto getClusterId = new QState(&mSM);
    auto waitForData = new QState(&mSM);
    auto failedSend = new QState(&mSM);
    auto send = new QState(&mSM);

    readSchema->addTransition(this, &KafkaProtobufProducer::schemaReady, getClusterId);
    getClusterId->addTransition(mProxy.get(), &HttpClient::initialized, waitForData);

    waitForData->addTransition(this, &KafkaProtobufProducer::newData, send);
    send->addTransition(mProxy.get(), &HttpClient::messageSent, waitForData);
    send->addTransition(mProxy.get(), &HttpClient::failed, failedSend);
    failedSend->addTransition(this, &KafkaProtobufProducer::newData, waitForData);

    connect(readSchema,  &QState::entered, this, &KafkaProtobufProducer::onRequestSchema);
    connect(getClusterId, &QState::entered, this, &KafkaProtobufProducer::onRequestClusterId);
    connect(waitForData, &QState::entered, this, &KafkaProtobufProducer::onWaitForData);
    connect(send, &QState::entered, this, &KafkaProtobufProducer::onSend);
    connect(failedSend, &QState::entered, this, &KafkaProtobufProducer::onFailedSend);

    mSM.setInitialState(readSchema);
    mSM.start();
}

QString KafkaProtobufProducer::randomId() {
    auto now = QDateTime::currentDateTimeUtc();
    auto epoch = now.toSecsSinceEpoch();
    QByteArray random;
    QFile f("/dev/random");
    if (f.open(QIODevice::ReadOnly)) {
        random = f.read(4);
    }

    return QString("protobuf-%1-%2").arg(random.toHex()).arg(epoch);
}


void KafkaProtobufProducer::onRequestSchema() {
    mRegistry->getSchemas();
}


void KafkaProtobufProducer::onRequestClusterId() {
    mProxy->initialize(randomId());
}


void KafkaProtobufProducer::onSchemaReceived(QList<SchemaRegistry::Schema> schemas) {
    updateSchemaIds(schemas);
    if (!mLocalSchemaFile.isEmpty()) {
	saveLocalSchema(schemas);
    }
    emit schemaReady();
}


void KafkaProtobufProducer::updateSchemaIds(const QList<SchemaRegistry::Schema>& schemas) {
    const auto kValueSuffix = QStringLiteral("-value");
    QMap<QString, qint32> schemaVersion;

    mTopicSchemaId.clear();
    for (const auto& schema: schemas) {
        const auto& s = schema.subject;
        if (!s.endsWith(kValueSuffix)) continue;

        auto topicLength = s.length() - kValueSuffix.length();
        auto topic = s.left(topicLength);
        if (!schemaVersion.contains(topic) || schemaVersion[topic] < schema.version) {
            //keep only the latest version
            mTopicSchemaId[topic] = schema.schemaId;
            schemaVersion[topic] = schema.version;
        }
    }
}

QList<SchemaRegistry::Schema> KafkaProtobufProducer::loadLocalSchema() {
    QFile f(mLocalSchemaFile);
    if (!f.open(QIODevice::ReadOnly)) {
	qWarning() << "Failed to create" << mLocalSchemaFile;
	return {};
    }
    QList<SchemaRegistry::Schema> result;
    auto doc = QJsonDocument::fromJson(f.readAll());
    for (const auto& item: doc.array()) {
	auto s = item.toObject();
	SchemaRegistry::Schema schema;
	schema.schemaId = s["schemaId"].toInt();
        schema.schema = s["schema"].toString();
        schema.schemaType = s["schemaType"].toString();
        schema.subject = s["subject"].toString();
        schema.version = s["version"].toInt();

	auto references = s["references"].toArray();
	for (const auto& ref: references) {
	    auto r = ref.toObject();
	    SchemaRegistry::Reference reference;
	    reference.name = r["name"].toString();
	    reference.subject = r["subject"].toString();
	    reference.version = r["version"].toInt();
	 
	    schema.references.append(reference);
	}
	result.append(schema);
    }
    return result;
}

void KafkaProtobufProducer::saveLocalSchema(const QList<SchemaRegistry::Schema>& schemas) {
    QFile f(mLocalSchemaFile);
    if (!f.open(QIODevice::WriteOnly | QIODevice::Text)) {
	qWarning() << "Failed to create" << mLocalSchemaFile;
	return;
    }
    QJsonArray array;
    for (const auto& schema: schemas) {
	QJsonObject s;
	s["schemaId"] = schema.schemaId;
        s["schema"] = schema.schema;
        s["schemaType"] = schema.schemaType;
        s["subject"] = schema.subject;
        s["version"] = schema.version;
	QJsonArray references;
	for(const auto& r: schema.references) {
	    QJsonObject ref;
	    ref["name"] = r.name;
	    ref["subject"] = r.subject;
	    ref["version"] = r.version;
	    references.append(ref);
	}
	s["references"] = references;
	array.append(s);
    }
    QJsonDocument doc(array);
    auto bytes = doc.toJson();
    f.write(bytes);
}


void KafkaProtobufProducer::onSchemaReadingFailed(const QString& reason) {
    qWarning() << "schema reading failed:" << reason;
    auto schemas = loadLocalSchema();
    if (!schemas.isEmpty()) {
	updateSchemaIds(schemas);
    }
    emit schemaReady();
}



void KafkaProtobufProducer::onWaitForData() {
    if (mPersistentQueue->size()) {
        emit newData();
    }
}

void KafkaProtobufProducer::onFailedSend() {
    qWarning() << "message sending has failed";
    emit newData();
}

void KafkaProtobufProducer::onSend() {
    if (!mPersistentQueue->size()) {
        qWarning() << "Empty queue for proxy send!";
        emit error();
        return;
    }

    QList<QByteArray> toSend;
    auto group = mPersistentQueue->next();
    if (group.isEmpty()) {
        qWarning() << "No data to send in persistent queue group";
        emit error();
        return;
    }

    auto common = group.first();
    qint32 schemaId = -1;
    auto it = mTopicSchemaId.find(common.topic);
    if (it != mTopicSchemaId.end()) {
        schemaId = *it;
    }
    for (const auto& item: group) {
        toSend << addSchemaRegistryId(schemaId, item.payload);
    }
    mProxy->sendBinary(common.key, common.topic, toSend);
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
    mPersistentQueue->append(data.topic, data.key, data.value);
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
    mLocalSchemaFile = settings.value("ConfluentSchemaRegistry/localSchema").toString();

    mProxy.reset(new KafkaProxyV2(proxyServer, proxyUser, proxyPass, mVerbose, kMediaBinary));
    //mProxy.reset(new KafkaProxyV3(proxyServer, proxyUser, proxyPass));

    mRegistry.reset(new SchemaRegistry(schemaServer, schemaUser, schemaPass, mVerbose));

    connect(mProxy.get(), &KafkaProxyV3::messageSent, this, &KafkaProtobufProducer::messageSent);
    connect(mProxy.get(), &KafkaProxyV3::failed, this, &KafkaProtobufProducer::failed);
    connect(mRegistry.get(), &SchemaRegistry::schemaList, this, &KafkaProtobufProducer::onSchemaReceived);
    connect(mRegistry.get(), &SchemaRegistry::failed, this, &KafkaProtobufProducer::onSchemaReadingFailed);
}


