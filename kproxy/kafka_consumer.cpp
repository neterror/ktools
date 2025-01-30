#include "kafka_consumer.h"
#include "kafka_proxy_v2.h"
#include <qstatemachine.h>
#include <QFinalState>

KafkaConsumer::KafkaConsumer(const QString& group, const QStringList& topics, bool verbose, const QString& mediaType)
{
    createProxy(verbose, mediaType);

    auto work = new QState(&mSM);
    auto success = new QFinalState(&mSM);
    auto error = new QFinalState(&mSM);

    work->addTransition(this, &KafkaConsumer::stopRequest, success);
    work->addTransition(this, &KafkaConsumer::failed, error);

    auto init = new QState(work);        //request instanceID
    auto subscribe = new QState(work);   //subscribe to the topic
    auto read = new QState(work);        //read message

    connect(init,      &QState::entered, [this, group] {mProxy->initialize(group);});
    connect(subscribe, &QState::entered, [this, topics] {mProxy->subscribe(topics);});
    connect(read,      &QState::entered, [this] {mProxy->getRecords();});

    connect(success,   &QState::entered, this, &KafkaConsumer::onSuccess);
    connect(error,     &QState::entered, this, &KafkaConsumer::onFailed);

    init->addTransition(mProxy.get(), &KafkaProxyV2::initialized, subscribe);
    subscribe->addTransition(mProxy.get(), &KafkaProxyV2::subscribed, read);

    read->addTransition(mProxy.get(), &KafkaProxyV2::readingComplete, read);

    //and report the receive message 
    connect(mProxy.get(), &KafkaProxyV2::receivedJson, this, &KafkaConsumer::receivedJson);
    connect(mProxy.get(), &KafkaProxyV2::receivedBinary, this, &KafkaConsumer::receivedBinary);
    connect(mProxy.get(), &KafkaProxyV2::finished, this, &KafkaConsumer::finished);

    connect(mProxy.get(), &KafkaProxyV2::receivedOffset, [this](QString topic, qint32 offset) {
        if (offset != -1) {
            mProxy->commitOffset(topic, offset);
        }
    });

    connect(mProxy.get(), &KafkaProxyV2::failed, [this](QString error){
        qWarning().noquote() << "KafkaProxyV2 error:" << error;
        emit failed(error);
    });


    mSM.setInitialState(work);
    work->setInitialState(init);
}


void KafkaConsumer::start() {
    mSM.start();
}

void KafkaConsumer::stop() {
    mProxy->stopReading();
    emit stopRequest();
}


void KafkaConsumer::onSuccess() {
    mProxy->deleteInstanceId();
}

void KafkaConsumer::onFailed() {
    mProxy->deleteInstanceId();
}


void KafkaConsumer::createProxy(bool verbose, const QString& mediaType) {
    QSettings settings;
    auto proxyServer = settings.value("ConfluentRestProxy/server").toString();
    auto proxyUser = settings.value("ConfluentRestProxy/user").toString();
    auto proxyPass = settings.value("ConfluentRestProxy/password").toString();

    mProxy.reset(new KafkaProxyV2(proxyServer, proxyUser, proxyPass, verbose, mediaType));
}
