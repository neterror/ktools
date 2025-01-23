#include "kafka_consumer.h"
#include "kafka_proxy_v2.h"
#include <qstatemachine.h>
#include <QFinalState>

KafkaConsumer::KafkaConsumer(KafkaProxyV2& proxy, QString group, QStringList topics) :
    mKafkaProxy(proxy), mTopics{topics}, mGroup{group}
{
    auto work = new QState(&mSM);
    auto success = new QFinalState(&mSM);
    auto error = new QFinalState(&mSM);

    work->addTransition(this, &KafkaConsumer::stopRequest, success);
    work->addTransition(&mKafkaProxy, &KafkaProxyV2::failed, error);

    auto init = new QState(work);        //request instanceID
    auto subscribe = new QState(work);   //subscribe to the topic
    auto read = new QState(work);        //read message

    connect(init,      &QState::entered, [this] {mKafkaProxy.requestInstanceId(mGroup);});
    connect(subscribe, &QState::entered, [this] {mKafkaProxy.subscribe(mTopics);});
    connect(read,      &QState::entered, [this] {mKafkaProxy.getRecords();});

    connect(success,   &QState::entered, this, &KafkaConsumer::onSuccess);
    connect(error,     &QState::entered, this, &KafkaConsumer::onFailed);

    init->addTransition(&mKafkaProxy, &KafkaProxyV2::obtainedInstanceId, subscribe);
    subscribe->addTransition(&mKafkaProxy, &KafkaProxyV2::subscribed, read);

    read->addTransition(&mKafkaProxy, &KafkaProxyV2::readingComplete, read);

    //and report the receive message 
    connect(&mKafkaProxy, &KafkaProxyV2::receivedJson, this, &KafkaConsumer::receivedJson);
    connect(&mKafkaProxy, &KafkaProxyV2::receivedBinary, this, &KafkaConsumer::receivedBinary);
    connect(&mKafkaProxy, &KafkaProxyV2::finished, this, &KafkaConsumer::finished);

    connect(&mKafkaProxy, &KafkaProxyV2::receivedOffset, [this](QString topic, qint32 offset) {
        if (offset != -1) {
            mKafkaProxy.commitOffset(topic, offset);
        }
    });


    mSM.setInitialState(work);
    work->setInitialState(init);
}

void KafkaConsumer::start() {
    mSM.start();
}

void KafkaConsumer::stop() {
    qWarning().noquote() << "Stop Request";
    mKafkaProxy.stopReading();
    emit stopRequest();
}


void KafkaConsumer::onSuccess() {
    qDebug() << "KafkaConsumer success";
    mKafkaProxy.deleteInstanceId();
}

void KafkaConsumer::onFailed() {
    qDebug() << "KafkaConsumer failed";
    mKafkaProxy.deleteInstanceId();
}
