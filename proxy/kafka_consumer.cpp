#include "kafka_consumer.h"
#include "kafka_proxy_v2.h"
#include <qstatemachine.h>
#include <QFinalState>

KafkaConsumer::KafkaConsumer(KafkaProxyV2& proxy, QString group, QString topic) :
    mKafkaProxy(proxy), mTopic{topic}, mGroup{group}
{
    auto work = new QState(&mSM);
    auto success = new QFinalState(&mSM);
    auto error = new QFinalState(&mSM);

    work->addTransition(this, &KafkaConsumer::stopRequest, success);
    work->addTransition(&mKafkaProxy, &KafkaProxyV2::failed, error);

    auto init = new QState(work);        //request instanceID
    auto subscribe = new QState(work);   //subscribe to the topic
    auto read = new QState(work);        //read message
    auto commit = new QState(work);      //commit the last read message

    connect(init,      &QState::entered, [this] {mKafkaProxy.requestInstanceId(mGroup);});
    connect(subscribe, &QState::entered, [this] {mKafkaProxy.subscribe(mTopic);});
    connect(read,      &QState::entered, [this] {mKafkaProxy.getRecords();});
    connect(commit,    &QState::entered, [this] {mKafkaProxy.commitOffset(mTopic, mLastOffset);});

    connect(success,   &QState::entered, this, &KafkaConsumer::onSuccess);
    connect(error,     &QState::entered, this, &KafkaConsumer::onFailed);

    init->addTransition(&mKafkaProxy, &KafkaProxyV2::obtainedInstanceId, subscribe);
    subscribe->addTransition(&mKafkaProxy, &KafkaProxyV2::subscribed, read);
    read->addTransition(this, &KafkaConsumer::commitOffset, commit); //after received report, read again
    read->addTransition(this, &KafkaConsumer::readAgain, read);

    commit->addTransition(&mKafkaProxy, &KafkaProxyV2::offsetCommitted, read);


    //and report the receive message 
    connect(&mKafkaProxy, &KafkaProxyV2::received, this, &KafkaConsumer::received);
    connect(&mKafkaProxy, &KafkaProxyV2::finished, this, &KafkaConsumer::finished);
    connect(&mKafkaProxy, &KafkaProxyV2::finished, this, &KafkaConsumer::finished);

    connect(&mKafkaProxy, &KafkaProxyV2::receivedOffset, [this](qint32 offset) {
        mLastOffset = offset;
        if (offset != -1) {
            qDebug() << "commit read offset" << mLastOffset;
            emit commitOffset();
        } else {
            qDebug() << "nothing read";
            emit readAgain();
        }
    });


    mSM.setInitialState(work);
    work->setInitialState(init);
}

void KafkaConsumer::start() {
    qDebug() << "KafkaConsumer::start";
    mSM.start();
}

void KafkaConsumer::stop() {
    qWarning().noquote() << "Stop Request";
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
