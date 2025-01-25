#include "kafka_producer.h"


KafkaProducer::KafkaProducer(KafkaProxyV3& proxy) : mProxy(proxy) {
    auto waitForData = new QState(&mSM);
    auto send = new QState(&mSM);

    waitForData->addTransition(this, &KafkaProducer::newData, send);
    send->addTransition(&mProxy, &KafkaProxyV3::messageSent, waitForData);
    send->addTransition(&mProxy, &KafkaProxyV3::failed, waitForData);

    connect(waitForData, &QState::entered, this, &KafkaProducer::onWaitForData);
    connect(send, &QState::entered, this, &KafkaProducer::onSend);

    mSM.setInitialState(waitForData);
    mSM.start();
}

KafkaProducer::~KafkaProducer() {
    qDebug().noquote() << "KafkaProduce::~KafkaProducer";
}


void KafkaProducer::onWaitForData() {
    if (!mQueue.isEmpty()) {
        emit newData();
    }
}


void KafkaProducer::onSend() {
    if (mQueue.isEmpty()) {
        qWarning() << "Empty queue for proxy send!";
        emit error();
        return;
    }
    auto data = mQueue.dequeue();
    mProxy.sendMessage(data.key, data.topic, data.value);
}

void KafkaProducer::send(OutputMessage data) {
    mQueue.emplace_back(std::move(data));
    emit newData();
}

void KafkaProducer::stop() {
    mSM.stop();
}
