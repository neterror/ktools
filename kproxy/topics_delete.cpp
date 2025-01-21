#include "topics_delete.h"
#include "kafka_proxy_v3.h"
#include <qregularexpression.h>

TopicsDelete::TopicsDelete(KafkaProxyV3& proxy) : mProxy(proxy) {
    connect(&mProxy, &KafkaProxyV3::topicList, this, &TopicsDelete::onTopicList);
    connect(&mProxy, &KafkaProxyV3::topicDeleted, this, &TopicsDelete::onDeleted);
}

TopicsDelete::~TopicsDelete() {
    qDebug() << "Topics delete disposed ";
}

void TopicsDelete::patternDelete(const QString& pattern) {
    mMarkedForDelete.clear();
    mPattern = pattern;
    mProxy.listTopics();
}


void TopicsDelete::onTopicList(QList<KafkaProxyV3::Topic> topics) {
    for (const auto& topic: topics) {
        QRegularExpression regex(mPattern);
        auto match = regex.match(topic.name);
        if (match.hasMatch()) {
            mMarkedForDelete.append(topic);
            qDebug() << "marked for delete" << topic.name;
        }
    }
    if (mMarkedForDelete.isEmpty()) {
        qDebug() << "no topics match the pattern";
        emit deleted();
    } else {
        emit confirm();
    }
}


void TopicsDelete::executeDelete() {
    onDeleted();
}


void TopicsDelete::onDeleted() {
    if (mMarkedForDelete.isEmpty()) {
        emit deleted();
    } else {
        auto target = mMarkedForDelete.dequeue();
        qDebug().noquote() << "deleting" << target.name;
        mProxy.deleteTopic(target.name);
    }
}
