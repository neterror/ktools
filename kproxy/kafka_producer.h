#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include "kafka_proxy_v3.h"


class KafkaProducer : public QObject {
    Q_OBJECT
    KafkaProxyV3& mProxy;
    QStateMachine mSM;
                        
    struct Message {
        QString topic;
        QString key;
        QJsonDocument value;
    };
    QQueue<Message> mQueue;
private slots:
    void onSend();
    void onWaitForData();
public:
    KafkaProducer(KafkaProxyV3& proxy);
    ~KafkaProducer();
    void send(Message messages);
    void stop();
signals:
    void newData();
    void error();
};

