#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include "kafka_proxy_v3.h"
#include "kafka_messages.h"

class KafkaProducer : public QObject {
    Q_OBJECT
    KafkaProxyV3& mProxy;
    QStateMachine mSM;
                        
    QQueue<OutputMessage> mQueue;
private slots:
    void onSend();
    void onWaitForData();
public:
    KafkaProducer(KafkaProxyV3& proxy);
    ~KafkaProducer();
    void send(OutputMessage messages);
    void stop();
signals:
    void newData();
    void error();
};

