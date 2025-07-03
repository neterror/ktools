#include "kafka_consumer.h"
#include "kafka_proxy_v2.h"
#include <qstatemachine.h>
#include <QFinalState>

KafkaConsumer::KafkaConsumer(const QString& group, const QStringList& topics, bool verbose, const QString& mediaType)
{
    createProxy(verbose, mediaType);
    mGroupName = group;

    auto work = new QState(&mSM);
    auto success = new QFinalState(&mSM);
    auto error = new QFinalState(&mSM);

    work->addTransition(this, &KafkaConsumer::stopRequest, success);
    work->addTransition(this, &KafkaConsumer::failed, error);

    auto init = new QState(work);        //request instanceID
    auto subscribe = new QState(work);   //subscribe to the topic
    auto read = new QState(work);        //read message
    auto commitOffsets = new QState(work);

    connect(init,          &QState::entered, [this, group] {mProxy->initialize(group);});
    connect(subscribe,     &QState::entered, [this, topics] {mProxy->subscribe(topics);});
    connect(read,          &QState::entered, [this] {mProxy->getRecords();});
    connect(commitOffsets, &QState::entered, [this] {mProxy->commitAllOffsets();});

    connect(success,   &QState::entered, this, &KafkaConsumer::onSuccess);
    connect(error,     &QState::entered, this, &KafkaConsumer::onFailed);

    init->addTransition(mProxy.get(), &KafkaProxyV2::initialized, subscribe);
    subscribe->addTransition(mProxy.get(), &KafkaProxyV2::subscribed, read);
    read->addTransition(mProxy.get(), &KafkaProxyV2::readingComplete, commitOffsets);
    commitOffsets->addTransition(mProxy.get(), &KafkaProxyV2::offsetCommitted, read);
    

    //and report the receive message 
    connect(mProxy.get(), &KafkaProxyV2::receivedJson, this, &KafkaConsumer::receivedJson);
    connect(mProxy.get(), &KafkaProxyV2::receivedBinary, this, &KafkaConsumer::receivedBinary);
    connect(mProxy.get(), &KafkaProxyV2::finished, this, &KafkaConsumer::finished);
    connect(mProxy.get(), &KafkaProxyV2::failed, this, &KafkaConsumer::failed);


    connect(mProxy.get(), &KafkaProxyV2::initialized, [this,group](QString instanceId) {
        auto fileName = instanceBackupFile(group);
        qDebug() << "try to create file to store instance" << instanceId << "of group" << group;
        QFile f(instanceBackupFile(group));
        if (f.open(QIODevice::WriteOnly)) {
            f.write(instanceId.toUtf8());
            qDebug().noquote() << "created backup file" << f.fileName() << "to store assigned instanceId" << instanceId;
        } else {
            qWarning().noquote() << "No instanceId backup was made";
        }
    });


    connect(mProxy.get(), &KafkaProxyV2::failed, [this](QString error){
        qWarning().noquote() << "KafkaProxyV2 error:" << error;
        emit failed(error);
    });


    //if there was an old intance, start the consumer only after deleting the old instance
    connect(mProxy.get(), &KafkaProxyV2::oldInstanceDeleted,[this](QString message) {
        qDebug() << "old instance deleted. Now start the client state machine";
        mSM.start();
    });
    

    mSM.setInitialState(work);
    work->setInitialState(init);
}

QString KafkaConsumer::instanceBackupFile(const QString& group) {
    return QString("/tmp/kproxy-group-%1").arg(group);
}


void KafkaConsumer::start() {
    QFile f(instanceBackupFile(mGroupName));
    if (!f.open(QIODevice::ReadOnly)) {
        mSM.start();
        return;
    }

    auto instanceId = f.readAll();
    qDebug() << "before starting, delete the old instanceId" << instanceId;
    mProxy->deleteOldInstanceId(instanceId, mGroupName); //the signal deleteOldInstance will invoke start of the state machine
}

void KafkaConsumer::stop() {
    mProxy->stopReading();
    emit stopRequest();
}


void KafkaConsumer::onSuccess() {
    mProxy->deleteInstanceId();
    QDir path;
    auto fileName = instanceBackupFile(mGroupName);
    if (QFile::exists(fileName)) {
        qDebug()<< "deleting " << fileName;
        path.remove(fileName);
    }
}

void KafkaConsumer::onFailed() {
    mProxy->deleteInstanceId();
    QDir path;
    auto fileName = instanceBackupFile(mGroupName);
    if (QFile::exists(fileName)) {
        qDebug()<< "deleting " << fileName;
        path.remove(fileName);
    }
}


void KafkaConsumer::createProxy(bool verbose, const QString& mediaType) {
    QSettings settings;
    auto proxyServer = settings.value("ConfluentRestProxy/server").toString();
    auto proxyUser = settings.value("ConfluentRestProxy/user").toString();
    auto proxyPass = settings.value("ConfluentRestProxy/password").toString();

    mProxy.reset(new KafkaProxyV2(proxyServer, proxyUser, proxyPass, verbose, mediaType));
}
