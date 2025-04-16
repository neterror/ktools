#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_consumer.h"
#include "kafka_messages.h"
#include <qjsonobject.h>
#include <qstringview.h>
#include <signal.h>
#include <unistd.h>

static KafkaConsumer* _consumer;

static void cleanExit(int) {
    if (_consumer) {
        qDebug().noquote() << "stopping the consumer";
        _consumer->stop();
    } else {
        QCoreApplication::quit();
    }
}

static void receivedBinary(qint32 schemaId, const InputMessage<QByteArray>& message) {
    auto base64 = message.value.toBase64();
    printf("%s\n", base64.constData());
    fflush(stdout);
}


int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"group", "group name", "group"},
            {"topic", "listen to topic", "topic"},
            {"verbose", "show debug prints"}
    });
    parser.process(app);

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

    qDebug().noquote() << "Connecting to server" << server;

    auto topic = parser.value("topic").trimmed();
    KafkaConsumer consumer(parser.value("group"), {topic}, parser.isSet("verbose"), kMediaBinary);

    _consumer = &consumer;
    signal(SIGINT, cleanExit);
    signal(SIGTERM, cleanExit);
    
    if (!parser.isSet("group") || !parser.isSet("topic")) {
        parser.showHelp();
    }


    QObject::connect(&consumer, &KafkaConsumer::finished, [](QString message) {
        qWarning().noquote() << message;
    });

    QObject::connect(&consumer, &KafkaConsumer::receivedBinary, [](auto schemaId, auto message) {receivedBinary(schemaId, message);});
    QObject::connect(&consumer, &KafkaConsumer::finished, [] {
        QCoreApplication::quit();
    });

    consumer.start();

    return app.exec();
}
