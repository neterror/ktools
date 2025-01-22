#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_consumer.h"
#include "kafka_proxy_v2.h"
#include <qjsonobject.h>
#include <signal.h>
#include <unistd.h>

static KafkaConsumer* _consumer;

static void cleanExit(int) {
    if (_consumer) {
        _consumer->stop();
    } else {
        QCoreApplication::quit();
    }
}


static void receivedMessage(const InputMessage& message) {
    QJsonObject msg;
    msg["info"] = QJsonObject {
        {"topic", message.topic},
        {"key", message.key},
        {"offset", message.offset},
        {"partition", message.partition},
    };
    msg["value"] = message.value;

    auto output = QJsonDocument(msg).toJson(QJsonDocument::Compact);;
    printf("%s\n\n", output.toStdString().c_str());
}


int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"group", "group name", "group"},
            {"topic", "listen on topic pattern", "topic-name"},
    });
    parser.process(app);

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

    qDebug().noquote() << "Connecting to server" << server;
    
    KafkaProxyV2 v2(server, user, password);
    KafkaConsumer consumer(v2, parser.value("group"), parser.value("topic"));

    _consumer = &consumer;
    signal(SIGINT, cleanExit);
    signal(SIGTERM, cleanExit);
    
    if (!parser.isSet("group") || !parser.isSet("topic")) {
        parser.showHelp();
    }


    QObject::connect(&v2, &KafkaProxyV2::failed, [](QString error) {
        qWarning().noquote() << error;
    });

    QObject::connect(&consumer, &KafkaConsumer::received, [](auto message) {receivedMessage(message);});
    QObject::connect(&consumer, &KafkaConsumer::finished, [] {
        QCoreApplication::quit();
    });

    consumer.start();

    return app.exec();
}
