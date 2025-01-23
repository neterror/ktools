#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_consumer.h"
#include "kafka_messages.h"
#include "kafka_proxy_v2.h"
#include <qjsonobject.h>
#include <qstringview.h>
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


static void receivedJson(const InputMessage<QJsonDocument>& message) {
    QJsonObject msg;
    msg["info"] = QJsonObject {
        {"topic", message.topic},
        {"key", message.key},
        {"offset", message.offset},
        {"partition", message.partition},
    };
    msg["value"] = message.value.object();

    auto output = QJsonDocument(msg).toJson(QJsonDocument::Compact);;
    printf("%s\n\n", output.toStdString().c_str());
}

static void receivedBinary(const InputMessage<QByteArray>& message) {
    qDebug() << "incoming binary message" << message.value << "from topic " << message.topic;
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
            {"media-type",  "protobuf or binary format", "type", kMediaProtobuf}
    });
    parser.process(app);
    auto mediaType = parser.value("media-type");
    if ((mediaType != kMediaProtobuf) && (mediaType != kMediaBinary)) {
        qCritical().noquote() << "the mediatype should be protobuf or binary";
        return -1;
    }

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

    qDebug().noquote() << "Connecting to server" << server;
    
    KafkaProxyV2 v2(server, user, password, parser.value("media-type"));
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

    QObject::connect(&consumer, &KafkaConsumer::receivedJson, [](auto message) {receivedJson(message);});
    QObject::connect(&consumer, &KafkaConsumer::receivedBinary, [](auto message) {receivedBinary(message);});
    QObject::connect(&consumer, &KafkaConsumer::finished, [] {
        QCoreApplication::quit();
    });

    consumer.start();

    return app.exec();
}
