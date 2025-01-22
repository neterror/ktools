#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_producer.h"
#include "kafka_proxy_v3.h"
#include "stdin_reader.h"


void sendMessage(KafkaProxyV3& v3, const QString& key, const QString& topic, const QString& fileName) {
    QFile f(fileName);
    if (!f.open(QIODevice::ReadOnly)) {
        qWarning() << "Failed to open" << fileName;
        QCoreApplication::quit();
        return;
    }
    QJsonParseError error;
    auto doc = QJsonDocument::fromJson(f.readAll(), &error);
    if (error.error != QJsonParseError::NoError) {
        qWarning().noquote() << "Json parser error:" << error.errorString() << "at offset" << error.offset;
        QCoreApplication::quit();
        return;
    }

    v3.sendMessage(key, topic, doc);
    QObject::connect(&v3, &KafkaProxyV3::messageSent, [] {
        qDebug().noquote() << "Success. Data sent";
        QCoreApplication::quit();
    });
}

void interactiveMode(KafkaProxyV3& v3, StdinReader& reader, const QString& topic, const QString& key) {
    auto producer = std::make_shared<KafkaProducer>(v3);
    QObject::connect(&reader, &StdinReader::data, [producer, &reader, topic, key](QString line) {
        if (line == "end") {
            qDebug().noquote() << "terminating";
            QObject::disconnect(&reader, nullptr, nullptr, nullptr);
            QCoreApplication::quit();
            return;
        }
        QJsonParseError error;
        auto doc = QJsonDocument::fromJson(line.toUtf8(), &error);
        if (doc.isObject()) {
            qDebug() << "sending on topic " << topic;
            producer->send({key, topic, doc});
        } else {
            qWarning().noquote() << error.errorString();
        }

    });
}


void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, StdinReader& reader) {
    if (!parser.isSet("topic")) {
        parser.showHelp();
    }

    if (parser.isSet("file")) {
        sendMessage(v3, parser.value("key"), parser.value("topic"), parser.value("file"));
        return;
    }

    if (parser.isSet("interactive")) {
        interactiveMode(v3, reader, parser.value("topic"), parser.value("key"));
        return;
    }

    parser.showHelp();
}

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"topic", "topic on which to send data", "send-topic"},
            {"key",   "kafka topic key", "topic-key"},
            {"file", "send json data", "json-file"},
            {"interactive", "read for json input on stdin"}
    });

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();
    qDebug().noquote() << "Connecting to server" << server;

    parser.process(app);
    KafkaProxyV3 v3(server, user, password);
    StdinReader reader;
    QObject::connect(&v3, &KafkaProxyV3::initialized, [&v3, &parser, &app, &reader](QString clusterId){
        executeCommands(v3, parser, reader);
    });

    QObject::connect(&v3, &KafkaProxyV3::failed, [](QString message){
        qDebug().noquote() << message;
        QCoreApplication::quit();
    });

    v3.getClusterId();
    return app.exec();
}
