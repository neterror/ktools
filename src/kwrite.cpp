#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_protobuf_producer.h"
#include "kafka_proxy_v3.h"
#include "stdin_reader.h"


void sendJson(KafkaProxyV3& v3, const QString& key, const QString& topic, const QString& fileName) {
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

    v3.sendJson(key, topic, doc);
    QObject::connect(&v3, &KafkaProxyV3::messageSent, [] {
        qDebug().noquote() << "Success. Data sent";
        QCoreApplication::quit();
    });
}


void sendBinary(KafkaProxyV3& v3,
                const QString& key,
                const QString& topic,
                const QString& binaryFile,
                const QString& schemaId)
{
    QFile f(binaryFile);
    if (!f.open(QIODevice::ReadOnly)) {
        qWarning() << "Failed to open" << binaryFile;
        QCoreApplication::quit();
        return;
    }
    v3.sendProtobuf(key, topic, schemaId.toInt(), f.readAll());
    QObject::connect(&v3, &KafkaProxyV3::messageSent, [] {
        qDebug().noquote() << "Success. Data sent";
        QCoreApplication::quit();
    });
}


bool sendProtobuf(const QString& key, const QString& topic, const QString& protofile) {
    QFile f(protofile);
    if (!f.open(QIODevice::ReadOnly)) {
        qWarning() << "Failed to open" << protofile;
        return false;
    }
    auto value = f.readAll();
    auto producer = std::make_shared<KafkaProtobufProducer>();
    QObject::connect(producer.get(), &KafkaProtobufProducer::failed, [](const QString& message){
        qWarning().noquote() << message;
        QCoreApplication::quit();
    });

    QObject::connect(producer.get(), &KafkaProtobufProducer::messageSent, [producer]{
        qWarning().noquote() << "message sent";
        QCoreApplication::quit();
    });

    producer->send({key, topic, value});
    return true;
}


void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, StdinReader& reader) {
    if (!parser.isSet("topic")) {
        parser.showHelp();
    }

    if (parser.isSet("json")) {
        sendJson(v3, parser.value("key"), parser.value("topic"), parser.value("json"));
        return;
    }

    if (parser.isSet("binary")) {
        sendBinary(v3, parser.value("key"), parser.value("topic"), parser.value("binary"), parser.value("schemaId"));
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
            {"json", "send json data", "json-file"},
            {"binary", "send binary data", "binary"},
            {"schemaId", "append schemaId to the binary data", "schemaId", "-1"},
            
            {"protobuf", "send protobuf binary file", "protobuf"}
    });

    parser.process(app);
    if (parser.isSet("protobuf") && parser.isSet("topic")) {
        bool sent = sendProtobuf(parser.value("key"), parser.value("topic"), parser.value("protobuf"));
        if (sent) {
            return app.exec();
        } else {
            return -1;
        }
    }

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

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
