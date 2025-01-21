#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_proxy_v3.h"


void sendMessage(KafkaProxyV3& v3, const QString& topic, const QString& key, const QString& fileName) {
    QFile f(fileName);
    if (!f.open(QIODevice::ReadOnly)) {
        qWarning() << "Failed to open" << fileName;
        QCoreApplication::quit();
        return;
    }
    auto doc = QJsonDocument::fromJson(f.readAll());
    v3.sendMessage(topic, key, doc);
    QObject::connect(&v3, &KafkaProxyV3::messageSent, [] {
        qDebug().noquote() << "Success. Data sent";
        QCoreApplication::quit();
    });
}

void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, QCoreApplication& app) {
    parser.process(app);
    if (parser.isSet("topic") && parser.isSet("json")) {
        sendMessage(v3, parser.value("topic"), parser.value("key"), parser.value("json"));
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
            {"json", "send json data", "json-file"}
    });

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();
    qDebug().noquote() << "Connecting to server" << server;

    KafkaProxyV3 v3(server, user, password);
    QObject::connect(&v3, &KafkaProxyV3::initialized, [&v3, &parser, &app](QString clusterId){
        executeCommands(v3, parser, app);
    });

    QObject::connect(&v3, &KafkaProxyV3::failed, [](QString message){
        qDebug().noquote() << message;
        QCoreApplication::quit();
    });

    v3.getClusterId();
    return app.exec();
}
