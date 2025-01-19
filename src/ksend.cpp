#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_proxy_v3.h"


void sendProtobufData(KafkaProxyV3& v3, const QString& topic, const QString& key, const QString& fileName) {
    QFile f(fileName);
    if (!f.open(QIODevice::ReadOnly)) {
        qWarning() << "Failed to open" << fileName;
        QCoreApplication::quit();
        return;
    }

    auto doc = QJsonDocument::fromJson(f.readAll());
    v3.sendProtobufData(topic, key, doc);
    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg) {
        if (!success) {
            qDebug().noquote() << "error: " << msg;
        } else {
            qDebug().noquote() << "Success. Data sent";
        }
        QCoreApplication::quit();
    });
}

void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, QCoreApplication& app) {
    parser.process(app);
    

    if (parser.isSet("topic") && parser.isSet("json")) {
        sendProtobufData(v3, parser.value("topic"), parser.value("key"), parser.value("json"));
        return;
    }

    //no command to process
    parser.showHelp();
}


void startInitializion(KafkaProxyV3& v3) {
    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg){
        if (success) {
            //            qDebug().noquote() << "clusterId: " << msg;
        } else {
            qDebug() << "Failed to establish connection with the server: " << msg;
            QCoreApplication::quit();
        }
    });
    v3.getClusterId();
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


    KafkaProxyV3 v3(server, user, password);
    QObject::connect(&v3, &KafkaProxyV3::initialized, [&v3, &parser, &app](bool success){
        if(success) {
            QObject::disconnect(&v3, nullptr, nullptr, nullptr); 
            executeCommands(v3, parser, app);
        } else {
            qWarning() << "Failed to obtain the clusterId";
            QCoreApplication::quit();
        }
    });
    startInitializion(v3);
    return app.exec();
}


