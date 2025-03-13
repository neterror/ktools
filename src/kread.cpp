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

static void receivedBinary(qint32 schemaId, const InputMessage<QByteArray>& message) {
    static int counter = 0;
    qDebug() << "schemaId = " << schemaId;
    auto name = QString("msg%1_schema_id%2_topic_%3.bin").arg(message.offset).arg(schemaId).arg(message.topic);
    QFile f(name);
    if (f.open(QIODevice::WriteOnly)) {
        f.write(message.value);
        f.close();
        qDebug().noquote() << "binary message in" << name;
    }
}



static void stdoutOutput(QtMsgType type, const QMessageLogContext&, const QString &msg) {
    QByteArray localMsg = msg.toLocal8Bit();
    printf("%s\n", localMsg.constData());
}


int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    qInstallMessageHandler(stdoutOutput);

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"group", "group name", "group"},
            {"topics", "comma separated list of topics to listen", "list of topics"},
            {"media-type",  "protobuf or binary format", "type", kMediaProtobuf},
            {"verbose", "show debug prints"}
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

    auto topics = parser.value("topics").trimmed();
    KafkaConsumer consumer(parser.value("group"), topics.split(","), parser.isSet("verbose"), parser.value("media-type"));

    _consumer = &consumer;
    signal(SIGINT, cleanExit);
    signal(SIGTERM, cleanExit);
    
    if (!parser.isSet("group") || !parser.isSet("topics")) {
        parser.showHelp();
    }


    QObject::connect(&consumer, &KafkaConsumer::finished, [](QString message) {
        qWarning().noquote() << message;
    });

    QObject::connect(&consumer, &KafkaConsumer::receivedJson, [](auto message) {receivedJson(message);});
    QObject::connect(&consumer, &KafkaConsumer::receivedBinary, [](auto schemaId, auto message) {receivedBinary(schemaId, message);});
    QObject::connect(&consumer, &KafkaConsumer::finished, [] {
        QCoreApplication::quit();
    });

    consumer.start();

    return app.exec();
}
