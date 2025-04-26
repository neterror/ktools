#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "http_client.h"
#include "kafka_proxy_v3.h"
#include "kafka_proxy_v2.h"

void printTableRow(const QStringList &row, const QList<int> &columnWidths) {
    QString formattedRow;
    for (int i = 0; i < row.size(); ++i) {
        formattedRow += QString("%1").arg(row[i], -columnWidths[i]);
    }
    qDebug().noquote() << formattedRow; // Prevent additional quotes in QDebug output
}


void listGroups(KafkaProxyV3& v3) {
    QObject::connect(&v3, &KafkaProxyV3::groupList, [](auto groups){
        printTableRow({"GroupID", "State"}, {80, 10});
        qDebug().noquote() << "--------------------------------------------------------------------------------------";

        for (const auto& group: groups) {
            printTableRow({group.name, group.state}, {80, 10});
        }
        QCoreApplication::quit();
    });
    v3.listGroups();
}

void listConsumers(KafkaProxyV3& v3, const QString& groupName) {
    QObject::connect(&v3, &KafkaProxyV3::consumerList, [](auto result){
        printTableRow({"GroupID", "ConsumerID", "ClientID"}, {40, 40, 40});
        qDebug().noquote() << "-----------------------------------------------";

        for (const auto& consumer: result) {
            qDebug().noquote() << "groupId:    " << consumer.groupId;
            qDebug().noquote() << "consumerId: " << consumer.consumerId;
            qDebug().noquote() << "clientId:   " << consumer.clientId;
            qDebug().noquote() << "-------------------------------------";
        }
        QCoreApplication::quit();
    });

    v3.getGroupConsumers(groupName);
}


void showGroupLag(KafkaProxyV3& v3, const QString& groupName) {
    QObject::connect(&v3, &KafkaProxyV3::groupLags, [&v3](auto result){
        printTableRow({"topic", "pos", "end", "lag", "group"}, {35,5,5,5,10});
        qDebug().noquote() << "-------------------------------------------------------------";

        for (const KafkaProxyV3::GroupLag& lag: result) {
            printTableRow({
                    lag.topic,
                    QString("%1").arg(lag.currentOffset),
                    QString("%1").arg(lag.endOffset),
                    QString("%1").arg(lag.lag),
                    lag.groupName,
                },
                {35,5,5,5,10});
        }
        QCoreApplication::quit();
    });
    v3.getGroupLag(groupName);
}

void showLagSummary(KafkaProxyV3& v3, const QString& groupName) {
    QObject::connect(&v3, &KafkaProxyV3::groupLagSummary, [&v3](KafkaProxyV3::GroupLagSummary result){
        printTableRow({"topic", "max-lag", "total-lag", "group"}, {35,5,5,10});
        qDebug().noquote() << "-------------------------------------------------------------";
        printTableRow({
                    result.topic,
                    QString("%1").arg(result.maxLag),
                    QString("%1").arg(result.totalLag),
                    result.groupName,
                },
                {35,5,5,10});

        QCoreApplication::quit();
    });
    v3.getGroupLagSummary(groupName);
}



void v2Commands(KafkaProxyV2& v2, QCommandLineParser& parser) {
    auto offset = parser.value("set-offset").toInt();
    v2.commitOffset(parser.value("topic"), offset);
    QObject::connect(&v2, &KafkaProxyV2::offsetCommitted, [offset]{
        qDebug().noquote() << "Reading position set to" << offset;
        QCoreApplication::quit();
    });
}



void v3Commands(KafkaProxyV3& v3, QCommandLineParser& parser) {
    if (parser.isSet("list")) {
        listGroups(v3);
        return;
    }

    if (!parser.isSet("group")) {
        qDebug().noquote() << "the group name is mandatory";
        parser.showHelp();
    }
    auto group = parser.value("group");

    if (parser.isSet("consumers")) {
        listConsumers(v3, group);
        return;
    }

    if (parser.isSet("lag")) {
        showGroupLag(v3, group);
        return;
    }

    if (parser.isSet("lag-summary")) {
        showLagSummary(v3, group);
        return;
    }
    
    //no command to process""
    parser.showHelp();
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
            {"list", "list the groups"},
            {"group", "Group name", "group name"},

            {"consumers", "list the groups"},
            {"delete-v2-instance", "delete v2 instance", "instanceId"},

            {"lag", "Get group lag data"},
            {"lag-summary", "Get lat summary"},
            {"set-offset", "Set reading offset", "set-offset"},
            {"verbose", "verbose logging"},
            {"topic", "change the offset of topic", "topic"}
            
    });

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

    qDebug().noquote() << "Connecting to server" << server;

    parser.process(app);
    std::unique_ptr<KafkaProxyV2> v2;
    std::unique_ptr<KafkaProxyV3> v3;

    bool verbose = parser.isSet("verbose");

    if (parser.isSet("delete-v2-instance")) {
        v2.reset(new KafkaProxyV2(server, user, password, verbose));
        v2->deleteOldInstanceId(parser.value("delete-v2-instance"), parser.value("group"));
        QObject::connect(v2.get(), &KafkaProxyV2::oldInstanceDeleted, [](QString message){
            qDebug().noquote() << message;
            QCoreApplication::quit();
        });
        return app.exec();
    }



    if (parser.isSet("set-offset")) {
        if (!parser.isSet("group") || !parser.isSet("topic")) {
            qDebug() << "For set-offset specify topic and group";
            return -1;
        }

        v2.reset(new KafkaProxyV2(server, user, password, verbose));
        QObject::connect(v2.get(), &HttpClient::initialized, [&v2, &parser, &app](QString instanceId){
            v2Commands(*v2, parser);
        });
        QObject::connect(v2.get(), &KafkaProxyV2::failed, [](QString message){
            qDebug().noquote() << message;
            QCoreApplication::quit();
        });
        v2->initialize(parser.value("group"));
    } else {
        v3.reset(new KafkaProxyV3(server, user, password, verbose));
        QObject::connect(v3.get(), &KafkaProxyV3::initialized, [&v3, &parser, &app](QString clusterId){
            v3Commands(*v3, parser);
        });

        QObject::connect(v3.get(), &KafkaProxyV3::failed, [](QString message){
            qDebug().noquote() << message;
            QCoreApplication::quit();
        });
        v3->initialize("");
    }

    return app.exec();
}
