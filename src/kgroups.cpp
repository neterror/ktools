#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include "kafka_proxy_v3.h"

void printTableRow(const QStringList &row, const QList<int> &columnWidths) {
    QString formattedRow;
    for (int i = 0; i < row.size(); ++i) {
        formattedRow += QString("%1").arg(row[i], -columnWidths[i]);
    }
    qDebug().noquote() << formattedRow; // Prevent additional quotes in QDebug output
}


void listGroups(KafkaProxyV3& v3, QCoreApplication& app) {
    QObject::connect(&v3, &KafkaProxyV3::groupList, [](auto groups){
        printTableRow({"GroupID", "State"}, {30, 10});
        qDebug().noquote() << "----------------------------------------";

        for (const auto& group: groups) {
            printTableRow({group.name, group.state}, {30, 10});
        }
        QCoreApplication::quit();
    });
    v3.listGroups();
}

void listConsumers(KafkaProxyV3& v3, const QString& groupName, QCoreApplication& app) {
    QObject::connect(&v3, &KafkaProxyV3::consumerList, [](auto result){
        printTableRow({"GroupID", "ConsumerID", "ClientID"}, {30, 40, 40});
        qDebug().noquote() << "----------------------------------------";

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


void showGroupLag(KafkaProxyV3& v3, const QString& groupName, QCoreApplication& app) {
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

void showLagSummary(KafkaProxyV3& v3, const QString& groupName, QCoreApplication& app) {
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



void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, QCoreApplication& app) {
    parser.process(app);
    if (parser.isSet("list")) {
        listGroups(v3, app);
        return;
    }

    if (!parser.isSet("group")) {
        qDebug().noquote() << "the group name is mandatory";
        parser.showHelp();
    }
    auto group = parser.value("group");

    if (parser.isSet("consumers")) {
        listConsumers(v3, group, app);
        return;
    }

    if (parser.isSet("lag")) {
        showGroupLag(v3, group, app);
        return;
    }

    if (parser.isSet("lag-summary")) {
        showLagSummary(v3, group, app);
        return;
    }
    
    //no command to process""
    parser.showHelp();
}


int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"list", "list the groups"},
            {"group", "Group name", "group name"},

            {"consumers", "list the groups"},
            //todo
            {"lag", "Get group lag data"},
            {"lag-summary", "Get lat summary"},
            {"set-offset", "Set reading offset", "set-offset"},
            
    });

    QSettings settings;
    auto server = settings.value("ConfluentRestProxy/server").toString();
    auto user = settings.value("ConfluentRestProxy/user").toString();
    auto password = settings.value("ConfluentRestProxy/password").toString();

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
