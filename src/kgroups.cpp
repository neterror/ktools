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


void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, QCoreApplication& app) {
    parser.process(app);
    if (parser.isSet("groups")) {
        printTableRow({"GroupID", "State"}, {30, 10});
        qDebug().noquote() << "----------------------------------------";
        QObject::connect(&v3, &KafkaProxyV3::groups, [](auto groups){
            for (const auto& group: groups) {
                printTableRow({group.name, group.state}, {30, 10});
            }
            QCoreApplication::quit();
        });
        v3.listGroups();
        return;
    }

    if (parser.isSet("consumers")) {
        printTableRow({"GroupID", "ConsumerID", "ClientID"}, {30, 40, 40});
        qDebug().noquote() << "----------------------------------------";
        QObject::connect(&v3, &KafkaProxyV3::consumers, [](auto result){
            for (const auto& consumer: result) {
                qDebug().noquote() << "groupId:    " << consumer.groupId;
                qDebug().noquote() << "consumerId: " << consumer.consumerId;
                qDebug().noquote() << "clientId:   " << consumer.clientId;
                qDebug().noquote() << "-------------------------------------";
            }
            QCoreApplication::quit();
        });

        v3.getGroupConsumers(parser.value("consumers"));
        return;
    }

    //no command to process""
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
            {"groups", "list the groups"},
            {"consumers", "list the groups", "group"},
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
