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


void listTopics(KafkaProxyV3& v3) {
    QObject::connect(&v3, &KafkaProxyV3::topics, [](QList<KafkaProxyV3::Topic> topics){
        auto columns = QList<int>{30};
        printTableRow({"Topic"}, columns);
        qDebug().noquote() << "----------------------------------------";
        for (const auto& topic: topics) {
            printTableRow({topic.name}, columns);
        }
    });

    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg) {
        if (!success) {
            qDebug() << msg;
        }
        QCoreApplication::quit();
    });
        
    v3.listTopics();
}

void readTopicConfig(KafkaProxyV3& v3, const QString& topic) {
    QObject::connect(&v3, &KafkaProxyV3::topicConfig, [](QList<KafkaProxyV3::TopicConfig> configs){
        auto columns = QList<int>{40,30,20};
        printTableRow({"Name", "Value", "Options"}, columns);
        qDebug().noquote() << "--------------------------------------------------------------------------------";
        for (const auto& config: configs) {
            QString options = QString("default: %1, readOnly: %2, sensitive: %3")
                .arg(config.isDefault).arg(config.isReadOnly).arg(config.isSensitive);

            printTableRow({config.name, config.value, options}, columns);
        }
    });

    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg) {
        if (!success) {
            qDebug() << msg;
        }
        QCoreApplication::quit();
    });
        
    v3.readTopicConfig(topic);
}


void createTopic(KafkaProxyV3& v3, const QString& name, bool isCompact, qint32 replicationFactor) {
    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg) {
        if (!success) {
            qDebug().noquote() << msg;
        }
        QCoreApplication::quit();
    });
    v3.createTopic(name, isCompact, replicationFactor);
}


void deleteTopic(KafkaProxyV3& v3, const QString& name) {
    QObject::connect(&v3, &KafkaProxyV3::ready, [](bool success, QString msg) {
        if (!success) {
            qDebug().noquote() << msg;
        }
        QCoreApplication::quit();
    });
    v3.deleteTopic(name);
}


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
    
    if (parser.isSet("list")) {
        listTopics(v3);
        return;
    }

    if (parser.isSet("config")) {
        readTopicConfig(v3, parser.value("config"));
        return;
    }


    if (parser.isSet("create")) {
        auto replicationFactor = 1;
        if (parser.isSet("set-replication-factor")) {
            replicationFactor = parser.value("set-replication-factor").toInt();
        }
        createTopic(v3, parser.value("create"), parser.isSet("set-compact"), replicationFactor);
        return;
    }

    if (parser.isSet("delete")) {
        deleteTopic(v3, parser.value("delete"));
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
            {"list", "get the topics"},
            {"config", "read topic details", "topic-name"},
            {"create", "create topic", "topic-name"},
            {"delete", "delete topic", "topic-name"},
            {"set-compact", "set cleanup.policy = true"},
            {"set-replication-factor", "set topic-replication", "replication-factor"},
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


