#include <QtCore>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qjsondocument.h>
#include <qregularexpression.h>
#include "kafka_proxy_v3.h"
#include "topics_delete.h"

void printTableRow(const QStringList &row, const QList<int> &columnWidths) {
    QString formattedRow;
    for (int i = 0; i < row.size(); ++i) {
        formattedRow += QString("%1").arg(row[i], -columnWidths[i]);
    }
    qDebug().noquote() << formattedRow; // Prevent additional quotes in QDebug output
}


void listTopics(KafkaProxyV3& v3, const QString& pattern) {

    QObject::connect(&v3, &KafkaProxyV3::topicList, [pattern](QList<KafkaProxyV3::Topic> topics){
        QRegularExpression regex;
        if (!pattern.isEmpty()) {
            regex.setPattern(pattern);
        }

        auto columns = QList<int>{30};
        printTableRow({"Topic"}, columns);
        qDebug().noquote() << "----------------------------------------";
        for (const auto& topic: topics) {
            if (!regex.isValid() || regex.match(topic.name).hasMatch()) {
                printTableRow({topic.name}, columns);
            }
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
        QCoreApplication::quit();
    });

    v3.readTopicConfig(topic);
}


void createTopic(KafkaProxyV3& v3, const QString& name, bool isCompact, qint32 replicationFactor) {
    QObject::connect(&v3, &KafkaProxyV3::topicCreated, [name] {
        qDebug().noquote() << "topic" << name << "is created";
        QCoreApplication::quit();
    });
    v3.createTopic(name, isCompact, replicationFactor);
}


void deleteTopic(KafkaProxyV3& v3, const QString& name) {
    QObject::connect(&v3, &KafkaProxyV3::topicDeleted, [name]() {
        qDebug().noquote() << "topic " << name << "deleted";
        QCoreApplication::quit();
    });
    v3.deleteTopic(name);
}


void deleteMany(KafkaProxyV3& v3, const QString& pattern) {
    auto patternDelete = std::make_shared<TopicsDelete>(v3);
    QObject::connect(patternDelete.get(), &TopicsDelete::confirm, [patternDelete]() {
        QObject::disconnect(patternDelete.get(), &TopicsDelete::confirm, nullptr, nullptr);

        qDebug().noquote() << "Are you sure? Type yes to delete the topics";
        QTextStream inputStream(stdin);
        QString line = inputStream.readLine();  // Reads a single line from stdin
        if (line.trimmed() == "yes") {
            patternDelete->executeDelete();
        } else {
            qDebug().noquote() << "delete cancelled";
            QCoreApplication::quit();
        }
    });


    QObject::connect(patternDelete.get(), &TopicsDelete::deleted, [patternDelete]() {
        QObject::disconnect(patternDelete.get(), &TopicsDelete::deleted, nullptr, nullptr);
        QCoreApplication::quit();
    });
    
    patternDelete->patternDelete(pattern);
}


void executeCommands(KafkaProxyV3& v3, QCommandLineParser& parser, QCoreApplication& app) {
    parser.process(app);
    
    if (parser.isSet("list")) {
        listTopics(v3, "");
        return;
    }

    if (parser.isSet("pattern")) {
        listTopics(v3, parser.value("pattern"));
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

    if (parser.isSet("delete-many")) {
        deleteMany(v3, parser.value("delete-many"));
        return;
    }


    //no command to process
    parser.showHelp();
}



int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");

    parser.addHelpOption();
    parser.addOptions({
            {"list", "get the topics"},
            {"pattern", "get the topics", "pattern", "."},
            {"config", "read topic details", "topic-name"},
            {"create", "create topic", "topic-name"},
            {"delete", "delete topic", "topic-name"},
            {"delete-many", "delete multiple topics matching the pattern", "pattern"},
            {"set-compact", "set cleanup.policy = true"},
            {"set-replication-factor", "set topic-replication", "replication-factor"},
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
