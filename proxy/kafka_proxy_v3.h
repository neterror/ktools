#pragma once
#include "http_client.h"
#include <qjsondocument.h>

class KafkaProxyV3 : public HttpClient {
    Q_OBJECT
    QString mClusterID;

    static QJsonArray getDataArray(QRestReply& reply, QString& msg);

public:
    struct Topic {
        QString name;
        bool isInternal;
        qint32 partitionsCount;
        qint32 replicationFactor;
    };

    struct TopicConfig {
        QString name;
        QString value;

        bool isDefault;
        bool isReadOnly;
        bool isSensitive;

    };

    struct Group {
        QString name;
        QString state;
    };

    struct Consumer {
        QString groupId;
        QString consumerId;
        QString clientId;
    };

    KafkaProxyV3(QString server, QString user, QString password);
    void getClusterId();

    void listTopics();
    void listGroups();
    void readTopicConfig(const QString& name);
    
    void createTopic(const QString& topic, bool isCompact, qint32 replicationFactor);
    void deleteTopic(const QString& topic);

    void sendProtobufData(const QString& topic, const QString& key, const QJsonDocument& json);

    void getGroupConsumers(const QString& group);
signals:
    void initialized(bool success);
    void topics(QList<Topic> data);
    void groups(QList<Group> data);
    void consumers(QList<Consumer> data);
    void topicConfig(QList<TopicConfig> configs);
};
