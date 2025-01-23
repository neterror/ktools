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


    struct GroupLag {
        QString groupName;
        QString consumerId;
        QString topic;
        qint32 currentOffset;
        qint32 endOffset;
        qint32 lag;
    };

    struct GroupLagSummary {
        QString groupName;
        QString topic;
        QString consumerId;
        qint32 maxLag;
        qint32 totalLag;
    };
    

    KafkaProxyV3(QString server, QString user, QString password);
    void getClusterId();

    void listTopics();
    void listGroups();
    void readTopicConfig(const QString& name);
    
    void createTopic(const QString& topic, bool isCompact, qint32 replicationFactor, qint32 partitionsCount);
    void deleteTopic(const QString& topic);

    void sendMessage(const QString& key, const QString& topic, const QJsonDocument& json);
    void getGroupConsumers(const QString& group);
    void getGroupLag(const QString& group);
    void getGroupLagSummary(const QString& group);

signals:
    void initialized(QString clusterId);
    void topicList(QList<Topic> data);
    void groupList(QList<Group> data);
    void consumerList(QList<Consumer> data);
    void groupLags(QList<GroupLag> data);
    void groupLagSummary(GroupLagSummary data);

    void topicConfig(QList<TopicConfig> configs);
    void topicCreated();
    void topicDeleted();
    void messageSent();
    void failed(QString message);
};
