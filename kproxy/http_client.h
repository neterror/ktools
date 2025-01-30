#pragma once

#include <QtCore>
#include <QtNetwork>

class HttpClient : public QObject {
    Q_OBJECT
    QNetworkAccessManager mNetworkManager;
protected:
    QRestAccessManager mRest;
    QString mServer;
    QString mUser;
    QString mPassword;

    QString baseUrl(const QString& path) const;
    QNetworkRequest requestV2(const QString& path, const QString& type = "") const;
    QNetworkRequest requestV3(const QString& path) const;

private slots:
    void onAuthenticationRequired(QNetworkReply *reply, QAuthenticator *authenticator);
public:
    HttpClient(QString server, QString user, QString password);

    virtual void initialize(QString name) {}
    virtual void sendBinary(const QString& key, const QString& topic, const QList<QByteArray>& data) {}
    virtual void sendJson(const QString& key, const QString& topic, const QJsonDocument& json) {}

signals:
    void initialized(QString data);
    void messageSent();
    void failed(QString message);
};
