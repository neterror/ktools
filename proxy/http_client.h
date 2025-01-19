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
    QNetworkRequest requestV2(QString path, bool protobufContent = false) const;
    QNetworkRequest requestV3(QString path) const;

private slots:
    void onAuthenticationRequired(QNetworkReply *reply, QAuthenticator *authenticator);
public:
    HttpClient(QString server, QString user, QString password);

signals:
    void ready(bool success, QString message = "");
};
