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
};
