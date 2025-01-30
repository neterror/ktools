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
    QElapsedTimer mTimer;
    bool mVerbose;

    QString baseUrl(const QString& path) const;
    QNetworkRequest requestV2(const QString& path, const QString& type = "") const;
    QNetworkRequest requestV3(const QString& path) const;

    void debugLog(const QString& log) {
        if (mVerbose) {
            int elapsed = mTimer.elapsed();
            qDebug().noquote() << QString("[%1] %2").arg(elapsed, 7, 10, QChar('0')).arg(log);
        }
    }
                                                        

private slots:
    void onAuthenticationRequired(QNetworkReply *reply, QAuthenticator *authenticator);
public:
    HttpClient(QString server, QString user, QString password, bool verbose);

    virtual void initialize(QString name) {}
    virtual void sendBinary(const QString& key, const QString& topic, const QList<QByteArray>& data) {}
    virtual void sendJson(const QString& key, const QString& topic, const QJsonDocument& json) {}

signals:
    void initialized(QString data);
    void messageSent();
    void failed(QString message);
};
