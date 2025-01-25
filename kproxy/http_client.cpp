#include "http_client.h"
#include <qhttpheaders.h>

HttpClient::HttpClient(QString server, QString user, QString password) :
    mRest(&mNetworkManager), mServer{server}, mUser{user}, mPassword{password}
{
    mNetworkManager.setAutoDeleteReplies(true);
    mNetworkManager.setProxy(QNetworkProxy::NoProxy);
    connect(&mNetworkManager, &QNetworkAccessManager::authenticationRequired, this, &HttpClient::onAuthenticationRequired);
}


void HttpClient::onAuthenticationRequired(QNetworkReply *reply, QAuthenticator *authenticator) {
    authenticator->setUser(mUser);
    authenticator->setPassword(mPassword);
}

QString HttpClient::baseUrl(const QString& path) const {
    return QString("%1/%2").arg(mServer).arg(path);
}


QNetworkRequest HttpClient::requestV3(const QString& path) const{
    auto request = QNetworkRequest(QUrl{baseUrl(path)});
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/json");
    request.setRawHeader("Accept", "application/json");
    return request;
}


QNetworkRequest HttpClient::requestV2(const QString& path, const QString& type) const{
    auto request = QNetworkRequest(QUrl{baseUrl(path)});
    QHttpHeaders headers;
    auto contentType = QString("application/vnd.kafka");
    if (!type.isEmpty()) {
        contentType += ".";
        contentType += type;
    }
    contentType += ".v2+json";
    request.setHeader(QNetworkRequest::ContentTypeHeader, contentType);
    request.setRawHeader("Accept", contentType.toUtf8());
    return request;
}


