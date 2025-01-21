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


QNetworkRequest HttpClient::requestV3(QString path) const{
    auto request = QNetworkRequest(QUrl{baseUrl(path)});
    QHttpHeaders headers;
    headers.append(QHttpHeaders::WellKnownHeader::ContentType, "application/json");
    headers.append(QHttpHeaders::WellKnownHeader::Accept, "application/json");
    request.setHeaders(headers);
    return request;
}


QNetworkRequest HttpClient::requestV2(QString path, bool protobufContent) const{
    auto request = QNetworkRequest(QUrl{baseUrl(path)});
    QHttpHeaders headers;
    if (protobufContent) {
        headers.append(QHttpHeaders::WellKnownHeader::ContentType, "application/vnd.kafka.protobuf.v2+json");
        headers.append(QHttpHeaders::WellKnownHeader::Accept, "application/vnd.kafka.protobuf.v2+json");
    } else {
        headers.append(QHttpHeaders::WellKnownHeader::ContentType, "application/vnd.kafka.v2+json");
    }
    request.setHeaders(headers);
    return request;
}


