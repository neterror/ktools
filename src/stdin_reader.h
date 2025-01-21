#pragma once
#include <QtCore>

class StdinReader : public QObject {
    Q_OBJECT

public:
    StdinReader(QObject* parent = nullptr);
private slots:
    void handleStdin();
private:
    QSocketNotifier* stdinNotifier;
signals:
    void data(QString line);
};
