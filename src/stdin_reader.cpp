#include "stdin_reader.h"

StdinReader::StdinReader(QObject* parent) : QObject(parent) {
    stdinNotifier = new QSocketNotifier(fileno(stdin), QSocketNotifier::Read, this);
    connect(stdinNotifier, &QSocketNotifier::activated, this, &StdinReader::handleStdin);
}

void StdinReader::handleStdin() {
    QTextStream input(stdin, QIODevice::ReadOnly);
    QString line = input.readLine(); // Read one line from stdin
    if (!line.isNull()) {
        emit data(line);
    }
}

