#include "schema_registry.h"
#include <QtCore>
#include <qcommandlineoption.h>
#include <qcoreapplication.h>

void printTableRow(const QStringList &row, const QList<int> &columnWidths) {
    QString formattedRow;
    for (int i = 0; i < row.size(); ++i) {
        // Align left and pad the column with spaces
        formattedRow += QString("%1").arg(row[i], -columnWidths[i]);
    }
    qDebug().noquote() << formattedRow; // Prevent additional quotes in QDebug output
}

QStringList toStringList(const SchemaRegistry::Schema& schema) {
    QStringList result;
    result << QString("%1").arg(schema.schemaId)
           << schema.subject
           << QString("%1").arg(schema.version);


    QStringList reflist;
    for (const auto& reference: schema.references) {
        reflist << QString("(subject %1, version %2)").arg(reference.subject).arg(reference.version);
    }
    if (!reflist.isEmpty()) {
        result << reflist.join(",");
    }
    
    return result;
}

void registerProtobuf(SchemaRegistry& registry, const QString& fileName, const QString& subject,
                      const QString& referenceSubject, const QString& referenceVersion) {
    if (subject.isEmpty()) {
        qWarning() << "Missing subject. Specify with --new-subject";
        exit(-1);
    }

    QFile f(fileName);
    if (!f.open(QIODevice::ReadOnly)) {
        qCritical() << "Failed to read" << fileName;
        exit(-1);
    }
    
    int refVersion = -1;
    if (!referenceVersion.isEmpty()) {
        refVersion = referenceVersion.toInt();
    }

    if (!referenceSubject.isEmpty() && (refVersion == -1)) {
        qCritical() << "reference version is required";
        exit(-1);
    }

    QObject::connect(&registry, &SchemaRegistry::ready, [subject](bool success, QString msg){
        qDebug().noquote() << msg;

        QCoreApplication::quit();
    });

    registry.registerProtobuf(subject, f.readAll(), referenceSubject, refVersion);
}

void listSchemas(SchemaRegistry& registry) {
    QObject::connect(&registry, &SchemaRegistry::registeredSchemas, [](const QList<SchemaRegistry::Schema>& schemas){
        QList<int> columns = {10, 40, 10, 20};
        printTableRow({"SchemaId", "Subject", "Version", "Reference"}, columns);
        qDebug().noquote() << "-------------------------------------------------------";
        for (const auto& schema: schemas) {

            auto row = toStringList(schema);
            printTableRow(row, columns);

        }
        QCoreApplication::quit();
    });
    registry.getSchemas();
}

void deleteSchema(SchemaRegistry& registry, const QString& subject, qint32 version) {
    registry.deleteSchema(subject, version);
    QObject::connect(&registry, &SchemaRegistry::ready, [subject](bool success){
        if (success) {
            qDebug().noquote() << subject << "deleted";
        }
        QCoreApplication::quit();
    });
}

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");


    parser.addHelpOption();
    parser.addOptions({
            {"new-file", "register protobuf file", "file.proto"},
            {"new-subject", "The subject(name) in the registry of the file", "name"},
            {"ref-subject", "The protobuf reference of the newly registered", "reference subject"},
            {"ref-version", "The protobuf reference version", "version"},

            {"delete-subject", "delete schema subject", "subject"},
            {"delete-version", "delete schema subject", "version"},
            {"list", "list registered schemas"},
    });
    parser.process(app);

    QSettings settings;
    auto server = settings.value("ConfluentSchemaRegistry/server").toString();
    auto user = settings.value("ConfluentSchemaRegistry/user").toString();
    auto password = settings.value("ConfluentSchemaRegistry/password").toString();

    SchemaRegistry registry(server, user, password);


    bool processed = false;
    if (parser.isSet("new-file")) {
        registerProtobuf(registry, parser.value("new-file"), parser.value("new-subject"),
                         parser.value("ref-subject"), parser.value("ref-version"));

        processed = true;
    } 

    if (!processed && parser.isSet("list")) {
        listSchemas(registry);
        processed = true;
    }

    if (!processed && parser.isSet("delete-subject")) {
        if (!parser.isSet("delete-version")) {
            qCritical() << "subject version is required ";
        } else {
            auto version = parser.value("delete-version").toInt();
            deleteSchema(registry, parser.value("delete-subject"), version);
            processed = true;
        }
    }


    if (!processed) {
        parser.showHelp();
    }

    return app.exec();
}
