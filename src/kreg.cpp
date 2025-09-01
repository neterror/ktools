#include "schema_create.h"
#include "schema_registry.h"
#include <QtCore>
#include <qcommandlineoption.h>
#include <qcoreapplication.h>
#include "version.h"

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


void registerProtobuf(SchemaCreate& creator, const QString& fileName, const QString& subject, QString referenceIds) {
    if (subject.isEmpty()) {
        qWarning() << "Missing subject. Specify with --subject";
        exit(-1);
    }

    QFile f(fileName);
    if (!f.open(QIODevice::ReadOnly)) {
        qCritical() << "Failed to read" << fileName;
        exit(-1);
    }
    

    QList<qint32> references;
    if (!referenceIds.isEmpty()) {
        for(const auto& t: referenceIds.split(",")) {
            bool ok;
            auto id = t.toInt(&ok);
            if (ok) {
                references.append(id);
            }
        }
    }

    QObject::connect(&creator, &SchemaCreate::created, [subject](qint32 schemaId){
        qDebug().noquote() << "created schemaId" << schemaId;
        QCoreApplication::quit();
    });
    
    QObject::connect(&creator, &SchemaCreate::error, [](QString message){
        qWarning().noquote() << message;
        QCoreApplication::quit();
    });

    creator.createSchema(subject, f.readAll(), "PROTOBUF", references);
}

void listSchemas(SchemaRegistry& registry) {
    QObject::connect(&registry, &SchemaRegistry::schemaList, [](const QList<SchemaRegistry::Schema>& schemas){
        QList<int> columns = {10, 50, 10, 20};
        printTableRow({"SchemaId", "Subject", "Version", "Reference"}, columns);
        qDebug().noquote() << "-------------------------------------------------------------------";
        for (const auto& schema: schemas) {
            auto row = toStringList(schema);
            printTableRow(row, columns);

        }
        QCoreApplication::quit();
    });
    registry.getSchemas();
}

void readSchema(SchemaRegistry& registry, quint32 schemaId) {
    QObject::connect(&registry, &SchemaRegistry::schemaText, [](QString text){
	qDebug().noquote() << text;
        QCoreApplication::quit();
    });
    registry.readSchema(schemaId);
}


void deleteSchemaBySubject(SchemaRegistry& registry, const QString& subject, bool permanent) {
    registry.deleteSchema(subject, permanent);
    QObject::connect(&registry, &SchemaRegistry::schemaDeleted, [subject](bool success){
        if (success) {
            qDebug().noquote() << "successfully deleted schema" << subject;
        } else {
            qDebug().noquote() << "failed to delete schema" << subject;
        }

        QTimer::singleShot(100, QCoreApplication::instance(), &QCoreApplication::quit);
    });
}

void getSchemaIdBySubject(SchemaRegistry& registry, const QString& subject) {
    registry.getLatestSchemaId(subject);
    QObject::connect(&registry, &SchemaRegistry::subjectSchemaId, [](QString subject, qint32 schemaId){
        qDebug().noquote() << schemaId;
        QTimer::singleShot(100, [schemaId]{QCoreApplication::exit(schemaId);});
    });
}
 


static void stdoutOutput(QtMsgType type, const QMessageLogContext&, const QString &msg) {
    QByteArray localMsg = msg.toLocal8Bit();
    printf("%s\n", localMsg.constData());
}

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QCommandLineParser parser;


    qInstallMessageHandler(stdoutOutput);

    app.setOrganizationName("abrites");
    app.setApplicationName("ktools");
    app.setApplicationVersion(APP_VERSION);

    parser.addHelpOption();
    parser.addOptions({
            {"schema", "register protobuf file", "file.proto"},
            {"subject", "The subject(name) in the registry of the file", "name"},
            {"reference", "Comma separated list of reference schemaIds", "schemaId list"},
            {"verbose", "verbose logging"},
            {"delete", "delete schema subject", "subject"},
            {"permanent", "permanent delete flag"},
            {"schemaId", "retrieve the schema id of the specified subject", "subject"},
            {"list", "list registered schemas"},
            {"read", "read schemaId", "schemaId"},
    });
    parser.process(app);

    QSettings settings;
    auto server = settings.value("ConfluentSchemaRegistry/server").toString();
    auto user = settings.value("ConfluentSchemaRegistry/user").toString();
    auto password = settings.value("ConfluentSchemaRegistry/password").toString();

    SchemaRegistry registry(server, user, password, parser.isSet("verbose"));
    std::unique_ptr<SchemaCreate> schemaCreate;


    QObject::connect(&registry, &SchemaRegistry::failed, [](QString message){
        qWarning().noquote() << message;
        QCoreApplication::quit();
    });

    bool processed = false;
    if (parser.isSet("schema")) {
        schemaCreate.reset(new SchemaCreate(registry));
        registerProtobuf(*schemaCreate, parser.value("schema"), parser.value("subject"), parser.value("reference"));
        processed = true;
    } 

    if (!processed && parser.isSet("list")) {
        listSchemas(registry);
        processed = true;
    }

    if (!processed && parser.isSet("read")) {
        readSchema(registry, parser.value("read").toUInt());
        processed = true;
    }


    if (!processed && parser.isSet("delete")) {
        deleteSchemaBySubject(registry, parser.value("delete"), parser.isSet("permanent"));
        processed = true;
    }

    if (!processed && parser.isSet("schemaId")) {
        getSchemaIdBySubject(registry, parser.value("schemaId"));
        processed = true;
    }
    

    if (!processed) {
        parser.showHelp();
    }

    return app.exec();
}
