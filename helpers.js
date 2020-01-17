import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid, query } from 'mu';
const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/organizations/141d9d6b-54af-4d17-b313-8d1c30bc3f5b/LoketAdmin';
const separator = ';'
export function generateCSV(fields, data) {
  console.log(fields)
  let result = ''
  const headerString = fields.join(separator)
  result += `${headerString}\n`
  const csvRows = data.map((row) => {
    const dataRow = fields.map((header) => row[header])
    return dataRow.join(separator)
  })
  result += `${csvRows.join('\n')}`
  return result
}

export async function createFileOnDisk({name, format, size, extension, created, location}) {
  const logicalFileUuid = uuid()
  const logicalFileURI = `http://data.lblod.info/files/${logicalFileUuid}`
  const physicalFileUuid = uuid()
  const physicalFileURI = `share://${location}`
  const queryString = `
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(logicalFileURI)} a nfo:FileDataObject;
          mu:uuid ${sparqlEscapeString(logicalFileUuid)};
          nfo:fileName ${sparqlEscapeString(name)};
          dct:format ${sparqlEscapeString(format)};
          nfo:fileSize ${sparqlEscapeInt(size)};
          dbpedia:fileExtension ${sparqlEscapeString(extension)};
          dct:created ${sparqlEscapeDateTime(created)} .
        ${sparqlEscapeUri(physicalFileURI)} a nfo:FileDataObject;
          mu:uuid ${sparqlEscapeString(physicalFileUuid)};
          nfo:fileName ${sparqlEscapeString(name)};
          dct:format ${sparqlEscapeString(format)};
          nfo:fileSize ${sparqlEscapeInt(size)};
          dbpedia:fileExtension ${sparqlEscapeString(extension)};
          dct:created ${sparqlEscapeDateTime(created)};
          nie:dataSource ${sparqlEscapeUri(logicalFileURI)}.
      }
    }
  `
  query(queryString)
  return logicalFileURI
}

export async function createReport(file, {title, description}) {
  const reportUUID = uuid()
  const reportURI = `http://data.lblod.info/id/reports/${reportUUID}`
  const queryString = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(reportURI)} a <http://lblod.data.gift/vocabularies/reporting/Report>;
          mu:uuid ${sparqlEscapeString(reportUUID)};
          dct:title ${sparqlEscapeString(title)};
          dct:description ${sparqlEscapeString(description)};
          dct:created ${sparqlEscapeDateTime(new Date())};
          prov:generated ${sparqlEscapeUri(file)} .
      }
    }
  `
  const queryResult = await query(queryString)
  console.log(queryResult)
}