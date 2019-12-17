import { app, query, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/application';

app.get('/', async ( req, res ) => {
  const data = await queryAgregations()
  const response = await createReport(data)
  console.log(response)
  res.json(response)
} );

async function queryAgregations() {
  const logLevelsQuery = `
    PREFIX rlog: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/rlog#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    select ?logLevelName ?logLevelId where { 
      ?logLevelId skos:prefLabel ?logLevelName .
      ?logLevelId a rlog:Level .
    }
  `
  const logLevelsQueryResponse = await query(logLevelsQuery)
  const logLevels = logLevelsQueryResponse.results.bindings.map((item) => {
    return {
      name: item.logLevelName.value,
      uri: item.logLevelId.value
    }
  })
  const entriesPromises = logLevels.map((item) => queryEntries(item.uri))
  const entries = await Promise.all(entriesPromises)
  const logLevelsWithEntries = logLevels.map((item, index) => {
    item.entries = entries[index]
    return item
  })
  const allEntries = await queryEntries()
  return {
    aggregated: logLevelsWithEntries,
    entries: allEntries
  }
}


async function queryEntries(logLevel) {
  const entriesQuery = `
    PREFIX rlog: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/rlog#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    
    select * where { 
      ${logLevel ? `?logId rlog:level ${sparqlEscapeUri(logLevel)} .` : ""}
      ?logId a rlog:Entry .
      ?logId dct:source ?logSource .
      ?logId rlog:className ?logClassName .
      ?logId rlog:message ?logMessage .
      ?logId rlog:date ?logDate .
      ?logId rlog:level ?logLevel .
      ?logId ext:specificInformation ?logSpecificInformation .
    }
  `
  const entriesQueryResponse = await query(entriesQuery)
  const entries = entriesQueryResponse.results.bindings.map((item) => {
    return {
      uri: item.logId.value,
      source: item.logSource.value,
      className: item.logClassName.value,
      message: item.logMessage.value,
      date: item.logDate.value,
      level: item.logLevel.value,
      specificInformation: item.logSpecificInformation.value
    }
  })
  return entries
}

async function createReport(data) {
  const id = uuid()
  const uri = `http://lblod.data.gift/log-reports/${id}`
  const reportContentURI = await createReportContent(data)
  const author = "Log report service"
  const creationDate = (new Date()).toISOString()
  const reportQuery = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} a ext:LogReport;
                                mu:uuid ${sparqlEscapeString(id)};
                                dct:created ${sparqlEscapeDateTime(creationDate)};
                                dct:creator ${sparqlEscapeString(author)};
                                ext:reportContent ${sparqlEscapeUri(reportContentURI)} .
      }
    }
  `
  const response = await query(reportQuery)
  console.log(response)
  return response
}

async function createReportContent(data) {
  const id = uuid()
  const uri = `http://lblod.data.gift/report-contents/${id}`
  const createAggregatePromises = data.aggregated.map((aggregate) => createAggregate(aggregate))
  const createAggregateURIs = await Promise.all(createAggregatePromises)
  const aggregateQueryPart = createAggregateURIs.map((aggregateURI) => `${sparqlEscapeUri(uri)} ext:aggregates ${sparqlEscapeUri(aggregateURI)} .`)
  const entriesQueryPart = data.entries.map((entry) => `${sparqlEscapeUri(uri)} ext:entries ${sparqlEscapeUri(entry.uri)} .`)
  const reportContentQuery = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} a ext:ReportContent;
                                mu:uuid ${sparqlEscapeString(id)} .
        ${aggregateQueryPart.join(' ')}
        ${entriesQueryPart.join(' ')}
      }
    }
  `
  const response = await query(reportContentQuery)
  console.log(response)
  return uri
}

async function createAggregate(aggregate) {
  const id = uuid()
  const {name, entries} = aggregate
  const uri = `http://lblod.data.gift/aggregates/${id}`
  const entriesQueryPart = entries.map((entry) => `${sparqlEscapeUri(uri)} ext:entries ${sparqlEscapeUri(entry.uri)} .`)
  const aggregateQuery = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} a ext:Aggregate;
                                mu:uuid ${sparqlEscapeString(id)};
                                skos:prefLabel ${sparqlEscapeString(name)};
                                ext:logCount ${sparqlEscapeInt(entries.length)};
                                mu:uuid ${sparqlEscapeString(id)} .
        ${entriesQueryPart.join(' ')}
      }
    }
  `
  const response = await query(aggregateQuery)
  console.log(response)
  return uri
}

app.get('/query', function( req, res ) {
  var myQuery = `
    SELECT *
    WHERE {
      GRAPH <http://mu.semte.ch/application> {
        ?s ?p ?o.
      }
    }`;

  query( myQuery )
    .then( function(response) {
      res.send( JSON.stringify( response ) );
    })
    .catch( function(err) {
      res.send( "Oops something went wrong: " + JSON.stringify( err ) );
    });
} );

app.use(errorHandler);