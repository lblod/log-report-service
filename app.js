import { app, query, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';
import cron from 'node-cron'

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/application';

const AgentUuid = "F0572CB9-40F3-487A-9D44-BF6603F98F9A"

app.get('/', async ( req, res ) => {
  const response = await reportGeneration()
  res.json(response)
} );

async function reportGeneration() {
  const periodStart = new Date()
  const periodEnd = new Date()
  periodEnd.setDate(periodStart.getDate() - 1)
  const period = {start: periodStart, end: periodEnd}
  const data = await queryData(period)
  const response = await createReport(data, period)
  return response
}

async function queryData(period) {
  const logLevelsQuery = `
    PREFIX rlog: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/rlog#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    select distinct ?logLevelName ?logLevelId where { 
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
  const entriesPromises = logLevels.map((item) => queryEntries(period, item.uri))
  const entries = await Promise.all(entriesPromises)
  const logLevelsWithEntries = logLevels.map((item, index) => {
    item.entries = entries[index]
    return item
  })
  const allEntries = await queryEntries(period)
  return {
    aggregated: logLevelsWithEntries,
    entries: allEntries
  }
}


async function queryEntries(period, logLevel) {
  const entriesQuery = `
    PREFIX rlog: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/rlog#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    
    select distinct * where { 
      ${logLevel ? `?logId rlog:level ${sparqlEscapeUri(logLevel)} .` : ""}
      ?logId a rlog:Entry .
      ?logId dct:source ?logSource .
      ?logId rlog:className ?logClassName .
      ?logId rlog:message ?logMessage .
      ?logId rlog:date ?logDate .
      ?logId rlog:level ?logLevel .
      ?logId ext:specificInformation ?logSpecificInformation .
      FILTER(?logDate > ${sparqlEscapeDateTime(period.start)} && ?logDate < ${sparqlEscapeDateTime(period.end)})
    }
  `
  const entriesQueryResponse = await query(entriesQuery)
  console.log(entriesQueryResponse)
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
  console.log(entries.length)
  return entries
}

async function createReport(data, period) {
  const id = uuid()
  const uri = `http://lblod.data.gift/log-reports/${id}`
  const periodUri = await createPeriod(period)
  const reportContentURI = await createReportContent(data)
  const author = `http://lblod.data.gift/agents/${AgentUuid}`
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
                                dct:creator ${sparqlEscapeUri(author)};
                                ext:reportContent ${sparqlEscapeUri(reportContentURI)};
                                ext:reportPeriod ${sparqlEscapeUri(periodUri)} .
      }
    }
  `
  const response = await query(reportQuery)
  console.log(response)
  return response
}

async function createPeriod({start, end}) {
  const id = uuid()
  const uri = `http://lblod.data.gift/periods/${id}`
  const periodQuery = `
    PREFIX gleif: <http://gleif.org/ontology/Base/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} a gleif:Period;
                                mu:uuid ${sparqlEscapeString(id)};
                                gleif:hasStart ${sparqlEscapeDateTime(start)};
                                gleif:hasEnd ${sparqlEscapeDateTime(end)} .
      }
    }
  `
  const response = await query(periodQuery)
  return uri
}

async function createReportContent(data) {
  const id = uuid()
  const uri = `http://lblod.data.gift/report-contents/${id}`
  data.aggregated.map((aggregate) => console.log(aggregate.entries))
  const createAggregatePromises = data.aggregated.map((aggregate) => createAggregate(aggregate))
  const createAggregateURIs = await Promise.all(createAggregatePromises)
  const aggregateQueryPart = createAggregateURIs.map((aggregateURI) => `${sparqlEscapeUri(uri)} ext:aggregates ${sparqlEscapeUri(aggregateURI)} .`)
  const entriesQueryPart = data.entries.map((entry) => `${sparqlEscapeUri(uri)} ext:entries ${sparqlEscapeUri(entry.uri)} .
                                                        ${sparqlEscapeUri(entry.uri)} ext:belongsToReport ${sparqlEscapeUri(uri)} .`)
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
  console.log(reportContentQuery)
  const response = await query(reportContentQuery)
  console.log(response)
  return uri
}

async function createAggregate(aggregate) {
  const id = uuid()
  const logLevelUri = aggregate.uri
  const entries = aggregate.entries
  console.log(entries.length)
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
                                ext:hasLogLevel ${sparqlEscapeUri(logLevelUri)};
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

cron.schedule('0 5 * * *', () => {
  reportGeneration()
});