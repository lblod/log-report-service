import cron from 'node-cron'
import { app, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';
import {generateCSV, createFileOnDisk} from '../helpers.js'
import fs from 'fs'
import { querySudo as query } from '@lblod/mu-auth-sudo';

export async function generateReport() {
  console.log('Generate Inzendingen Report')
  const queryString = `
    PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    select ?type (COUNT(?type) as ?typeCount) where {
      GRAPH ?g {
        ?uri a toezicht:InzendingVoorToezicht;
          toezicht:decisionType ?typeURI.
      }
      GRAPH ?h {
        ?typeURI skos:prefLabel ?type.
      }
    }
  `
  const queryResponse = await query(queryString)
  const reportData = queryResponse.results.bindings.map((inzendingen) => ({
    type: inzendingen.type.value,
    typeCount: inzendingen.typeCount.value,
  }))
  const fileName = `inzendingenReport-${uuid()}`
  const fileExtension = 'csv'
  const fileFormat = 'text/csv'
  const csv = generateCSV(['type', 'typeCount'], reportData)
  fs.writeFileSync(`/data/files/${fileName}.${fileExtension}`, csv)
  const fileStats = fs.statSync(`/data/files/${fileName}.${fileExtension}`)
  const fileInfo = {
    name: fileName,
    extension: fileExtension,
    format: fileFormat,
    created: new Date(fileStats.birthtime),
    size: fileStats.size,
    location: `${fileName}.${fileExtension}`
  }
  createFileOnDisk(fileInfo)
}

cron.schedule('0 0 * * *', () => {
  generateReport()
});