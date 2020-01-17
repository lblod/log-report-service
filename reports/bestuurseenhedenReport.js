import cron from 'node-cron'
import { app, query, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';
import {generateCSV, createFileOnDisk} from '../helpers.js'
import fs from 'fs'

export async function generateReport() {
  console.log('Generate Bestuurseenheden Report')
  const queryString = `
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    
    select distinct ?name ?type ?province ?uri where {
      ?uri a besluit:Bestuurseenheid;
        skos:prefLabel ?name;
        ext:inProvincie ?provinceURI;
        besluit:classificatie ?typeURI .
      ?provinceURI rdfs:label ?province.
      ?typeURI skos:prefLabel ?type .
    }
  `
  const queryResponse = await query(queryString)
  const reportData = queryResponse.results.bindings.map((bestuurseenheid) => ({
    name: bestuurseenheid.name.value,
    type: bestuurseenheid.type.value,
    province: bestuurseenheid.province.value,
    uri: bestuurseenheid.uri.value,
  }))
  const fileName = `bestuurseenhedenReport-${uuid()}`
  const fileExtension = 'csv'
  const fileFormat = 'text/csv'
  const csv = generateCSV(['name', 'type', 'province', 'uri'], reportData)
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
  console.log(fileInfo)
  createFileOnDisk(fileInfo)
}

cron.schedule('0 0 * * *', () => {
  generateReport()
});