import { app, query, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';
import {generateReport} from './reports/berichtencentrumMessages'
import './reports/index'

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/application';

app.get('/test', async (req, res) => {
  await generateReport()  
  res.send('Done ^^')
})

app.use(errorHandler);
