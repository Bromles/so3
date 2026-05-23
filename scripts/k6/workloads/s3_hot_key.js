/* global __ENV */
import {
  buildClients,
  keyByDistribution,
  putGetHeadDeleteCycle,
  researchOptions
} from '../lib/research.js'

const SCENARIO = 's3_hot_key'
const clients = buildClients()

export const options = researchOptions({ duration: '30s', vus: 10 })

export default async function () {
  const distribution = __ENV.KEY_DISTRIBUTION || '90_10'
  const keyInfo = keyByDistribution(distribution, SCENARIO)
  await putGetHeadDeleteCycle({ clients, scenario: SCENARIO, keyInfo })
}
