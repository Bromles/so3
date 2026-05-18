import {
  buildClients,
  keyByDistribution,
  putGetHeadDeleteCycle,
  researchOptions,
} from "../lib/research.js";

const SCENARIO = "s3_leaderless";
const clients = buildClients();

export const options = researchOptions({ duration: "30s", vus: 10 });

export default async function () {
  const keyInfo = keyByDistribution(__ENV.KEY_DISTRIBUTION || "uniform", SCENARIO);
  await putGetHeadDeleteCycle({ clients, scenario: SCENARIO, keyInfo });
}
