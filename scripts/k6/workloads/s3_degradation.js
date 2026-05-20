import {
  DEFAULT_PHASE,
  buildClients,
  keyByDistribution,
  putGetHeadDeleteCycle,
  researchOptions,
} from "../lib/research.js";

const SCENARIO = "s3_degradation";
const clients = buildClients();

// Fault scenarios expect errors during degraded phases — treat them as data, not failures.
export const options = researchOptions({
  duration: "30s",
  vus: 10,
  thresholds: { s3_errors: ["rate<1.0"], http_req_failed: ["rate<1.0"] },
});

export default async function () {
  const keyInfo = keyByDistribution(__ENV.KEY_DISTRIBUTION || "uniform", SCENARIO);
  await putGetHeadDeleteCycle({
    clients,
    scenario: SCENARIO,
    keyInfo,
    phase: DEFAULT_PHASE,
  });
}
