export interface ReportingFailureOptionConfig {
  delayMs: number;
  failureRate: number;
  random?: () => number;
}

export const FAILURE_OPTION_REPORTING_CONFIG: ReportingFailureOptionConfig = {
  delayMs: 7000,
  failureRate: 0.5,
  random: () => Math.random(),
};
