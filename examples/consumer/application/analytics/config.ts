export interface AnalyticsFailureOptionConfig {
  delayMs: number;
  failureRate: number;
  random?: () => number;
}

export const FAILURE_OPTION_ANALYTICS_CONFIG: AnalyticsFailureOptionConfig = {
  delayMs: 3000,
  failureRate: 0.2,
  random: () => Math.random(),
};
