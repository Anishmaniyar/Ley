export interface NotificationFailureOptionConfig {
  delayMs: number;
  failureRate: number;
  random?: () => number;
}

export const FAILURE_OPTION_NOTIFICATION_CONFIG: NotificationFailureOptionConfig =
  {
    delayMs: 5000,
    failureRate: 0.4,
    random: () => Math.random(),
  };
