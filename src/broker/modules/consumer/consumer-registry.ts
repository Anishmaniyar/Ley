// Owns the map and registration and unregistration

import AppError from "../../../shared/errors/appError";

interface ConsumerRegistration {
  destination: string;
  registeredAt: Date;
  status: "active";
}

const consumerRegistrationMap = new Map<string, ConsumerRegistration>();

export function register(dest: string): ConsumerRegistration {
  if (consumerRegistrationMap.has(dest)) {
    throw new AppError("Consumer with same destination already exists", 409);
  }

  const registration: ConsumerRegistration = {
    destination: dest,
    registeredAt: new Date(),
    status: "active",
  };

  consumerRegistrationMap.set(dest, registration);

  return registration;
}

export function unregister(dest: string): void {
  consumerRegistrationMap.delete(dest);
}
