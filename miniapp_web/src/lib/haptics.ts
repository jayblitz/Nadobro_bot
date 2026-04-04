import { getWebApp } from "./telegram";

export function hapticImpact(style: "light" | "medium" | "heavy" = "medium") {
  getWebApp()?.HapticFeedback.impactOccurred(style);
}

export function hapticSuccess() {
  getWebApp()?.HapticFeedback.notificationOccurred("success");
}

export function hapticError() {
  getWebApp()?.HapticFeedback.notificationOccurred("error");
}

export function hapticSelection() {
  getWebApp()?.HapticFeedback.selectionChanged();
}
