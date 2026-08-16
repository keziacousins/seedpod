/**
 * Time formatting utilities
 *
 * Handles conversion from UTC timestamps (from server) to local timezone
 * and provides consistent formatting across the UI.
 */

/**
 * Parse a timestamp string and ensure it's treated as UTC.
 *
 * v2 serializes aware datetimes, so every server timestamp already carries a
 * `+00:00` offset (naive datetimes are banned in `seedpod/core/`). v1 emitted
 * them bare, which is why several call sites used to do `new Date(s + "Z")` —
 * that yields `...+00:00Z`, an Invalid Date, against a v2 server. Always parse
 * through here: it appends `Z` only when there is no offset already.
 *
 * @param {string} isoString - ISO timestamp from server (may or may not have Z suffix)
 * @returns {Date} Date object in local timezone
 */
export function parseUTC(isoString) {
  if (!isoString) return null;

  // If already has Z suffix or timezone offset (+/-HH:MM), use as-is
  if (isoString.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(isoString)) {
    return new Date(isoString);
  }

  // Otherwise, treat as UTC by adding Z suffix
  return new Date(isoString + "Z");
}

/**
 * Format a timestamp as full date and time in local timezone
 * @param {string} isoString - ISO timestamp from server
 * @returns {string} Formatted string like "1/15/2025, 2:30:45 PM"
 */
export function formatDateTime(isoString) {
  const date = parseUTC(isoString);
  if (!date) return "-";

  return date.toLocaleString();
}

/**
 * Format a timestamp as time only in local timezone
 * @param {string} isoString - ISO timestamp from server
 * @returns {string} Formatted string like "14:30:45"
 */
export function formatTime(isoString) {
  const date = parseUTC(isoString);
  if (!date) return "-";

  return date.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

/**
 * Format a timestamp as date only in local timezone
 * @param {string} isoString - ISO timestamp from server
 * @returns {string} Formatted string like "1/15/2025"
 */
export function formatDate(isoString) {
  const date = parseUTC(isoString);
  if (!date) return "-";

  return date.toLocaleDateString();
}
