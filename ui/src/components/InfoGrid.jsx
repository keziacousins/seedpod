/**
 * InfoGrid - Standard two-column grid layout for detail pages
 *
 * Provides consistent spacing and layout for key-value pairs in cards.
 * Based on the successful "Current Deployment" card pattern.
 *
 * Usage:
 *   <InfoGrid>
 *     <InfoGridRow label="Status">
 *       <StatusBadge status={status} />
 *     </InfoGridRow>
 *     <InfoGridRow label="Deployment ID">
 *       <span className="mono-text">{id}</span>
 *     </InfoGridRow>
 *     <InfoGridRow label="Error" fullWidth>
 *       <span className="error-text">{error}</span>
 *     </InfoGridRow>
 *   </InfoGrid>
 */

export function InfoGrid({ children }) {
  return (
    <div className="info-grid">
      {children}
    </div>
  );
}

export function InfoGridRow({ label, children, fullWidth = false }) {
  if (fullWidth) {
    return (
      <div className="info-grid-row info-grid-row-full">
        <span className="info-grid-label">{label}:</span>
        <div className="info-grid-value">{children}</div>
      </div>
    );
  }

  return (
    <div className="info-grid-row">
      <span className="info-grid-label">{label}:</span>
      <div className="info-grid-value">{children}</div>
    </div>
  );
}
