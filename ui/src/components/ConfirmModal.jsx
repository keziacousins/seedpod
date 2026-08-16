import { useState } from "preact/hooks";
import { Modal } from "./Modal";

/**
 * Reusable confirmation modal to replace browser confirm() dialogs.
 * Built on the base Modal component for escape key and accessibility support.
 *
 * Usage:
 *   const [showConfirm, setShowConfirm] = useState(false);
 *
 *   {showConfirm && (
 *     <ConfirmModal
 *       title="Delete Item"
 *       message="Are you sure you want to delete this item?"
 *       confirmLabel="Delete"
 *       onConfirm={handleDelete}
 *       onCancel={() => setShowConfirm(false)}
 *     />
 *   )}
 */
export function ConfirmModal({
  title,
  message,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  confirmClass = "btn-danger",
  onConfirm,
  onCancel,
  loading = false,
}) {
  const [error, setError] = useState(null);

  const handleConfirm = async () => {
    setError(null);
    try {
      await onConfirm();
    } catch (err) {
      setError(err.message || "An error occurred");
    }
  };

  return (
    <Modal onClose={onCancel} className="modal-sm" title={title}>
      {typeof message === "string" ? <p>{message}</p> : message}
      {error && <div className="modal-error">{error}</div>}
      <div className="modal-actions">
        <button className="btn-secondary" onClick={onCancel} disabled={loading}>
          {cancelLabel}
        </button>
        <button
          className={confirmClass}
          onClick={handleConfirm}
          disabled={loading}
        >
          {loading ? "..." : confirmLabel}
        </button>
      </div>
    </Modal>
  );
}
