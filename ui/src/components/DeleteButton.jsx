/**
 * DeleteButton - Reusable trash icon button for delete/revoke actions
 *
 * Features:
 * - Consistent trash icon SVG
 * - Built-in click handling with event propagation control
 * - Support for disabled state
 * - Styled via .btn-delete-icon class in CSS
 *
 * Usage:
 *   <DeleteButton onClick={() => handleDelete(id)} title="Delete item" />
 *   <DeleteButton onClick={handleRevoke} disabled={!active} title="Revoke key" />
 */
export function DeleteButton({ onClick, disabled = false, title = "Delete" }) {
  const handleClick = (e) => {
    e.stopPropagation(); // Prevent row click when in table
    if (!disabled && onClick) {
      onClick(e);
    }
  };

  return (
    <button
      className="btn-delete-icon"
      onClick={handleClick}
      disabled={disabled}
      title={title}
    >
      <svg
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M6.5 1.75a.25.25 0 01.25-.25h2.5a.25.25 0 01.25.25V3h-3V1.75zm4.5 0V3h2.25a.75.75 0 010 1.5H13v8.75A1.75 1.75 0 0111.25 15h-6.5A1.75 1.75 0 013 13.25V4.5h-.25a.75.75 0 010-1.5H5V1.75C5 .784 5.784 0 6.75 0h2.5C10.216 0 11 .784 11 1.75zM4.5 4.5v8.75c0 .138.112.25.25.25h6.5a.25.25 0 00.25-.25V4.5h-7zm2.5 2.5a.75.75 0 01.75.75v4.5a.75.75 0 01-1.5 0v-4.5A.75.75 0 017 7zm3.25.75a.75.75 0 00-1.5 0v4.5a.75.75 0 001.5 0v-4.5z"
          fill="currentColor"
        />
      </svg>
    </button>
  );
}
