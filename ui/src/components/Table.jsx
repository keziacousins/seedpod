import { route } from "preact-router";

export function Table({ columns, data, onRowClick, keyField = "id" }) {
  const handleRowClick = (row) => {
    if (onRowClick) {
      onRowClick(row);
    }
  };

  return (
    <table className="data-table">
      <thead>
        <tr>
          {columns.map((col) => (
            <th
              key={col.key}
              style={col.width ? { width: col.width } : undefined}
            >
              {col.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.length === 0 ? (
          <tr>
            <td colSpan={columns.length} className="empty-state">
              No data available
            </td>
          </tr>
        ) : (
          data.map((row) => (
            <tr
              key={row[keyField]}
              onClick={() => handleRowClick(row)}
              className={onRowClick ? "clickable" : ""}
            >
              {columns.map((col) => (
                <td
                  key={col.key}
                  style={col.width ? { width: col.width } : undefined}
                >
                  {col.render ? col.render(row[col.key], row) : row[col.key]}
                </td>
              ))}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
