import { Link } from 'preact-router';

export function Breadcrumb({ path }) {
  return (
    <div className="breadcrumb">
      {path.map((item, index) => (
        <span key={index}>
          {item.href ? (
            <Link href={item.href}>{item.label}</Link>
          ) : (
            <span className="current">{item.label}</span>
          )}
          {index < path.length - 1 && <span className="separator"> / </span>}
        </span>
      ))}
    </div>
  );
}
