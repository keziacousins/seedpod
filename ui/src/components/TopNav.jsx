import { ConnectionStatus } from "./ConnectionStatus";

export function TopNav({
  user,
  onLogout,
  onToggleHud,
  hudVisible,
  eventCount,
}) {
  return (
    <nav className="top-nav">
      <div className="nav-left">
        <div className="logo">
          <picture>
            <source
              srcset="/logo-dark.svg"
              media="(prefers-color-scheme: dark)"
            />
            <img src="/logo-light.svg" alt="Seedpod" className="logo-icon" />
          </picture>
          <span className="logo-text">Seedpod</span>
        </div>
      </div>
      <div className="nav-right">
        {user && (
          <>
            <ConnectionStatus
              onToggleHud={onToggleHud}
              hudVisible={hudVisible}
              eventCount={eventCount}
            />
            <span className="user-info">{user.username}</span>
            <button onClick={onLogout} className="btn-logout">
              Logout
            </button>
          </>
        )}
      </div>
    </nav>
  );
}
