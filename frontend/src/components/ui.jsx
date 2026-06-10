// Small shared presentational components used across pages.
import "./ui.css";

export function Panel({ title, action, children, className = "", bodyClass = "", style }) {
  return (
    <section className={`panel ${className}`} style={style}>
      {(title || action) && (
        <div className="panel-head">
          <span className="panel-title">{title}</span>
          {action}
        </div>
      )}
      <div className={`panel-body ${bodyClass}`}>{children}</div>
    </section>
  );
}

// A labelled instrument readout. `tone` tints the value + accent rail.
export function StatCard({ label, value, unit, sub, tone = "neutral" }) {
  return (
    <div className={`stat tone-${tone}`}>
      <div className="stat-rail" />
      <div className="eyebrow">{label}</div>
      <div className="stat-value mono">
        {value}
        {unit && <span className="stat-unit"> {unit}</span>}
      </div>
      {sub && <div className="stat-sub mono">{sub}</div>}
    </div>
  );
}

export function ViolationBadge({ type }) {
  const isAvg = type === "AVERAGE";
  return (
    <span className={`badge ${isAvg ? "badge-average" : "badge-instant"}`}>
      <span className="dot" />
      {isAvg ? "AVERAGE" : "INSTANT"}
    </span>
  );
}

// status: "connecting" | "live" | "offline"
export function ConnectionDot({ status, label = "LIVE FEED" }) {
  return (
    <span className={`conn conn-${status}`}>
      <span className="conn-dot" />
      <span className="conn-label mono">
        {status === "live" ? label : status === "connecting" ? "CONNECTING" : "OFFLINE"}
      </span>
    </span>
  );
}

export function Spinner({ label = "Loading" }) {
  return (
    <div className="empty">
      <div className="spinner" />
      <span className="mono faint">{label}…</span>
    </div>
  );
}

export function EmptyState({ children }) {
  return <div className="empty mono">{children}</div>;
}

export function ErrorState({ error }) {
  return (
    <div className="empty">
      <span style={{ color: "var(--danger)" }} className="mono">
        ⚠ {String(error?.message || error)}
      </span>
    </div>
  );
}
